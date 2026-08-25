// SPDX-License-Identifier: Apache-2.0

package api

import (
	"context"
	"net/http"
	"strconv"
)

// The middleware chain (issue §1/§3): recover → request id → conn limit → body limit →
// authz (#16 slots in here without re-plumbing) → router. Every link passes a response
// only through the envelope-intercepting wrapper, so no stdlib plain-text error can
// reach a client from inside the chain.

// chained wraps next in the full chain, with the envelope-intercepting writer sitting
// directly on the router so a stdlib plain-text response is swapped for an envelope.
func (s *Server) chained(next http.Handler) http.Handler {
	inner := envelopeHandler{srv: s, next: next}
	return s.recoverMW(s.requestIDMW(s.connLimitMW(s.bodyLimitMW(s.authzMW(inner)))))
}

// envelopeHandler serves next with w wrapped in the interceptor.
type envelopeHandler struct {
	srv  *Server
	next http.Handler
}

func (h envelopeHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	h.next.ServeHTTP(h.srv.newEnvelopeWriter(w), r)
}

// recoverMW turns a panicking handler into a 500 internal envelope plus an error log.
// The daemon never dies for an operational condition; the connection keeps serving.
func (s *Server) recoverMW(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer func() {
			if rec := recover(); rec != nil {
				s.logger.Error("api.panic", "request_id", requestIDOf(r), "panic", rec)
				s.writeError(w, &routerErrorRecovery{rec: rec})
			}
		}()
		next.ServeHTTP(w, r)
	})
}

// routerErrorRecovery classifies a recovered panic onto internal without leaking the
// panic value into the wire message.
type routerErrorRecovery struct{ rec any }

func (*routerErrorRecovery) Error() string { return "handler panicked" }

// contextKey carries the per-request identity between links.
type contextKey int

const ctxRequestID contextKey = iota

func contextWithRequestID(r *http.Request, id string) context.Context {
	return context.WithValue(r.Context(), ctxRequestID, id)
}

// requestIDMW stamps every request with a fresh ULID and echoes it back as
// Messq-Request-Id, so an operator can correlate a log line, an event row and a
// client-visible header.
func (s *Server) requestIDMW(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		reqID := s.reqGen.NewString()
		w.Header().Set("Messq-Request-Id", reqID)
		next.ServeHTTP(w, r.WithContext(contextWithRequestID(r, reqID)))
	})
}

// requestIDOf returns the request's stamped id, or "" before the middleware ran.
func requestIDOf(r *http.Request) string {
	v, ok := r.Context().Value(ctxRequestID).(string)
	if !ok {
		return ""
	}
	return v
}

// connLimitMW rejects over-cap connections with 503 busy BEFORE any handler runs: an
// overloaded broker answers typed backpressure, not unbounded memory (I11).
func (s *Server) connLimitMW(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !s.conns.acquire() {
			s.writeError(w, busyError("connection limit reached (--max-conns "+
				strconv.Itoa(s.cfg.MaxConns)+")"))
			return
		}
		defer s.conns.release()
		next.ServeHTTP(w, r)
	})
}

// busyError is the writer-submit / connection-limit condition. It is a distinct type so
// mapCode needs no sentinel for it: the cap itself is the classification.
type busyError string

func (e busyError) Error() string { return string(e) }

// bodyLimitMW bounds EVERY body at --max-request-bytes before parse. Endpoints with a
// tighter cap (publish bodies per stream) wrap again inside their handler.
func (s *Server) bodyLimitMW(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Body != nil {
			r.Body = http.MaxBytesReader(w, r.Body, s.cfg.MaxRequestBytes)
		}
		next.ServeHTTP(w, r)
	})
}

// authzMW is the named no-op chain link #16 replaces with bearer-token authentication
// and role checks. Until then LOCAL TRUST applies: the Unix socket's file permissions
// are the boundary (D12).
func (s *Server) authzMW(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		next.ServeHTTP(w, r) // no-op until #16
	})
}

// newEnvelopeWriter wraps w so responses the ROUTER writes as plain text — a wrong
// method's 405 with its Allow header, or any stray stdlib Error() call — come out as
// envelopes instead. Responses our handlers wrote (Content-Type application/json set
// before WriteHeader) pass through untouched. The wrapper implements Unwrap, without
// which http.NewResponseController deadlines silently return errNotSupported.
func (s *Server) newEnvelopeWriter(w http.ResponseWriter) *envelopeWriter {
	return &envelopeWriter{srv: s, ResponseWriter: w}
}

type envelopeWriter struct {
	http.ResponseWriter
	srv      *Server
	replaced bool
}

// ensure the wrapper never hides flush/hijack support from ResponseController users.
var (
	_ http.Flusher = (*envelopeWriter)(nil)
)

func (e *envelopeWriter) Unwrap() http.ResponseWriter { return e.ResponseWriter }

func (e *envelopeWriter) Flush() {
	if f, ok := e.ResponseWriter.(http.Flusher); ok {
		f.Flush()
	}
}

// WriteHeader intercepts plain-text 404/405 responses and swaps in the envelope,
// preserving whatever headers the router already set (Allow on 405). A 404 never
// reaches here when the catch-all route handled it first; this is the safety net.
func (e *envelopeWriter) WriteHeader(status int) {
	ct := e.Header().Get("Content-Type")
	if status != http.StatusNotFound && status != http.StatusMethodNotAllowed || ct == "application/json" {
		e.ResponseWriter.WriteHeader(status)
		return
	}
	e.replaced = true
	code := CodeNotFound
	if status == http.StatusMethodNotAllowed {
		code = CodeMethodNotAllowed
	}
	e.srv.writeEnvelope(e, status, ErrorBody{
		Code:    code,
		Message: codeStatusMessage(code),
	})
}

// Write swallows the plain-text payload of a replaced response; everything else passes.
func (e *envelopeWriter) Write(p []byte) (int, error) {
	if e.replaced {
		// The discarded bytes are the point: the plain-text payload of a replaced
		// stdlib error must never join its envelope.
		return len(p), nil
	}
	return e.ResponseWriter.Write(p)
}

// connLimiter is the accept-side semaphore behind --max-conns. acquire/release are
// non-blocking by design: a full broker refuses instantly instead of queueing callers.
type connLimiter struct {
	sem chan struct{}
}

func newConnLimiter(n int) connLimiter {
	return connLimiter{sem: make(chan struct{}, n)}
}

func (l connLimiter) acquire() bool {
	select {
	case l.sem <- struct{}{}:
		return true
	default:
		return false
	}
}

func (l connLimiter) release() {
	select {
	case <-l.sem:
	default:
	}
}

// hold takes a slot for a test, failing the test when none is free, and returns the
// release function.
func (l connLimiter) hold(t interface{ Fatalf(string, ...any) }) func() {
	if !l.acquire() {
		t.Fatalf("connLimiter.hold: no free slot")
	}
	return l.release
}

// codeStatusMessage renders the one-line message the router-level envelopes carry;
// these conditions have no underlying error to render.
func codeStatusMessage(c Code) string {
	if c == CodeNotFound {
		return "no such route"
	}
	if c == CodeMethodNotAllowed {
		return "method not allowed on this route"
	}
	return string(c)
}
