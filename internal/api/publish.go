// SPDX-License-Identifier: Apache-2.0

package api

import (
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/textproto"
	"strconv"
	"strings"

	"github.com/a-holm/messq/internal/errs"
	"github.com/a-holm/messq/internal/queue"
	"github.com/a-holm/messq/internal/store"
)

// handlePublishMessage is POST /v1/streams/{stream}/messages: a raw-body publish. The
// subject comes from ?subject= or the Messq-Subject header; the body is the request
// body; Messq-Msg-Id supplies idempotency, Messq-Header-* the user headers, and
// Messq-Trace-Id / traceparent the trace id. 201 stores, 200 reports a dedup hit.
func (s *Server) handlePublishMessage(w http.ResponseWriter, r *http.Request) {
	stream := r.PathValue("stream")
	if err := queue.ValidateExistingStreamName(stream); err != nil {
		s.writeError(w, err)
		return
	}

	// The stream config is the authority for the per-stream body cap and the fast-path
	// subject/size checks; GetStream also turns a missing stream into a 404.
	info, err := s.store.GetStream(r.Context(), stream)
	if err != nil {
		s.writeError(w, err)
		return
	}
	sc := info.Config()

	subject, err := publishSubject(r)
	if err != nil {
		s.writeError(w, err)
		return
	}
	headers, err := publishHeaders(r)
	if err != nil {
		s.writeError(w, err)
		return
	}
	msgID := r.Header.Get("Messq-Msg-Id")
	if msgIDErr := queue.ValidateMsgID(msgID); msgIDErr != nil {
		s.writeError(w, msgIDErr)
		return
	}
	explicitTrace := r.Header.Get("Messq-Trace-Id")
	if traceErr := queue.ValidateTraceIDToken(explicitTrace); traceErr != nil {
		s.writeError(w, traceErr)
		return
	}
	traceID := queue.ResolveTraceID(explicitTrace, r.Header.Get("traceparent"), rand.Reader)

	body, err := readBody(w, r, sc.MaxMsgSize)
	if err != nil {
		s.writeError(w, err)
		return
	}

	req := queue.PublishReq{
		Subject: subject,
		Headers: headers,
		Body:    body,
		MsgID:   msgID,
		TraceID: traceID,
	}
	if vErr := queue.ValidatePublish(sc, req, s.limits); vErr != nil {
		s.writeError(w, vErr)
		return
	}

	ack, err := s.store.Publish(r.Context(), store.PublishCmd{Stream: stream, Req: req})
	if err != nil {
		s.writeError(w, err)
		return
	}

	// The 2xx is sent only after the commit's fsync returned (D4). This is the fault point
	// between commit and reply: a no-op except under `-tags messq_fault` with MESSQ_FAULT
	// armed, where it crashes the process so the reply never leaves. #8's harness and the
	// internal/cli crash tests exercise it.
	faultAfterCommit()

	w.Header().Set("Messq-Seq", strconv.FormatInt(ack.Seq, 10))
	w.Header().Set("Messq-Msg-Id", ack.ID)
	w.Header().Set("Messq-Trace-Id", ack.TraceID)
	status := http.StatusCreated
	if ack.Duplicate {
		status = http.StatusOK
	}
	s.writeJSON(w, status, ack)
}

// readBody returns the bounded request body. A known Content-Length over the stream's
// per-message cap is refused without reading a byte; a chunked body is streamed through
// http.MaxBytesReader so it can never buffer more than the cap plus one.
func readBody(w http.ResponseWriter, r *http.Request, maxMsgSize int64) ([]byte, error) {
	if r.ContentLength > maxMsgSize {
		return nil, &queue.TooLargeError{What: "body", Size: r.ContentLength, Limit: maxMsgSize}
	}
	r.Body = http.MaxBytesReader(w, r.Body, maxMsgSize+1)
	body, err := io.ReadAll(r.Body)
	if err != nil {
		var maxErr *http.MaxBytesError
		if errors.As(err, &maxErr) {
			return nil, errs.E(errs.ErrTooLarge, "api.publishMessage",
				"body exceeds the stream's max_msg_size of %d bytes", maxMsgSize)
		}
		return nil, errs.E(errs.ErrBadRequest, "api.publishMessage", "read body: %v", err)
	}
	return body, nil
}

// publishSubject resolves the publish subject from ?subject= and the Messq-Subject
// header. Both absent is refused (naming both ways); both present and different is
// refused; identical values are accepted.
func publishSubject(r *http.Request) (string, error) {
	query := r.URL.Query().Get("subject")
	header := r.Header.Get("Messq-Subject")
	switch {
	case query == "" && header == "":
		return "", errs.E(errs.ErrBadRequest, "api.publishMessage",
			"publish needs a subject: pass ?subject= or the Messq-Subject header")
	case query != "" && header != "" && query != header:
		return "", errs.E(errs.ErrBadRequest, "api.publishMessage",
			"subject is given twice and differs: ?subject=%q vs Messq-Subject=%q", query, header)
	case header != "":
		return header, nil
	default:
		return query, nil
	}
}

// publishHeaders gathers the user headers from Messq-Header-* request headers: the
// prefix is stripped, the remaining key canonicalised, and a repeated name refused.
// An empty result is nil, so a header-less publish stores NULL (the common case).
func publishHeaders(r *http.Request) (map[string]string, error) {
	const prefix = "Messq-Header-"
	headers := make(map[string]string)
	for key, values := range r.Header {
		if !strings.HasPrefix(key, prefix) {
			continue
		}
		canonical := textproto.CanonicalMIMEHeaderKey(strings.TrimPrefix(key, prefix))
		if len(values) > 1 {
			return nil, errs.E(errs.ErrBadRequest, "api.publishMessage",
				"header %q was repeated; a repeated header name is ambiguous", canonical)
		}
		if _, dup := headers[canonical]; dup {
			return nil, errs.E(errs.ErrBadRequest, "api.publishMessage",
				"header %q was repeated (differing only in case)", canonical)
		}
		headers[canonical] = values[0]
	}
	if len(headers) == 0 {
		return nil, nil
	}
	return headers, nil
}

// batchEntry is one NDJSON line of messages:batch. body_b64 is canonical; body (a UTF-8
// string) is a convenience; both is a bad request. The other fields map straight onto
// queue.PublishReq.
type batchEntry struct {
	Subject string            `json:"subject"`
	BodyB64 string            `json:"body_b64"`
	Body    string            `json:"body"`
	MsgID   string            `json:"msg_id"`
	Headers map[string]string `json:"headers"`
	TraceID string            `json:"trace_id"`
}

// publishReq decodes one entry into a PublishReq, reporting shape errors with the entry's
// 1-based line index. Subject/size/header validation happens later, per line, against the
// authoritative stream config.
func (e batchEntry) publishReq(line int) (queue.PublishReq, error) {
	if e.BodyB64 != "" && e.Body != "" {
		return queue.PublishReq{}, errs.E(errs.ErrBadRequest, "api.publishBatch",
			"line %d: body and body_b64 are both present; send one", line)
	}
	var body []byte
	if e.BodyB64 != "" {
		decoded, err := base64.StdEncoding.DecodeString(e.BodyB64)
		if err != nil {
			return queue.PublishReq{}, errs.E(errs.ErrBadRequest, "api.publishBatch",
				"line %d: body_b64 is not valid base64: %v", line, err)
		}
		body = decoded
	} else {
		body = []byte(e.Body)
	}
	return queue.PublishReq{
		Subject: e.Subject,
		Headers: e.Headers,
		Body:    body,
		MsgID:   e.MsgID,
		TraceID: e.TraceID,
	}, nil
}

// batchLineError wraps a per-entry validation error with its 1-based line index while
// preserving the error's type, so the mapping still resolves it to the right code.
func batchLineError(line int, err error) error {
	return errs.E(err, "", "line %d: %s", line, errorMessage(err))
}

// handlePublishBatch is POST /v1/streams/{stream}/messages:batch: NDJSON in, one
// transaction out. The body is bounded by the --max-batch-bytes ceiling via a
// Content-Length pre-check and http.MaxBytesReader; every entry is validated against the
// stream config so a failure names its line index and stores nothing.
func (s *Server) handlePublishBatch(w http.ResponseWriter, r *http.Request) {
	stream := r.PathValue("stream")
	if err := queue.ValidateExistingStreamName(stream); err != nil {
		s.writeError(w, err)
		return
	}

	info, err := s.store.GetStream(r.Context(), stream)
	if err != nil {
		s.writeError(w, err)
		return
	}
	sc := info.Config()

	if r.ContentLength > s.maxBatchBytes {
		s.writeError(w, errs.E(errs.ErrTooLarge, "api.publishBatch",
			"batch body is %d bytes, limit is %d", r.ContentLength, s.maxBatchBytes))
		return
	}
	r.Body = http.MaxBytesReader(w, r.Body, s.maxBatchBytes+1)

	var reqs []queue.PublishReq
	dec := json.NewDecoder(r.Body)
	for line := 1; ; line++ {
		var entry batchEntry
		if decErr := dec.Decode(&entry); decErr != nil {
			if errors.Is(decErr, io.EOF) {
				break
			}
			var maxErr *http.MaxBytesError
			if errors.As(decErr, &maxErr) {
				s.writeError(w, errs.E(errs.ErrTooLarge, "api.publishBatch",
					"batch body exceeds the %d-byte limit", s.maxBatchBytes))
				return
			}
			s.writeError(w, errs.E(errs.ErrBadRequest, "api.publishBatch",
				"line %d: invalid NDJSON: %v", line, decErr))
			return
		}
		req, reqErr := entry.publishReq(line)
		if reqErr != nil {
			s.writeError(w, reqErr)
			return
		}
		if vErr := queue.ValidatePublish(sc, req, s.limits); vErr != nil {
			s.writeError(w, batchLineError(line, vErr))
			return
		}
		reqs = append(reqs, req)
	}

	if len(reqs) == 0 {
		s.writeError(w, errs.E(errs.ErrBadRequest, "api.publishBatch", "batch holds no entries"))
		return
	}

	ack, err := s.store.PublishBatch(r.Context(), store.BatchCmd{Stream: stream, Reqs: reqs})
	if err != nil {
		s.writeError(w, err)
		return
	}
	s.writeJSON(w, http.StatusCreated, ack)
}
