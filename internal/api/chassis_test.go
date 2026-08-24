// SPDX-License-Identifier: Apache-2.0

package api

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// The chassis guarantees (G1, G10, G14): no response escapes the envelope, the route
// registry is the ONE place patterns live, panics never take the daemon down, and the
// middleware chain bounds every request before a handler runs.

func TestUnknownPathIsAnEnvelope(t *testing.T) {
	t.Parallel()

	srv := New(Config{Logger: discardLogger()})
	rec := httptest.NewRecorder()
	req := httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/nope", nil)
	srv.Handler().ServeHTTP(rec, req)

	if rec.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want 404", rec.Code)
	}
	body := rec.Body.String()
	if strings.Contains(body, "404 page not found") {
		t.Fatalf("stdlib plain text escaped: %q", body)
	}
	var env Envelope
	if err := json.Unmarshal([]byte(body), &env); err != nil {
		t.Fatalf("body is not an envelope (%v): %q", err, body)
	}
	if env.Error.Code != CodeNotFound {
		t.Errorf("code = %q, want not_found", env.Error.Code)
	}
}

func TestWrongMethodIsAnEnvelopeWithAllow(t *testing.T) {
	t.Parallel()

	srv := New(Config{Logger: discardLogger()})
	rec := httptest.NewRecorder()
	req := httptest.NewRequestWithContext(context.Background(), http.MethodPost, "/healthz", nil)
	srv.Handler().ServeHTTP(rec, req)

	if rec.Code != http.StatusMethodNotAllowed {
		t.Fatalf("status = %d, want 405", rec.Code)
	}
	if allow := rec.Header().Get("Allow"); !strings.Contains(allow, http.MethodGet) {
		t.Errorf("Allow = %q, want it to name GET", allow)
	}
	var env Envelope
	if err := json.Unmarshal(rec.Body.Bytes(), &env); err != nil {
		t.Fatalf("body is not an envelope (%v): %q", err, rec.Body.String())
	}
	if env.Error.Code != CodeMethodNotAllowed {
		t.Errorf("code = %q, want method_not_allowed", env.Error.Code)
	}
}

func TestPanicBecomesInternalAndServerKeepsServing(t *testing.T) {
	t.Parallel()

	srv := New(Config{Logger: discardLogger()})
	boom := srv.chained(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		panic("test explosion")
	}))

	rec := httptest.NewRecorder()
	boom.ServeHTTP(rec, httptest.NewRequestWithContext(context.Background(),
		http.MethodGet, "/__test/panic", nil))

	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("status = %d, want 500 (body %q)", rec.Code, rec.Body.String())
	}
	var env Envelope
	if err := json.Unmarshal(rec.Body.Bytes(), &env); err != nil {
		t.Fatalf("body is not an envelope (%v): %q", err, rec.Body.String())
	}
	if env.Error.Code != CodeInternal {
		t.Errorf("code = %q, want internal", env.Error.Code)
	}

	// The same process still answers afterwards.
	rec2 := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rec2, httptest.NewRequestWithContext(context.Background(),
		http.MethodGet, "/healthz", nil))
	if rec2.Code != http.StatusOK {
		t.Errorf("post-panic healthz status = %d, want 200", rec2.Code)
	}
}

func TestRequestIDOnEveryResponse(t *testing.T) {
	t.Parallel()

	srv := New(Config{Logger: discardLogger()})
	rec := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rec, httptest.NewRequestWithContext(context.Background(),
		http.MethodGet, "/healthz", nil))

	reqID := rec.Header().Get("Messq-Request-Id")
	if reqID == "" {
		t.Fatal("Messq-Request-Id missing on a 200")
	}
	if len(reqID) != 26 { // ULID canonical length
		t.Errorf("request id %q is not ULID-shaped", reqID)
	}

	// Distinct per request.
	rec2 := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rec2, httptest.NewRequestWithContext(context.Background(),
		http.MethodGet, "/healthz", nil))
	if rec2.Header().Get("Messq-Request-Id") == reqID {
		t.Error("two requests shared one request id")
	}
}

func TestConnLimiterReturnsBusy(t *testing.T) {
	t.Parallel()

	srv := New(Config{MaxConns: 1, Logger: discardLogger()})
	handler := srv.Handler()

	// One held slot exhausts the cap of one.
	release := srv.conns.hold(t)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, httptest.NewRequestWithContext(context.Background(),
		http.MethodGet, "/healthz", nil))

	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503 (body %q)", rec.Code, rec.Body.String())
	}
	var env Envelope
	if err := json.Unmarshal(rec.Body.Bytes(), &env); err != nil {
		t.Fatalf("body is not an envelope (%v): %q", err, rec.Body.String())
	}
	if env.Error.Code != CodeBusy {
		t.Errorf("code = %q, want busy", env.Error.Code)
	}
	if ra := rec.Header().Get("Retry-After"); ra == "" {
		t.Error("503 busy must carry Retry-After")
	}

	// After release the slot frees.
	release()
	rec2 := httptest.NewRecorder()
	handler.ServeHTTP(rec2, httptest.NewRequestWithContext(context.Background(),
		http.MethodGet, "/healthz", nil))
	if rec2.Code != http.StatusOK {
		t.Errorf("post-release status = %d, want 200", rec2.Code)
	}
}
