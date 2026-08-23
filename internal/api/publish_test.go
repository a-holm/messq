// SPDX-License-Identifier: Apache-2.0

package api

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/a-holm/messq/internal/clock"
	"github.com/a-holm/messq/internal/queue"
	"github.com/a-holm/messq/internal/store"
)

// doPublish issues a raw-body publish with the given headers and returns the recorder.
func doPublish(t *testing.T, h http.Handler, path, body string, headers map[string]string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequestWithContext(context.Background(), http.MethodPost, path, strings.NewReader(body))
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec
}

// createStreamWith creates a stream from cfg, failing the test on error.
func createStreamWith(t *testing.T, st *store.Store, cfg queue.StreamConfig) {
	t.Helper()
	if _, _, err := st.CreateStream(context.Background(), cfg, "test"); err != nil {
		t.Fatalf("create %s: %v", cfg.Name, err)
	}
}

// ordersSubjectCfg is an orders stream that accepts only the orders.> pattern, so a
// subject mismatch is constructible.
func ordersSubjectCfg() queue.StreamConfig {
	cfg := queue.DefaultConfig("orders")
	cfg.Subjects = []string{"orders.>"}
	return cfg
}

func TestPublishMessage(t *testing.T) {
	st, srv := newStreamsServer(t)
	createStreamWith(t, st, ordersSubjectCfg())

	rec := doPublish(t, srv.Handler(),
		"/v1/streams/orders/messages?subject=orders.eu.created", "hello",
		map[string]string{"Messq-Msg-Id": "ord-1", "Messq-Trace-Id": "trace-abc-123"})
	if rec.Code != http.StatusCreated {
		t.Fatalf("status = %d, want 201 (body %q)", rec.Code, rec.Body.String())
	}

	if seq := rec.Header().Get("Messq-Seq"); seq == "" {
		t.Errorf("Messq-Seq header is empty")
	}
	if id := rec.Header().Get("Messq-Msg-Id"); id == "" {
		t.Errorf("Messq-Msg-Id header is empty")
	}
	if tr := rec.Header().Get("Messq-Trace-Id"); tr != "trace-abc-123" {
		t.Errorf("Messq-Trace-Id = %q, want the explicit trace id", tr)
	}

	var ack store.Ack
	if err := json.Unmarshal(rec.Body.Bytes(), &ack); err != nil {
		t.Fatalf("body is not an Ack: %v (%s)", err, rec.Body.String())
	}
	if ack.Stream != "orders" || ack.Seq != 1 || ack.ID == "" || ack.Duplicate {
		t.Errorf("ack = %+v, want orders/1/non-empty id/first publish", ack)
	}
	if ack.TraceID != "trace-abc-123" {
		t.Errorf("ack.trace_id = %q, want the explicit trace id", ack.TraceID)
	}
	if ack.ID != rec.Header().Get("Messq-Msg-Id") {
		t.Errorf("Messq-Msg-Id header %q does not match ack.id %q", rec.Header().Get("Messq-Msg-Id"), ack.ID)
	}

	// The body round-trips byte-identical.
	msg, err := st.PeekSeq(context.Background(), "orders", 1)
	if err != nil {
		t.Fatalf("peek: %v", err)
	}
	if string(msg.Body) != "hello" {
		t.Errorf("stored body = %q, want hello", msg.Body)
	}
}

func TestPublishMessageSubjectHeader(t *testing.T) {
	st, srv := newStreamsServer(t)
	createStreamWith(t, st, ordersSubjectCfg())

	rec := doPublish(t, srv.Handler(), "/v1/streams/orders/messages", "hi",
		map[string]string{"Messq-Subject": "orders.eu.created"})
	if rec.Code != http.StatusCreated {
		t.Fatalf("status = %d, want 201 (body %q)", rec.Code, rec.Body.String())
	}
	var ack store.Ack
	if err := json.Unmarshal(rec.Body.Bytes(), &ack); err != nil {
		t.Fatalf("decode ack: %v", err)
	}
	if ack.Seq != 1 {
		t.Errorf("seq = %d, want 1", ack.Seq)
	}
}

func TestPublishMessageSubjectBothAgree(t *testing.T) {
	st, srv := newStreamsServer(t)
	createStreamWith(t, st, ordersSubjectCfg())

	rec := doPublish(t, srv.Handler(),
		"/v1/streams/orders/messages?subject=orders.eu.created", "hi",
		map[string]string{"Messq-Subject": "orders.eu.created"})
	if rec.Code != http.StatusCreated {
		t.Fatalf("status = %d, want 201 (body %q)", rec.Code, rec.Body.String())
	}
}

func TestPublishMessageSubjectConflict(t *testing.T) {
	st, srv := newStreamsServer(t)
	createStreamWith(t, st, ordersSubjectCfg())

	rec := doPublish(t, srv.Handler(),
		"/v1/streams/orders/messages?subject=orders.eu.created", "hi",
		map[string]string{"Messq-Subject": "orders.us.created"})
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400 (body %q)", rec.Code, rec.Body.String())
	}
	if env := decodeError(t, rec); env.Code != "bad_request" {
		t.Errorf("code = %q, want bad_request", env.Code)
	}
}

func TestPublishMessageNoSubject(t *testing.T) {
	st, srv := newStreamsServer(t)
	createStreamWith(t, st, ordersSubjectCfg())

	rec := doPublish(t, srv.Handler(), "/v1/streams/orders/messages", "hi", nil)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400 (body %q)", rec.Code, rec.Body.String())
	}
	env := decodeError(t, rec)
	if env.Code != "bad_request" {
		t.Errorf("code = %q, want bad_request", env.Code)
	}
	// The message must name both ways to supply the subject.
	if !strings.Contains(env.Message, "?subject") || !strings.Contains(env.Message, "Messq-Subject") {
		t.Errorf("message %q does not name both ?subject= and Messq-Subject", env.Message)
	}
}

func TestPublishMessageDedup(t *testing.T) {
	st, srv := newStreamsServer(t)
	createStreamWith(t, st, ordersSubjectCfg())

	first := doPublish(t, srv.Handler(),
		"/v1/streams/orders/messages?subject=orders.eu.created", "hello",
		map[string]string{"Messq-Msg-Id": "ord-dup"})
	if first.Code != http.StatusCreated {
		t.Fatalf("first status = %d, want 201 (body %q)", first.Code, first.Body.String())
	}

	again := doPublish(t, srv.Handler(),
		"/v1/streams/orders/messages?subject=orders.eu.created", "hello",
		map[string]string{"Messq-Msg-Id": "ord-dup"})
	if again.Code != http.StatusOK {
		t.Fatalf("retry status = %d, want 200 (body %q)", again.Code, again.Body.String())
	}

	var a, b store.Ack
	if err := json.Unmarshal(first.Body.Bytes(), &a); err != nil {
		t.Fatalf("decode first: %v", err)
	}
	if err := json.Unmarshal(again.Body.Bytes(), &b); err != nil {
		t.Fatalf("decode retry: %v", err)
	}
	if !b.Duplicate {
		t.Errorf("retry duplicate = %v, want true", b.Duplicate)
	}
	if a.Seq != b.Seq || a.ID != b.ID {
		t.Errorf("retry receipt %+v differs from original %+v", b, a)
	}
}

func TestPublishMessageUserHeaders(t *testing.T) {
	st, srv := newStreamsServer(t)
	createStreamWith(t, st, ordersSubjectCfg())

	rec := doPublish(t, srv.Handler(), "/v1/streams/orders/messages?subject=orders.eu.created", "x",
		map[string]string{"Messq-Header-Tenant-Id": "acme", "Messq-Header-Content-Type": "application/json"})
	if rec.Code != http.StatusCreated {
		t.Fatalf("status = %d, want 201 (body %q)", rec.Code, rec.Body.String())
	}

	msg, err := st.PeekSeq(context.Background(), "orders", 1)
	if err != nil {
		t.Fatalf("peek: %v", err)
	}
	want := map[string]string{"Tenant-Id": "acme", "Content-Type": "application/json"}
	if !mapsEqual(msg.Headers, want) {
		t.Errorf("stored headers = %v, want %v", msg.Headers, want)
	}
}

func TestPublishMessageRepeatedHeader(t *testing.T) {
	st, srv := newStreamsServer(t)
	createStreamWith(t, st, ordersSubjectCfg())

	req := httptest.NewRequestWithContext(context.Background(), http.MethodPost,
		"/v1/streams/orders/messages?subject=orders.eu.created", strings.NewReader("x"))
	// Two values for the same header name collapse into one slice entry.
	req.Header.Add("Messq-Header-Tenant", "a")
	req.Header.Add("Messq-Header-Tenant", "b")
	rec := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400 (body %q)", rec.Code, rec.Body.String())
	}
	if env := decodeError(t, rec); env.Code != "bad_request" {
		t.Errorf("code = %q, want bad_request", env.Code)
	}
}

func TestPublishMessageReservedHeader(t *testing.T) {
	st, srv := newStreamsServer(t)
	createStreamWith(t, st, ordersSubjectCfg())

	rec := doPublish(t, srv.Handler(), "/v1/streams/orders/messages?subject=orders.eu.created", "x",
		map[string]string{"Messq-Header-Messq-Foo": "bar"})
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400 (body %q)", rec.Code, rec.Body.String())
	}
	if env := decodeError(t, rec); env.Code != "reserved_header" {
		t.Errorf("code = %q, want reserved_header", env.Code)
	}
}

func TestPublishMessageTraceparent(t *testing.T) {
	st, srv := newStreamsServer(t)
	createStreamWith(t, st, ordersSubjectCfg())

	// A version-00 traceparent whose trace-id field is 4bf92f…4736.
	rec := doPublish(t, srv.Handler(), "/v1/streams/orders/messages?subject=orders.eu.created", "x",
		map[string]string{"traceparent": "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"})
	if rec.Code != http.StatusCreated {
		t.Fatalf("status = %d, want 201 (body %q)", rec.Code, rec.Body.String())
	}
	if tr := rec.Header().Get("Messq-Trace-Id"); tr != "4bf92f3577b34da6a3ce929d0e0e4736" {
		t.Errorf("Messq-Trace-Id = %q, want the traceparent's trace id", tr)
	}
}

func TestPublishMessageBadSubject(t *testing.T) {
	st, srv := newStreamsServer(t)
	createStreamWith(t, st, ordersSubjectCfg())

	rec := doPublish(t, srv.Handler(), "/v1/streams/orders/messages?subject=orders.*.created", "x", nil)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400 (body %q)", rec.Code, rec.Body.String())
	}
	if env := decodeError(t, rec); env.Code != "bad_subject" {
		t.Errorf("code = %q, want bad_subject", env.Code)
	}
}

func TestPublishMessageSubjectMismatch(t *testing.T) {
	st, srv := newStreamsServer(t)
	createStreamWith(t, st, ordersSubjectCfg())

	rec := doPublish(t, srv.Handler(), "/v1/streams/orders/messages?subject=other.a", "x", nil)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400 (body %q)", rec.Code, rec.Body.String())
	}
	env := decodeError(t, rec)
	if env.Code != "subject_mismatch" {
		t.Errorf("code = %q, want subject_mismatch", env.Code)
	}
	if !strings.Contains(env.Message, "orders.>") {
		t.Errorf("message %q does not name the accepted patterns", env.Message)
	}
}

// countReader counts Read calls and returns EOF, so a test can prove the handler never
// buffered a body it was told (via Content-Length) is oversized.
type countReader struct{ reads *int }

func (c countReader) Read([]byte) (int, error) {
	*c.reads++
	return 0, io.EOF
}

func TestPublishMessageTooLargeNoBuffering(t *testing.T) {
	st, srv := newStreamsServer(t)
	cfg := queue.DefaultConfig("orders")
	cfg.MaxMsgSize = 10
	createStreamWith(t, st, cfg)

	var reads int
	req := httptest.NewRequestWithContext(context.Background(), http.MethodPost,
		"/v1/streams/orders/messages?subject=orders.a", io.NopCloser(countReader{reads: &reads}))
	req.ContentLength = 1000 // claim a 1000-byte body we never actually send
	rec := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rec, req)

	if rec.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("status = %d, want 413 (body %q)", rec.Code, rec.Body.String())
	}
	if env := decodeError(t, rec); env.Code != "too_large" {
		t.Errorf("code = %q, want too_large", env.Code)
	}
	if reads != 0 {
		t.Errorf("handler read the body %d times; Content-Length should reject without buffering", reads)
	}
}

func TestPublishMessageTooLargeChunked(t *testing.T) {
	st, srv := newStreamsServer(t)
	cfg := queue.DefaultConfig("orders")
	cfg.MaxMsgSize = 10
	createStreamWith(t, st, cfg)

	// A body wrapped in a plain io.Reader is "chunked" (ContentLength -1): the handler
	// can only bound it with MaxBytesReader, not a Content-Length pre-check.
	req := httptest.NewRequestWithContext(context.Background(), http.MethodPost,
		"/v1/streams/orders/messages?subject=orders.a", io.NopCloser(strings.NewReader(strings.Repeat("x", 20))))
	rec := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rec, req)

	if rec.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("status = %d, want 413 (body %q)", rec.Code, rec.Body.String())
	}
	if env := decodeError(t, rec); env.Code != "too_large" {
		t.Errorf("code = %q, want too_large", env.Code)
	}
}

func TestPublishMessageEmptyBody(t *testing.T) {
	st, srv := newStreamsServer(t)
	createStreamWith(t, st, ordersSubjectCfg())

	rec := doPublish(t, srv.Handler(), "/v1/streams/orders/messages?subject=orders.eu.created", "", nil)
	if rec.Code != http.StatusCreated {
		t.Fatalf("status = %d, want 201 (body %q)", rec.Code, rec.Body.String())
	}
	msg, err := st.PeekSeq(context.Background(), "orders", 1)
	if err != nil {
		t.Fatalf("peek: %v", err)
	}
	if len(msg.Body) != 0 {
		t.Errorf("stored body length = %d, want 0 (empty blob)", len(msg.Body))
	}
}

func TestPublishMessageMissingStream(t *testing.T) {
	_, srv := newStreamsServer(t)

	rec := doPublish(t, srv.Handler(), "/v1/streams/missing/messages?subject=a.b", "x", nil)
	if rec.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want 404 (body %q)", rec.Code, rec.Body.String())
	}
	if env := decodeError(t, rec); env.Code != "not_found" {
		t.Errorf("code = %q, want not_found", env.Code)
	}
}

func TestPublishBatch(t *testing.T) {
	st, srv := newStreamsServer(t)
	createStreamWith(t, st, ordersSubjectCfg())

	body := "{\"subject\":\"orders.eu.created\",\"body_b64\":\"aGVsbG8=\",\"msg_id\":\"b1\"}\n" +
		"{\"subject\":\"orders.us.created\",\"body\":\"world\",\"headers\":{\"Tenant\":\"acme\"}}\n"
	rec := doJSON(t, srv.Handler(), http.MethodPost, "/v1/streams/orders/messages:batch", body)
	if rec.Code != http.StatusCreated {
		t.Fatalf("status = %d, want 201 (body %q)", rec.Code, rec.Body.String())
	}

	var ack store.BatchAck
	if err := json.Unmarshal(rec.Body.Bytes(), &ack); err != nil {
		t.Fatalf("body is not a BatchAck: %v (%s)", err, rec.Body.String())
	}
	if ack.Stream != "orders" {
		t.Errorf("stream = %q, want orders", ack.Stream)
	}
	if len(ack.Results) != 2 {
		t.Fatalf("results = %d, want 2 (%s)", len(ack.Results), rec.Body.String())
	}
	if ack.Results[0].Seq != 1 || ack.Results[1].Seq != 2 {
		t.Errorf("seqs = %d/%d, want contiguous 1/2", ack.Results[0].Seq, ack.Results[1].Seq)
	}
	if ack.Results[0].Duplicate || ack.Results[1].Duplicate {
		t.Errorf("fresh batch reported duplicates: %+v", ack.Results)
	}

	// The base64 body decoded correctly.
	msg, err := st.PeekSeq(context.Background(), "orders", 1)
	if err != nil {
		t.Fatalf("peek 1: %v", err)
	}
	if string(msg.Body) != "hello" {
		t.Errorf("first stored body = %q, want hello", msg.Body)
	}
}

func TestPublishBatchBodyAndBodyB64(t *testing.T) {
	st, srv := newStreamsServer(t)
	createStreamWith(t, st, ordersSubjectCfg())

	rec := doJSON(t, srv.Handler(), http.MethodPost, "/v1/streams/orders/messages:batch",
		"{\"subject\":\"orders.eu.created\",\"body\":\"x\",\"body_b64\":\"eA==\"}\n")
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400 (body %q)", rec.Code, rec.Body.String())
	}
	env := decodeError(t, rec)
	if env.Code != "bad_request" {
		t.Errorf("code = %q, want bad_request", env.Code)
	}
	if !strings.Contains(env.Message, "line 1") {
		t.Errorf("message %q does not name line 1", env.Message)
	}
}

func TestPublishBatchLineErrorAllOrNothing(t *testing.T) {
	st, srv := newStreamsServer(t)
	createStreamWith(t, st, ordersSubjectCfg())

	// Line 2 has a subject the stream does not accept.
	body := "{\"subject\":\"orders.eu.created\",\"body\":\"ok\"}\n" +
		"{\"subject\":\"other.a\",\"body\":\"bad\"}\n"
	rec := doJSON(t, srv.Handler(), http.MethodPost, "/v1/streams/orders/messages:batch", body)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400 (body %q)", rec.Code, rec.Body.String())
	}
	env := decodeError(t, rec)
	if env.Code != "subject_mismatch" {
		t.Errorf("code = %q, want subject_mismatch", env.Code)
	}
	if !strings.Contains(env.Message, "line 2") {
		t.Errorf("message %q does not name line 2", env.Message)
	}

	// All-or-nothing: the valid first line must not have been stored.
	page, err := st.ListMessages(context.Background(), store.ListQuery{Stream: "orders"})
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(page.Messages) != 0 {
		t.Errorf("batch stored %d messages after a line-2 failure, want none", len(page.Messages))
	}
}

func TestPublishBatchTooLarge(t *testing.T) {
	clk := clock.NewFake(time.UnixMilli(1_700_000_000_000))
	st := openTestStore(t, clk, store.DurabilityRelaxed)
	createStreamWith(t, st, ordersSubjectCfg())
	srv := New(st, clk, discardLogger(), time.Minute, queue.DefaultLimits(), 16)

	var reads int
	req := httptest.NewRequestWithContext(context.Background(), http.MethodPost,
		"/v1/streams/orders/messages:batch", io.NopCloser(countReader{reads: &reads}))
	req.ContentLength = 1000
	rec := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rec, req)

	if rec.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("status = %d, want 413 (body %q)", rec.Code, rec.Body.String())
	}
	if env := decodeError(t, rec); env.Code != "too_large" {
		t.Errorf("code = %q, want too_large", env.Code)
	}
	if reads != 0 {
		t.Errorf("batch handler read the body %d times; Content-Length should reject without buffering", reads)
	}
}

func TestPublishBatchEmpty(t *testing.T) {
	st, srv := newStreamsServer(t)
	createStreamWith(t, st, ordersSubjectCfg())

	rec := doJSON(t, srv.Handler(), http.MethodPost, "/v1/streams/orders/messages:batch", "")
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400 (body %q)", rec.Code, rec.Body.String())
	}
	if env := decodeError(t, rec); env.Code != "bad_request" {
		t.Errorf("code = %q, want bad_request", env.Code)
	}
}

func mapsEqual(got, want map[string]string) bool {
	if len(got) != len(want) {
		return false
	}
	for k, v := range want {
		if got[k] != v {
			return false
		}
	}
	return true
}
