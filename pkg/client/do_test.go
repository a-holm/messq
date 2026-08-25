// SPDX-License-Identifier: Apache-2.0

package client

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

// countingTransport records every open and close of response bodies so an imbalance
// fails loudly: a client that leaks a body eventually wedges connection pools.
type countingTransport struct {
	inner http.RoundTripper
	open  func()
	close func()
}

func (ct *countingTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	resp, err := ct.inner.RoundTrip(req)
	if err == nil && resp.Body != nil {
		ct.open()
		resp.Body = countedBody{ReadCloser: resp.Body, done: ct.close}
	}
	return resp, err
}

type countedBody struct {
	io.ReadCloser
	done func()
}

func (b countedBody) Close() error {
	err := b.ReadCloser.Close()
	b.done()
	return err
}

// newTestClient builds a Client against ts with body counting wired through t.Cleanup,
// so EVERY request in EVERY test must end with its body closed.
func newTestClient(t *testing.T, ts *httptest.Server, opts ...Option) *Client {
	t.Helper()
	c, err := New(ts.URL, opts...)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	var opens, closes int
	tr := &countingTransport{
		inner: ts.Client().Transport,
		open:  func() { opens++ },
		close: func() { closes++ },
	}
	if tr.inner == nil {
		tr.inner = http.DefaultTransport
	}
	t.Cleanup(func() {
		if opens != closes {
			t.Errorf("response bodies: %d opened, %d closed — a body leaked", opens, closes)
		}
	})
	c.hc.Transport = tr
	return c
}

func TestDoRoundTripRequestShape(t *testing.T) {
	t.Parallel()

	var gotMethod, gotPath, gotQuery string
	var gotContentType, gotUA, gotAuth, gotBody string
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotMethod, gotPath, gotQuery = r.Method, r.URL.Path, r.URL.RawQuery
		gotContentType = r.Header.Get("Content-Type")
		gotUA = r.Header.Get("User-Agent")
		gotAuth = r.Header.Get("Authorization")
		b, _ := io.ReadAll(r.Body)
		gotBody = string(b)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"ok":true}`))
	}))
	defer ts.Close()

	c := newTestClient(t, ts,
		WithCredential(TokenCredential("msq1_id_secret")),
		WithUserAgent("messq-test/1"))
	type echo struct {
		OK bool `json:"ok"`
	}
	res, err := do[echo](context.Background(), c, http.MethodPost, "/v1/things",
		map[string][]string{"dry_run": {"1"}}, map[string]int{"n": 3})
	if err != nil {
		t.Fatalf("do: %v", err)
	}
	if !res.OK {
		t.Error("decoded res.OK = false")
	}
	if gotMethod != http.MethodPost || gotPath != "/v1/things" || gotQuery != "dry_run=1" {
		t.Errorf("request line = %s %s?%s", gotMethod, gotPath, gotQuery)
	}
	if !strings.HasPrefix(gotContentType, "application/json") {
		t.Errorf("Content-Type = %q", gotContentType)
	}
	if gotUA != "messq-test/1" {
		t.Errorf("User-Agent = %q", gotUA)
	}
	if gotAuth != "Bearer msq1_id_secret" {
		t.Errorf("Authorization = %q", gotAuth)
	}
	if gotBody != `{"n":3}` {
		t.Errorf("body = %q", gotBody)
	}
}

func TestDoDrainsAndClosesEveryBody(t *testing.T) {
	t.Parallel()

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		// Bigger than a socket buffer, forces real reads before the drain; valid JSON
		// so the round trip itself succeeds.
		_, _ = w.Write([]byte(`{"pad":"` + strings.Repeat("x", 8192) + `"}`))
	}))
	defer ts.Close()

	c := newTestClient(t, ts)
	for range 4 {
		if _, err := do[struct{}](context.Background(), c, http.MethodGet, "/", nil, nil); err != nil {
			t.Fatalf("do: %v", err)
		}
	}
	// The imbalance itself is asserted by newTestClient's cleanup.
}

func TestDoDecodesErrorEnvelope(t *testing.T) {
	t.Parallel()

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Retry-After", "2")
		w.Header().Set("Messq-Request-Id", "req-9")
		w.WriteHeader(http.StatusConflict)
		_, _ = w.Write([]byte(`{"error":{"code":"stale_ack","message":"token attempt moved on",` +
			`"next":["re-fetch"],"detail":{"current_attempt":2},"trace_id":"tr-1"}}`))
	}))
	defer ts.Close()

	c := newTestClient(t, ts)
	_, err := do[struct{}](context.Background(), c, http.MethodPost, "/v1/ack", nil, nil)
	var e *Error
	if !errors.As(err, &e) {
		t.Fatalf("err = %T(%v), want *client.Error", err, err)
	}
	if e.Code != "stale_ack" || e.Message != "token attempt moved on" || e.Status != http.StatusConflict {
		t.Errorf("envelope decode = %+v", e)
	}
	if len(e.Next) != 1 || e.Next[0] != "re-fetch" {
		t.Errorf("Next = %v", e.Next)
	}
	if e.Detail["current_attempt"] != float64(2) || e.TraceID != "tr-1" || e.RequestID != "req-9" {
		t.Errorf("Detail/TraceID/RequestID = %v/%q/%q", e.Detail, e.TraceID, e.RequestID)
	}
	if e.RetryAfter != 2*time.Second {
		t.Errorf("RetryAfter = %v, want 2s", e.RetryAfter)
	}
	if !errors.Is(err, ErrStaleAck) {
		t.Error("errors.Is(err, ErrStaleAck) = false")
	}
}

func TestDoSurvivesNonJSONFailure(t *testing.T) {
	t.Parallel()

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusBadGateway)
		_, _ = w.Write([]byte("<html>proxy exploded</html>"))
	}))
	defer ts.Close()

	c := newTestClient(t, ts)
	_, err := do[struct{}](context.Background(), c, http.MethodGet, "/", nil, nil)
	var e *Error
	if !errors.As(err, &e) {
		t.Fatalf("err = %T(%v), want *client.Error", err, err)
	}
	if e.Status != http.StatusBadGateway {
		t.Errorf("Status = %d, want 502", e.Status)
	}
	if Classify(err) == KindOK {
		t.Error("Classify(502) = KindOK")
	}
}

func TestDoIgnoresUnknownResponseFields(t *testing.T) {
	t.Parallel()

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"seq":3,"from_the_future":true,"nested":{"later":[]}}`))
	}))
	defer ts.Close()

	c := newTestClient(t, ts)
	type shape struct {
		Seq int64 `json:"seq"`
	}
	res, err := do[shape](context.Background(), c, http.MethodGet, "/", nil, nil)
	if err != nil {
		t.Fatalf("lenient decode failed: %v", err)
	}
	if res.Seq != 3 {
		t.Errorf("Seq = %d", res.Seq)
	}
}

func TestTransportErrorsClassifyUnreachable(t *testing.T) {
	t.Parallel()

	c, err := New("tcp://127.0.0.1:1") // nothing listens there
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	_, derr := do[struct{}](context.Background(), c, http.MethodGet, "/", nil, nil)
	if !errors.Is(derr, ErrUnreachable) {
		t.Fatalf("err = %v, want ErrUnreachable", derr)
	}
	if got := Classify(derr); got != KindUnavailable {
		t.Errorf("Classify = %v, want KindUnavailable", got)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, cerr := do[struct{}](ctx, c, http.MethodGet, "/", nil, nil)
	if !errors.Is(cerr, context.Canceled) {
		t.Errorf("cancelled ctx: err = %v, want unwrapped context.Canceled", cerr)
	}
}
