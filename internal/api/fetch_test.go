// SPDX-License-Identifier: Apache-2.0

package api

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/a-holm/messq/internal/clock"
	"github.com/a-holm/messq/internal/queue"
	"github.com/a-holm/messq/internal/store"
)

// The G5/G7/G12 fetch guarantees: clamps echo honestly, flow-blocked and paused
// consumers never park, the wait budget lands exactly (fake clock — no sleeps), a
// publish wakes a parked fetch immediately, shutdown drains parked handlers, and a
// parked fetch never misses a matching publish.

// fetchFixture is a stream + consumer + configured server.
type fetchFixture struct {
	srv *Server
	st  *store.Store
	clk *clock.Fake
}

func newFetchFixture(t *testing.T, mutate func(*queue.ConsumerConfig)) *fetchFixture {
	t.Helper()

	clk := clock.NewFake(time.UnixMilli(1_700_000_000_000))
	st := openTestStore(t, clk, store.DurabilityFull)
	if _, _, err := st.CreateStream(context.Background(), queue.DefaultConfig("orders"), actorAPI); err != nil {
		t.Fatalf("create stream: %v", err)
	}
	cfg := queue.DefaultConsumerConfig("w")
	if mutate != nil {
		mutate(&cfg)
	}
	start, startErr := queue.ParseStartPosition("first")
	if startErr != nil {
		t.Fatalf("parse start position: %v", startErr)
	}
	if _, cErr := st.CreateConsumer(context.Background(), "orders", cfg, start, actorAPI); cErr != nil {
		t.Fatalf("create consumer: %v", cErr)
	}
	srv := New(Config{
		Store: st, Clock: clk, Logger: discardLogger(),
		MaxFetchWait:     30 * time.Second,
		FetchEmptyDamper: 5 * time.Millisecond,
		// The wake-ordering property under test has nothing to do with the production
		// submit window, but the handler wraps every store call in it: under -race +
		// -coverpkg parallel load a starved writer can blow the 5s default, the fetch
		// answers busy (503), and the round looks exactly like a lost wakeup. Fixture
		// budgets are effectively unbounded; a test that needs the window sets it
		// explicitly.
		WriterSubmitTimeout: time.Hour,
	})
	// Attach the group-commit engine with the registry as its event sink — without it
	// the store runs the engine-less fallback, which has no fan-out pump and can
	// never wake a parked fetch.
	wr, err := st.NewWriter(store.Config{}, store.WithEventSink(srv.waiters))
	if err != nil {
		t.Fatalf("attach writer: %v", err)
	}
	t.Cleanup(func() {
		if cErr := wr.Close(context.Background()); cErr != nil {
			t.Logf("close writer: %v", cErr)
		}
	})
	return &fetchFixture{srv: srv, st: st, clk: clk}
}

func (f *fetchFixture) do(t *testing.T, body string) chan *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequestWithContext(context.Background(), http.MethodPost,
		"/v1/streams/orders/consumers/w/fetch", strings.NewReader(body))
	rec := httptest.NewRecorder()
	done := make(chan *httptest.ResponseRecorder, 1)
	go func() {
		f.srv.Handler().ServeHTTP(rec, req)
		done <- rec
	}()
	return done
}

// decodeFetchResponse decodes a fetch response and refuses to guess: an error envelope
// (busy, commit_unknown, …) unmarshals into a zero fetchResponse — nil messages, empty
// hold — which once masqueraded as a lost wakeup under load (a blown submit window
// answers 503 busy). Any non-200 or error envelope is now a named failure naming the
// transcript, so infrastructure trouble is attributed to infrastructure.
func decodeFetchResponse(tb testing.TB, rec *httptest.ResponseRecorder) fetchResponse {
	tb.Helper()
	if rec.Code != http.StatusOK {
		tb.Fatalf("fetch returned HTTP %d, want 200: %s", rec.Code, rec.Body.String())
	}
	var env Envelope
	if json.Unmarshal(rec.Body.Bytes(), &env) == nil && env.Error.Code != "" {
		tb.Fatalf("fetch returned an error envelope, want a fetch shape: %s", rec.Body.String())
	}
	var out fetchResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &out); err != nil {
		tb.Fatalf("fetch response not JSON (%v): %q", err, rec.Body.String())
	}
	return out
}

// waitForParked spins until exactly n handlers are parked (the repo's Gosched idiom —
// no sleeps).
func (f *fetchFixture) waitForParked(n int64) {
	waitFor(func() bool { return f.srv.waiters.Parked() == n })
}

func TestFetchClampsAndEchoesEffectiveValues(t *testing.T) {
	t.Parallel()

	f := newFetchFixture(t, nil)

	done := f.do(t, `{"batch":100000,"max_bytes":99999999,"wait_ms":999999999}`)
	f.waitForParked(1)
	f.clk.Advance(31 * time.Second)
	out := decodeFetchResponse(t, <-done)

	if out.Batch != 1024 || out.MaxBytes != 8<<20 || out.WaitMS != 30000 {
		t.Fatalf("effective = (batch %d, max_bytes %d, wait_ms %d), want caps applied and echoed",
			out.Batch, out.MaxBytes, out.WaitMS)
	}
	if out.HoldReason == "" || len(out.Messages) != 0 {
		t.Errorf("hold_reason=%q messages=%d at budget end, want the empty hold with []",
			out.HoldReason, len(out.Messages))
	}

	// Omitted batch defaults to 1, not to the cap.
	done = f.do(t, `{}`)
	f.waitForParked(1)
	f.clk.Advance(31 * time.Second)
	out = decodeFetchResponse(t, <-done)
	if out.Batch != 1 {
		t.Errorf("omitted batch echoed %d, want 1", out.Batch)
	}
}

func TestFetchRejectsQueryParamsAndNegativeWait(t *testing.T) {
	t.Parallel()

	f := newFetchFixture(t, nil)

	req := httptest.NewRequestWithContext(context.Background(), http.MethodPost,
		"/v1/streams/orders/consumers/w/fetch?batch=2", strings.NewReader(`{}`))
	rec := httptest.NewRecorder()
	f.srv.Handler().ServeHTTP(rec, req)
	var env Envelope
	if err := json.Unmarshal(rec.Body.Bytes(), &env); err != nil || env.Error.Code != CodeBadRequest {
		t.Fatalf("query params gave (%v), want bad_request envelope", rec.Body.String())
	}
	if !strings.Contains(env.Error.Message, "JSON body") {
		t.Errorf("message %q does not name the JSON body", env.Error.Message)
	}

	req = httptest.NewRequestWithContext(context.Background(), http.MethodPost,
		"/v1/streams/orders/consumers/w/fetch", strings.NewReader(`{"wait_ms":-5}`))
	rec = httptest.NewRecorder()
	f.srv.Handler().ServeHTTP(rec, req)
	env = Envelope{}
	if err := json.Unmarshal(rec.Body.Bytes(), &env); err != nil || rec.Code != http.StatusBadRequest {
		t.Fatalf("negative wait_ms gave %d %q, want 400", rec.Code, rec.Body.String())
	}
}

// wakeGuard bounds one park→…→response wait against a REGRESSION hang, never against
// latency: the fixture's fake clock stands still, so a truly lost wakeup parks the
// handler forever and only this guard turns that into a named failure instead of the
// package-level timeout. It is sized far beyond any observed scheduling stall under
// -race + -coverpkg parallel load (whole-package runtimes are tens of seconds), so a
// green property can never lose to it and a red one is real.
const wakeGuard = 5 * time.Minute

func TestPausedFetchReturnsImmediatelyAndOccupiesNoSlot(t *testing.T) {
	t.Parallel()

	f := newFetchFixture(t, func(c *queue.ConsumerConfig) { c.Paused = true })

	done := f.do(t, `{"batch":4,"wait_ms":20000}`)
	select {
	case rec := <-done:
		out := decodeFetchResponse(t, rec)
		if out.HoldReason != string(store.HoldPaused) {
			t.Fatalf("hold_reason = %q, want paused", out.HoldReason)
		}
		if got := f.srv.waiters.Parked(); got != 0 {
			t.Errorf("Parked() = %d after a paused fetch, want 0 — parking cannot help here", got)
		}
	case <-clock.System{}.NewTimer(wakeGuard).C():
		t.Fatal("paused fetch parked instead of returning immediately")
	}
}

func TestEmptyFetchReturnsExactlyAtWaitBudget(t *testing.T) {
	t.Parallel()

	f := newFetchFixture(t, nil)

	done := f.do(t, `{"batch":8,"wait_ms":5000}`)
	f.waitForParked(1)

	// The handler is parked, so it has NOT returned early.
	select {
	case rec := <-done:
		t.Fatalf("fetch returned before its budget: %s", rec.Body.String())
	default:
	}

	// Crossing the deadline completes it with an empty array and a hold reason.
	f.clk.Advance(5001 * time.Millisecond)
	out := decodeFetchResponse(t, <-done)
	if out.Messages == nil {
		t.Error("messages must marshal as [] not null")
	}
	if len(out.Messages) != 0 {
		t.Fatalf("%d messages from an empty stream", len(out.Messages))
	}
	if out.HoldReason == "" {
		t.Error("an empty-at-deadline fetch must name its hold reason")
	}
}

func TestPublishWakesParkedFetchImmediately(t *testing.T) {
	t.Parallel()

	f := newFetchFixture(t, nil)

	done := f.do(t, `{"batch":8,"wait_ms":60000}`)
	f.waitForParked(1)

	// Commit a matching publish WITHOUT advancing the fake clock: only the wake path
	// can complete this fetch now.
	if _, err := f.st.Publish(context.Background(), store.PublishCmd{
		Stream: "orders",
		Req:    queue.PublishReq{Subject: "orders.eu.created", Body: []byte(`{}`)},
	}); err != nil {
		t.Fatalf("publish: %v", err)
	}

	select {
	case rec := <-done:
		out := decodeFetchResponse(t, rec)
		if len(out.Messages) != 1 {
			t.Fatalf("%d messages delivered on wake, want 1", len(out.Messages))
		}
		if out.HoldReason != "" {
			t.Errorf("hold_reason = %q on a delivery, want empty", out.HoldReason)
		}
	case <-clock.System{}.NewTimer(wakeGuard).C():
		t.Fatal("publish did not wake the parked fetch — lost wakeup")
	}
}

func TestFilterMissDoesNotWakeBeforeDeadline(t *testing.T) {
	t.Parallel()

	f := newFetchFixture(t, func(c *queue.ConsumerConfig) { c.Filters = []string{"orders.us.*"} })

	done := f.do(t, `{"batch":8,"wait_ms":10000}`)
	f.waitForParked(1)
	if _, err := f.st.Publish(context.Background(), store.PublishCmd{
		Stream: "orders",
		Req:    queue.PublishReq{Subject: "orders.eu.created", Body: []byte(`{}`)},
	}); err != nil {
		t.Fatalf("publish: %v", err)
	}
	select {
	case rec := <-done:
		t.Fatalf("non-matching publish woke the fetch early: %s", rec.Body.String())
	default:
	}
	// The message does not match this consumer's filters, so it is NOT claimable by
	// them: at the budget the fetch ends empty (the publish never woke it early).
	f.clk.Advance(10_001 * time.Millisecond)
	out := decodeFetchResponse(t, <-done)
	if len(out.Messages) != 0 {
		t.Fatalf("filter-mismatched message delivered: got %d", len(out.Messages))
	}
}

func TestShutdownDrainsParkedFetch(t *testing.T) {
	t.Parallel()

	f := newFetchFixture(t, nil)

	done := f.do(t, `{"batch":8,"wait_ms":60000}`)
	f.waitForParked(1)

	// Serve's ctx-done branch does exactly this, in this order.
	close(f.srv.closing)
	f.srv.waiters.ReleaseAll()

	out := decodeFetchResponse(t, <-done)
	if out.HoldReason != holdShuttingDown {
		t.Fatalf("hold_reason = %q, want shutting_down", out.HoldReason)
	}
	if len(out.Messages) != 0 {
		t.Errorf("%d messages on shutdown drain, want empty", len(out.Messages))
	}
}

// recT is a minimal testing.TB stand-in for pinning decodeFetchResponse's refusal
// paths: Fatalf is documented as noreturn, so the stub panics to keep that contract.
type recT struct {
	testing.TB
	failed bool
	msg    string
}

func (r *recT) Helper() {}

func (r *recT) Fatalf(format string, args ...any) {
	r.failed = true
	r.msg = fmt.Sprintf(format, args...)
	panic(r)
}

// TestDecodeFetchResponseRejectsErrorEnvelopes pins the decoder's honesty: an error
// envelope or any non-200 must be a NAMED failure, never a zero fetchResponse. This is
// the regression lock for the load flake where a blown submit window answered busy/503
// and the rapid-interleaving assertion read it as "0 messages after wake".
func TestDecodeFetchResponseRejectsErrorEnvelopes(t *testing.T) {
	t.Parallel()

	rec := func(code int, body string) *httptest.ResponseRecorder {
		r := httptest.NewRecorder()
		r.Code = code
		r.Body.WriteString(body)
		return r
	}

	// A well-formed empty fetch still decodes.
	out := decodeFetchResponse(t, rec(http.StatusOK,
		`{"messages":[],"hold_reason":"empty","retry_after_ms":0,"pending":0,"backlog":0,"batch":1,"max_bytes":8,"wait_ms":5}`))
	if out.HoldReason != "empty" || out.Messages == nil || out.Batch != 1 {
		t.Fatalf("well-formed fetch decoded as %+v, want the recorded shape", out)
	}

	for name, tc := range map[string]struct {
		code int
		body string
		want string
	}{
		"envelope on a 200": {
			http.StatusOK,
			`{"error":{"code":"busy","message":"writer did not accept the command within --writer-submit-timeout","next":[],"trace_id":"t"}}`,
			"error envelope",
		},
		"busy status": {
			http.StatusServiceUnavailable,
			`{"error":{"code":"busy","message":"writer did not accept the command within --writer-submit-timeout","next":[],"trace_id":"t"}}`,
			"HTTP 503",
		},
	} {
		rt := &recT{}
		func() {
			// Fatalf is documented noreturn and the stub honours that by panicking.
			// Contain the panic here, but re-panic anything that is not our own
			// recorded refusal.
			defer func() {
				if p := recover(); p != nil && !rt.failed {
					panic(p)
				}
			}()
			decodeFetchResponse(rt, rec(tc.code, tc.body))
		}()
		if !rt.failed {
			t.Fatalf("%s: decodeFetchResponse accepted a response it must refuse", name)
		}
		if !strings.Contains(rt.msg, tc.want) {
			t.Fatalf("%s: refusal %q does not name %q", name, rt.msg, tc.want)
		}
	}
}

// TestNoLostWakeupUnderRapidInterleaving drives repeated park→publish→deliver rounds;
// each round's fetch must complete via the wake while its clock stands still. The
// -race build is what makes this worth running.
func TestNoLostWakeupUnderRapidInterleaving(t *testing.T) {
	t.Parallel()

	f := newFetchFixture(t, nil)

	const rounds = 25
	for i := range rounds {
		subjectName := fmt.Sprintf("orders.rapid.%d", i)
		body := `{"batch":1,"wait_ms":30000}`
		done := f.do(t, body)
		f.waitForParked(1)

		if _, err := f.st.Publish(context.Background(), store.PublishCmd{
			Stream: "orders",
			Req:    queue.PublishReq{Subject: subjectName, Body: []byte(`{}`)},
		}); err != nil {
			t.Fatalf("round %d publish: %v", i, err)
		}
		select {
		case rec := <-done:
			out := decodeFetchResponse(t, rec)
			if len(out.Messages) != 1 {
				t.Fatalf("round %d: %d messages after wake, want exactly this round's message",
					i, len(out.Messages))
			}
			// Ack so the next round is not flow-blocked by max_ack_pending.
			token, tErr := queue.ParseToken(out.Messages[0].AckToken)
			if tErr != nil {
				t.Fatalf("round %d token %q: %v", i, out.Messages[0].AckToken, tErr)
			}
			if _, sErr := f.st.Settle(context.Background(), store.SettleCmd{Items: []store.SettleItem{
				{Token: token, Verb: queue.VerbAck},
			}}); sErr != nil {
				t.Fatalf("round %d ack: %v", i, sErr)
			}
		case <-clock.System{}.NewTimer(wakeGuard).C():
			t.Fatalf("round %d: wake lost", i)
		}
	}
}
