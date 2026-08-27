// SPDX-License-Identifier: Apache-2.0

package api

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/a-holm/messq/internal/clock"
	"github.com/a-holm/messq/internal/queue"
	"github.com/a-holm/messq/internal/store"
)

// Issue #15 §6 / G8: POST …/consumers is a DECLARATIVE full-document upsert — an
// identical re-POST is 200 changed:false with no consumer.update event, a DIFFERENT
// config on a taken name is 409 consumer_exists whose next is the equivalent PATCH.
// PATCH is sparse; filters need ?allow_filter_change=1 (a silent filter rewrite strands
// in-flight rows against the old filters — #9 claims them stale anyway, so the honest
// answer is permission + a seek back-fill naming). Pause/resume are idempotent.

func newUpsertServer(t *testing.T) (*Server, *store.Store) {
	t.Helper()
	clk := clock.NewFake(time.UnixMilli(1_700_000_000_000))
	st := openTestStore(t, clk, store.DurabilityFull)
	mustCreateStream(t, st, queue.DefaultConfig("orders"))
	srv := New(Config{Store: st, Clock: clk, Logger: discardLogger()})
	return srv, st
}

func postConsumerBody(t *testing.T, h http.Handler, body string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequestWithContext(context.Background(), http.MethodPost,
		"/v1/streams/orders/consumers", strings.NewReader(body))
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec
}

func decodeEnv(t *testing.T, rec *httptest.ResponseRecorder) Envelope {
	t.Helper()
	var env Envelope
	if err := json.Unmarshal(rec.Body.Bytes(), &env); err != nil {
		t.Fatalf("not an envelope (%v): %s", err, rec.Body.String())
	}
	return env
}

func TestConsumerUpsertIdenticalTwiceIsChangedFalseNoEvent(t *testing.T) {
	t.Parallel()

	srv, st := newUpsertServer(t)
	body := `{"name":"worker","start":"first"}`
	handler := srv.Handler()

	first := postConsumerBody(t, handler, body)
	if first.Code != http.StatusCreated {
		t.Fatalf("first POST status = %d, want 201 (body %s)", first.Code, first.Body.String())
	}

	before := eventCount(t, st, "consumer.update")
	second := postConsumerBody(t, handler, body)
	if second.Code != http.StatusOK {
		t.Fatalf("second POST status = %d, want 200 (body %s)", second.Code, second.Body.String())
	}
	var resp struct {
		Changed bool `json:"changed"`
	}
	if err := json.Unmarshal(second.Body.Bytes(), &resp); err != nil {
		t.Fatalf("not JSON (%v): %s", err, second.Body.String())
	}
	if resp.Changed {
		t.Error("identical upsert reported changed:true")
	}
	if after := eventCount(t, st, "consumer.update"); after != before {
		t.Errorf("no-churn violated: consumer.update delta = %d", after-before)
	}
}

func TestConsumerUpsertDifferentConfigIsConflict(t *testing.T) {
	t.Parallel()

	srv, st := newUpsertServer(t)
	body := `{"name":"worker","start":"first","ack_wait_ms":30000}`
	if rec := postConsumerBody(t, srv.Handler(), body); rec.Code != http.StatusCreated {
		t.Fatalf("first POST status = %d (body %s)", rec.Code, rec.Body.String())
	}

	diff := postConsumerBody(t, srv.Handler(), `{"name":"worker","start":"first","ack_wait_ms":60000}`)
	if diff.Code != http.StatusConflict {
		t.Fatalf("status = %d, want 409 (body %s)", diff.Code, diff.Body.String())
	}
	env := decodeEnv(t, diff)
	if env.Error.Code != CodeConsumerExists {
		t.Errorf("code = %q, want consumer_exists", env.Error.Code)
	}
	next := strings.Join(env.Error.Next, "\n")
	if !strings.Contains(next, "consumer edit") && !strings.Contains(next, "PATCH") {
		t.Errorf("next should point at the equivalent PATCH: %q", next)
	}
	info, err := st.GetConsumer(context.Background(), "orders", "worker")
	if err != nil {
		t.Fatalf("get consumer: %v", err)
	}
	if info.AckWaitMS != 30000 {
		t.Errorf("stored ack_wait_ms = %d, want the original 30000", info.AckWaitMS)
	}
}

func TestConsumerOmittedFieldsTakeDefaultsOnCreate(t *testing.T) {
	t.Parallel()

	srv, st := newUpsertServer(t)
	rec := postConsumerBody(t, srv.Handler(), `{"name":"bare","start":"seq:1"}`)
	if rec.Code != http.StatusCreated {
		t.Fatalf("status = %d, want 201 (body %s)", rec.Code, rec.Body.String())
	}
	info, err := st.GetConsumer(context.Background(), "orders", "bare")
	if err != nil {
		t.Fatalf("get consumer: %v", err)
	}
	def := queue.DefaultConsumerConfig("bare")
	if info.AckWaitMS != def.AckWait.Milliseconds() || info.MaxAckPending != def.MaxAckPending {
		t.Errorf("omitted fields did not take defaults: ack_wait=%d max_ack_pending=%d, want %d/%d",
			info.AckWaitMS, info.MaxAckPending, def.AckWait.Milliseconds(), def.MaxAckPending)
	}
}

func TestConsumerPatchFiltersNeedAllowFilterChange(t *testing.T) {
	t.Parallel()

	srv, st := newUpsertServer(t)
	if rec := postConsumerBody(t, srv.Handler(),
		`{"name":"worker","start":"first","filters":["orders.>"]}`); rec.Code != http.StatusCreated {
		t.Fatalf("create status = %d (body %s)", rec.Code, rec.Body.String())
	}
	handler := srv.Handler()

	patch := httptest.NewRequestWithContext(context.Background(), http.MethodPatch,
		"/v1/streams/orders/consumers/worker", strings.NewReader(`{"filters":["other.>"]}`))
	prec := httptest.NewRecorder()
	handler.ServeHTTP(prec, patch)

	if prec.Code != http.StatusConflict {
		t.Fatalf("filter change without allow param: status = %d, want 409 (body %s)",
			prec.Code, prec.Body.String())
	}
	env := decodeEnv(t, prec)
	if env.Error.Code != CodeWouldChangeFilters {
		t.Errorf("code = %q, want would_change_filters", env.Error.Code)
	}
	next := strings.Join(env.Error.Next, "\n")
	if !strings.Contains(next, "allow_filter_change=1") && !strings.Contains(next, "seek") {
		t.Errorf("next should name allow_filter_change or seek back-fill: %q", next)
	}

	allowed := httptest.NewRequestWithContext(context.Background(), http.MethodPatch,
		"/v1/streams/orders/consumers/worker?allow_filter_change=1",
		strings.NewReader(`{"filters":["other.>"]}`))
	arec := httptest.NewRecorder()
	handler.ServeHTTP(arec, allowed)
	if arec.Code != http.StatusOK {
		t.Fatalf("with ?allow_filter_change=1: status = %d, want 200 (body %s)", arec.Code, arec.Body.String())
	}
	info, err := st.GetConsumer(context.Background(), "orders", "worker")
	if err != nil || len(info.Filters) != 1 || info.Filters[0] != "other.>" {
		t.Errorf("filters not applied with permission: %+v (%v)", info.Filters, err)
	}
}

func TestPauseResumeIdempotent(t *testing.T) {
	t.Parallel()

	srv, _ := newUpsertServer(t)
	handler := srv.Handler()
	if rec := postConsumerBody(t, handler, `{"name":"p","start":"first"}`); rec.Code != http.StatusCreated {
		t.Fatalf("create status = %d (body %s)", rec.Code, rec.Body.String())
	}

	resumeFirst := doJSON(t, handler, http.MethodPost, "/v1/streams/orders/consumers/p/resume", "")
	if resumeFirst.Code != http.StatusOK {
		t.Fatalf("resume on running consumer: status = %d, want 200", resumeFirst.Code)
	}
	var m map[string]any
	if err := json.Unmarshal(resumeFirst.Body.Bytes(), &m); err != nil {
		t.Fatalf("resume body not JSON: %v (%s)", err, resumeFirst.Body.String())
	}
	if changed, ok := m["changed"].(bool); !ok || changed {
		t.Errorf("resume of un-paused consumer must be idempotent changed:false: %v", m)
	}

	pauseRec := doJSON(t, handler, http.MethodPost, "/v1/streams/orders/consumers/p/pause", "")
	if pauseRec.Code != http.StatusOK {
		t.Fatalf("pause status = %d, want 200 (body %s)", pauseRec.Code, pauseRec.Body.String())
	}
	var pm map[string]any
	if err := json.Unmarshal(pauseRec.Body.Bytes(), &pm); err != nil {
		t.Fatalf("pause body not JSON: %v", err)
	}
	if pm["paused"] != true || pm["changed"] != true {
		t.Errorf("pause response wrong: %v", pm)
	}

	pause2 := doJSON(t, handler, http.MethodPost, "/v1/streams/orders/consumers/p/pause", "")
	var pm2 map[string]any
	if err := json.Unmarshal(pause2.Body.Bytes(), &pm2); err != nil || pm2["changed"] != false {
		t.Errorf("second pause must be idempotent: %v (%v)", pm2, err)
	}
}

// eventCount counts events rows matching one vocabulary name through the store read
// path, so no-churn assertions check reality rather than response fields.
func eventCount(t *testing.T, st *store.Store, name string) int64 {
	t.Helper()
	page, err := st.Events(context.Background(), store.EventFilter{Events: []string{name}})
	if err != nil {
		t.Fatalf("read events %s: %v", name, err)
	}
	return int64(len(page.Events))
}
