// SPDX-License-Identifier: Apache-2.0

package api

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/a-holm/messq/internal/clock"
	"github.com/a-holm/messq/internal/store"
)

// Issue #15 §2: POST /v1/admin/log-level is the one runtime-mutable setting that is
// not a signal (D8), wired through a LevelSetter seam (#19 backs it with the process
// slog.LevelVar); /metrics is a mount point, not an implementation — nil means
// 503 not_implemented, NEVER 404 (a 404 claims the endpoint does not exist).

// fakeLevelSetter records every SetLevel so the test can see what actually landed.
type fakeLevelSetter struct {
	mu    sync.Mutex
	level slog.Level
	calls []slog.Level
}

func (f *fakeLevelSetter) Level() slog.Level {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.level
}

func (f *fakeLevelSetter) SetLevel(l slog.Level) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls = append(f.calls, l)
	f.level = l
	return nil
}

func newAdminServer(t *testing.T, ls *fakeLevelSetter) (*Server, *store.Store) {
	t.Helper()
	clk := clock.NewFake(time.UnixMilli(1_700_000_000_000))
	st := openTestStore(t, clk, store.DurabilityFull)
	srv := New(Config{Store: st, Clock: clk, Logger: discardLogger(), LevelSetter: ls})
	return srv, st
}

func postJSON(t *testing.T, h http.Handler, path, body string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequestWithContext(context.Background(), http.MethodPost,
		path, strings.NewReader(body))
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec
}

// adminEventCount counts events rows named admin.action.
func adminEventCount(t *testing.T, st *store.Store) int64 {
	t.Helper()
	page, err := st.Events(context.Background(), store.EventFilter{Events: []string{"admin.action"}})
	if err != nil {
		t.Fatalf("read events: %v", err)
	}
	return int64(len(page.Events))
}

func TestLogLevelUnknownLevelIsBadRequest(t *testing.T) {
	t.Parallel()

	srv, _ := newAdminServer(t, &fakeLevelSetter{level: slog.LevelInfo})
	rec := postJSON(t, srv.Handler(), "/v1/admin/log-level", `{"level":"trace"}`)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400 (body %s)", rec.Code, rec.Body.String())
	}
	var env Envelope
	if err := json.Unmarshal(rec.Body.Bytes(), &env); err != nil {
		t.Fatalf("not an envelope (%v): %s", err, rec.Body.String())
	}
	if env.Error.Code != CodeBadRequest {
		t.Errorf("code = %q, want bad_request", env.Error.Code)
	}
	for _, want := range []string{"debug", "info", "warn", "error"} {
		if !strings.Contains(env.Error.Message, want) {
			t.Errorf("message does not name legal level %q: %q", want, env.Error.Message)
		}
	}
}

func TestLogLevelChangeEchoesPreviousAndWritesOneEvent(t *testing.T) {
	t.Parallel()

	ls := &fakeLevelSetter{level: slog.LevelInfo}
	srv, st := newAdminServer(t, ls)
	before := adminEventCount(t, st)

	rec := postJSON(t, srv.Handler(), "/v1/admin/log-level", `{"level":"debug"}`)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200 (body %s)", rec.Code, rec.Body.String())
	}
	var body struct {
		Level    string `json:"level"`
		Previous string `json:"previous"`
		Changed  bool   `json:"changed"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("body not JSON (%v): %s", err, rec.Body.String())
	}
	if body.Level != "debug" || body.Previous != "info" || !body.Changed {
		t.Errorf("echo = %+v, want {debug info true}", body)
	}
	if got := ls.Level(); got != slog.LevelDebug {
		t.Errorf("setter level = %v, want debug", got)
	}
	if after := adminEventCount(t, st); after != before+1 {
		t.Errorf("admin.action events delta = %d, want exactly 1", after-before)
	}
}

func TestLogLevelNoOpWritesNoEvent(t *testing.T) {
	t.Parallel()

	ls := &fakeLevelSetter{level: slog.LevelWarn}
	srv, st := newAdminServer(t, ls)
	before := adminEventCount(t, st)

	rec := postJSON(t, srv.Handler(), "/v1/admin/log-level", `{"level":"warn"}`)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	var body struct {
		Changed bool `json:"changed"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("body not JSON: %v", err)
	}
	if body.Changed {
		t.Error("no-op reported changed:true")
	}
	if after := adminEventCount(t, st); after != before {
		t.Errorf("no-op wrote %d events, want 0", after-before)
	}
}

func TestMetricsMountNilIsNotImplementedNever404(t *testing.T) {
	t.Parallel()

	srv, _ := newAdminServer(t, &fakeLevelSetter{level: slog.LevelInfo})
	rec := get(t, srv.Handler(), "/metrics")

	if rec.Code == http.StatusNotFound {
		t.Fatal("/metrics answered 404 — the mount point must exist while #21 has not injected a handler")
	}
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503", rec.Code)
	}
	var env Envelope
	if err := json.Unmarshal(rec.Body.Bytes(), &env); err != nil {
		t.Fatalf("not an envelope (%v): %s", err, rec.Body.String())
	}
	if env.Error.Code != CodeNotImplemented {
		t.Errorf("code = %q, want not_implemented", env.Error.Code)
	}
}

func TestMetricsMountDelegatesToInjectedHandler(t *testing.T) {
	t.Parallel()

	srv, _ := newAdminServer(t, &fakeLevelSetter{level: slog.LevelInfo})
	srv.cfg.Metrics = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-Messq-Metrics", "injected")
		w.WriteHeader(http.StatusOK)
	})
	rec := get(t, srv.Handler(), "/metrics")
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want the injected handler's 200", rec.Code)
	}
	if rec.Header().Get("X-Messq-Metrics") != "injected" {
		t.Error("the request did not reach the injected handler")
	}
}
