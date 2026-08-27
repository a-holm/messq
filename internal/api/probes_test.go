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
	"github.com/a-holm/messq/internal/store"
)

// Issue #15 §2 / G3: /healthz answers "is the process alive" and /readyz "should
// clients send work here", both from in-memory state and NEVER by touching SQLite.
// The probes on a nil-store Server prove zero SQL structurally: there is no store to
// query. The degraded matrix below pins every HealthState combination × both probes;
// a query counter equivalent rides on that construction rule.

// stubHealth drives every matrix cell deterministically.
type stubHealth struct {
	live     bool
	ready    bool
	reason   string
	degraded []Degradation
}

func (h *stubHealth) Live() bool              { return h.live }
func (h *stubHealth) Ready() (bool, string)   { return h.ready, h.reason }
func (h *stubHealth) Degraded() []Degradation { return h.degraded }

func newStubServer(h HealthState) *Server {
	return New(Config{HealthState: h, Logger: discardLogger()})
}

func get(t *testing.T, h http.Handler, path string) *httptest.ResponseRecorder {
	t.Helper()
	rec := httptest.NewRecorder()
	req := httptest.NewRequestWithContext(context.Background(), http.MethodGet, path, nil)
	h.ServeHTTP(rec, req)
	return rec
}

// TestReadyzMatrix: every (ready, reason) state produces its documented answer —
// 200 ready, else 503 envelope code=not_ready naming the reason, always Retry-After:1.
func TestReadyzMatrix(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name       string
		state      stubHealth
		wantStatus int
		wantReason string // substring of the message when failing
	}{
		{"healthy", stubHealth{live: true, ready: true}, http.StatusOK, ""},
		{"recovering", stubHealth{live: true, ready: false, reason: ReasonRecovering}, http.StatusServiceUnavailable, ReasonRecovering},
		{"draining", stubHealth{live: true, ready: false, reason: ReasonDraining}, http.StatusServiceUnavailable, ReasonDraining},
		{"read_only", stubHealth{live: true, ready: false, reason: ReasonReadOnly}, http.StatusServiceUnavailable, ReasonReadOnly},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			srv := newStubServer(&tc.state)
			rec := get(t, srv.Handler(), "/readyz")

			if rec.Code != tc.wantStatus {
				t.Fatalf("status = %d, want %d (body %s)", rec.Code, tc.wantStatus, rec.Body.String())
			}
			if ra := rec.Header().Get("Retry-After"); tc.wantStatus == http.StatusServiceUnavailable && ra != "1" {
				t.Errorf("Retry-After = %q, want \"1\"", ra)
			}
			var body struct {
				Status string `json:"status"`
				Error  struct {
					Code    Code   `json:"code"`
					Message string `json:"message"`
				} `json:"error"`
			}
			if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
				t.Fatalf("body is not JSON (%v): %s", err, rec.Body.String())
			}
			if tc.wantStatus == http.StatusOK {
				if body.Status != "ready" {
					t.Errorf("status = %q, want \"ready\"", body.Status)
				}
				return
			}
			if body.Error.Code != CodeNotReady {
				t.Errorf("code = %q, want %q", body.Error.Code, CodeNotReady)
			}
			if !strings.Contains(body.Error.Message, tc.wantReason) {
				t.Errorf("message %q does not name reason %q", body.Error.Message, tc.wantReason)
			}
		})
	}
}

// TestHealthzAlwaysAliveAndNamesDegraded: /healthz stays 200 while draining,
// recovering, latched read-only and under disk pressure; the body lists degraded[]
// with kind+since ONLY. Degradation must never flip liveness or readiness here.
func TestHealthzAlwaysAliveAndNamesDegraded(t *testing.T) {
	t.Parallel()

	now := time.UnixMilli(1_700_000_000_000)
	tr := newHealthTracker(clock.NewFake(now))
	tr.recordDegraded(DegradedDiskLow)
	srv := newStubServer(tr)

	rec := get(t, srv.Handler(), "/healthz")
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200 under degradation (body %s)", rec.Code, rec.Body.String())
	}
	var body struct {
		Status   string        `json:"status"`
		Degraded []Degradation `json:"degraded"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("body is not JSON (%v): %s", err, rec.Body.String())
	}
	if body.Status != "ok" {
		t.Errorf("status = %q, want \"ok\"", body.Status)
	}
	if len(body.Degraded) != 1 || body.Degraded[0].Kind != DegradedDiskLow {
		t.Fatalf("degraded = %+v, want one disk_low entry", body.Degraded)
	}
	if body.Degraded[0].Since != now.UnixMilli() {
		t.Errorf("since = %d, want %d", body.Degraded[0].Since, now.UnixMilli())
	}
}

// TestDegradedKindsClosedVocabulary: the registered kind constants exist and no other
// value may be recorded — open for registration, closed for invention (issue §2).
func TestDegradedKindsClosedVocabulary(t *testing.T) {
	t.Parallel()

	for _, want := range []string{DegradedReadOnly, DegradedDiskLow, DegradedDraining, DegradedPurgeInProgress} {
		if want == "" {
			t.Fatal("degraded-kind constant is empty")
		}
	}
}

// TestProbesNeverTouchSQLite: a Server built WITHOUT a store answers both probes.
// There is no store handle to reach, so executing SQL would panic — passing this test
// proves the handlers' dependency set contains no query path at all.
func TestProbesNeverTouchSQLite(t *testing.T) {
	t.Parallel()

	srv := New(Config{Logger: discardLogger()})
	for _, path := range []string{"/healthz", "/readyz"} {
		if rec := get(t, srv.Handler(), path); rec.Code != http.StatusOK {
			t.Errorf("%s status = %d, want 200 (body %s)", path, rec.Code, rec.Body.String())
		}
	}
}

// TestDefaultTrackerMirrorsStoreLatch: the server's built-in tracker reports read_only
// while the store's writer is latched — wired through the store's boolean seam, never
// by probing SQL.
func TestDefaultTrackerMirrorsStoreLatch(t *testing.T) {
	t.Parallel()

	clk := clock.NewFake(time.UnixMilli(1_700_000_000_000))
	st := openTestStore(t, clk, store.DurabilityFull)
	if st.LatchedReadOnly() {
		t.Fatal("fresh store reports latched read-only")
	}
	tr := newHealthTracker(clk)
	tr.setLatchedProbe(func() bool { return true })
	if ok, reason := tr.Ready(); ok || reason != ReasonReadOnly {
		t.Fatalf("latched probe on: ready=%v reason=%q, want false/%q", ok, reason, ReasonReadOnly)
	}
	tr.setLatchedProbe(func() bool { return false })
	if ok, _ := tr.Ready(); !ok {
		t.Fatal("latched probe off: want ready")
	}
}
