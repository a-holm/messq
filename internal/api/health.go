// SPDX-License-Identifier: Apache-2.0

package api

import (
	"log/slog"
	"sync"
	"time"

	"github.com/a-holm/messq/internal/clock"
)

// The probe layer (issue #15 §2, PLAN §4.5): /healthz answers "is the process alive"
// and /readyz "should clients send work here", both from IN-MEMORY state and never by
// touching SQLite. A probe that runs a query is a probe that times out exactly when
// the queue is busiest — and a readiness flap under load gets the node killed by its
// supervisor, which is how a latency incident becomes an outage.
//
// Adjudicated semantics this file must not drift from:
//   - PLAN §4.5: /readyz is NOT tied to disk pressure (don't let the orchestrator kill
//     the node that is draining itself); disk pressure only feeds degraded[].
//   - §4.3/D4: the read-only latch DOES flip /readyz — a latched process is on its way
//     out; a dead disk and disk pressure get different answers.

// Degradation is one active degradation report: kind plus the moment it started.
// It carries kind and since ONLY — no version, count or path — so the two unauthenticated
// probes stay useless for fingerprinting (issue #15 §2).
type Degradation struct {
	Kind  string `json:"kind"`
	Since int64  `json:"since"` // unix ms of the moment the kind was first recorded
}

// The registered degradation kinds. Open for registration, closed for invention:
// every value here is a named constant with an owning issue, and the issue's doc text
// lists exactly these four.
const (
	DegradedReadOnly        = "read_only"         // #6 fsyncgate writer latch
	DegradedDiskLow         = "disk_low"          // #27 --min-free-bytes pressure
	DegradedDraining        = "draining"          // #17 graceful shutdown state
	DegradedPurgeInProgress = "purge_in_progress" // #15 chunked purge running
)

// The distinct /readyz refusal reasons (issue acceptance: 503 with a DISTINCT status
// string per failure class).
const (
	ReasonRecovering = "recovering"
	ReasonDraining   = "draining"
	ReasonReadOnly   = "read_only"
)

// HealthState is the seam between the probes and whatever owns lifecycle truth.
// #17's state machine and #27's disk watcher supply their views through it or through
// the default tracker below; tests stub it directly. Live/Degraded are defined by the
// issue sketch verbatim.
type HealthState interface {
	Live() bool            // process can serve HTTP
	Ready() (bool, string) // false + reason: recovering | draining | read_only
	Degraded() []Degradation
}

// healthTracker is the default HealthState: degraded kinds recorded with timestamps,
// an optional draining flag for #17, and a latched-read-only probe supplied at wiring
// time so readiness mirrors the store's fsyncgate without any SQL.
type healthTracker struct {
	clk      clock.Clock
	logger   *slog.Logger
	mu       sync.Mutex
	degraded map[string]time.Time
	draining bool

	latchedProbe func() bool // nil-safe: treated as false
}

func newHealthTracker(clk clock.Clock) *healthTracker {
	return &healthTracker{
		clk:      clk,
		logger:   slog.Default(),
		degraded: make(map[string]time.Time),
	}
}

func (t *healthTracker) setLatchedProbe(fn func() bool) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.latchedProbe = fn
}

// recordDegraded starts (or keeps) one kind's since-timestamp.
func (t *healthTracker) recordDegraded(kind string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if _, ok := t.degraded[kind]; !ok {
		t.degraded[kind] = t.clk.Now()
	}
}

// clearDegraded ends one kind's window.
func (t *healthTracker) clearDegraded(kind string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	delete(t.degraded, kind)
}

// Live reports whether the HTTP surface can answer at all. Nothing recorded here can
// stop the process from answering — even read-only mode serves reads.
func (*healthTracker) Live() bool { return true }

// Ready answers whether clients should send work: not draining, not latched read-only.
// Disk pressure deliberately does NOT enter this answer (§4.5).
func (t *healthTracker) Ready() (bool, string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.draining {
		return false, ReasonDraining
	}
	if t.latchedProbe != nil && t.latchedProbe() {
		return false, ReasonReadOnly
	}
	return true, ""
}

// Degraded returns the active kinds ordered deterministically (kind asc), each carrying
// the earliest time it was observed still-active.
func (t *healthTracker) Degraded() []Degradation {
	t.mu.Lock()
	defer t.mu.Unlock()
	out := make([]Degradation, 0, len(t.degraded))
	for kind, since := range t.degraded {
		out = append(out, Degradation{Kind: kind, Since: since.UnixMilli()})
	}
	for i := 1; i < len(out); i++ { // insertion sort over ≤4 kinds: stable and allocation-free
		for j := i; j > 0 && out[j].Kind < out[j-1].Kind; j-- {
			out[j], out[j-1] = out[j-1], out[j]
		}
	}
	return out
}
