// SPDX-License-Identifier: Apache-2.0

package lifecycle

import (
	"context"
	"sync/atomic"
	"time"
)

// HealthState is the daemon's liveness/readiness truth, kept in atomics so HTTP
// handlers read it lock-free. It is the local shape of #15's api.HealthState seam
// (that package has no such type yet); when #15 lands, this struct either satisfies
// it directly or is adapted at the composition root.
type HealthState struct {
	draining atomic.Bool // /readyz flips to 503 "draining" on the §4.4 readiness flip
	readOnly atomic.Bool // D4: storage latched read-only; reads answer, mutations 503
}

// NewHealth returns a serving-fresh state.
func NewHealth() *HealthState { return &HealthState{} }

// SetDraining implements [Health]: called first in Drain, before anything stops.
func (h *HealthState) SetDraining() { h.draining.Store(true) }

// SetReadOnly latches D4's storage.fatal view: /healthz stays 200, /readyz reports
// read_only, and no mutation can be accepted again this process lifetime.
func (h *HealthState) SetReadOnly() { h.readOnly.Store(true) }

// Ready answers whether new work may be admitted: not draining and not read-only.
func (h *HealthState) Ready() bool { return !h.draining.Load() && !h.readOnly.Load() }

// Status renders the /readyz body word: serving | draining | read_only.
func (h *HealthState) Status() string {
	switch {
	case h.readOnly.Load():
		return "read_only"
	case h.draining.Load():
		return "draining"
	default:
		return "serving"
	}
}

// fatalEvent is logged when the writer's fatal latch fires. Local constant until
// #19's event vocabulary lands (same stubbing rule as server.start).
const fatalEvent = "lifecycle.storage_fatal"

// SuperviseFatal watches the writer's fatal channel (#6's Writer.Fatal(), injected
// as a bare channel so this package never reaches internal/writer, which would drag
// internal/store past layers.sh). On a fatal:
//
//	state absorbs into Fatal (from Ready or Draining), health latches read-only,
//	and the --fatal-drain window elapses on the Clock seam so /healthz, peek and
//	trace keep answering for operators — then it returns and the composition root
//	exits with exitcode.IOERR (74).
//
// It deliberately performs NO component teardown: teardown ends in the store's
// final commit + clean_shutdown marker, and a disk that EIO'd must not be trusted
// (G10). The missing marker makes the next start run quick_check, which is exactly
// the fail-safe S11.3 asks for. Coordination with a drain already mid-flight —
// whose own advance(Draining→Stopped) now fails against the absorbed Fatal state —
// belongs to the composition-root slice.
func (m *Manager) SuperviseFatal(ctx context.Context, fatal <-chan string, window time.Duration) bool {
	select {
	case <-ctx.Done():
		return false
	case cause := <-fatal:
		m.logger.Warn(fatalEvent, "cause", cause)
		if m.health != nil {
			m.health.SetReadOnly()
		}
		// Fatal is absorbing from both Ready and Draining; whichever CAS wins,
		// a concurrent drain can no longer reach Stopped.
		switch m.State() {
		case Ready:
			m.advance(Ready, Fatal)
		case Draining:
			m.advance(Draining, Fatal)
		case Starting, Recovering, Stopped, Fatal:
			// Nothing serving to absorb; leave the machine where it is.
		}

		timer := m.clock.NewTimer(window)
		defer func() { _ = timer.Stop() }()
		select {
		case <-timer.C():
		case <-ctx.Done():
		}
		return true
	}
}
