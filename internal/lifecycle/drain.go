// SPDX-License-Identifier: Apache-2.0

package lifecycle

import (
	"context"
	"time"
)

// DefaultDrainTimeout is A1's register value for the graceful-drain budget; the
// --drain-timeout flag (internal/cli) overrides it at runtime.
const DefaultDrainTimeout = 10 * time.Second

// APIServer is the HTTP surface seam the drain orchestrates — #14's server object,
// taken as an interface so this package never names internal/api's concrete server.
// ReleaseAll parks long-polls BEFORE Shutdown waits on handlers (G6); Close is the
// force path for a Shutdown that outlived its budget or failed outright.
type APIServer interface {
	ReleaseAll()
	Shutdown(ctx context.Context) error
	Close() error
}

// Health is the readiness seam (#15's HealthState, local until that lands): the flip
// happens first so load balancers learn we are going away before we stop accepting.
type Health interface {
	SetDraining()
}

// Notifier is the sd_notify seam; slice 5 ships the unixgram client. A notify
// failure is never fatal — telling systemd is not a durability property.
type Notifier interface {
	Set(fields ...string) error
	Close() error
}

// nopNotifier is the default when nothing is wired: running under a plain terminal
// must behave exactly like running under systemd minus the datagrams.
type nopNotifier struct{}

func (nopNotifier) Set(...string) error { return nil }
func (nopNotifier) Close() error        { return nil }

// drainStartEvent is logged when a drain begins. Local constant until #19's event
// vocabulary lands (same stubbing rule as server.start).
const drainStartEvent = "lifecycle.drain_start"

// drainForcedEvent is logged when the budget or a Shutdown error forces the close.
const drainForcedEvent = "lifecycle.drain_forced"

// drainNotifyEvent is logged at WARN when the STOPPING datagram cannot be delivered.
const drainNotifyEvent = "lifecycle.notify_error"

// DrainResult reports how one drain run ended.
type DrainResult struct {
	// Forced is true when the API had to be force-closed: the budget expired with
	// handlers still parked, or Shutdown returned an error. Forced is a fact about
	// the HTTP surface only — teardown and the final commit still run (G4).
	Forced bool
}

// Drain runs the §4.4 sequence verbatim over the injected seams:
//
//	readiness flips → STOPPING=1 → ReleaseAll → Shutdown(budget) →
//	[force-close if budget blown or Shutdown errored] → reverse stop → Stopped.
//
// It refuses outside Ready (the machine has not served anything worth draining) and
// is inert once a drain ran: a doubled signal must not double-stop components. The
// reverse stop is the final-commit path — the last component's Stop is where
// server.stop + clean_shutdown commit once the composition root wires the store.
func (m *Manager) Drain(ctx context.Context, reason string) DrainResult {
	m.drainMu.Lock()
	defer m.drainMu.Unlock()

	if m.drained {
		return m.lastDrain
	}
	if !m.advance(Ready, Draining) {
		return DrainResult{} // refused outside Ready; nothing happened
	}
	m.logger.Info(drainStartEvent, "reason", reason, "budget", m.drainTimeout)

	// Readiness first: a load balancer must learn before we stop accepting.
	if m.health != nil {
		m.health.SetDraining()
	}
	// A failed notify is never fatal — telling systemd is not a durability
	// property — but the one WARN is honest about it.
	if err := m.notify.Set("STOPPING=1"); err != nil {
		m.logger.Warn(drainNotifyEvent, "err", err)
	}

	forced := m.shutdownAPI(ctx)
	if forced {
		m.logger.Warn(drainForcedEvent, "reason", reason)
	}

	// Reverse stop = the final-commit path. Runs even when forced (G4): expiry is
	// a budget on in-flight handlers, never an exit from the shutdown sequence.
	m.teardownAll(ctx)
	m.advance(Draining, Stopped)

	res := DrainResult{Forced: forced}
	m.drained = true
	m.lastDrain = res
	return res
}

// teardownAll stops every declared component in exact reverse and marks the manager
// stopped. A drain is only reachable from Ready, and Ready means Start completed, so
// this is the started prefix by contract; going by declaration keeps the store's
// final-commit Stop last even if start bookkeeping ever disagrees with the state.
func (m *Manager) teardownAll(ctx context.Context) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.stopped {
		return
	}
	for i := len(m.comps) - 1; i >= 0; i-- {
		m.stopComponent(ctx, m.comps[i])
	}
	m.started = 0
	m.stopped = true
}

// shutdownAPI releases long-polls, then waits for Shutdown under the drain budget.
// It reports whether the API ended up force-closed. The budget is enforced through
// the Clock seam so a stepped fake clock neither shortens nor hangs it.
func (m *Manager) shutdownAPI(ctx context.Context) bool {
	if m.api == nil {
		return false
	}
	m.api.ReleaseAll()

	sctx, cancel := context.WithCancel(ctx)
	defer cancel()

	timer := m.clock.NewTimer(m.drainTimeout)
	defer func() { _ = timer.Stop() }()

	done := make(chan error, 1)
	go func() { done <- m.api.Shutdown(sctx) }()

	var forced bool
	select {
	case err := <-done:
		forced = err != nil
	case <-timer.C():
		// Budget blown with handlers still parked: cancel them out and wait for
		// Shutdown to observe it, so no goroutine leaks into teardown.
		forced = true
		cancel()
		<-done
	case <-m.escCh:
		// A second TERM/INT: the operator said stop waiting. Same contract as the
		// budget path — force close, but teardown and the final commit still run.
		forced = true
		cancel()
		<-done
	}
	if !forced {
		return false
	}
	if err := m.api.Close(); err != nil {
		m.logger.Warn(drainForcedEvent, "err", err)
	}
	return true
}
