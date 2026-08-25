// SPDX-License-Identifier: Apache-2.0

package lifecycle

import (
	"context"
	"os"
	"sync/atomic"
	"syscall"
)

// WatchedSignals returns exactly the signals the daemon intercepts. SIGQUIT is
// deliberately absent: not registering it preserves the runtime's dump-goroutines
// behaviour, because the kernel never tells the process about a signal it does not
// forward. The composition root passes this set to signal.Notify.
func WatchedSignals() []os.Signal {
	return []os.Signal{syscall.SIGTERM, syscall.SIGINT, syscall.SIGHUP}
}

const (
	// signalEscalateEvent is logged at WARN on the second TERM/INT: the operator
	// has said "stop waiting" and the drain budget is cut short.
	signalEscalateEvent = "lifecycle.signal_escalate"

	// signalExitEvent is logged before the third signal's hard os.Exit(1).
	signalExitEvent = "lifecycle.signal_exit"

	// sighupIgnoredEvent is logged when SIGHUP arrives outside READY (mid-drain
	// or terminal states): a reload can never resurrect a draining process.
	sighupIgnoredEvent = "lifecycle.sighup_ignored"
)

// ServeSignals consumes sigs until ctx ends, applying the signal→action table:
//
//	first  TERM/INT → Drain;  second → escalate (break out of the drain wait);
//	third           → exit(1); SIGHUP at Ready → onSighup, otherwise ignored.
//
// sigs is injected so tests drive the loop without a real process; the
// composition root feeds it from signal.Notify(WatchedSignals()...).
func (m *Manager) ServeSignals(ctx context.Context, sigs <-chan os.Signal, onSighup func()) {
	var terms atomic.Int32
	for {
		select {
		case <-ctx.Done():
			return
		case sig := <-sigs:
			switch sig {
			case syscall.SIGTERM, syscall.SIGINT:
				// One escalation bucket: TERM and INT mean the same thing to an
				// impatient operator.
				switch terms.Add(1) {
				case 1:
					go func() { _ = m.Drain(ctx, "SIGTERM") }()
				case 2:
					m.logger.Warn(signalEscalateEvent)
					m.escalate()
				default:
					m.logger.Warn(signalExitEvent, "signal", sig.String())
					if m.exit != nil {
						m.exit(1)
					}
					return // mirrors process death; unreachable under a real exit
				}
			case syscall.SIGHUP:
				if m.State() != Ready {
					m.logger.Warn(sighupIgnoredEvent)
					continue
				}
				if onSighup != nil {
					onSighup()
				}
			default:
				// Unreachable with WatchedSignals registration; ignore rather
				// than act on a signal we did not ask for.
				continue
			}
		}
	}
}

// escalate force-breaks a running drain's API wait. Idempotent: every signal past
// the second lands either here as a no-op or in the exit branch above.
func (m *Manager) escalate() {
	m.escOnce.Do(func() { close(m.escCh) })
}
