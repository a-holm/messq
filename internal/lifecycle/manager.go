// SPDX-License-Identifier: Apache-2.0

// Package lifecycle is the daemon's process state machine and component supervisor
// (issue #17, PLAN §3.2's signal/lifecycle goroutine): ordered start-up, exact-reverse
// shutdown, and the one-way process states every other slice (drain, signals, reload,
// sd_notify, health) hangs off.
//
// Components are injected interfaces, so the manager performs no I/O itself: the
// composition root in internal/cli wires the concrete store, writer and API components
// in. The package never imports internal/store or internal/queue concretely (scripts/
// layers.sh enforces this); when the health seam arrives it takes api.HealthState as an
// interface type only.
package lifecycle

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/a-holm/messq/internal/clock"
)

// State is one position in the daemon's one-way process machine (issue #17 §2):
//
//	Starting → Recovering → Ready ⇄ Draining → Stopped
//	                       ↘ Fatal ↙ (from Ready or Draining)
//
// Every move is monotone except the Ready→Ready reload self-transition; Fatal and
// Stopped are absorbing. A CompareAndSwap guards each move, so a SIGHUP racing a
// SIGTERM cannot resurrect a draining process.
type State uint8

const (
	// Starting holds before any component has been touched.
	Starting State = iota
	// Recovering holds while store.Open replays the WAL, runs quick_check and reclaims
	// leases; no listener exists yet (PLAN §4.4: recovery precedes bind).
	Recovering
	// Ready is the serving state: listeners bound, server.start emitted, READY=1 sent.
	Ready
	// Draining holds from the readiness flip to the last component's Stop.
	Draining
	// Stopped is the clean terminal state: clean_shutdown written, exit 0.
	Stopped
	// Fatal is the read-only terminal state (D4): reads keep answering for --fatal-drain,
	// no clean_shutdown marker, exit 74.
	Fatal
)

// String renders the state for logs and sd_notify STATUS lines.
func (s State) String() string {
	switch s {
	case Starting:
		return "STARTING"
	case Recovering:
		return "RECOVERING"
	case Ready:
		return "READY"
	case Draining:
		return "DRAINING"
	case Stopped:
		return "STOPPED"
	case Fatal:
		return "FATAL"
	}
	panic(fmt.Sprintf("lifecycle: unknown state %d", uint8(s)))
}

// legalMoves is the closed transition table. A move absent here is refused before the
// compare-and-swap, so Draining→Ready (a reload resurrecting a draining daemon) and every
// move out of Stopped or Fatal is impossible by construction. Ready→Ready is the reload
// self-transition and is legal precisely because SIGHUP must not change service state.
var legalMoves = map[State][]State{
	Starting:   {Recovering},
	Recovering: {Ready},
	Ready:      {Ready, Draining, Fatal},
	Draining:   {Stopped, Fatal},
	Stopped:    {},
	Fatal:      {},
}

func transitionLegal(from, to State) bool {
	for _, next := range legalMoves[from] {
		if next == to {
			return true
		}
	}
	return false
}

// Component is one wired piece of the daemon. Start returns when the component is up, not
// when its work is finished; Stop is idempotent and must respect its context.
type Component interface {
	Name() string
	Start(ctx context.Context) error
	Stop(ctx context.Context) error
}

// DefaultStopTimeout bounds one component's Stop when [Config.StopTimeout] is unset.
const DefaultStopTimeout = 10 * time.Second

// stopTimeoutEvent is logged at WARN when a component's Stop outlives its budget; the
// manager abandons it and keeps tearing down, because the process must always reach exit.
const stopTimeoutEvent = "lifecycle.stop_timeout"

// stopErrorEvent is logged at WARN when a component's Stop returns a non-timeout error.
// Local constant until #19's event vocabulary lands (same stubbing rule as server.start).
const stopErrorEvent = "lifecycle.stop_error"

// Config carries the manager's tunables.
type Config struct {
	// StopTimeout is the budget given to each component's Stop as its own sub-context.
	// Zero selects [DefaultStopTimeout].
	StopTimeout time.Duration
	// DrainTimeout is the §4.4 budget for in-flight handlers during Shutdown.
	// Zero selects [DefaultDrainTimeout] (A1's register value; --drain-timeout
	// overrides at runtime).
	DrainTimeout time.Duration
}

// Manager starts components in declaration order and stops them in the exact reverse,
// whatever the failure pattern in between. The zero state is Starting.
type Manager struct {
	logger       *slog.Logger
	comps        []Component
	stopTimeout  time.Duration
	drainTimeout time.Duration

	// Seams the drain orchestrates; nil-safe except notify, which defaults to a
	// nop so an unwired daemon behaves like one running outside systemd.
	clock  clock.Clock
	api    APIServer
	health Health
	notify Notifier

	mu      sync.Mutex
	started int  // comps[:started] hold successful Starts
	stopped bool // a StopAll (or a failed Start) already ran

	drainMu   sync.Mutex
	drained   bool        // a Drain already ran; further calls are inert echoes
	lastDrain DrainResult // what the first drain returned

	state atomic.Uint32
}

// NewManager returns a manager over comps in start order. A nil logger discards output;
// library packages must not spew to stderr by default.
func NewManager(logger *slog.Logger, cfg Config, comps ...Component) *Manager {
	if logger == nil {
		logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	}
	stopTimeout := cfg.StopTimeout
	if stopTimeout <= 0 {
		stopTimeout = DefaultStopTimeout
	}
	drainTimeout := cfg.DrainTimeout
	if drainTimeout <= 0 {
		drainTimeout = DefaultDrainTimeout
	}
	return &Manager{
		logger:       logger,
		comps:        append([]Component(nil), comps...),
		stopTimeout:  stopTimeout,
		drainTimeout: drainTimeout,
		clock:        clock.System{},
		notify:       nopNotifier{},
	}
}

// State reports the current process state.
func (m *Manager) State() State {
	//nolint:gosec // advance writes only table-validated states (0–5), so the
	// narrowing load can never lose bits.
	return State(uint8(m.state.Load()))
}

// advance moves the machine from→to iff the move is in the table and the machine still
// sits at from. It reports whether the move happened.
func (m *Manager) advance(from, to State) bool {
	if !transitionLegal(from, to) {
		return false
	}
	return m.state.CompareAndSwap(uint32(from), uint32(to))
}

// Start runs the components in declaration order. On the first failure everything that
// already started is stopped in reverse and the component's error is returned wrapped,
// named by component; the failed component itself is not stopped — it never came up.
func (m *Manager) Start(ctx context.Context) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.stopped {
		return errors.New("lifecycle: manager already stopped")
	}
	for i, c := range m.comps {
		if err := c.Start(ctx); err != nil {
			m.stopStartedLocked(ctx)
			return fmt.Errorf("lifecycle: component %q failed to start: %w", c.Name(), err)
		}
		m.started = i + 1
	}
	return nil
}

// StopAll stops what started, in the exact reverse of start order, and is idempotent: a
// second call is a no-op, which is what keeps a doubled signal from double-stopping a
// component. Each Stop gets its own sub-context bounded by Config.StopTimeout; a Stop
// that outlives the budget is abandoned with a lifecycle.stop_timeout WARN and teardown
// continues — the process must always reach exit.
func (m *Manager) StopAll(ctx context.Context) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.stopped {
		return nil
	}
	m.stopStartedLocked(ctx)
	m.stopped = true
	return nil
}

// stopStartedLocked tears down comps[:m.started] in reverse. The caller holds m.mu.
func (m *Manager) stopStartedLocked(ctx context.Context) {
	for i := m.started - 1; i >= 0; i-- {
		m.stopComponent(ctx, m.comps[i])
	}
	m.started = 0
}

// stopComponent gives one component its own bounded context and logs, never propagates,
// its failure: a wedged component must not take the rest of the shutdown down with it.
func (m *Manager) stopComponent(ctx context.Context, c Component) {
	sctx, cancel := context.WithTimeout(ctx, m.stopTimeout)
	defer cancel()

	err := c.Stop(sctx)
	switch {
	case errors.Is(sctx.Err(), context.DeadlineExceeded):
		m.logger.Warn(stopTimeoutEvent, "component", c.Name(), "timeout", m.stopTimeout)
	case err != nil && !errors.Is(err, context.Canceled):
		// A parent-cancelled Stop is the process going away anyway; anything else is
		// worth a line but never worth aborting the remaining components for.
		m.logger.Warn(stopErrorEvent, "component", c.Name(), "err", err)
	}
}
