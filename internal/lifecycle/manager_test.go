// SPDX-License-Identifier: Apache-2.0

package lifecycle

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"
)

// journal records the exact call order across all fakes, so ordering assertions read as one
// timeline instead of per-component counters.
type journal struct {
	mu     sync.Mutex
	events []string
}

func (j *journal) add(format string, args ...any) {
	j.mu.Lock()
	defer j.mu.Unlock()
	j.events = append(j.events, fmt.Sprintf(format, args...))
}

func (j *journal) snapshot() []string {
	j.mu.Lock()
	defer j.mu.Unlock()
	return append([]string(nil), j.events...)
}

// fakeComp is a recording Component. When blockStop is true its Stop blocks until its context
// is done — the hung-Stop case the manager must survive.
type fakeComp struct {
	name      string
	startErr  error
	blockStop bool

	j      *journal
	mu     sync.Mutex
	starts int
	stops  int
}

func (f *fakeComp) Name() string { return f.name }

func (f *fakeComp) Start(_ context.Context) error {
	f.mu.Lock()
	f.starts++
	f.mu.Unlock()
	f.j.add("start:%s", f.name)
	return f.startErr
}

func (f *fakeComp) Stop(ctx context.Context) error {
	f.mu.Lock()
	f.stops++
	f.mu.Unlock()
	f.j.add("stop:%s", f.name)
	if f.blockStop {
		<-ctx.Done()
		return ctx.Err()
	}
	return nil
}

func (f *fakeComp) stopCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.stops
}

func newFakeComp(j *journal, name string) *fakeComp {
	return &fakeComp{name: name, j: j}
}

func toComponents(comps []*fakeComp) []Component {
	out := make([]Component, 0, len(comps))
	for _, c := range comps {
		out = append(out, c)
	}
	return out
}

// hungStopDeadline is a real-time backstop so that a broken manager turns into a test failure
// (via go test's own timeout budget) rather than an unbounded hang of the assertion itself.
func hungStopDeadline(t *testing.T) <-chan struct{} {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	t.Cleanup(cancel)
	return ctx.Done()
}

func discardLogger() *loggerCapture {
	return &loggerCapture{}
}

func (h *loggerCapture) asLogger() *slog.Logger { return slog.New(h) }

// loggerCapture is a slog.Handler that keeps every record for assertion.
type loggerCapture struct {
	mu      sync.Mutex
	records []capturedRecord
}

type capturedRecord struct {
	level string
	msg   string
	attr  string
}

func (h *loggerCapture) Enabled(_ context.Context, _ slog.Level) bool { return true }

func (h *loggerCapture) Handle(_ context.Context, r slog.Record) error {
	var b strings.Builder
	r.Attrs(func(a slog.Attr) bool {
		b.WriteString(" ")
		b.WriteString(a.Key)
		b.WriteString("=")
		b.WriteString(a.Value.String())
		return true
	})
	h.mu.Lock()
	defer h.mu.Unlock()
	h.records = append(h.records, capturedRecord{
		level: r.Level.String(),
		msg:   r.Message,
		attr:  b.String(),
	})
	return nil
}

func (h *loggerCapture) WithAttrs(_ []slog.Attr) slog.Handler { return h }

func (h *loggerCapture) WithGroup(_ string) slog.Handler { return h }

func (h *loggerCapture) count(msg string) int {
	h.mu.Lock()
	defer h.mu.Unlock()
	n := 0
	for _, r := range h.records {
		if r.msg == msg {
			n++
		}
	}
	return n
}

func (h *loggerCapture) anyContains(substr string) bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	for _, r := range h.records {
		if strings.Contains(r.msg+" "+r.attr, substr) {
			return true
		}
	}
	return false
}

// walk advances the machine along a legal path and fails the test if any step is refused,
// so transition-table tests start from the state they name rather than from a raw constructor
// call.
func walk(t *testing.T, m *Manager, steps ...State) {
	t.Helper()
	cur := m.State()
	for _, next := range steps {
		if !m.advance(cur, next) {
			t.Fatalf("advance(%v, %v) refused while walking to %v", cur, next, next)
		}
		cur = next
	}
}

func allStates() []State {
	return []State{Starting, Recovering, Ready, Draining, Stopped, Fatal}
}

// legalTransitions is the closed move set of the state machine, hard-coded here rather than
// derived from the implementation map: deriving expectations from the code under test would
// make the table test unable to fail.
var legalTransitions = map[State][]State{
	Starting:   {Recovering},
	Recovering: {Ready},
	// Ready→Ready is the reload self-transition (SIGHUP flips READY through RELOADING
	// semantics without ever leaving service).
	Ready:    {Ready, Draining, Fatal},
	Draining: {Stopped, Fatal},
	Stopped:  {},
	Fatal:    {},
}

func isLegal(from, to State) bool {
	for _, next := range legalTransitions[from] {
		if next == to {
			return true
		}
	}
	return false
}

func TestAdvanceAcceptsEveryLegalTransition(t *testing.T) {
	t.Parallel()

	for _, from := range allStates() {
		for _, to := range legalTransitions[from] {
			t.Run(fmt.Sprintf("%v->%v", from, to), func(t *testing.T) {
				t.Parallel()

				m := NewManager(discardLogger().asLogger(), Config{})
				walkTo(t, m, from)

				if !m.advance(from, to) {
					t.Fatalf("advance(%v, %v) refused a legal transition", from, to)
				}
				if got := m.State(); got != to {
					t.Fatalf("state after advance(%v, %v) = %v, want %v", from, to, got, to)
				}
			})
		}
	}
}

// walkTo drives the machine to from along the unique spine Starting→Recovering→Ready and then
// the branch that reaches from, refusing to fabricate states the machine cannot hold.
func walkTo(t *testing.T, m *Manager, target State) {
	t.Helper()

	switch target {
	case Starting:
	case Recovering:
		walk(t, m, Recovering)
	case Ready:
		walk(t, m, Recovering, Ready)
	case Draining:
		walk(t, m, Recovering, Ready, Draining)
	case Stopped:
		walk(t, m, Recovering, Ready, Draining, Stopped)
	case Fatal:
		walk(t, m, Recovering, Ready, Fatal)
	default:
		t.Fatalf("unknown state %v", target)
	}
}

func TestAdvanceRejectsEveryIllegalTransition(t *testing.T) {
	t.Parallel()

	for _, from := range allStates() {
		for _, to := range allStates() {
			if isLegal(from, to) {
				continue
			}
			t.Run(fmt.Sprintf("%v-!>%v", from, to), func(t *testing.T) {
				t.Parallel()

				m := NewManager(discardLogger().asLogger(), Config{})
				walkTo(t, m, from)

				// Draining→Ready is the named resurrection move: a SIGHUP racing a
				// SIGTERM must never pull a draining daemon back to ready.
				if m.advance(from, to) {
					t.Fatalf("advance(%v, %v) accepted an illegal transition (state now %v)", from, to, m.State())
				}
				if got := m.State(); got != from {
					t.Fatalf("refused advance changed the state: %v → %v", from, got)
				}
			})
		}
	}
}

func TestStartRunsComponentsInDeclarationOrder(t *testing.T) {
	t.Parallel()

	j := &journal{}
	comps := []*fakeComp{newFakeComp(j, "store"), newFakeComp(j, "writer"), newFakeComp(j, "api")}
	m := NewManager(discardLogger().asLogger(), Config{}, toComponents(comps)...)

	if err := m.Start(context.Background()); err != nil {
		t.Fatalf("Start: unexpected error: %v", err)
	}

	want := []string{"start:store", "start:writer", "start:api"}
	got := j.snapshot()
	if strings.Join(got, ",") != strings.Join(want, ",") {
		t.Fatalf("start order = %v, want %v", got, want)
	}
}

func TestStopAllIsExactReverseOfStart(t *testing.T) {
	t.Parallel()

	j := &journal{}
	comps := []*fakeComp{newFakeComp(j, "store"), newFakeComp(j, "writer"), newFakeComp(j, "api")}
	m := NewManager(discardLogger().asLogger(), Config{}, toComponents(comps)...)

	if err := m.Start(context.Background()); err != nil {
		t.Fatalf("Start: unexpected error: %v", err)
	}
	if err := m.StopAll(context.Background()); err != nil {
		t.Fatalf("StopAll: unexpected error: %v", err)
	}

	want := []string{"start:store", "start:writer", "start:api", "stop:api", "stop:writer", "stop:store"}
	got := j.snapshot()
	if strings.Join(got, ",") != strings.Join(want, ",") {
		t.Fatalf("start/stop sequence = %v, want %v", got, want)
	}
}

func TestMidStartFailureStopsOnlyStartedComponentsInReverse(t *testing.T) {
	t.Parallel()

	j := &journal{}
	boom := errors.New("bind: address already in use")
	api := &fakeComp{name: "api", j: j, startErr: boom}
	comps := []*fakeComp{newFakeComp(j, "store"), newFakeComp(j, "writer"), api}
	m := NewManager(discardLogger().asLogger(), Config{}, toComponents(comps)...)

	err := m.Start(context.Background())
	if err == nil {
		t.Fatal("Start returned nil for a failing component")
	}
	if !errors.Is(err, boom) {
		t.Fatalf("Start error = %v, want it to wrap the component error", err)
	}
	if !strings.Contains(err.Error(), "api") {
		t.Fatalf("Start error %q does not name the failing component", err)
	}

	want := []string{"start:store", "start:writer", "start:api", "stop:writer", "stop:store"}
	got := j.snapshot()
	if strings.Join(got, ",") != strings.Join(want, ",") {
		t.Fatalf("failed-start sequence = %v, want %v (the failed component is not stopped, everything before it is, in reverse)", got, want)
	}
}

func TestStopAllIsIdempotentAcrossCalls(t *testing.T) {
	t.Parallel()

	j := &journal{}
	store := newFakeComp(j, "store")
	comps := []*fakeComp{store, newFakeComp(j, "writer")}
	m := NewManager(discardLogger().asLogger(), Config{}, toComponents(comps)...)

	if err := m.Start(context.Background()); err != nil {
		t.Fatalf("Start: unexpected error: %v", err)
	}
	if err := m.StopAll(context.Background()); err != nil {
		t.Fatalf("first StopAll: unexpected error: %v", err)
	}
	// A doubled signal must not double-stop: the second StopAll is a no-op.
	if err := m.StopAll(context.Background()); err != nil {
		t.Fatalf("second StopAll: unexpected error: %v", err)
	}

	for _, c := range comps {
		if got := c.stopCount(); got != 1 {
			t.Fatalf("component %s stopped %d times, want exactly 1", c.name, got)
		}
	}
}

func TestHungStopIsAbandonedLoggedAndDoesNotBlockTheRest(t *testing.T) {
	t.Parallel()

	j := &journal{}
	logs := &loggerCapture{}
	hang := &fakeComp{name: "writer", j: j, blockStop: true}
	comps := []*fakeComp{newFakeComp(j, "store"), hang, newFakeComp(j, "api")}
	m := NewManager(logs.asLogger(), Config{StopTimeout: 20 * time.Millisecond}, toComponents(comps)...)

	ctx := context.Background()
	if err := m.Start(ctx); err != nil {
		t.Fatalf("Start: unexpected error: %v", err)
	}

	done := make(chan error, 1)
	go func() { done <- m.StopAll(ctx) }()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("StopAll returned %v past a hung component; the manager must move on", err)
		}
	case <-hungStopDeadline(t):
		t.Fatal("StopAll did not return: a hung Stop blocked the whole shutdown")
	}

	want := []string{"start:store", "start:writer", "start:api", "stop:api", "stop:writer", "stop:store"}
	got := j.snapshot()
	if strings.Join(got, ",") != strings.Join(want, ",") {
		t.Fatalf("sequence around a hung Stop = %v, want %v", got, want)
	}

	if logs.count(stopTimeoutEvent) != 1 {
		t.Fatalf("%s logged %d times, want exactly 1", stopTimeoutEvent, logs.count(stopTimeoutEvent))
	}
	if !logs.anyContains("component=writer") {
		t.Fatalf("%s line does not name the hung component; capture: %+v", stopTimeoutEvent, logs.records)
	}
}
