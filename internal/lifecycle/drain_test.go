// SPDX-License-Identifier: Apache-2.0

package lifecycle

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/a-holm/messq/internal/clock"
)

// drainFakes bundles the seams Drain orchestrates: a Health flip, a Notifier, an APIServer
// whose Shutdown can be made to block, and the clock. Everything records into one journal
// so sequence assertions read as a single timeline.
type drainFakes struct {
	j    *journal
	logs *loggerCapture
	clk  *clock.Fake
	h    *fakeHealth
	n    *fakeNotifier
	api  *fakeAPI
}

func newDrainFakes() *drainFakes {
	j := &journal{}
	return &drainFakes{
		j:    j,
		logs: &loggerCapture{},
		clk:  clock.NewFake(time.Unix(0, 0)),
		h:    &fakeHealth{j: j},
		n:    &fakeNotifier{j: j},
		api:  &fakeAPI{j: j},
	}
}

func (d *drainFakes) manager(t *testing.T, comps ...Component) *Manager {
	t.Helper()
	m := NewManager(d.logs.asLogger(), Config{
		DrainTimeout: 10 * time.Second,
		StopTimeout:  10 * time.Second,
	}, comps...)
	m.clock = d.clk
	m.api = d.api
	m.health = d.h
	m.notify = d.n
	return m
}

// ready walks the machine to Ready, the only state a serving daemon drains from.
func (d *drainFakes) ready(t *testing.T, m *Manager) *Manager {
	t.Helper()
	walk(t, m, Recovering, Ready)
	return m
}

func (d *drainFakes) journalHas(want string) bool {
	for _, e := range d.j.snapshot() {
		if e == want {
			return true
		}
	}
	return false
}

// fakeHealth records the readiness flip; #15's HealthState replaces it at the wiring slice.
type fakeHealth struct {
	j        *journal
	draining bool
	readonly bool
}

func (f *fakeHealth) SetDraining() {
	f.draining = true
	f.j.add("health:draining")
}

func (f *fakeHealth) SetReadOnly() {
	f.readonly = true
}

// fakeNotifier captures sd_notify datagram lines and journals them on the shared
// timeline; the unixgram client is slice 5. "=1" is trimmed so journal entries read
// as verbs (notify:STOPPING), matching the other fakes.
type fakeNotifier struct {
	j     *journal
	lines []string
}

func (f *fakeNotifier) Set(fields ...string) error {
	for _, field := range fields {
		f.lines = append(f.lines, field)
		f.j.add("notify:%s", strings.TrimSuffix(field, "=1"))
	}
	return nil
}

func (f *fakeNotifier) Close() error { return nil }

// fakeAPI is the APIServer seam. blockShutdown makes Shutdown hang until Close or ctx
// cancellation, which is how a parked long-poll holds the drain hostage.
type fakeAPI struct {
	j             *journal
	blockShutdown bool
	shutdownErr   error
	closeCalled   bool
}

func (f *fakeAPI) ReleaseAll() { f.j.add("api:release") }

func (f *fakeAPI) Shutdown(ctx context.Context) error {
	f.j.add("api:shutdown")
	if f.blockShutdown {
		<-ctx.Done()
		return ctx.Err()
	}
	return f.shutdownErr
}

func (f *fakeAPI) Close() error {
	f.closeCalled = true
	f.j.add("api:close")
	return nil
}

func TestDrainRunsTheSection44SequenceInOrder(t *testing.T) {
	t.Parallel()

	d := newDrainFakes()
	// Declared in start order [store, writer]: the exact-reverse teardown must end
	// with the store, whose Stop is the final-commit + clean_shutdown path.
	comps := []*fakeComp{newFakeComp(d.j, "store"), newFakeComp(d.j, "writer")}
	m := d.ready(t, d.manager(t, toComponents(comps)...))

	res := m.Drain(context.Background(), "SIGTERM")

	if res.Forced {
		t.Fatalf("uncontended drain reports forced=true (%+v)", res)
	}
	want := []string{
		"health:draining", // readiness flips first: a load balancer learns before we stop accepting
		"notify:STOPPING",
		"api:release", // long polls released before Shutdown parks on them
		"api:shutdown",
		"stop:writer", // then the exact reverse of start order
		"stop:store",
	}
	got := d.j.snapshot()
	if strings.Join(got, ",") != strings.Join(want, ",") {
		t.Fatalf("drain sequence = %v\nwant           %v", got, want)
	}
	if m.State() != Stopped {
		t.Fatalf("state after drain = %v, want STOPPED", m.State())
	}
	if !d.h.draining {
		t.Fatal("health was never flipped to draining")
	}
	var stopping bool
	for _, line := range d.n.lines {
		if line == "STOPPING=1" {
			stopping = true
			break
		}
	}
	if !stopping {
		t.Fatalf("sd_notify lines %v carry no STOPPING=1", d.n.lines)
	}
}

// TestExpiredBudgetStillStopsEverything is the slice's named red: a drain that hits its
// budget force-closes the API and STILL runs the final-commit path (the reverse stop that
// ends in the store's commit + clean_shutdown marker). Expiry is a budget, not an exit.
func TestExpiredBudgetStillStopsEverything(t *testing.T) {
	t.Parallel()

	d := newDrainFakes()
	d.api.blockShutdown = true
	comps := []*fakeComp{newFakeComp(d.j, "store"), newFakeComp(d.j, "writer")}
	m := d.ready(t, d.manager(t, toComponents(comps)...))

	done := make(chan DrainResult, 1)
	go func() { done <- m.Drain(context.Background(), "SIGTERM") }()

	// The blocked Shutdown arms nothing itself; the budget timer is what must fire.
	d.clk.BlockUntil(1)
	d.clk.Advance(10 * time.Second)

	select {
	case res := <-done:
		if !res.Forced {
			t.Fatal("budget expiry did not record forced=true")
		}
	case <-hungStopDeadline(t):
		t.Fatal("Drain did not return after its budget expired")
	}

	for _, want := range []string{"api:close", "stop:writer", "stop:store"} {
		if !d.journalHas(want) {
			t.Fatalf("expired-budget drain skipped %q; sequence = %v", want, d.j.snapshot())
		}
	}
	if m.State() != Stopped {
		t.Fatalf("state after expired-budget drain = %v, want STOPPED (the marker still gets written)", m.State())
	}
}

func TestShutdownErrorIsForcedButNeverAbortsTheCommit(t *testing.T) {
	t.Parallel()

	d := newDrainFakes()
	d.api.shutdownErr = errors.New("http: Server closed early")
	store := newFakeComp(d.j, "store")
	m := d.ready(t, d.manager(t, store))

	res := m.Drain(context.Background(), "SIGTERM")
	if !res.Forced {
		t.Fatal("a failing Shutdown must mark the drain forced")
	}
	if !d.journalHas("api:close") {
		t.Fatal("a failing Shutdown did not force-close the API")
	}
	if !d.journalHas("stop:store") {
		t.Fatal("a failing Shutdown skipped the component teardown (the final commit path)")
	}
}

func TestDrainRefusedOutsideReady(t *testing.T) {
	t.Parallel()

	d := newDrainFakes()
	m := d.manager(t) // left at STARTING

	res := m.Drain(context.Background(), "SIGTERM")
	if res.Forced {
		t.Fatal("impossible drain reported forced")
	}
	if len(d.j.snapshot()) != 0 {
		t.Fatalf("drain from STARTING did work: %v", d.j.snapshot())
	}
	if m.State() != Starting {
		t.Fatalf("refused drain changed the state to %v", m.State())
	}
}

func TestSecondDrainIsInert(t *testing.T) {
	t.Parallel()

	d := newDrainFakes()
	m := d.ready(t, d.manager(t))

	first := m.Drain(context.Background(), "SIGTERM")
	before := len(d.j.snapshot())
	second := m.Drain(context.Background(), "SIGTERM")

	if len(d.j.snapshot()) != before {
		t.Fatalf("second drain did work: %v", d.j.snapshot()[before:])
	}
	if second.Forced != first.Forced || m.State() != Stopped {
		t.Fatalf("second drain = %+v, want an inert echo of the first", second)
	}
}
