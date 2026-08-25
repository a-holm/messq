// SPDX-License-Identifier: Apache-2.0

package lifecycle

import (
	"context"
	"os"
	"runtime"
	"sync/atomic"
	"syscall"
	"testing"
	"time"
)

// newSignalManager wires the drain fakes into a manager with a custom drain budget
// and a captured exit seam, so the loop's escalation ladder can be driven without
// killing the test process. exitCode starts at -1 so "exit never ran" reads as -1,
// not as a legitimate 0.
func newSignalManager(t *testing.T, d *drainFakes, budget time.Duration, exitCode *atomic.Int32) *Manager {
	t.Helper()
	comps := []*fakeComp{newFakeComp(d.j, "store"), newFakeComp(d.j, "writer")}
	m := NewManager(d.logs.asLogger(), Config{
		DrainTimeout: budget,
		StopTimeout:  10 * time.Second,
	}, toComponents(comps)...)
	m.clock = d.clk
	m.api = d.api
	m.health = d.h
	m.notify = d.n
	m.exit = func(code int) { exitCode.CompareAndSwap(-1, int32(code)) }
	return m
}

// waitUntil polls cond with the scheduler until it holds or the real-time backstop
// fires; a broken loop becomes a test failure instead of a hang.
func waitUntil(t *testing.T, deadline <-chan struct{}, cond func() bool, msg string) {
	t.Helper()
	for !cond() {
		select {
		case <-deadline:
			t.Fatal(msg)
		default:
		}
		runtime.Gosched()
	}
}

// parkInShutdown sends the first SIGTERM and blocks until the drain is provably
// waiting inside Shutdown (its budget timer armed), so later signals have a defined
// racing partner.
func parkInShutdown(t *testing.T, m *Manager, d *drainFakes, sigc chan os.Signal, deadline <-chan struct{}) {
	t.Helper()
	sigc <- syscall.SIGTERM
	d.clk.BlockUntil(1)
	waitUntil(t, deadline, func() bool { return m.State() == Draining }, "first SIGTERM never reached DRAINING")
}

func TestFirstTermRunsExactlyOneFullDrain(t *testing.T) {
	t.Parallel()

	d := newDrainFakes()
	var exitCode atomic.Int32
	exitCode.Store(-1)
	m := newSignalManager(t, d, 10*time.Second, &exitCode)
	m = d.ready(t, m)

	sigc := make(chan os.Signal, 4)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go m.ServeSignals(ctx, sigc, nil)

	deadline := hungStopDeadline(t)
	sigc <- syscall.SIGTERM
	waitUntil(t, deadline, func() bool { return m.State() == Stopped }, "one SIGTERM never drained the daemon")

	want := []string{
		"health:draining",
		"notify:STOPPING",
		"api:release",
		"api:shutdown",
		"stop:writer",
		"stop:store",
	}
	if got := d.j.snapshot(); len(got) != len(want) {
		t.Fatalf("signal-driven drain sequence = %v\nwant               %v", got, want)
	}
	if code := exitCode.Load(); code != -1 {
		t.Fatalf("a single clean SIGTERM exited with %d", code)
	}
}

// TestSecondTermEscalatesPastABlockedShutdown is the slice's named red: a doubled
// SIGTERM must break out of a parked Shutdown immediately (escalate), not sit out
// the rest of the drain budget.
func TestSecondTermEscalatesPastABlockedShutdown(t *testing.T) {
	t.Parallel()

	d := newDrainFakes()
	d.api.blockShutdown = true
	var exitCode atomic.Int32
	exitCode.Store(-1)
	m := newSignalManager(t, d, time.Hour, &exitCode) // the budget alone would hang the test
	m = d.ready(t, m)

	sigc := make(chan os.Signal, 4)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go m.ServeSignals(ctx, sigc, nil)

	deadline := hungStopDeadline(t)
	parkInShutdown(t, m, d, sigc, deadline)

	sigc <- syscall.SIGTERM // the escalate: no fake-clock advance happens below
	waitUntil(t, deadline, func() bool { return m.State() == Stopped },
		"a doubled SIGTERM did not break out of the blocked Shutdown")

	if !d.journalHas("api:close") || !d.journalHas("stop:store") {
		t.Fatalf("escalation skipped the forced close or teardown: %v", d.j.snapshot())
	}
	if code := exitCode.Load(); code != -1 {
		t.Fatalf("the second signal exited (%d); only the third may", code)
	}
}

func TestThirdTermExitsWithOne(t *testing.T) {
	t.Parallel()

	d := newDrainFakes()
	var exitCode atomic.Int32
	exitCode.Store(-1)
	m := newSignalManager(t, d, time.Hour, &exitCode)
	m = d.ready(t, m)

	sigc := make(chan os.Signal, 8)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go m.ServeSignals(ctx, sigc, nil)

	deadline := hungStopDeadline(t)
	sigc <- syscall.SIGTERM
	sigc <- syscall.SIGTERM
	sigc <- syscall.SIGTERM
	waitUntil(t, deadline, func() bool { return exitCode.Load() == 1 },
		"the third SIGTERM did not exit with 1")
}

func TestSighupDuringDrainIsIgnored(t *testing.T) {
	t.Parallel()

	d := newDrainFakes()
	d.api.blockShutdown = true
	var exitCode atomic.Int32
	exitCode.Store(-1)
	var reloads atomic.Int32
	m := newSignalManager(t, d, time.Hour, &exitCode)
	m = d.ready(t, m)

	sigc := make(chan os.Signal, 8)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go m.ServeSignals(ctx, sigc, func() { reloads.Add(1) })

	deadline := hungStopDeadline(t)
	parkInShutdown(t, m, d, sigc, deadline)

	sigc <- syscall.SIGHUP  // arrives mid-drain: must be a no-op
	sigc <- syscall.SIGTERM // escalates the drain so it completes deterministically
	waitUntil(t, deadline, func() bool { return m.State() == Stopped },
		"the escalating SIGTERM did not complete the drain")
	if reloads.Load() != 0 {
		t.Fatalf("SIGHUP during DRAINING reloaded %d time(s)", reloads.Load())
	}
	if !d.journalHas("api:close") {
		t.Fatalf("the ignored SIGHUP disturbed the drain: %v", d.j.snapshot())
	}
	cancel()
}

func TestSighupAtReadyReloadsOnce(t *testing.T) {
	t.Parallel()

	d := newDrainFakes()
	var exitCode atomic.Int32
	exitCode.Store(-1)
	var reloads atomic.Int32
	m := newSignalManager(t, d, 10*time.Second, &exitCode)
	m = d.ready(t, m)

	sigc := make(chan os.Signal, 8)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go m.ServeSignals(ctx, sigc, func() { reloads.Add(1) })

	deadline := hungStopDeadline(t)
	sigc <- syscall.SIGHUP
	waitUntil(t, deadline, func() bool { return reloads.Load() == 1 },
		"SIGHUP at READY never invoked the reloader")
	if m.State() != Ready {
		t.Fatalf("reload changed service state to %v; SIGHUP must not", m.State())
	}
}

// TestWatchedSignalsExcludeSigquit proves non-registration by construction: the set
// handed to signal.Notify names exactly TERM/INT/HUP, so SIGQUIT keeps its default
// core-dump behaviour because the kernel never tells us about it.
func TestWatchedSignalsExcludeSigquit(t *testing.T) {
	t.Parallel()

	watched := WatchedSignals()
	want := map[os.Signal]bool{
		syscall.SIGTERM: false,
		syscall.SIGINT:  false,
		syscall.SIGHUP:  false,
	}
	for _, sig := range watched {
		if _, ok := want[sig]; !ok {
			t.Fatalf("WatchedSignals intercepts %v, which is not ours to catch", sig)
		}
		want[sig] = true
	}
	for sig, seen := range want {
		if !seen {
			t.Fatalf("WatchedSignals misses %v", sig)
		}
	}
}
