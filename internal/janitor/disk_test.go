// SPDX-License-Identifier: Apache-2.0

package janitor

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/a-holm/messq/internal/clock"
	"github.com/a-holm/messq/internal/queue"
)

// The disk monitor is housekeeping's own COMPONENT (#17 contract), not a Job on the
// tick list: --janitor-interval 0 disables all bounded jobs, but disk safety runs
// regardless (brief §4 decision 3). The state machine itself is queue.NextDiskState,
// the pure hysteretic planner from slice 1; this component only supplies samples and
// fans transitions out to a gauge/health seam. Determinism rides the same fake-clock
// pumping discipline as the core scheduler tests above.

type fakeProbe struct {
	mu    sync.Mutex
	free  int64
	err   error
	calls int
}

func (f *fakeProbe) Free(string) (int64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls++
	return f.free, f.err
}

func (f *fakeProbe) callCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.calls
}

func (f *fakeProbe) set(free int64, err error) {
	f.mu.Lock()
	f.free = free
	f.err = err
	f.mu.Unlock()
}

const diskTickPeriod = time.Minute

// diskPump advances the fake clock enough times for cond to come true. The fake
// ticker drops ticks like time.Ticker, so a co-tenant load spike can eat several
// advances; 200 bounded rounds keep "will the sample ever land" deterministic here
// without wall-clock sleeps.
func diskPump(fc *clock.Fake, cond func() bool) bool {
	for i := 0; i < 200 && !cond(); i++ {
		fc.BlockUntil(1)
		fc.Advance(diskTickPeriod)
	}
	return waitFor(cond)
}

func TestDiskMonitorTransitionsThroughHysteresis(t *testing.T) {
	fc := clock.NewFake(time.UnixMilli(1_700_000_000_000))
	fp := &fakeProbe{free: 10_000}
	var mu sync.Mutex
	var states []queue.DiskState
	m := NewDiskMonitor(DiskMonitorConfig{
		Path:     "/tmp/data",
		Policy:   queue.DiskPolicy{MinFree: 1000, Recover: 2.0},
		Probe:    fp,
		Interval: diskTickPeriod,
		OnState: func(s queue.DiskState) {
			mu.Lock()
			states = append(states, s)
			mu.Unlock()
		},
		Log: discardLogger(),
	}, fc)
	if err := m.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	t.Cleanup(func() {
		if cerr := m.Stop(context.Background()); cerr != nil {
			t.Logf("stop disk monitor: %v", cerr)
		}
	})
	transitions := func() int {
		mu.Lock()
		defer mu.Unlock()
		return len(states)
	}

	// Below MinFree: enters DiskLow exactly once.
	fp.set(500, nil)
	if !diskPump(fc, func() bool {
		return m.State() == queue.DiskLow && transitions() == 1
	}) {
		t.Fatalf("state after sample below MinFree = %v with %d transitions", m.State(), transitions())
	}

	// Recovery hysteresis: between MinFree and MinFree*Recover nothing changes.
	fp.set(1500, nil) // >= MinFree but < MinFree*Recover
	if !diskPump(fc, func() bool { return fp.callCount() >= 2 }) {
		t.Fatal("second tick never landed")
	}
	if got := m.State(); got != queue.DiskLow {
		t.Fatalf("mid-state exit too early: %v, want still DiskLow", got)
	}
	if got := transitions(); got != 1 {
		t.Fatalf("hysteresis band fired a transition: total=%d want 1", got)
	}

	// At or above MinFree*Recover: leaves once.
	fp.set(2500, nil)
	if !diskPump(fc, func() bool { return m.State() == queue.DiskOK && transitions() == 2 }) {
		t.Fatalf("recovered state = %v with %d transitions, want DiskOK after exactly two",
			m.State(), transitions())
	}

	// Repeat steady-state samples never re-fire OnState.
	for i := 0; i < 3 && transitions() == 2; i++ {
		fc.BlockUntil(1)
		fc.Advance(diskTickPeriod)
		if !waitFor(func() bool { return true }) {
			t.Fatal("unreachable")
		}
	}
	if got := transitions(); got != 2 {
		t.Fatalf("steady-state fired extra transitions: %d", got)
	}
}

func TestDiskMonitorDisabledPolicyNeverLeavesDiskOK(t *testing.T) {
	fc := clock.NewFake(time.UnixMilli(1_700_000_000_000))
	fp := &fakeProbe{free: -1} // even an absurd negative free must not alarm
	m := NewDiskMonitor(DiskMonitorConfig{
		Path:     "/tmp/data",
		Policy:   queue.DiskPolicy{MinFree: 0}, // documented "bad idea" switch: fully disabled
		Probe:    fp,
		Interval: diskTickPeriod,
		Log:      discardLogger(),
	}, fc)
	if err := m.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	t.Cleanup(func() {
		if cerr := m.Stop(context.Background()); cerr != nil {
			t.Logf("stop disk monitor: %v", cerr)
		}
	})

	// Prove the sampler goroutine is alive and delivering before pinning the bound.
	if !diskPump(fc, func() bool { return fp.callCount() >= 1 }) {
		t.Fatal("monitor never sampled")
	}
	// Whatever samples then land (ticker drops make exact counts load-dependent),
	// MinFree<=0 must hold DiskOK forever: the pure planner never emits a transition.
	for i := 0; i < 20; i++ {
		fc.BlockUntil(1)
		fc.Advance(diskTickPeriod)
	}
	if !waitFor(func() bool { return true }) {
		t.Fatal("unreachable")
	}
	if got := m.State(); got != queue.DiskOK {
		t.Fatalf("MinFree=0 disabled the guard; state = %v", got)
	}
}

func TestDiskMonitorProbeErrorKeepsLastState(t *testing.T) {
	fc := clock.NewFake(time.UnixMilli(1_700_000_000_000))
	fp := &fakeProbe{free: 10_000}
	m := NewDiskMonitor(DiskMonitorConfig{
		Path:     "/tmp/data",
		Policy:   queue.DiskPolicy{MinFree: 1000},
		Probe:    fp,
		Interval: diskTickPeriod,
		Log:      discardLogger(),
	}, fc)
	if err := m.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	t.Cleanup(func() {
		if cerr := m.Stop(context.Background()); cerr != nil {
			t.Logf("stop disk monitor: %v", cerr)
		}
	})

	fp.set(0, errors.New("statfs failed"))
	sampledWithError := func() bool { return fp.callCount() >= 1 && m.State() == queue.DiskOK }
	if !diskPump(fc, sampledWithError) {
		t.Fatal("first (failing) probe never landed")
	}

	fp.set(5, nil)
	resumed := func() bool { return fp.callCount() >= 2 && m.State() == queue.DiskLow }
	if !diskPump(fc, resumed) {
		t.Fatalf("healthy probe after error did not resume transitions: %v", m.State())
	}
}

func TestStatfsFreeOnTempDirIsSane(t *testing.T) {
	// The one real-filesystem smoke test the brief asks for: the production probe
	// reports plausible positive numbers on any healthy mount point.
	const MiB = int64(1 << 20)
	free, err := StatfsProbe{}.Free(t.TempDir())
	if err != nil {
		t.Fatalf("statfs smoke: %v", err)
	}
	if free < 4*MiB {
		t.Fatalf("statfs reported %d bytes free on t.TempDir(), implausibly small", free)
	}
}
