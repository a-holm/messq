// SPDX-License-Identifier: Apache-2.0

package janitor

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"path/filepath"
	"testing"
	"time"

	"github.com/a-holm/messq/internal/clock"
	"github.com/a-holm/messq/internal/queue"
	"github.com/a-holm/messq/internal/store"
)

// Integration-touch tests for the constructor and seam paths that only make sense
// against a real store handle. Everything else stays on fakes.

func openIntegrationStore(t *testing.T) *store.Store {
	t.Helper()
	// §10's 0700 floor refuses t.TempDir() itself (0775 on this box): the real data
	// directory must be a subdir the store creates itself.
	dir := filepath.Join(t.TempDir(), "data")
	dur, dErr := store.ParseDurability("relaxed")
	if dErr != nil {
		t.Fatalf("parse durability: %v", dErr)
	}
	st, _, err := store.Open(context.Background(), store.Options{
		DataDir:    dir,
		Clock:      clock.NewFake(time.UnixMilli(1_700_000_000_000)),
		Logger:     slog.New(slog.NewTextHandler(io.Discard, nil)),
		Durability: dur,
	})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() {
		if cerr := st.Close(context.Background()); cerr != nil {
			t.Logf("close: %v", cerr)
		}
	})
	return st
}

func TestJobIdentitiesAreClosedAndCheap(t *testing.T) {
	st := openIntegrationStore(t)
	jobs := []Job{
		&ReaperJob{client: st},
		NewRetentionJob(st, 1),
		NewDedupJobForStore(st, DedupCursor{Start: 0, Limit: 4}),
		NewEventsJob(st, TrimPolicy{MaxAgeMs: 1000, MaxRows: 10}),
	}
	wantNames := []string{"reaper", "retention", "dedup", "events"}
	for i, j := range jobs {
		if j.Name() != wantNames[i] {
			t.Errorf("job %d Name = %q, want %q", i, j.Name(), wantNames[i])
		}
		if j.Every() != 0 {
			t.Errorf("job %s Every = %v, want every-tick zero", j.Name(), j.Every())
		}
	}
}

func TestSampleLagReturnsRowsWithoutSpecialCasingEmptyStreams(t *testing.T) {
	st := openIntegrationStore(t)
	ctx := context.Background()
	for _, name := range []string{"orders", "jobs"} {
		cfg := queue.DefaultConfig(name)
		if _, _, err := st.CreateStream(ctx, cfg, "t"); err != nil {
			t.Fatalf("create %s: %v", name, err)
		}
	}
	samples, err := SampleLag(ctx, st.RO())
	if err != nil {
		t.Fatalf("SampleLag: %v", err)
	}
	if len(samples) != 0 {
		t.Fatalf("samples = %d, want 0 before any consumer exists", len(samples))
	}
}

func TestStatfsProbeFailsLoudlyOnMissingPath(t *testing.T) {
	if _, err := (StatfsProbe{}).Free("/definitely/not/a/mount/point"); err == nil {
		t.Fatal("statfs on a missing path must return an error, not fake numbers")
	}
}

func TestBudgetRowsLeftAndTakeExhaustion(t *testing.T) {
	b := &Budget{}
	b.initForTest(3)
	if !b.Take(2) {
		t.Fatal("Take(2) within allowance must succeed")
	}
	if b.RowsLeft() != 1 {
		t.Fatalf("RowsLeft = %d, want 1", b.RowsLeft())
	}
	if b.Take(5) {
		t.Fatal("Take(5) past the remaining allowance must be refused")
	}
	if b.RowsLeft() != 1 {
		t.Fatalf("a refused Take must not debit the allowance: %d", b.RowsLeft())
	}
	if err := b.Wait(context.Background(), time.Second); err != nil {
		t.Fatalf("Wait inside an unexpired slice = %v", err)
	}
	// A slice whose wall share is already spent refuses to wait any more. The stub
	// clock reads the zero time, so the test pins a fake-clock Now AFTER the deadline
	// to make "expired" observable.
	fc := clock.NewFake(time.UnixMilli(1_800_000_000_000))
	b.deadline = fc.Now().Add(-time.Second)
	b.clk = fc
	if err := b.Wait(context.Background(), time.Second); !errors.Is(err, ErrBudgetExpired) {
		t.Fatalf("Wait inside an expired slice = %v, want ErrBudgetExpired", err)
	}
}

func TestDedupJobForStoreSweepsRealStreamsAcrossRotation(t *testing.T) {
	st := openIntegrationStore(t)
	ctx := context.Background()
	for _, name := range []string{"a", "b", "c"} {
		cfg := queue.DefaultConfig(name)
		if _, _, err := st.CreateStream(ctx, cfg, "t"); err != nil {
			t.Fatalf("create %s: %v", name, err)
		}
	}
	j := NewDedupJobForStore(st, DedupCursor{Start: 0, Limit: 2})
	b := &Budget{}
	b.initForTest(100)

	first, rErr := j.Run(ctx, b)
	if rErr != nil {
		t.Fatalf("first sweep: %v", rErr)
	}
	if !first.More {
		t.Fatal("limit=2 over three streams must advertise More")
	}
	second, rErr := j.Run(ctx, b)
	if rErr != nil {
		t.Fatalf("second sweep: %v", rErr)
	}
	if second.More {
		t.Fatal("the completing window must not advertise More")
	}
}

func TestStatsJobSurfacesSamplerErrors(t *testing.T) {
	want := errors.New("ro pool offline")
	j := NewStatsJob(func() ([]LagSample, error) { return nil, want },
		StatsConfig{Threshold: 1, Log: discardLogger()},
		clock.NewFake(time.UnixMilli(1_700_000_000_000)))
	if _, err := j.Run(context.Background(), &Budget{}); !errors.Is(err, want) {
		t.Fatalf("err = %v, want the sampler's failure", err)
	}
}

// TestContractAccessorsPinned holds the scheduler contract's trivial accessors to
// their literal values — a rename here breaks wiring and closed-set labels together.
func TestContractAccessorsPinned(t *testing.T) {
	m := NewDiskMonitor(DiskMonitorConfig{Probe: &fakeProbe{}, Log: discardLogger()},
		clock.NewFake(time.UnixMilli(1_700_000_000_000)))
	if m.Name() != "disk-monitor" {
		t.Fatalf("monitor name = %q", m.Name())
	}
	if m.ctx() == nil {
		t.Fatal("un-started monitor must still answer ctx with a usable context")
	}

	jan := &Janitor{}
	if jan.Name() != "janitor" {
		t.Fatalf("janitor name = %q", jan.Name())
	}

	stub := realClockStub{}
	_ = stub.Now()
	_ = stub.Since(time.Time{})
	if stub.NewTimer(time.Second) != nil || stub.NewTicker(time.Second) != nil {
		t.Fatal("the stub arms nothing; hand-armed budgets never wait")
	}
	if err := stub.Sleep(context.Background(), time.Second); err != nil {
		t.Fatalf("stub sleep = %v, want a silent no-op", err)
	}

	fakeReaperClient := &fakeReaper{}
	rj := NewReaperJob(fakeReaperClient)
	if rj.client != ReaperClient(fakeReaperClient) {
		t.Fatal("constructor did not store its client verbatim")
	}
	cp := &CheckpointJob{W: soloFunc(func(context.Context, store.Cmd) (store.Result, error) { return nil, nil })}
	vj := &VacuumJob{W: cp.W}
	sj := NewStatsJob(func() ([]LagSample, error) { return nil, nil },
		StatsConfig{}, clock.NewFake(time.UnixMilli(0)))
	for _, tc := range []struct {
		j    Job
		name string
	}{{rj, "reaper"}, {cp, "checkpoint"}, {vj, "vacuum"}, {sj, "stats"}} {
		if tc.j.Name() != tc.name {
			t.Fatalf("name = %q, want %q (§9.4 closed label set)", tc.j.Name(), tc.name)
		}
	}
}
