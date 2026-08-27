// SPDX-License-Identifier: Apache-2.0

package janitor

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/a-holm/messq/internal/clock"

	"github.com/a-holm/messq/internal/store"
)

// The janitor jobs are thin ADAPTERS between the scheduler's Job contract and the
// store's command surface. Their tests run against in-package fakes so adapter
// mapping is deterministic: how results translate, when More survives, and that ctx
// and budget are honoured between calls. The real SQL behaviour lives — exhaustively
// tested — inside internal/store.

// armedBudget hands a Budget with unlimited rows and an unexpired deadline whose
// expiry is forced once expireAfter Runs have consumed it (the fake clock stub that
// Budget.initForTest arms never expires on its own).
func armedBudget(t *testing.T, rows int64) *Budget {
	t.Helper()
	b := &Budget{}
	b.initForTest(rows)
	return b
}

func TestRetentionJobMapsResult(t *testing.T) {
	want := store.RetentionResult{
		Deleted: 5, FreedBytes: 640, BlockedCount: 1, BlockedBytes: 16, More: true,
	}
	fr := &fakeRetention{res: want}
	j := NewRetentionJob(fr, 64)

	res, err := j.Run(context.Background(), armedBudget(t, 1000))
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if res.Rows != want.Deleted || res.Bytes != want.FreedBytes || !res.More {
		t.Fatalf("adapter mapped {Rows:%d Bytes:%d More:%v}, want rows=%d bytes=%d more=true",
			res.Rows, res.Bytes, res.More, want.Deleted, want.FreedBytes)
	}
	if fr.got.Batch != 64 {
		t.Fatalf("RetentionCmd.Batch = %d, want the job's configured batch", fr.got.Batch)
	}
	if j.Name() != "retention" || j.Every() != 0 {
		t.Fatalf("identity = (%q,%v), want retention/every-tick", j.Name(), j.Every())
	}
}

func TestReaperJobResumesWhilePendingAndStopsOnBudget(t *testing.T) {
	t.Run("drains pending chunks then rests", func(t *testing.T) {
		fr := &fakeReaper{chunks: []store.ReapResumeResult{
			{Removed: 10_000, Pending: true},
			{Removed: 10_000, Pending: true},
			{Removed: 3, Pending: false},
		}}
		j := NewReaperJob(fr)

		res, err := j.Run(context.Background(), armedBudget(t, 100_000))
		if err != nil {
			t.Fatalf("Run: %v", err)
		}
		if res.Rows != 20_003 || fr.calls != 3 {
			t.Fatalf("(rows,calls) = (%d,%d), want (20003,3): the draining chunk counts too",
				res.Rows, fr.calls)
		}
		if res.More {
			t.Fatal("More set after the marker table drained")
		}
	})
}

func TestEventsJobLoopsUntilQuiescent(t *testing.T) {
	fe := &fakeEvents{mores: []bool{true, true}}
	j := NewEventsJob(fe, TrimPolicy{})

	res, err := j.Run(context.Background(), armedBudget(t, 1000))
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if fe.calls != 3 {
		t.Fatalf("TrimEvents calls = %d, want exactly three (two More rounds + closing probe)",
			fe.calls)
	}
	if res.More {
		t.Fatal("More survived past quiescence")
	}
	if fe.got.MaxAgeMs != 0 || fe.got.MaxRows != 0 {
		t.Fatalf("policy leaked wrong bounds: %+v", fe.got)
	}
}

func TestDedupJobSweepsRotatingWindowsOfStreams(t *testing.T) {
	fd := &fakeDedup{streams: []string{"a", "b", "c"}}
	j := NewDedupJob(fd, DedupCursor{Start: 0, Limit: 2})

	first, err := j.Run(context.Background(), armedBudget(t, 1000))
	if err != nil {
		t.Fatalf("first Run: %v", err)
	}
	if got := fd.swept(); len(got) != 2 || got[0] != "a" || got[1] != "b" {
		t.Fatalf("first window swept %v, want [a b] under limit 2", got)
	}
	_ = first

	second, err := j.Run(context.Background(), armedBudget(t, 1000))
	if err != nil {
		t.Fatalf("second Run: %v", err)
	}
	if got := fd.sweptFrom(2); len(got) != 1 || got[0] != "c" {
		t.Fatalf("rotated window swept %v, want [c]", got)
	}
	if second.More {
		t.Fatal("a complete rotation must not advertise More")
	}
}

func TestCheckpointJobOnlyFiresAboveWalBound(t *testing.T) {
	t.Run("idle below the bound", func(t *testing.T) {
		fw := &fakeSolo{}
		j := CheckpointJob{W: fw, WalMaxBytes: 100, WalBytes: func() (int64, error) { return 20, nil }}
		if _, err := j.Run(context.Background(), armedBudget(t, 1000)); err != nil {
			t.Fatalf("Run: %v", err)
		}
		if fw.checkpoints != 0 {
			t.Fatalf("submitted %d checkpoints while the WAL sits in its lower half", fw.checkpoints)
		}
	})
	t.Run("TRUNCATE above the bound", func(t *testing.T) {
		fw := &fakeSolo{}
		j := CheckpointJob{W: fw, WalMaxBytes: 100, WalBytes: func() (int64, error) { return 5000, nil }}
		if _, err := j.Run(context.Background(), armedBudget(t, 1000)); err != nil {
			t.Fatalf("Run: %v", err)
		}
		if fw.checkpoints != 1 || fw.lastMode != store.CheckpointTruncate {
			t.Fatalf("(checkpoints,mode)=(%d,%q), want (1,TRUNCATE)", fw.checkpoints, fw.lastMode)
		}
	})
}

func TestVacuumJobSelfDisablesWhenPagesNeverMove(t *testing.T) {
	freeBefore, freeAfter := int64(40), int64(40) // PRAGMA completes, nothing moves
	var submitted bool
	j := VacuumJob{
		W: soloFunc(func(_ context.Context, _ store.Cmd) (store.Result, error) {
			submitted = true
			return store.VacuumResult{}, nil
		}),
		Pages:    128,
		Freelist: func() (int64, error) { return freeBefore, nil },
		// after == before simulates modernc's no-op finding
		FreelistAfter: func() (int64, error) { return freeAfter, nil },
		Log:           discardLogger(),
	}
	res, err := j.Run(context.Background(), armedBudget(t, 1000))
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if !submitted {
		t.Fatal("expected one VacuumCmd submission during the probe tick")
	}

	// Once it has diagnosed the no-op it stops spending writer cycles on it.
	submitted = false
	if _, rErr := j.Run(context.Background(), armedBudget(t, 1000)); rErr != nil {
		t.Fatalf("Run after disable: %v", rErr)
	}
	if submitted {
		t.Fatal("self-disabled vacuum job still submits commands")
	}
	_ = res
}

// ---- fakes -----------------------------------------------------------------------

type fakeRetention struct {
	res store.RetentionResult
	err error
	got store.RetentionCmd
}

func (f *fakeRetention) Retention(_ context.Context, c store.RetentionCmd) (store.RetentionResult, error) {
	f.got = c
	return f.res, f.err
}

type fakeReaper struct {
	chunks []store.ReapResumeResult
	calls  int
}

func (f *fakeReaper) ReapResume(_ context.Context) (store.ReapResumeResult, error) {
	if f.calls >= len(f.chunks) {
		return store.ReapResumeResult{}, errors.New("unexpected extra call")
	}
	out := f.chunks[f.calls]
	f.calls++
	return out, nil
}

type fakeEvents struct {
	mores []bool // More returned by each successive call
	calls int
	got   store.TrimEventsCmd
}

func (f *fakeEvents) TrimEvents(_ context.Context, c store.TrimEventsCmd) (store.TrimEventsResult, error) {
	more := false
	if f.calls < len(f.mores) {
		more = f.mores[f.calls]
	}
	f.calls++
	f.got = c
	return store.TrimEventsResult{More: more}, nil
}

type fakeDedup struct {
	streams []string
	log     []string
}

func (f *fakeDedup) ListStreams(_ context.Context) ([]string, error) { return f.streams, nil }

func (f *fakeDedup) SweepDedup(_ context.Context, stream string) (int64, error) {
	f.log = append(f.log, stream)
	return 0, nil
}

func (f *fakeDedup) swept() []string { return append([]string(nil), f.log...) }

func (f *fakeDedup) sweptFrom(n int) []string { return append([]string(nil), f.log[n:]...) }

type soloFunc func(context.Context, store.Cmd) (store.Result, error)

func (fn soloFunc) Do(ctx context.Context, cmd store.Cmd) (store.Result, error) {
	return fn(ctx, cmd)
}

type fakeSolo struct {
	checkpoints int
	lastMode    string
}

func (f *fakeSolo) Do(_ context.Context, cmd store.Cmd) (store.Result, error) {
	switch cp := cmd.(type) {
	case store.CheckpointCmd:
		f.checkpoints++
		f.lastMode = cp.Mode
		return store.CheckpointResult{}, nil
	default:
		return nil, errors.New("fakeSolo: unexpected command")
	}
}

// discardLogger silences the vacuum self-disable warning in the assertion above.
func discardLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// ---- slice-9 policy alignment (issue body §job table) -----------------------------

func TestCheckpointJobHalfBoundUsesPassive(t *testing.T) {
	fw := &fakeSolo{}
	j := CheckpointJob{
		W:           fw,
		WalMaxBytes: 256,
		WalBytes:    func() (int64, error) { return 129, nil }, // > max/2, < max
	}
	if _, err := j.Run(context.Background(), armedBudget(t, 1000)); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if fw.checkpoints != 1 || fw.lastMode != store.CheckpointPassive {
		t.Fatalf("(checkpoints,mode)=(%d,%q), want (1,PASSIVE) between the bounds",
			fw.checkpoints, fw.lastMode)
	}
}

func TestVacuumJobOnlyFiresAboveFreelistThreshold(t *testing.T) {
	submissions := 0
	j := VacuumJob{
		W: soloFunc(func(_ context.Context, _ store.Cmd) (store.Result, error) {
			submissions++
			return store.VacuumResult{}, nil
		}),
		Pages:         2000,
		FreelistPages: 10_000,
		Freelist:      func() (int64, error) { return 100, nil }, // below threshold
		FreelistAfter: func() (int64, error) { return 100, nil },
		Log:           discardLogger(),
	}
	if _, err := j.Run(context.Background(), armedBudget(t, 1000)); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if submissions != 0 {
		t.Fatalf("submitted %d vacuum commands under the freelist threshold", submissions)
	}
}

func TestStatsJobEmitsConsumerLagRateLimited(t *testing.T) {
	fc := clock.NewFake(time.UnixMilli(1_700_000_000_000))
	var buf bytes.Buffer
	log := slog.New(slog.NewTextHandler(&buf, nil))
	samples := []LagSample{
		{Stream: "orders", Consumer: "slow", Lag: 900},
		{Stream: "orders", Consumer: "fast", Lag: 5},
	}
	j := NewStatsJob(func() ([]LagSample, error) { return samples, nil },
		StatsConfig{Threshold: 100, Interval: time.Minute, Log: log}, fc)

	b := armedBudget(t, 1000)
	ctx := context.Background()
	for i := 0; i < 3; i++ { // three rapid ticks inside one report interval
		res, err := j.Run(ctx, b)
		if err != nil {
			t.Fatalf("tick %d: %v", i, err)
		}
		if res.Rows != 1 { // only the ONE consumer past the threshold counts as work
			t.Fatalf("tick %d rows=%d, want exactly the single lagging consumer", i, res.Rows)
		}
	}
	out := buf.String()
	if strings.Count(out, "consumer.lag") != 1 {
		t.Fatalf("consumer.lag emitted %d times within its interval, want 1:\n%s",
			strings.Count(out, "consumer.lag"), out)
	}
	if !strings.Contains(out, "slow") || strings.Contains(out, "fast") {
		t.Fatalf("lag row named wrong consumer(s):\n%s", out)
	}

	// Once the interval rolls over, the next lagging consumer reports again.
	fc.Advance(time.Minute)
	buf.Reset()
	if _, err := j.Run(ctx, b); err != nil {
		t.Fatalf("after interval: %v", err)
	}
	if strings.Count(buf.String(), "consumer.lag") != 1 {
		t.Fatalf("interval rollover did not re-report:\n%s", buf.String())
	}
}

// ---- #27 metrics hook ------------------------------------------------------------

type spyMetrics struct {
	mu       sync.Mutex
	observed map[string]int
}

func (s *spyMetrics) ObserveJob(name string, _ time.Duration) {
	s.mu.Lock()
	s.observed[name]++
	s.mu.Unlock()
}

func (s *spyMetrics) count(name string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.observed[name]
}

// The scheduler must hand every completed Run to the injected Metrics seam exactly
// once, labelled by the job's closed-set name — the wire shape behind
// messq_janitor_duration_seconds{job} (#21's projection keeps its cardinality).
func TestJanitorObservesEveryRunDuration(t *testing.T) {
	fc := clock.NewFake(time.UnixMilli(1_700_000_000_000))
	spy := &spyMetrics{observed: make(map[string]int)}
	drainer := &recordingJob{name: "retention", order: &recorder{}, moreN: 1}
	j, jErr := New(Config{
		Interval: tickInterval,
		Clock:    fc,
		Logger:   slog.New(slog.NewTextHandler(io.Discard, nil)),
		Metrics:  spy,
	}, []Job{drainer})
	if jErr != nil {
		t.Fatalf("New: %v", jErr)
	}
	if err := j.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	t.Cleanup(func() {
		if cerr := j.Stop(context.Background()); cerr != nil {
			t.Logf("stop janitor: %v", cerr)
		}
	})

	diskPump(fc, func() bool { return drainer.count() >= 2 && spy.count("retention") >= 2 })
	if !waitFor(func() bool { return drainer.count() >= 2 }) {
		t.Fatalf("drainer never reached two runs (%d)", drainer.count())
	}

	// Quiesce BEFORE counting: extra ticks may legitimately land under load, so an
	// exact mid-flight comparison would flake exactly like a fixed-count probe. Once
	// Stop has taken effect no further Run can start, and ObserveJob fires in the
	// same goroutine right after each Run returns — the two counters MUST agree.
	if err := j.Stop(context.Background()); err != nil {
		t.Logf("stop janitor: %v", err)
	}
	if !waitFor(func() bool {
		return spy.count("retention") == drainer.count()
	}) {
		t.Fatalf("metrics saw %d observations but the job ran %d times: every completed Run must be observed",
			spy.count("retention"), drainer.count())
	}
}
