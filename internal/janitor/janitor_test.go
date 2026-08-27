// SPDX-License-Identifier: Apache-2.0

package janitor

import (
	"context"
	"io"
	"log/slog"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/a-holm/messq/internal/clock"
)

// The #27 janitor core tests (brief §5.4, PLAN §3.2's last census row). ONE goroutine on
// a tick, N bounded jobs run strictly in wiring order, per-tick fair budgets that cut a
// hogging job off at its share so the next job always gets its turn, ±10 % jitter on the
// tick, More-driven re-entry while budget remains, and a Stop that cancels and NEVER
// awaits a running sweep. Everything runs on the #3 Clock seam — no Sleeps, no real
// timers, nothing flaky under box load.

const tickInterval = time.Second

// recordingJob counts runs and optionally parks Run until released.
type recordingJob struct {
	name  string
	every time.Duration
	moreN int  // return More=true for the first moreN runs
	burn  bool // spin against this job's budget slice until it expires
	park  chan struct{}

	mu    sync.Mutex
	runs  int
	order *recorder
}

func (j *recordingJob) Name() string         { return j.name }
func (j *recordingJob) Every() time.Duration { return j.every }

func (j *recordingJob) count() int {
	j.mu.Lock()
	defer j.mu.Unlock()
	return j.runs
}

func (j *recordingJob) Run(ctx context.Context, b *Budget) (Result, error) {
	j.mu.Lock()
	j.runs++
	more := j.runs <= j.moreN
	burn := j.burn
	park := j.park
	order := j.order
	j.mu.Unlock()
	if order != nil {
		state := "ok"
		if b.Expired() {
			state = "expired"
		}
		order.add(j.name + "|" + state)
	}

	if burn {
		for !b.Expired() && ctx.Err() == nil {
			if err := b.Wait(ctx, 100*time.Millisecond); err != nil {
				break // slice expired or cancelled: yield the writer
			}
		}
	}
	if park != nil {
		<-park
	}
	return Result{Rows: 1, More: more}, nil
}

// recorder is the cross-goroutine event log every ordering assertion reads.
type recorder struct {
	mu sync.Mutex
	n  []string
}

func (r *recorder) add(e string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.n = append(r.n, e)
}

func (r *recorder) snapshot() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]string(nil), r.n...)
}

func (r *recorder) length() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.n)
}

func newHarness(t *testing.T, cfg Config, jobs ...Job) (*clock.Fake, *Janitor) {
	t.Helper()
	fc := clock.NewFake(time.UnixMilli(1_700_000_000_000))
	cfg.Clock = fc
	if cfg.Logger == nil {
		cfg.Logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	}
	j, err := New(cfg, jobs)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	if err := j.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}
	t.Cleanup(func() {
		if err := j.Stop(context.Background()); err != nil {
			t.Logf("stop: %v", err)
		}
	})
	return fc, j
}

func waitFor(cond func() bool) bool {
	for i := 0; i < 2000000 && !cond(); i++ {
		runtime.Gosched()
	}
	return cond()
}

// pumpTicks advances the fake clock a tick at a time until cond holds or the attempt
// budget runs out. The fake ticker drops an unread tick exactly like time.Ticker, so a
// single Advance races the loop goroutine to the receive; pumping turns "did the tick
// land?" into a bounded deterministic wait instead of a coin flip.
func pumpTicks(fc *clock.Fake, period time.Duration, done func() bool) {
	for i := 0; i < 50 && !done(); i++ {
		fc.BlockUntil(1)
		fc.Advance(period)
		runtime.Gosched()
	}
}

func TestJanitorRunsJobsInListOrderEveryTick(t *testing.T) {
	rec := &recorder{}
	a := &recordingJob{name: "disk", order: rec}
	b := &recordingJob{name: "retention", order: rec}
	fc, _ := newHarness(t, Config{Interval: tickInterval, Budget: 250 * time.Millisecond}, a, b)

	pumpTicks(fc, tickInterval, func() bool { return rec.length() >= 2 })
	if !waitFor(func() bool { return rec.length() >= 2 }) {
		t.Fatalf("after one tick: %v", rec.snapshot())
	}
	pumpTicks(fc, tickInterval, func() bool { return rec.length() >= 4 })
	if !waitFor(func() bool { return rec.length() >= 4 }) {
		t.Fatalf("after two ticks: %v", rec.snapshot())
	}
	want := []string{"disk", "retention", "disk", "retention"}
	got := rec.snapshot()
	for i := range want {
		// Strip the budget-state suffix: with a 250ms tick budget the second tick's
		// slices may legitimately start already-expired under aggressive pumping —
		// ORDER is this test's subject; slice liveness is FairShare's.
		name, _, _ := strings.Cut(got[i], "|")
		if name != want[i] {
			t.Fatalf("order = %v, want %v (entry %d: %q != %q)", got, want, i, name, want[i])
		}
	}
}

func TestJanitorStaggersEveryJobs(t *testing.T) {
	rec := &recorder{}
	every := &recordingJob{name: "checkpoint", every: 3 * tickInterval, order: rec}
	tick := &recordingJob{name: "dedup", order: rec}
	fc, _ := newHarness(t, Config{Interval: tickInterval, Budget: 250 * time.Millisecond}, tick, every)

	pumpTicks(fc, tickInterval, func() bool { return every.count() >= 2 })
	if !waitFor(func() bool { return every.count() >= 2 }) {
		t.Fatalf("Every(3s) job never ran twice: %v", rec.snapshot())
	}
	// The staggering property: between two runs of the Every-job there are at least two
	// every-tick runs — it is not swept along each tick. (A fresh Every-job also runs on
	// the very first tick: warm-up beats waiting a full period before first doing work.)
	got := rec.snapshot()
	prev := -1
	for i, e := range got {
		if e != "checkpoint" {
			continue
		}
		if prev >= 0 && i-prev < 3 {
			t.Fatalf("staggering broken: %v (checkpoint at %d too close to %d)", got, i, prev)
		}
		prev = i
	}
}

func TestJanitorReentersWhileMoreAndBudgetRemain(t *testing.T) {
	rec := &recorder{}
	drainer := &recordingJob{name: "retention", order: rec, moreN: 2} // needs 3 slices of work
	fc, _ := newHarness(t, Config{Interval: tickInterval, Budget: time.Minute}, drainer)

	pumpTicks(fc, tickInterval, func() bool { return drainer.count() >= 3 })
	if !waitFor(func() bool { return drainer.count() >= 3 }) {
		t.Fatalf("More=true job was not re-entered within its tick: %d runs", drainer.count())
	}
	// More=false must END the re-entry: after the tick's work is done the count is
	// exactly 3 and never climbs. A mutant that re-enters unconditionally blows past.
	for i := 0; i < 200000 && drainer.count() == 3; i++ {
		runtime.Gosched()
	}
	if got := drainer.count(); got != 3 {
		t.Fatalf("re-entry ran %d times in one tick, want exactly 3: More=false must end it", got)
	}
}

func TestJanitorFairShareCutsOffAHoggingJob(t *testing.T) {
	rec := &recorder{}
	hog := &recordingJob{name: "vacuum", order: rec, burn: true, moreN: 1 << 30}
	next := &recordingJob{name: "stats", order: rec}
	fc, _ := newHarness(t, Config{Interval: tickInterval, Budget: time.Second}, hog, next)

	pumpTicks(fc, tickInterval, func() bool { return hog.count() >= 1 })
	// Feed the hog's pacing Waits — but no further than the tick budget itself
	// (8×100ms < 1s). With fair shares the hog is cut off at its 500ms slice and
	// stats enters with a LIVE slice inside this tick; a mutant handing the hog the
	// whole budget keeps it burning past every advance we supply, and stats never
	// appears.
	for i := 0; i < 8 && rec.length() < 2; i++ {
		fc.Advance(100 * time.Millisecond)
		runtime.Gosched()
	}
	if !waitFor(func() bool { return rec.length() >= 2 }) {
		t.Fatalf("hogging job starved its successor: order %v, hog runs %d",
			rec.snapshot(), hog.count())
	}
	got := rec.snapshot()
	// The scheduler CUT THE HOG OFF and moved on: its successor ran immediately after
	// it, inside the same tick. A whole-budget hog keeps re-entering and the successor
	// never appears.
	if !strings.HasPrefix(got[0], "vacuum|") || !strings.HasPrefix(got[1], "stats|") {
		t.Fatalf("successor did not directly follow the cut-off hog: %v", got)
	}
}

func TestJanitorStopCancelsAndNeverAwaits(t *testing.T) {
	fc := clock.NewFake(time.UnixMilli(1_700_000_000_000))
	park := make(chan struct{})
	parked := &recordingJob{name: "retention", park: park}
	cfg := Config{
		Interval: tickInterval,
		Budget:   250 * time.Millisecond,
		Clock:    fc,
		Logger:   slog.New(slog.NewTextHandler(io.Discard, nil)),
	}
	j, err := New(cfg, []Job{parked})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := j.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}

	pumpTicks(fc, tickInterval, func() bool { return parked.count() == 1 })
	if !waitFor(func() bool { return parked.count() == 1 }) {
		t.Fatal("job never started running")
	}

	// #17's contract: Stop cancels and NEVER awaits. Call it SYNCHRONOUSLY while the
	// sweep is parked inside Run: the moment this call returns, the park is still held —
	// program order proves Stop did not wait for the sweep. A mutant that awaits hangs
	// right here; the package timeout names this test as the red.
	if stopErr := j.Stop(context.Background()); stopErr != nil {
		t.Fatalf("Stop: %v", stopErr)
	}
	select {
	case <-park:
		t.Fatal("precondition broken: the park released itself")
	default:
	}
	if parked.count() != 1 {
		t.Fatalf("job ran %d times across Stop, want exactly 1 (still parked)", parked.count())
	}

	// The cancelled context reached the job: releasing the park lets it observe Done.
	close(park)
	cancel()
}

func TestJanitorJitterSeesTheBasePeriod(t *testing.T) {
	var mu sync.Mutex
	var bases []time.Duration
	fc := clock.NewFake(time.UnixMilli(1_700_000_000_000))
	cfg := Config{
		Interval: tickInterval,
		Budget:   250 * time.Millisecond,
		Clock:    fc,
		Logger:   slog.New(slog.NewTextHandler(io.Discard, nil)),
		Jitter: func(d time.Duration) time.Duration {
			mu.Lock()
			bases = append(bases, d)
			mu.Unlock()
			return d / 2 // deterministic: the tick fires at half-period
		},
	}
	j, err := New(cfg, []Job{&recordingJob{name: "disk"}})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := j.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}
	t.Cleanup(func() {
		if err := j.Stop(context.Background()); err != nil {
			t.Logf("stop: %v", err)
		}
	})

	fc.BlockUntil(1)
	fc.Advance(tickInterval / 2)
	fc.BlockUntil(1)
	fc.Advance(tickInterval / 2)

	mu.Lock()
	defer mu.Unlock()
	for i, b := range bases {
		if b != tickInterval {
			t.Fatalf("jitter input %d = %v, want the un-jittered base %v", i, b, tickInterval)
		}
	}
}

func TestJanitorZeroIntervalDisablesWithBanner(t *testing.T) {
	handler := captureHandler{}
	j, err := New(Config{
		Interval: 0,
		Budget:   250 * time.Millisecond,
		Clock:    clock.NewFake(time.UnixMilli(1_700_000_000_000)),
		Logger:   slog.New(&handler),
	}, []Job{&recordingJob{name: "disk"}})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := j.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}
	if !waitFor(func() bool { return handler.has("janitor.disabled") }) {
		t.Fatal("disabled janitor did not log its loud banner")
	}
	if j.armedForTest() {
		t.Fatal("a disabled janitor armed a ticker anyway")
	}
	if err := j.Stop(context.Background()); err != nil {
		t.Logf("stop: %v", err)
	}
}

type captureHandler struct {
	mu   sync.Mutex
	msgs []string
}

func (h *captureHandler) Enabled(_ context.Context, _ slog.Level) bool { return true }
func (h *captureHandler) Handle(_ context.Context, r slog.Record) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.msgs = append(h.msgs, r.Message)
	return nil
}
func (h *captureHandler) WithAttrs([]slog.Attr) slog.Handler { return h }
func (h *captureHandler) WithGroup(string) slog.Handler      { return h }
func (h *captureHandler) has(m string) bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	for _, x := range h.msgs {
		if x == m {
			return true
		}
	}
	return false
}

func TestBudgetTakeBoundsRows(t *testing.T) {
	b := &Budget{}
	b.initForTest(10)
	if !b.Take(6) || !b.Take(4) {
		t.Fatal("Take refused within the row allowance")
	}
	if b.Take(1) {
		t.Fatal("Take accepted past the allowance")
	}
	if !b.Take(0) {
		t.Fatal("Take(0) must always succeed: metadata-only steps stay free")
	}
}
