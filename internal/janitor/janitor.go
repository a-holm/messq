// SPDX-License-Identifier: Apache-2.0

// Package janitor is the broker's housekeeping (issue #27, PLAN §3.2's last census row):
// ONE goroutine on a tick (60 s in production, ±10 % jitter) running N bounded jobs in
// wiring order — disk → reaper → retention → dedup → events → checkpoint → vacuum →
// stats, the order the serve layer wires them in. Every mutation a job makes is a #6
// writer command; planning reads never touch the writer. No goroutine per stream, per
// job, or per sweep.
//
// The two invariants this package exists to hold:
//
//   - A job can only ever take its fair slice of the tick's writer budget. One huge
//     delete cannot stall the ack path: the slice deadline cuts it off and the leftover
//     work resumes next tick (the interference gate, brief §5.4).
//   - Stop cancels and NEVER awaits (#17's Component contract): a running sweep aborts
//     between transactions when its context goes done, because every batch is its own
//     transaction and a partially applied sweep is exactly as valid as a complete one.
package janitor

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/a-holm/messq/internal/clock"
)

// Job is one bounded piece of housekeeping. Implementations live with their features
// (disk monitor, reaper, retention, dedup trim, event trim, checkpoint, vacuum, stats);
// this package owns only the scheduling contract.
type Job interface {
	// Name labels the job for logs and metrics ("retention", "checkpoint", ...). It is
	// a closed-set label, never a stream or subject name.
	Name() string

	// Every is how often the job runs. 0 means every tick. Values below one tick run
	// at most once per tick.
	Every() time.Duration

	// Run performs one bounded slice of the job's work. It MUST respect ctx (Stop)
	// and b (its budget): returning with work left over is normal and expected — set
	// Result.More and the remainder resumes next tick. Run must not block without
	// consulting either.
	Run(ctx context.Context, b *Budget) (Result, error)
}

// Result reports what one slice did.
type Result struct {
	Rows  int64 // rows written/deleted by this slice
	Bytes int64 // bytes reclaimed by this slice
	More  bool  // work remains: reschedule immediately if budget remains
}

// Budget is one job's slice of the tick: a wall deadline plus a row allowance. Jobs
// check Expired between batches, wait through Wait (never the time package), and count
// row-heavy steps through Take.
type Budget struct {
	deadline  time.Time
	rowsLeft  int64
	unlimited bool
	clk       clock.Clock
}

// Expired reports whether this slice's wall share is spent. The deadline binds
// unconditionally — an unlimited ROW allowance never lifts the wall-clock cap, or one
// hogging job could hold the writer forever. A ZERO deadline (never produced by New
// or Start; hand-armed test budgets) means "no wall bound".
func (b *Budget) Expired() bool {
	if b.deadline.IsZero() {
		return false
	}
	return !b.clk.Now().Before(b.deadline)
}

// Wait sleeps for d through the clock seam, returning early when ctx is cancelled or
// the slice expires. It is how a multi-batch job paces itself without forbidigo's
// banned time calls.
func (b *Budget) Wait(ctx context.Context, d time.Duration) error {
	if b.Expired() {
		return ErrBudgetExpired
	}
	if err := b.clk.Sleep(ctx, d); err != nil {
		return err
	}
	if b.Expired() {
		return ErrBudgetExpired
	}
	return nil
}

// Take reserves rows of the tick's row allowance, reporting whether the allowance
// admits them. Take(0) always succeeds: metadata-only steps stay free even on an empty
// allowance.
func (b *Budget) Take(rows int64) bool {
	if rows <= 0 {
		return true
	}
	if b.unlimited {
		return true
	}
	if rows > b.rowsLeft {
		return false
	}
	b.rowsLeft -= rows
	return true
}

// RowsLeft reports the remaining row allowance (diagnostics).
func (b *Budget) RowsLeft() int64 { return b.rowsLeft }

// initForTest arms a Budget by hand for unit tests.
func (b *Budget) initForTest(rows int64) {
	b.rowsLeft = rows
	b.unlimited = false
	b.deadline = time.Time{}
	b.clk = realClockStub{}
}

// ErrBudgetExpired is returned by Budget.Wait when the slice's share is spent.
var ErrBudgetExpired = errors.New("janitor: budget slice expired")

// realClockStub satisfies clock.Clock minimally for hand-armed test budgets that never
// wait or read the wall.
type realClockStub struct{}

func (realClockStub) Now() time.Time                       { return time.Time{} }
func (realClockStub) Since(time.Time) time.Duration        { return 0 }
func (realClockStub) NewTimer(time.Duration) clock.Timer   { return nil }
func (realClockStub) NewTicker(time.Duration) clock.Ticker { return nil }
func (realClockStub) Sleep(context.Context, time.Duration) error {
	return nil
}

// Jitter perturbs a tick period. Production passes the ±10 % jitter; tests pass
// deterministic functions or nil (no jitter). Keeping it a seam is what keeps every
// timing assertion above exact.
type Jitter func(d time.Duration) time.Duration

// Config configures a [Janitor].
type Config struct {
	// Interval is the tick period. 0 disables all housekeeping: Start logs the
	// janitor.disabled banner at WARN and arms nothing (--janitor-interval 0 is a
	// documented dev-only setting; the disk monitor is NOT housekeeping and runs
	// regardless, as its own component).
	Interval time.Duration

	// Budget is the per-tick wall-clock budget shared fairly between due jobs.
	// <= 0 means 250ms.
	Budget time.Duration

	// MaxRowsPerTick caps the rows all jobs together may touch per tick via
	// Budget.Take. 0 = unlimited.
	MaxRowsPerTick int64

	// Clock is the seam. Required.
	Clock clock.Clock

	// Logger receives janitor.sweep diagnostics and the disabled banner.
	Logger *slog.Logger

	// Jitter perturbs each tick period; nil jitters nothing (tests).
	Jitter Jitter
}

const defaultBudget = 250 * time.Millisecond

// armedForTest reports whether Start armed a ticker (same-package test seam).
func (j *Janitor) armedForTest() bool { return j.armed }

// Janitor is the single housekeeping goroutine. Construct with [New]; drive with
// Start/Stop per lifecycle.Component.
type Janitor struct {
	cfg  Config
	jobs []Job
	log  *slog.Logger

	tickC clock.Ticker
	armed bool
	// loopCtx and cancelFn are written once by Start before the loop goroutine
	// launches and only read after; Stop closes over the cancel func via sync.Once.
	// context.Context lives behind an atomic so containedctx stays happy.
	loopCtx    atomic.Pointer[context.Context]
	cancelFn   context.CancelFunc
	cancelOnce sync.Once
}

// New validates the configuration and pins the job list. The ORDER of jobs is
// load-bearing (the disk state feeds everything downstream; the reaper finishes
// authorised deletions before retention starts new ones), so the caller wires the list
// in canonical order and this constructor preserves it verbatim.
func New(cfg Config, jobs []Job) (*Janitor, error) {
	if cfg.Clock == nil {
		return nil, errors.New("janitor: New needs a non-nil clock")
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	for _, j := range jobs {
		if j == nil || j.Name() == "" {
			return nil, fmt.Errorf("janitor: job list holds an unnamed job")
		}
	}
	if cfg.Budget <= 0 {
		cfg.Budget = defaultBudget
	}
	return &Janitor{cfg: cfg, jobs: append([]Job(nil), jobs...), log: cfg.Logger}, nil
}

// Name implements lifecycle.Component.
func (j *Janitor) Name() string { return "janitor" }

// Start implements lifecycle.Component: it spawns the tick goroutine (or logs the
// disabled banner) and returns immediately — up, not finished.
func (j *Janitor) Start(ctx context.Context) error {
	if j.cfg.Interval <= 0 {
		j.log.Warn("janitor.disabled",
			"hint", "--janitor-interval 0 turns off ALL housekeeping; dev/test use only")
		return nil
	}
	loop, cancel := context.WithCancel(ctx)
	j.loopCtx.Store(&loop)
	j.cancelFn = cancel
	period := j.cfg.Interval
	if j.cfg.Jitter != nil {
		period = j.cfg.Jitter(j.cfg.Interval)
	}
	j.tickC = j.cfg.Clock.NewTicker(period)
	j.armed = true
	go j.loop()
	return nil
}

// loop is THE goroutine. Each tick runs due jobs strictly in list order; a due job gets
// budget/len(due) of wall clock as its slice and is re-entered while its Result.More is
// true and its slice still has room; a slice that expires moves straight to the next
// job. Leftover work resumes next tick.
func (j *Janitor) loop() {
	nextDue := map[string]time.Time{}
	for range j.tickC.C() {
		ctx := j.ctx()
		if ctx.Err() != nil {
			return
		}
		due := j.dueJobs(nextDue)
		if len(due) == 0 {
			continue
		}
		share := j.cfg.Budget / time.Duration(len(due))
		now := j.cfg.Clock.Now()
		tickEnd := now.Add(j.cfg.Budget)
		for _, job := range due {
			if ctx.Err() != nil {
				return
			}
			sliceEnd := j.cfg.Clock.Now().Add(share)
			if sliceEnd.After(tickEnd) {
				sliceEnd = tickEnd
			}
			b := &Budget{
				deadline:  sliceEnd,
				rowsLeft:  j.cfg.MaxRowsPerTick,
				unlimited: j.cfg.MaxRowsPerTick <= 0,
				clk:       j.cfg.Clock,
			}
			for {
				res, err := job.Run(ctx, b)
				if err != nil {
					j.log.Warn("janitor.job_error", "job", job.Name(), "error", err.Error())
					break
				}
				nextDue[job.Name()] = now.Add(*everyOf(job))
				if !res.More || b.Expired() || ctx.Err() != nil {
					break
				}
			}
		}
	}
}

// ctx returns the loop context armed at Start. Stored separately so Stop can cancel it
// without owning the parent.
func (j *Janitor) ctx() context.Context {
	if p := j.loopCtx.Load(); p != nil {
		return *p
	}
	return context.Background()
}

// dueJobs selects jobs whose Every has elapsed.
func (j *Janitor) dueJobs(nextDue map[string]time.Time) []Job {
	now := j.cfg.Clock.Now()
	var out []Job
	for _, job := range j.jobs {
		every := *everyOf(job)
		if every == 0 {
			out = append(out, job)
			continue
		}
		if d, ok := nextDue[job.Name()]; !ok || !now.Before(d) {
			out = append(out, job)
		}
	}
	return out
}

// everyOf returns the job's Every without retaining a reference into an interface.
func everyOf(job Job) *time.Duration {
	d := job.Every()
	return &d
}

// Stop implements lifecycle.Component: cancel first, return immediately after — a
// running sweep observes the cancelled context between transactions. Never awaited.
func (j *Janitor) Stop(context.Context) error {
	j.cancelOnce.Do(func() {
		if j.cancelFn != nil {
			j.cancelFn()
		}
	})
	return nil
}
