// SPDX-License-Identifier: Apache-2.0

package janitor

import (
	"context"
	"log/slog"
	"time"

	"github.com/a-holm/messq/internal/store"
)

// The job implementations (issue #27 §9): one thin adapter per housekeeping duty,
// each satisfying Job. Every adapter is deliberately narrow — a single-method client
// interface it actually needs — so tests drive them with fakes while the real SQL
// behaviour stays in internal/store where it belongs. Jobs NEVER touch time directly:
// pacing flows through Budget (the clock seam) and Stop through ctx.

// ---- retention ------------------------------------------------------------------

// RetentionClient is what the job needs from the store.
type RetentionClient interface {
	Retention(ctx context.Context, c store.RetentionCmd) (store.RetentionResult, error)
}

// RetentionJob enforces every stream's configured limits once per tick.
type RetentionJob struct {
	client RetentionClient
	batch  int
}

// NewRetentionJob wires the adapter; batch<=0 lets the store pick its default.
func NewRetentionJob(client RetentionClient, batch int) *RetentionJob {
	return &RetentionJob{client: client, batch: batch}
}

// Name implements Job.
func (j *RetentionJob) Name() string { return "retention" }

// Every implements Job: enforcement runs on every tick.
func (j *RetentionJob) Every() time.Duration { return 0 }

// Run performs ONE bounded slice: a single store command whose own window bounds how
// much leaves this tick. Result.More rides straight through — the store computed it
// against REAL deletions.
func (j *RetentionJob) Run(ctx context.Context, b *Budget) (Result, error) {
	res, err := j.client.Retention(ctx, store.RetentionCmd{Batch: j.batch})
	if err != nil {
		return Result{}, err
	}
	_ = b.Take(res.Deleted)
	return Result{Rows: res.Deleted, Bytes: res.FreedBytes, More: res.More}, nil
}

// ---- reaper ---------------------------------------------------------------------

// ReaperClient resumes interrupted, already-authorised stream deletions.
type ReaperClient interface {
	ReapResume(ctx context.Context) (store.ReapResumeResult, error)
}

// ReaperJob finishes an authorised delete's message chunks in the background, one
// bounded chunk per store call, until the marker table rests or the slice expires.
type ReaperJob struct {
	client ReaperClient
}

// NewReaperJob wires the adapter.
func NewReaperJob(client ReaperClient) *ReaperJob { return &ReaperJob{client: client} }

// Name implements Job.
func (j *ReaperJob) Name() string { return "reaper" }

// Every implements Job: resumption must start promptly after a crash, on every tick.
func (j *ReaperJob) Every() time.Duration { return 0 }

// Run drains chunk-after-chunk while rows are still owed and the budget holds.
func (j *ReaperJob) Run(ctx context.Context, b *Budget) (out Result, rerr error) {
	for {
		if ctx.Err() != nil {
			// Stop cancels; an interrupted authorised reap continues next tick by
			// design and MUST NOT surface as janitor.job_error.
			out.More = true
			return out, nil //nolint:nilerr // cancellation is the expected idle exit, not a failure
		}
		chunk, err := j.client.ReapResume(ctx)
		if err != nil {
			return out, err
		}
		out.Rows += chunk.Removed
		if !chunk.Pending || b.Expired() || !b.Take(chunk.Removed) {
			out.More = chunk.Pending && !b.Expired()
			_ = out
			break
		}
	}
	return out, nil
}

// ---- events ---------------------------------------------------------------------

// TrimPolicy mirrors --event-retention / --event-max-rows for the events job.
type TrimPolicy struct {
	MaxAgeMs int64 // 0 = no age bound
	MaxRows  int64 // 0 = no row ceiling
}

// EventsClient trims the audit journal.
type EventsClient interface {
	TrimEvents(ctx context.Context, c store.TrimEventsCmd) (store.TrimEventsResult, error)
}

// EventsJob keeps the journal inside its two §4.5 bounds, oldest-first, resuming
// leftover work while its budget slice lasts.
type EventsJob struct {
	client EventsClient
	policy TrimPolicy
}

// NewEventsJob wires the adapter.
func NewEventsJob(client EventsClient, policy TrimPolicy) *EventsJob {
	return &EventsJob{client: client, policy: policy}
}

// Name implements Job.
func (j *EventsJob) Name() string { return "events" }

// Every implements Job.
func (j *EventsJob) Every() time.Duration { return 0 }

// Run loops the bounded trim command until quiescent, expired, or cancelled.
func (j *EventsJob) Run(ctx context.Context, b *Budget) (out Result, rerr error) {
	for {
		if ctx.Err() != nil {
			return out, nil //nolint:nilerr // cancellation is the expected quiet exit, not a failure
		}
		res, err := j.client.TrimEvents(ctx, store.TrimEventsCmd{
			MaxAgeMs: j.policy.MaxAgeMs,
			MaxRows:  j.policy.MaxRows,
		})
		if err != nil {
			return out, err
		}
		out.Rows += res.Deleted
		if !res.More || b.Expired() || !b.Take(res.Deleted) {
			out.More = res.More && !b.Expired()
			break
		}
	}
	return out, nil
}

// ---- dedup ----------------------------------------------------------------------

// DedupClient lists streams and sweeps one stream's expired dedup keys.
type DedupClient interface {
	ListStreams(ctx context.Context) ([]string, error)
	SweepDedup(ctx context.Context, stream string) (int64, error)
}

// DedupJob retires #7's bare ticker by making key expiry one scheduled janitor job.
// One run walks at most `limit` streams per tick and ROTATES the starting index, so a
// large namespace gets full coverage across consecutive ticks with bounded work each.
type DedupJob struct {
	client DedupClient
	limit  int
	cursor int // rotation state, owned by the job
}

// DedupCursor bundles construction-time knobs (no positionals at the call site).
type DedupCursor struct{ Start, Limit int }

// NewDedupJob wires the adapter with an explicit rotation cursor; limit<=0 defaults
// to 32 streams per tick.
func NewDedupJob(client DedupClient, cur DedupCursor) *DedupJob {
	limit := cur.Limit
	if limit <= 0 {
		limit = defaultDedupLimit
	}
	return &DedupJob{client: client, limit: limit, cursor: cur.Start}
}

const defaultDedupLimit = 32

// Name implements Job.
func (j *DedupJob) Name() string { return "dedup" }

// Every implements Job.
func (j *DedupJob) Every() time.Duration { return 0 }

// Run sweeps the current rotation window of streams.
func (j *DedupJob) Run(ctx context.Context, b *Budget) (out Result, rerr error) {
	streams, err := j.client.ListStreams(ctx)
	if err != nil {
		return out, err
	}
	n := len(streams)
	if n == 0 {
		return out, nil
	}
	start := j.cursor
	// A single Run covers ONE contiguous un-wrapped window so nothing is swept twice
	// inside a cycle; leftover capacity defers to the next tick.
	bound := j.limit
	if remain := n - start; remain < bound {
		bound = remain
	}
	swept := 0
	for i := 0; i < bound && i < n-start; i++ {
		name := streams[start+i]
		cleared, sweepErr := j.client.SweepDedup(ctx, name)
		if sweepErr != nil {
			j.cursor = start + i // resume from the stream that failed
			return out, sweepErr
		}
		out.Rows += cleared
		swept++
		if b.Expired() {
			break
		}
	}
	j.cursor = (start + swept) % n
	out.More = swept == bound && start+swept < n
	return out, nil
}

// ---- checkpoint -----------------------------------------------------------------

// SoloSubmitter submits writer commands that must run SOLO between commit windows.
type SoloSubmitter interface {
	Do(ctx context.Context, cmd store.Cmd) (store.Result, error)
}

// CheckpointJob passes the WAL down when it grows past --wal-max-bytes: PASSIVE under
// normal load is wasted fsyncs, so below the bound the job does nothing at all.
type CheckpointJob struct {
	W           SoloSubmitter
	WalMaxBytes int64
	WalBytes    func() (int64, error) // read-only seam: current WAL size
}

// Name implements Job.
func (j *CheckpointJob) Name() string { return "checkpoint" }

// Every implements Job: checking a byte gauge is cheap enough for every tick.
func (j *CheckpointJob) Every() time.Duration { return 0 }

// Run checkpoints TRUNCATE when the WAL exceeds the bound.
func (j *CheckpointJob) Run(ctx context.Context, _ *Budget) (Result, error) {
	wal, err := j.WalBytes()
	if err != nil {
		return Result{}, err // a failed stat keeps yesterday's number logic honest: skip this tick
	}
	if wal < j.WalMaxBytes {
		return Result{}, nil
	}
	if _, subErr := j.W.Do(ctx, store.CheckpointCmd{Mode: store.CheckpointTruncate}); subErr != nil {
		return Result{}, subErr
	}
	return Result{}, nil
}

// VacuumStateRead reads the freelist page count around a step.
type freelistFn func() (int64, error)

// VacuumJob runs PRAGMA incremental_vacuum when the freelist has room to give back,
// and SELF-DISABLES with one warning when pages never move — modernc's documented
// driver finding (#27 solo amendment) or an auto_vacuum=NONE directory both land
// there, and neither is worth a writer round-trip every tick forever.
type VacuumJob struct {
	W             SoloSubmitter
	Pages         int        // pages offered per step (--vacuum-pages-per-tick default lives in serve wiring)
	Freelist      freelistFn // before submission: current freelist_count
	FreelistAfter freelistFn

	Log    *slog.Logger
	broken bool // set once after the no-op diagnosis
}

// Name implements Job.
func (j *VacuumJob) Name() string { return "vacuum" }

// Every implements Job: probing after disabling would be free, but the point IS to
// stop submitting — keep the tick cadence, gate the work.
func (j *VacuumJob) Every() time.Duration { return 0 }

// Run probes the freelist, takes one bounded step, and diagnoses no-op steps.
func (j *VacuumJob) Run(ctx context.Context, _ *Budget) (Result, error) {
	if j.broken {
		return Result{}, nil
	}
	before, pErr := j.Freelist()
	if pErr != nil {
		return Result{}, pErr
	}
	if before == 0 {
		return Result{}, nil // nothing to give back: zero-cost idle path
	}
	if _, sErr := j.W.Do(ctx, store.VacuumCmd{Pages: j.Pages}); sErr != nil {
		return Result{}, sErr
	}
	after, aErr := j.FreelistAfter()
	if aErr != nil {
		return Result{}, aErr
	}
	freed := before - after
	if freed <= 0 {
		j.broken = true
		if j.Log != nil {
			j.Log.Warn("janitor.vacuum_disabled",
				"pages_before", before, "pages_after", after,
				"reason", "incremental_vacuum moved no pages; auto_vacuum dir type mismatch or driver limitation")
		}
	}
	return Result{}, nil
}
