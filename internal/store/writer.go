// SPDX-License-Identifier: Apache-2.0

package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/a-holm/messq/internal/clock"
	"github.com/a-holm/messq/internal/errs"
	"github.com/a-holm/messq/internal/obs"
)

// The group-commit engine (issue #6, PLAN §4.3). One goroutine — the Writer — owns the sole
// read-write SQLite handle and nothing else in the process may touch it: HTTP handlers,
// sweeper and janitor submit [Cmd] values through [Writer.Do] and wait. That single
// constraint buys group commit (N commands share one fsync), the absence of the SQLITE_BUSY
// class (writes are serialised in the application), atomic multi-table transitions, and a
// concurrency model small enough for -race to cover completely (PLAN §3.2).
//
// The engine's rules, each pinned by a test named for it:
//
//   - A batch is never empty; an idle writer holds no timers and burns no CPU.
//   - No caller observes success before that batch's COMMIT returned nil.
//   - A business rejection ([CmdErr]) rolls back exactly its own savepoint; any other error
//     aborts the whole batch and latches the process read-only — the fsyncgate rule, never
//     retried (ADR-0005).
//   - Events reach the sink only after the commit that produced them is real.

// CmdKind labels a command for logs and diagnostics. It is an open vocabulary on purpose:
// feature issues (#7 publish, #9 claim, #10 settle, #11 sweep, #12 dead-letter) add their own
// spellings, and the engine never switches on it — it is carried, never interpreted.
type CmdKind string

// Result is the command-specific outcome of a successful apply. The engine carries it as
// `any`; typed wrappers at the feature layer assert the concrete shape.
type Result any

// Cmd is one unit of durable work, applied by the writer goroutine inside a batch
// transaction. Implementations live with their features; this file owns only the contract:
//
//   - Apply runs INSIDE the batch transaction, on the writer goroutine. It MUST NOT block,
//     sleep, read the wall clock, do network or file I/O, or call Writer.Do — the batch's
//     single timestamp arrives as now, and every row the batch writes shares it.
//   - A returned error wrapping a business sentinel via [CmdErr] rejects exactly this
//     command: its savepoint rolls back and its siblings still commit.
//   - Any other error — driver failure, constraint violation messq did not predict, panic —
//     aborts the whole batch and latches the process read-only (the fsyncgate rule): the
//     transaction state is no longer trustworthy enough to keep writing on top of.
type Cmd interface {
	// Kind is the closed-set label used for log fields. Never a message id or subject.
	Kind() CmdKind

	// Apply applies the command against tx. See the type contract above.
	Apply(ctx context.Context, tx *sql.Tx, now time.Time) (Result, []obs.Event, error)

	// Bytes estimates the payload this command will write, for the batch byte budget.
	// 0 for metadata-only commands. An oversized single command still commits alone: the
	// budget closes batches, it never rejects commands.
	Bytes() int
}

// CmdError marks an error as a BUSINESS rejection: dedup conflicts, ErrTooLarge,
// ErrStaleAck, ErrNotFound, ErrFlowControl — outcomes the caller is meant to see verbatim,
// rolled back to their own savepoint while siblings commit. Wrap once, at the return site:
//
//	return store.CmdErr(errs.ErrTooLarge)
//
// Anything not wrapped this way is treated as infrastructure damage and takes the whole
// batch down with it.
type CmdError struct {
	err error
}

// Error renders the wrapped error unchanged: the message a caller sees is the sentinel's.
func (e *CmdError) Error() string { return e.err.Error() }

// Unwrap keeps errors.Is and errors.As working through the marker to the original sentinel.
func (e *CmdError) Unwrap() error { return e.err }

// CmdErr marks err as a business rejection. Wrapping a nil produces nil, so a command can
// pass its error through unwrapped: `return res, evs, maybeCmdErr(err)`.
func CmdErr(err error) error {
	if err == nil {
		return nil
	}
	return &CmdError{err: err}
}

// IsCmdError reports whether err was marked as a business rejection by [CmdErr].
func IsCmdError(err error) bool {
	var ce *CmdError
	return errors.As(err, &ce)
}

// FatalError is the one fault the fsyncgate rule produces. It is emitted exactly once per
// process (the first error wins), logged as storage.fatal at ERROR, and surfaced on
// Writer.Fatal for the process supervisor — which exits non-zero after Config.FatalDrain of
// read-serving (exit code contract confirmed in #17; EX_IOERR proposed).
type FatalError struct {
	Op    string // "begin" | "apply" | "savepoint" | "release" | "commit"
	Err   error  // the underlying driver error, wrapped; errno preserved when present
	Class string // "eio" | "enospc" | "corrupt" | "unknown"
	At    time.Time
	Batch int // commands in the doomed batch
}

// Error makes FatalError printable in operator output.
func (e *FatalError) Error() string {
	return e.Err.Error()
}

// Unwrap reaches the driver error so callers can match errnos through it.
func (e *FatalError) Unwrap() error { return e.Err }

// classify names the storage-fault family of err for storage.fatal logs and the
// messq_commit_errors_total{class} label. Errno-bearing errors are matched through
// errors.Is so arbitrary wrapping does not hide them; SQLite's textual signatures cover the
// errno-less spellings modernc emits; anything else is unknown.
func classify(err error) string {
	if err == nil {
		return "unknown"
	}
	// Errno-bearing errors first: errors.Is sees through any wrapping layers.
	switch {
	case errors.Is(err, syscall.EIO):
		return "eio"
	case errors.Is(err, syscall.ENOSPC):
		return "enospc"
	}
	// SQLite's errno-less textual fallbacks (SQLITE_IOERR, SQLITE_CORRUPT, SQLITE_FULL as
	// spelled by sqlite3_errmsg). Lower-cased substring matches on the driver's own words;
	// deliberately conservative — unknown is always the safe label.
	msg := strings.ToLower(err.Error())
	switch {
	case strings.Contains(msg, "i/o error"), strings.Contains(msg, "input/output error"):
		return "eio"
	case strings.Contains(msg, "no space left"), strings.Contains(msg, "disk is full"):
		return "enospc"
	case strings.Contains(msg, "corrupt"), strings.Contains(msg, "malformed"),
		strings.Contains(msg, "not a database"), strings.Contains(msg, "encrypted"):
		return "corrupt"
	}
	return "unknown"
}

// Config configures a [Writer]. Zero values mean "default", except CommitWindow where zero
// is a documented setting (commit whatever is queued immediately); the flag layer owns the
// production default of 2ms.
type Config struct {
	// Durability selects synchronous=FULL (full) or synchronous=NORMAL (relaxed); zero is
	// full. NewWriter verifies the live PRAGMA synchronous against this mode and refuses to
	// start on mismatch.
	Durability Durability

	// CommitWindow lingers for more arrivals before closing a batch. 0 = commit whatever is
	// queued right now. Negative values are refused.
	CommitWindow time.Duration

	// CommitMaxBatch caps commands per transaction; <= 0 means 256.
	CommitMaxBatch int

	// CommitMaxBytes caps the estimated payload bytes per transaction; <= 0 means 8 MiB.
	CommitMaxBytes int64

	// QueueDepth bounds the command channel. A full channel blocks the caller: that is
	// backpressure, propagated to the socket. <= 0 means 2048.
	QueueDepth int

	// FatalDrain is how long the supervisor keeps serving reads after a fatal latch before
	// exiting; <= 0 means 2s. Consumed by #17's signal handling.
	FatalDrain time.Duration
}

const (
	defaultCommitMaxBatch = 256
	defaultCommitMaxBytes = int64(8) << 20
	defaultQueueDepth     = 2048
	defaultFatalDrain     = 2 * time.Second
)

// fillDefaults replaces unset budgets with their documented defaults and refuses a negative
// commit window. Budget fields are defaulted even when the window is refused, so a caller
// that mishandles the error still cannot run unbounded.
func (c *Config) fillDefaults() error {
	if c.CommitMaxBatch <= 0 {
		c.CommitMaxBatch = defaultCommitMaxBatch
	}
	if c.CommitMaxBytes <= 0 {
		c.CommitMaxBytes = defaultCommitMaxBytes
	}
	if c.QueueDepth <= 0 {
		c.QueueDepth = defaultQueueDepth
	}
	if c.FatalDrain <= 0 {
		c.FatalDrain = defaultFatalDrain
	}
	if c.CommitWindow < 0 {
		return fmt.Errorf("%w: commit window %v is negative", errs.ErrBadRequest, c.CommitWindow)
	}
	return nil
}

// warnRelaxedDurability is the slog message of the relaxed-mode banner. It is logged exactly
// once per process, at Writer construction, on whatever logger the store carries: never
// suppressible, never sampled (D11's loud-mode rule).
const warnRelaxedDurability = "durability=relaxed"

// eventBufferBatches bounds the fan-out pump's queue in batches (not events): even a burst of
// max-size batches cannot wedge the writer — overflow drops loudly instead.
const eventBufferBatches = 64

// request is one submitted command travelling to the writer goroutine and back. done closes
// when res/err hold the final answer; waiting on it — not on ctx — is what makes an accepted
// command un-cancellable.
type request struct {
	cmd  Cmd
	res  Result
	err  error
	done chan struct{}
}

// writerOptions collects everything NewWriter takes besides the handle, clock and Config.
type writerOptions struct {
	observer obs.CommitObserver
	sink     obs.Sink
	logger   *slog.Logger
	nodeID   string
	hooks    hooks
}

// WriterOption customises a [Writer] at construction.
type WriterOption interface {
	apply(*writerOptions)
}

type writerOptionFunc func(*writerOptions)

func (f writerOptionFunc) apply(o *writerOptions) { f(o) }

// WithCommitObserver routes commit observations to o. Default: discard.
func WithCommitObserver(o obs.CommitObserver) WriterOption {
	return writerOptionFunc(func(wo *writerOptions) { wo.observer = o })
}

// WithEventSink routes committed events to s. Default: discard (obs.NopSink).
func WithEventSink(s obs.Sink) WriterOption {
	return writerOptionFunc(func(wo *writerOptions) { wo.sink = s })
}

// withLogger sets the slog logger carrying storage.fatal and the relaxed banner. Used by
// Store.NewWriter and tests; production default outside the store layer is slog.Default().
func withLogger(l *slog.Logger) WriterOption {
	return writerOptionFunc(func(wo *writerOptions) { wo.logger = l })
}

// withNodeID stamps the store's node identity onto storage.fatal lines so an operator can
// correlate them with recovery logs.
func withNodeID(id string) WriterOption {
	return writerOptionFunc(func(wo *writerOptions) { wo.nodeID = id })
}

// withEventSink routes committed events to s (test injection twin of [WithEventSink]).
func withEventSink(s obs.Sink) WriterOption {
	return writerOptionFunc(func(wo *writerOptions) { wo.sink = s })
}

// hooks are the fault points reserved for #32's messq_fault grammar; production builds leave
// them nil. The reserved string names map one to one onto the fields:
//
//	store.tx.before_apply              — after BEGIN IMMEDIATE, before the first SAVEPOINT
//	store.tx.after_commit_before_reply — after COMMIT returned nil, before callers unblock
//
// (store.tx.before_commit joins with the fsyncgate slice.) beforeCommit-style injection of a
// commit error uses before_commit; this slice only needs before_apply as the one-transaction
// observation point the batch-rule tests drive.
type hooks struct {
	beforeApply            func()
	afterCommitBeforeReply func()
}

// withHooks installs the test/fault seams. Test-only; production code never calls it.
func withHooks(h hooks) WriterOption {
	return writerOptionFunc(func(wo *writerOptions) { wo.hooks = h })
}

// Writer is the single-writer group-commit engine. Construct with [Store.NewWriter] (the
// blessed path — the rw handle never leaves the package) or [NewWriter] with a hand-taken
// handle. All methods are safe for concurrent use.
type Writer struct {
	rw   *sql.DB // the ONLY reference in the process; handed over once by the Store
	clk  clock.Clock
	cfg  Config
	ch   chan *request // bounded; cap = QueueDepth; full channel == backpressure
	log  *slog.Logger
	node string

	obsrv  obs.CommitObserver
	events obs.Sink
	hooks  hooks

	fatalC   chan *FatalError // buffered 1: the supervisor may not be selecting yet
	closing  atomic.Bool
	lastNow  atomic.Int64  // monotonic guard for batch timestamps (UnixNano)
	done     chan struct{} // closed when run has returned
	stop     chan struct{} // closed by Close; run drains what is queued before exiting
	evCh     chan []obs.Event
	pumpDone chan struct{}
	closeOne sync.Once
}

// NewWriter builds the engine over rw and starts its goroutines. It refuses construction —
// before anything is spawned — unless the preconditions that make single-writer safety real
// hold: MaxOpenConns is exactly 1, PRAGMA journal_mode reads wal, and PRAGMA synchronous
// reads back the value cfg.Durability demands. A mismatch names both values and wraps
// [ErrPragmaMismatch]: refuse to start, never silently downgrade (D1).
func NewWriter(rw *sql.DB, clk clock.Clock, cfg Config, opts ...WriterOption) (*Writer, error) {
	if rw == nil {
		return nil, errors.New("store: NewWriter needs a non-nil rw handle")
	}
	if clk == nil {
		return nil, errors.New("store: NewWriter needs a non-nil clock")
	}
	if err := cfg.fillDefaults(); err != nil {
		return nil, err
	}
	if got := rw.Stats().MaxOpenConnections; got != 1 {
		return nil, fmt.Errorf("%w: writer requires SetMaxOpenConns(1), pool reports %d",
			errs.ErrBadRequest, got)
	}
	if err := verifyDurabilityPragmas(rw, cfg.Durability); err != nil {
		return nil, err
	}

	o := &writerOptions{observer: obs.NopCommitObserver{}, sink: obs.NopSink{}, logger: slog.Default()}
	for _, opt := range opts {
		opt.apply(o)
	}

	w := &Writer{
		rw:       rw,
		clk:      clk,
		cfg:      cfg,
		ch:       make(chan *request, cfg.QueueDepth),
		log:      o.logger,
		node:     o.nodeID,
		obsrv:    o.observer,
		events:   o.sink,
		hooks:    o.hooks,
		fatalC:   make(chan *FatalError, 1),
		done:     make(chan struct{}),
		stop:     make(chan struct{}),
		evCh:     make(chan []obs.Event, eventBufferBatches),
		pumpDone: make(chan struct{}),
	}
	if cfg.Durability == DurabilityRelaxed {
		w.log.Warn(warnRelaxedDurability,
			"guarantee", "survives SIGKILL",
			"risk", "acknowledged writes may be lost on power loss or kernel panic (never corrupted)",
			"hint", "use --durability=full for the strong guarantee")
	}
	go w.run()
	go w.pumpEvents()
	return w, nil
}

// verifyDurabilityPragmas reads journal_mode and synchronous back from the live pool's
// connection and refuses on any disagreement with the configured mode. This is the second
// half of D1's pragma defence: #5's connection hook verifies per pooled connection at open;
// here the engine re-reads through database/sql at construction, so a hook regression or a
// mismatched DSN cannot reach a running writer.
func verifyDurabilityPragmas(rw *sql.DB, d Durability) error {
	ctx := context.Background()

	var journal string
	if err := rw.QueryRowContext(ctx, `PRAGMA journal_mode`).Scan(&journal); err != nil {
		return fmt.Errorf("read back PRAGMA journal_mode: %w", err)
	}
	if normalizePragmaValue(journal) != "wal" {
		return fmt.Errorf("%w: PRAGMA journal_mode=%s, want wal", ErrPragmaMismatch, journal)
	}

	var synchronous int
	if err := rw.QueryRowContext(ctx, `PRAGMA synchronous`).Scan(&synchronous); err != nil {
		return fmt.Errorf("read back PRAGMA synchronous: %w", err)
	}
	want := d.Synchronous()
	if synchronous != want {
		return fmt.Errorf("%w: --durability=%s demands PRAGMA synchronous=%s(%d), connection reports %s(%d)",
			ErrPragmaMismatch, d, synchronousWord(d), want,
			synchronousObservedWord(synchronous), synchronous)
	}
	return nil
}

// synchronousObservedWord renders the live value the same way the demanded one is spelled,
// so the mismatch line reads as a comparison rather than two dialects.
func synchronousObservedWord(v int) string {
	switch v {
	case 0:
		return "OFF"
	case 1:
		return "NORMAL"
	case 2:
		return "FULL"
	case 3:
		return "EXTRA"
	default:
		return strconv.Itoa(v)
	}
}

// Fatal returns the channel delivering the fsyncgate's one fatal error. It is buffered, so
// the writer never blocks on a supervisor that has not started selecting; a supervisor that
// selects later still receives it.
func (w *Writer) Fatal() <-chan *FatalError {
	return w.fatalC
}

// Do submits one command to the writer and waits for its batch's outcome. The two waits are
// deliberately different:
//
//  1. Enqueueing is cancellable — a caller whose context expires before the command reached
//     the queue gets ctx.Err() and the command never runs.
//  2. Waiting is NOT cancellable in effect: once accepted, the command always runs (SEMANTICS
//     S2.3's total order must not develop holes for disconnected clients). A caller that
//     stops waiting gets ErrCommitUnknown — the write may or may not have happened, which is
//     exactly what idempotent retries with Messq-Msg-Id (#7) are for.
//
// A full channel blocks here: that is backpressure, propagating to the socket (PLAN §3.2).
func (w *Writer) Do(ctx context.Context, cmd Cmd) (Result, error) {
	if cmd == nil {
		return nil, fmt.Errorf("%w: nil command", errs.ErrBadRequest)
	}
	if w.closing.Load() {
		return nil, ErrWriterClosing
	}
	r := &request{cmd: cmd, done: make(chan struct{})}
	select {
	case w.ch <- r:
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-w.done:
		return nil, ErrWriterClosing
	}
	select {
	case <-r.done:
	case <-ctx.Done():
		return nil, fmt.Errorf("%w: %w", ErrCommitUnknown, ctx.Err())
	}
	return r.res, r.err
}

// run is the writer goroutine: it assembles batches and commits them, in enqueue order,
// until Close signals stop — and even then only after every already-queued command has been
// committed (drain-before-exit). A batch is never empty: takeBatch blocks for the first
// command, so an idle writer holds no timers and burns no CPU.
//
// w.ch is never closed: Close is a stop signal plus a drain, which removes the entire
// send-on-closed-channel class instead of racing it.
func (w *Writer) run() {
	defer close(w.done)
	batch := make([]*request, 0, w.cfg.CommitMaxBatch)
	var pending *request // an over-budget command held for the NEXT batch
	for {
		assembled, next, ok := w.takeBatch(batch[:0], pending)
		if !ok {
			return // stopped and fully drained
		}
		pending = next
		w.obsrv.ObserveQueueDepth(len(w.ch))
		w.commitBatch(assembled)
	}
}

// takeBatch assembles one batch under the three closing rules — commit-window,
// commit-max-batch, commit-max-bytes, whichever fires first:
//
//   - It blocks for the first command (or reports not-ok when stopped with an empty queue),
//     so a batch is never empty and no fsync is ever spent on nothing.
//   - With a positive CommitWindow it lingers on the clock seam: the fill select prefers the
//     channel while arrivals are queued, so under load batching self-clocks and the window
//     barely matters; at low rate a lone caller pays at most one window.
//   - With CommitWindow == 0 it drains whatever is queued right now — the documented
//     low-latency setting that still batches under load, for the same self-clocking reason.
//
// A command whose bytes would push the batch past CommitMaxBytes is held as `next` and opens
// the following batch; a lone oversized command therefore still commits alone — the budget
// closes batches, it never rejects commands.
func (w *Writer) takeBatch(batch []*request, held *request) ([]*request, *request, bool) {
	first := held
	if first == nil {
		select {
		case first = <-w.ch:
		case <-w.stop:
			// Draining: accept what is already queued, else tell run to exit.
			select {
			case first = <-w.ch:
			default:
				return nil, nil, false
			}
		}
	}
	batch = append(batch, first)
	bytes := int64(first.cmd.Bytes())

	fill := func(r *request) bool {
		rb := int64(r.cmd.Bytes())
		if len(batch) > 0 && bytes+rb > w.cfg.CommitMaxBytes {
			return false // over budget: hold for the next batch
		}
		batch = append(batch, r)
		bytes += rb
		return true
	}

	if w.cfg.CommitWindow <= 0 {
		for len(batch) < w.cfg.CommitMaxBatch {
			select {
			case r := <-w.ch:
				if !fill(r) {
					return batch, r, true
				}
			default:
				return batch, nil, true
			}
		}
		return batch, nil, true
	}

	timer := w.clk.NewTimer(w.cfg.CommitWindow)
	defer func() { timer.Stop() }()
fill:
	for len(batch) < w.cfg.CommitMaxBatch {
		// Prefer already-queued arrivals over the deadline: lingering happens only when the
		// queue is empty. A bare two-case select would choose RANDOMLY between a queued
		// command and a fired deadline, splitting batches arbitrarily under load.
		select {
		case r := <-w.ch:
			if !fill(r) {
				return batch, r, true
			}
			continue
		default:
		}
		select {
		case r := <-w.ch:
			if !fill(r) {
				return batch, r, true
			}
		case <-timer.C():
			break fill
		}
	}
	return batch, nil, true
}

// commitBatch applies one assembled batch inside a single BEGIN IMMEDIATE transaction: one
// fsync for N commands. Every command runs behind its own SAVEPOINT; the single batch
// timestamp comes from batchNow and reaches Apply unchanged, so all rows of a batch share it.
// Replies close strictly after COMMIT returned nil; events reach the pump after that.
func (w *Writer) commitBatch(batch []*request) {
	if w.hooks.beforeApply != nil {
		w.hooks.beforeApply() // store.tx.before_apply (#32): one firing == one transaction
	}
	now := w.batchNow()
	// NOT a caller's context: one client disconnecting must never abort a 256-command batch.
	ctx := context.Background()

	tx, err := w.rw.BeginTx(ctx, nil)
	if err != nil {
		w.abortBatch(batch, "begin", err)
		return
	}
	// Any exit below runs the transaction down explicitly: an abandoned tx would hold the
	// write lock until GC finalizes the driver object, wedging whoever comes next. A
	// rollback failure on an already-doomed batch is logged, not acted on.
	ok := false
	defer func() {
		if !ok {
			if rbErr := tx.Rollback(); rbErr != nil {
				w.log.Warn("writer.rollback", "node", w.node, "error", rbErr.Error())
			}
		}
	}()

	events := make([]obs.Event, 0, len(batch))
	for i, r := range batch {
		sp := "s" + strconv.Itoa(i)
		if _, spErr := tx.ExecContext(ctx, "SAVEPOINT "+sp); spErr != nil {
			w.abortBatch(batch, "savepoint", spErr)
			return
		}
		res, evs, applyErr := w.applyCommand(ctx, tx, r.cmd, now)
		switch applyErr {
		case nil:
			if _, relErr := tx.ExecContext(ctx, "RELEASE "+sp); relErr != nil {
				w.abortBatch(batch, "release", relErr)
				return
			}
			r.res = res
			events = append(events, evs...)
		default:
			if !IsCmdError(applyErr) {
				// Infrastructure damage: the batch state is not trustworthy.
				w.abortBatch(batch, "apply", applyErr)
				return
			}
			// Business rejection: undo exactly this command, keep its siblings.
			if _, rbErr := tx.ExecContext(ctx, "ROLLBACK TO "+sp); rbErr != nil {
				w.abortBatch(batch, "rollback_to", rbErr)
				return
			}
			if _, relErr := tx.ExecContext(ctx, "RELEASE "+sp); relErr != nil {
				w.abortBatch(batch, "release", relErr)
				return
			}
			r.err = applyErr
			close(r.done) // this waiter is finished; the batch continues
		}
	}

	start := w.clk.Now()
	err = tx.Commit()
	dur := w.clk.Since(start)
	w.obsrv.ObserveCommit(len(batch), dur, err)
	if err != nil {
		w.commitFailed(batch, err)
		return
	}
	ok = true

	if w.hooks.afterCommitBeforeReply != nil {
		w.hooks.afterCommitBeforeReply() // store.tx.after_commit_before_reply (#32)
	}
	for _, r := range batch { // 1. replies — callers unblock (rejected ones already left)
		if r.err == nil {
			close(r.done)
		}
	}
	if len(events) > 0 { // 2. fan-out — off the latency path, never blocking
		select {
		case w.evCh <- events:
		default:
			w.log.Warn("events.dropped",
				"node", w.node,
				"reason", "fan-out queue overflow; the events table remains complete")
		}
	}
}

// commitFailed answers every still-waiting caller of a batch whose COMMIT failed. This is
// the fsyncgate seam; the full classify-latch-refuse machinery is the next slice. Today it
// fails the batch as UNKNOWN — which the acceptance list demands regardless of latching,
// because a failed commit is never a definite no.
func (w *Writer) commitFailed(batch []*request, cause error) {
	w.failAll(batch, fmt.Errorf("%w: commit failed: %w", ErrCommitUnknown, cause))
}

// applyCommand runs one command body, guarding the writer against a panicking command: a bug
// in a command must never take down the goroutine. (Recovery-to-latch semantics land with
// the fault-injection slice.)
func (w *Writer) applyCommand(ctx context.Context, tx *sql.Tx, cmd Cmd, now time.Time) (res Result, evs []obs.Event, err error) {
	return cmd.Apply(ctx, tx, now)
}

// batchNow returns the batch's single wall-clock timestamp, guarded to be non-decreasing
// across batches within this process lifetime: a backwards NTP step must never make stored
// timestamps go backwards (PLAN §11 clock-jump family). Ties are expected and harmless —
// ordering is by seq, never by timestamp. Deadlines and the commit window run on monotonic
// durations via the Clock seam and are unaffected by any of this.
func (w *Writer) batchNow() time.Time {
	nano := w.clk.Now().UnixNano()
	for {
		last := w.lastNow.Load()
		if nano < last {
			nano = last
		}
		if w.lastNow.CompareAndSwap(last, nano) {
			return time.Unix(0, nano)
		}
	}
}

// abortBatch fails every request in a doomed batch with ErrCommitUnknown — never a definite
// failure: SQLite rolls back on our way out, but a begin/commit error can also mean the work
// landed. The latching classification of these errors is the fsyncgate slice's subject.
func (w *Writer) abortBatch(batch []*request, op string, cause error) {
	w.log.Warn("writer.batch_aborted", "node", w.node, "op", op, "error", cause.Error())
	w.failAll(batch, fmt.Errorf("%w: %s failed: %w", ErrCommitUnknown, op, cause))
}

// failAll answers every waiter in the batch with err exactly once each.
func (w *Writer) failAll(batch []*request, err error) {
	for _, r := range batch {
		r.err = err
		close(r.done)
	}
}

// pumpEvents is the fan-out pump: committed batches arrive here strictly post-commit and are
// handed to the sink off the reply path. The sink MUST NOT block (obs.Sink contract); a slow
// one therefore delays projections, never replies.
func (w *Writer) pumpEvents() {
	defer close(w.pumpDone)
	for evs := range w.evCh {
		w.events.Publish(evs)
	}
}

// Close stops the writer: new submissions are refused, already-accepted ones drain and
// commit, then the fan-out pump finishes its backlog, the goroutines exit and the rw handle
// is closed. It is idempotent. The bounded-drain refinement is a later slice's subject; this
// slice guarantees termination.
//
// Ordering matters here: evCh is closed only after run has returned — its commitBatch is the
// channel's only sender, so closing earlier would race a post-reply event hand-off.
func (w *Writer) Close(ctx context.Context) error {
	w.closing.Store(true)
	w.closeOne.Do(func() {
		close(w.stop)
	})
	<-w.done
	close(w.evCh)
	<-w.pumpDone
	if err := w.rw.Close(); err != nil {
		return fmt.Errorf("close writer handle: %w", err)
	}
	return nil
}
