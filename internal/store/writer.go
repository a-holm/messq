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

// writerOptions collects everything NewWriter takes besides the handle, clock and Config.
type writerOptions struct {
	observer obs.CommitObserver
	sink     obs.Sink
	logger   *slog.Logger
	nodeID   string
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

// Writer is the single-writer group-commit engine. Construct with [Store.NewWriter] (the
// blessed path — the rw handle never leaves the package) or [NewWriter] with a hand-taken
// handle. All methods are safe for concurrent use.
type Writer struct {
	rw   *sql.DB // the ONLY reference in the process; handed over once by the Store
	clk  clock.Clock
	cfg  Config
	log  *slog.Logger
	node string

	obsrv  obs.CommitObserver
	events obs.Sink

	fatalC   chan *FatalError // buffered 1: the supervisor may not be selecting yet
	closing  atomic.Bool
	done     chan struct{} // closed when run has returned
	stop     chan struct{} // closed by Close to retire the goroutines
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
		log:      o.logger,
		node:     o.nodeID,
		obsrv:    o.observer,
		events:   o.sink,
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

// eventBufferBatches bounds the fan-out pump's queue in batches (not events): even a burst of
// max-size batches cannot wedge the writer — overflow drops loudly instead (S9's rule).
const eventBufferBatches = 64

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

// run is the writer goroutine: batch assembly lands here in the next slice. For now it
// waits for Close's stop signal; requests cannot exist yet because Do does not.
func (w *Writer) run() {
	defer close(w.done)
	<-w.stop
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
// commit, then the goroutines exit and the rw handle is closed. It is idempotent. The drain
// semantics (bounded by ctx, behaviour after a fatal latch) are a later slice's subject;
// this slice guarantees only termination.
func (w *Writer) Close(ctx context.Context) error {
	w.closing.Store(true)
	w.closeOne.Do(func() {
		close(w.stop)
		close(w.evCh)
	})
	<-w.done
	<-w.pumpDone
	if err := w.rw.Close(); err != nil {
		return fmt.Errorf("close writer handle: %w", err)
	}
	return nil
}
