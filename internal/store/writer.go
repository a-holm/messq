// SPDX-License-Identifier: Apache-2.0

package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"syscall"
	"time"

	"github.com/a-holm/messq/internal/errs"
	"github.com/a-holm/messq/internal/obs"
)

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
