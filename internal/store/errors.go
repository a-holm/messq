// SPDX-License-Identifier: Apache-2.0

package store

import (
	"errors"
	"fmt"

	"github.com/a-holm/messq/internal/errs"
)

// The store-local sentinel set. These carry the storage-specific failure modes that the core
// taxonomy in internal/errs deliberately does not know about; each maps into the CLI's
// teaching-error format (SEMANTICS §8), so callers above the store wrap them with context
// instead of replacing them.
//
// Where a failure mode already exists in the core set, the sentinel here wraps it: matching
// either name works with errors.Is, and the CLI exit-code mapping (#23) keyed on internal/errs
// fires without a second registry entry.
var (
	// ErrDataDirLocked is raised when the LOCK file in <data-dir> is held by another process.
	// It wraps [errs.ErrLocked]: "a data directory held by another process" is already part of
	// the user-facing vocabulary.
	ErrDataDirLocked = fmt.Errorf("%w", errs.ErrLocked)

	// ErrDataDirPerms is raised at startup when <data-dir> or messq.db carry group/other bits,
	// or the directory is owned by another user. Secrets at rest demand 0700/0600.
	ErrDataDirPerms = errors.New("data directory or database file permissions are too broad")

	// ErrSchemaTooNew is raised when the applied schema version exceeds what this binary
	// ships. It wraps [errs.ErrSchemaNewer]: downgrading a live deployment is refused, never
	// guessed around.
	ErrSchemaTooNew = fmt.Errorf("%w", errs.ErrSchemaNewer)

	// ErrMigrationDrift is raised when an already-applied migration file no longer matches its
	// recorded checksum: the binary running is not the binary that migrated this directory.
	ErrMigrationDrift = errors.New("an already-applied migration file has changed")

	// ErrPragmaMismatch is raised by the connection hook when a pragma read back from a pooled
	// connection differs from the enforced expectation — the "silently synchronous=NORMAL"
	// trap D1 exists to kill.
	ErrPragmaMismatch = errors.New("pragma read back with an unexpected value")

	// ErrCorrupt is raised when quick_check/integrity_check reports damage during startup
	// recovery.
	ErrCorrupt = errors.New("integrity check failed")

	// ErrWriterTaken is returned by the second TakeWriter call. Exactly one owner (the #6
	// writer goroutine) holds the rw handle; the rule is enforced, not documented.
	ErrWriterTaken = errors.New("read-write handle already taken")

	// ErrCommitUnknown is returned to every caller of a batch whose COMMIT failed, and to a
	// caller that stopped waiting before its commit landed. It is deliberately never a
	// definite failure: SQLite rolls back on a failed commit, but the error can also arrive
	// after the fsync completed, so "it did not happen" is a claim messq refuses to make.
	// Callers retry idempotently (the Messq-Msg-Id dedup exists for exactly this); the API
	// layer (#14) maps it to a 5xx whose documented meaning is unknown-outcome.
	ErrCommitUnknown = errors.New("commit outcome unknown: the batch may or may not be durable")

	// ErrWriterLatched wraps [errs.ErrReadOnly] when the fsyncgate has fired: the process
	// latched read-only after an unrecoverable storage fault and refuses every further
	// write, including ack-class commands. Matching either name works with errors.Is.
	ErrWriterLatched = fmt.Errorf("%w", errs.ErrReadOnly)

	// ErrWriterClosing wraps [errs.ErrShuttingDown] for commands offered to a Writer whose
	// Close has begun (or finished): nothing was applied and nothing will be.
	ErrWriterClosing = fmt.Errorf("%w", errs.ErrShuttingDown)
)
