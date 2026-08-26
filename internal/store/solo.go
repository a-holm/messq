// SPDX-License-Identifier: Apache-2.0

package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/a-holm/messq/internal/errs"
	"github.com/a-holm/messq/internal/obs"
)

// The #27 Solo amendment (§3, G6): housekeeping PRAGMAs are SOLO commands. They run on
// the writer goroutine, alone, OUTSIDE every batch transaction, strictly between commit
// windows — because wal_checkpoint and incremental_vacuum cannot run inside a transaction
// at all, and because a second read-write connection running them could trip #6's
// BEGIN IMMEDIATE into SQLITE_BUSY and latch the broker read-only. Housekeeping must
// never be able to declare a storage fatality.
//
// Error classification is the load-bearing half: a failure here is replied to the caller
// and logged, NEVER routed through doomed/doomedCommit. A SQLITE_BUSY checkpoint means a
// reader is holding the WAL back; the janitor retries next tick and, after enough
// consecutive starves, logs store.checkpoint_starved. Killing readers or latching over a
// busy checkpoint would turn one long `messq verify --deep` into an outage.

// Solo marks a command that runs alone, outside every batch transaction. The writer
// intercepts Solo values on dequeue (and mid-assembly, where they close the batch they
// arrived during) and executes them via ApplySolo against the raw rw handle. The set of
// solo commands is deliberately tiny: anything that must not be batched should be rare,
// bounded, and worth its own fsync story.
type Solo interface {
	Cmd

	// Solo is the marker. It takes no arguments and returns nothing: the type is the
	// contract, the writer owns the execution rules.
	Solo()

	// ApplySolo runs against the raw read-write handle — no transaction wraps it, which
	// is precisely what PRAGMAs require. Implementations MUST be bounded in time: they
	// occupy the writer goroutine between commit windows.
	ApplySolo(ctx context.Context, rw *sql.DB, now time.Time) (Result, []obs.Event, error)
}

// isSolo reports whether c travels outside the batch machinery.
func isSolo(c Cmd) bool {
	_, ok := c.(Solo)
	return ok
}

// Checkpoint modes, verbatim PRAGMA spellings.
const (
	CheckpointPassive  = "PASSIVE"
	CheckpointTruncate = "TRUNCATE"
)

// The writer command vocabulary this file adds.
const (
	kindCheckpoint CmdKind = "checkpoint"
	kindVacuum     CmdKind = "vacuum"
)

// CheckpointCmd asks the writer connection to run PRAGMA wal_checkpoint(Mode).
// PASSIVE checkpoints whatever it can without waiting; TRUNCATE additionally resets the
// WAL file to zero length once every reader is out of the frames. A checkpoint that
// cannot complete because readers hold the WAL replies Busy=true — a starved attempt the
// caller counts, not an error.
type CheckpointCmd struct {
	Mode string // CheckpointPassive | CheckpointTruncate
}

// Kind labels the command for logs: "checkpoint".
func (c CheckpointCmd) Kind() CmdKind { return kindCheckpoint }

// Bytes is metadata-only: the command writes no rows of its own.
func (c CheckpointCmd) Bytes() int { return 0 }

// Solo is the marker; see the package-level Solo contract.
func (c CheckpointCmd) Solo() {}

// Apply refuses the batch path loudly. A checkpoint inside a batch transaction would
// either fail outright (no PRAGMA wal_checkpoint in-tx) or silently degrade — both worse
// than a visible business rejection that leaves its siblings untouched. The type system
// makes reaching this hard; the refusal makes misuse unmistakable if it happens anyway.
func (c CheckpointCmd) Apply(context.Context, *sql.Tx, time.Time) (Result, []obs.Event, error) {
	return nil, nil, CmdErr(errors.New(
		"store: CheckpointCmd is a Solo command; it must never run inside a batch"))
}

// CheckpointResult reports one checkpoint attempt. Busy mirrors PRAGMA's busy column:
// true when readers or writers prevented completing the checkpoint (the WAL was not
// reset even under TRUNCATE).
type CheckpointResult struct {
	Busy         bool
	WALPages     int64 // frames in the WAL when the attempt ran
	Checkpointed int64 // frames the attempt moved into the database file
}

// ApplySolo runs PRAGMA wal_checkpoint against the raw handle.
func (c CheckpointCmd) ApplySolo(ctx context.Context, rw *sql.DB, _ time.Time) (Result, []obs.Event, error) {
	q := `PRAGMA wal_checkpoint(PASSIVE)`
	if c.Mode == CheckpointTruncate {
		q = `PRAGMA wal_checkpoint(TRUNCATE)`
	} else if c.Mode != CheckpointPassive {
		return nil, nil, fmt.Errorf("checkpoint mode %q is neither PASSIVE nor TRUNCATE", c.Mode)
	}
	var busy, wal, ckpt int64
	if err := rw.QueryRowContext(ctx, q).Scan(&busy, &wal, &ckpt); err != nil {
		return nil, nil, fmt.Errorf("wal_checkpoint(%s): %w", c.Mode, err)
	}
	return CheckpointResult{Busy: busy != 0, WALPages: wal, Checkpointed: ckpt}, nil, nil
}

// VacuumCmd asks the writer connection to run PRAGMA incremental_vacuum(Pages),
// moving up to Pages freelist pages back into the database file. Requires the data dir
// to have been created with auto_vacuum=INCREMENTAL (#5 sets exactly that); on a NONE
// directory the PRAGMA is a silent no-op, which the janitor detects and self-disables on.
type VacuumCmd struct {
	Pages int // 1..hard cap; the janitor passes --vacuum-pages-per-tick
}

// Kind labels the command for logs: "vacuum".
func (c VacuumCmd) Kind() CmdKind { return kindVacuum }

// Bytes is metadata-only.
func (c VacuumCmd) Bytes() int { return 0 }

// Solo is the marker; see the package-level Solo contract.
func (c VacuumCmd) Solo() {}

// Apply refuses the batch path, mirroring CheckpointCmd.
func (c VacuumCmd) Apply(context.Context, *sql.Tx, time.Time) (Result, []obs.Event, error) {
	return nil, nil, CmdErr(errors.New(
		"store: VacuumCmd is a Solo command; it must never run inside a batch"))
}

// VacuumResult reports one incremental-vacuum step.
type VacuumResult struct {
	FreedPages int64 // pages actually moved out of the freelist this step
}

// maxVacuumPages caps one step so a corrupted budget cannot stall the writer goroutine
// indefinitely; the flag layer's default (2000) sits far below it.
const maxVacuumPages = 1 << 16

// ApplySolo runs PRAGMA incremental_vacuum against the raw handle.
//
// DRIVER FINDING (2026-08-26, modernc.org/sqlite as pinned in go.mod): the PRAGMA
// completes without error but frees NO pages — an empty result set, freelist_count
// unchanged, on a file with auto_vacuum=INCREMENTAL set at creation and a non-empty
// freelist. Callers therefore cannot treat "no error" as progress; the janitor's vacuum
// job must read freelist_count back before/after and warn when pages do not move (the
// same honesty rule as the auto_vacuum=NONE self-disable). File-shrink acceptance for
// issue #27 needs a different mechanism (full VACUUM under budget, or issue #30's
// VACUUM INTO swap); that ruling belongs to the orchestrator, not to this amendment.
func (c VacuumCmd) ApplySolo(ctx context.Context, rw *sql.DB, _ time.Time) (Result, []obs.Event, error) {
	if c.Pages <= 0 || c.Pages > maxVacuumPages {
		return nil, nil, fmt.Errorf("%w: vacuum pages %d, want 1..%d",
			errs.ErrBadRequest, c.Pages, maxVacuumPages)
	}
	q := `PRAGMA incremental_vacuum(` + strconv.Itoa(c.Pages) + `)` //nolint:gosec // G202: Pages is an int validated to 1..maxVacuumPages above; there is no string input to inject.
	rows, err := rw.QueryContext(ctx, q)
	if err != nil {
		return nil, nil, fmt.Errorf("incremental_vacuum(%d): %w", c.Pages, err)
	}
	defer func() {
		if cerr := rows.Close(); cerr != nil {
			_ = cerr
		}
	}()
	// A conforming SQLite replies with one column (pages moved). The pinned driver
	// instead hands back a zero-column row when it moves nothing — treat that as
	// FreedPages 0 rather than an error.
	if !rows.Next() {
		return VacuumResult{FreedPages: 0}, nil, rows.Err()
	}
	cols, cerr := rows.Columns()
	if cerr != nil {
		return nil, nil, fmt.Errorf("incremental_vacuum(%d): columns: %w", c.Pages, cerr)
	}
	if len(cols) == 0 {
		return VacuumResult{FreedPages: 0}, nil, rows.Err()
	}
	var freed int64
	if err := rows.Scan(&freed); err != nil {
		return nil, nil, fmt.Errorf("incremental_vacuum(%d): scan: %w", c.Pages, err)
	}
	if err := rows.Err(); err != nil {
		return nil, nil, fmt.Errorf("incremental_vacuum(%d): rows: %w", c.Pages, err)
	}
	return VacuumResult{FreedPages: freed}, nil, nil
}
