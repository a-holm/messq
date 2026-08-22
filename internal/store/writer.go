// SPDX-License-Identifier: Apache-2.0

package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/a-holm/messq/internal/clock"
	"github.com/a-holm/messq/internal/errs"
)

// The #6 seam. Issue #6's single-writer engine owns the rw handle and turns a queue of
// commands into one fsynced transaction. Until that engine merges, this file is the
// thin local stand-in: every state-changing command runs through [Store.runWrite],
// which gives it exactly the transaction shape #6 will give it — BEGIN IMMEDIATE, one
// SAVEPOINT per command, domain errors rolled back to their savepoint while I/O-class
// failures abort the whole transaction.
//
// When #6 lands, only this file changes: the command bodies below keep their
// func(tx txLike) error shape and are handed to the writer queue as-is, and runWrite
// becomes an enqueue instead of an execute.

// txLike is the transaction-handle seam the command layer codes against. *sql.Tx and
// *sql.Conn both satisfy it today; #6's writer transaction will satisfy it unchanged,
// which is why publishTx takes this instead of a concrete type.
type txLike interface {
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
}

// isDomainError reports whether err is a typed rejection whose failure is the caller's
// doing rather than the storage medium's. Only domain errors may roll back a single
// savepoint while their batch-mates commit; SQLITE_IOERR*/FULL/CORRUPT and context
// cancellations are I/O-class and fatal to the whole transaction (D4).
func isDomainError(err error) bool {
	for _, sentinel := range []error{
		errs.ErrNotFound,
		errs.ErrConflict,
		errs.ErrBadRequest,
		errs.ErrBadSubject,
		errs.ErrTooLarge,
		errs.ErrStreamFull,
	} {
		if errors.Is(err, sentinel) {
			return true
		}
	}
	return false
}

// runWrite runs fn inside one IMMEDIATE transaction on the writer handle, wrapped in a
// single-command savepoint (the cmd_0 of a one-element #6 batch):
//
//	fn(tx) nil        → RELEASE cmd_0, COMMIT   (fsync under full durability)
//	fn(tx) domain err → ROLLBACK TO cmd_0, RELEASE, COMMIT; the waiter gets err
//	fn(tx) I/O err    → ROLLBACK everything; startup-class refusal semantics
//
// The mutex serialises writers locally, standing in for #6's single goroutine.
func (s *Store) runWrite(ctx context.Context, op string, fn func(tx txLike) error) error {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()

	s.mu.Lock()
	rw := s.rw
	s.mu.Unlock()
	if rw == nil {
		return errs.E(errs.ErrShuttingDown, op, "no read-write handle: store closing or opened read-only")
	}

	conn, err := rw.Conn(ctx)
	if err != nil {
		return fmt.Errorf("%s: acquire writer connection: %w", op, err)
	}
	defer func() {
		if cerr := conn.Close(); cerr != nil {
			s.logger.Warn("writer.conn", "op", op, "error", cerr.Error())
		}
	}()

	if _, berr := conn.ExecContext(ctx, `BEGIN IMMEDIATE`); berr != nil {
		return fmt.Errorf("%s: begin immediate: %w", op, berr)
	}
	committed := false
	defer func() {
		if committed {
			return
		}
		if _, rbErr := conn.ExecContext(context.WithoutCancel(ctx), `ROLLBACK`); rbErr != nil {
			err = errors.Join(err, fmt.Errorf("rollback %s: %w", op, rbErr))
		}
	}()

	const sp = `cmd_0`
	if _, err := conn.ExecContext(ctx, `SAVEPOINT `+sp); err != nil {
		return fmt.Errorf("%s: savepoint: %w", op, err)
	}

	if fnErr := fn(conn); fnErr != nil {
		if !isDomainError(fnErr) {
			return fmt.Errorf("%s: %w", op, fnErr) // deferred ROLLBACK undoes all
		}
		if _, err := conn.ExecContext(ctx, `ROLLBACK TO `+sp); err != nil {
			return fmt.Errorf("%s: rollback to savepoint: %w", op, errors.Join(fnErr, err))
		}
		if _, err := conn.ExecContext(ctx, `RELEASE `+sp); err != nil {
			return fmt.Errorf("%s: release savepoint: %w", op, errors.Join(fnErr, err))
		}
		if _, err := conn.ExecContext(ctx, `COMMIT`); err != nil {
			return fmt.Errorf("%s: commit after domain rejection: %w", op, errors.Join(fnErr, err))
		}
		committed = true
		return fnErr
	}

	if _, err := conn.ExecContext(ctx, `RELEASE `+sp); err != nil {
		return fmt.Errorf("%s: release savepoint: %w", op, err)
	}
	if _, err := conn.ExecContext(ctx, `COMMIT`); err != nil {
		return fmt.Errorf("%s: commit: %w", op, err)
	}
	committed = true
	return nil
}

// nowMS is the wall clock through the seam, in the unix-millisecond unit every
// persisted timestamp uses.
func nowMS(clk clock.Clock) int64 { return clk.Now().UnixMilli() }
