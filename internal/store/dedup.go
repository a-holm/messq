// SPDX-License-Identifier: Apache-2.0

package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/a-holm/messq/internal/errs"
	"github.com/a-holm/messq/internal/queue"
)

// Dedup-key lifecycle (issue §4): the partial unique index must stay bounded, so keys
// are NULLed once they are older than the stream's window. The window is measured
// from published_at of the original message, not from the sweep, and the key commits
// with its message — so a publisher retry after a broker restart still dedups inside
// the window. #27's janitor takes this command over with its full job list.

// sweepBound caps one call: the serve ticker invokes SweepDedup per stream every
// --dedup-sweep-interval (60 s default), and no single invocation may monopolise a
// commit window or grow one transaction without bound.
const sweepBound = 10_000

// SweepDedup NULLs at most sweepBound expired dedup keys of one stream and reports
// how many went. Keys younger than the stream's live dedup_window_ms survive; an
// unknown name is errs.ErrNotFound.
func (s *Store) SweepDedup(ctx context.Context, stream string) (int64, error) {
	if err := queue.ValidateExistingStreamName(stream); err != nil {
		return 0, err
	}
	var cleared int64
	err := s.runWrite(ctx, "store.SweepDedup", func(tx txLike) error {
		var window int64
		err := tx.QueryRowContext(ctx,
			`SELECT dedup_window_ms FROM streams WHERE name = ?`, stream).Scan(&window)
		switch {
		case errors.Is(err, sql.ErrNoRows):
			return errs.E(errs.ErrNotFound, "store.SweepDedup",
				"stream %q does not exist", stream)
		case err != nil:
			return fmt.Errorf("read dedup window of %q: %w", stream, err)
		}

		res, xErr := tx.ExecContext(ctx, `
			UPDATE messages SET dedup_key = NULL
			 WHERE rowid IN (
			   SELECT rowid FROM messages
			    WHERE stream = ?1 AND published_at < ?2 AND dedup_key IS NOT NULL
			    ORDER BY published_at LIMIT ?3)`,
			stream, nowMS(s.clk)-window, sweepBound)
		if xErr != nil {
			return fmt.Errorf("expire dedup keys of %q: %w", stream, xErr)
		}
		cleared, xErr = res.RowsAffected()
		if xErr != nil {
			return fmt.Errorf("count expired keys of %q: %w", stream, xErr)
		}
		return nil
	})
	if err != nil {
		return 0, err
	}
	return cleared, nil
}
