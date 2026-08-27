// SPDX-License-Identifier: Apache-2.0

package store

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/a-holm/messq/internal/obs"
)

// The event-table trim writer command (issue #27 §9): enforces the two bounds the
// housekeeping slice owes PLAN §4.5's audit journal — an age retention (--event-
// retention, 72h default) and a hard row ceiling (--event-max-rows). Oldest-first by
// construction (min(id)/min(ts)), bounded by a per-slice Batch so one bloated journal
// can never hold the group-commit engine hostage: leftover work sets Result.More and
// resumes next tick. The horizon EventHorizon reports derives from min(ts), so every
// trimmed row moves it honestly with zero extra bookkeeping.
//
// Housekeeping itself emits NO event rows here (G7: a journal trim writing journal
// rows would feed itself); metrics/log projection happens outside the store.

const (
	kindTrimEvents CmdKind = "events.trim"

	defaultTrimBatch = 512
)

// TrimEventsResult aggregates one bounded trim slice.
type TrimEventsResult struct {
	Deleted int64 // rows removed by this slice (both passes)
	More    bool  // work remained when the slice ended: reschedule
}

// TrimEventsCmd is one writer command. MaxAgeMs guards the age pass (rows older than
// now-MaxAgeMs leave first), MaxRows caps the journal size (newest survive), and
// Batch bounds THIS slice's deletions across both passes. A zero disables its bound.
type TrimEventsCmd struct {
	MaxAgeMs int64
	MaxRows  int64
	Batch    int
}

func (c TrimEventsCmd) Kind() CmdKind { return kindTrimEvents }
func (c TrimEventsCmd) Bytes() int    { return 0 } // metadata-only workload

// Store.TrimEvents applies one bounded trim slice.
func (s *Store) TrimEvents(ctx context.Context, req TrimEventsCmd) (TrimEventsResult, error) {
	if req.Batch <= 0 {
		req.Batch = defaultTrimBatch
	}
	res, err := s.enqueue(ctx, "store.TrimEvents", req)
	if err != nil {
		return TrimEventsResult{}, err
	}
	tr, ok := res.(TrimEventsResult)
	if !ok {
		return TrimEventsResult{},
			fmt.Errorf("store.TrimEvents: engine returned %T, want TrimEventsResult", res)
	}
	return tr, nil
}

// Apply runs inside the writer's batch transaction: alternate the two passes until
// either runs dry of demand or the slice's deletion budget is spent.
func (c TrimEventsCmd) Apply(ctx context.Context, tx *sql.Tx, now time.Time) (
	Result, []obs.Event, error,
) {
	nowMS := now.UnixMilli()
	var res TrimEventsResult
	budget := c.Batch
	if budget <= 0 {
		budget = defaultTrimBatch
	}

	for {
		spentWork := false

		if c.MaxAgeMs > 0 && budget > 0 {
			cutoff := nowMS - c.MaxAgeMs
			n, tErr := trimDeleteChunk(ctx, tx,
				`DELETE FROM events WHERE id IN (
					SELECT id FROM events WHERE ts < ? ORDER BY id ASC LIMIT ?)`,
				cutoff, int64(budget))
			if tErr != nil {
				return res, nil, tErr
			}
			if n > 0 {
				res.Deleted += n
				budget -= int(n)
				spentWork = true
			}
		}

		if c.MaxRows > 0 && budget > 0 {
			var total int64
			if qErr := tx.QueryRowContext(ctx,
				`SELECT count(*) FROM events`).Scan(&total); qErr != nil {
				return res, nil, fmt.Errorf("trim count events: %w", qErr)
			}
			if excess := total - c.MaxRows; excess > 0 {
				chunk := min64(excess, int64(budget))
				n, tErr := trimDeleteChunk(ctx, tx,
					`DELETE FROM events WHERE id IN (
						SELECT id FROM events ORDER BY id ASC LIMIT ?)`,
					chunk)
				if tErr != nil {
					return res, nil, tErr
				}
				if n > 0 {
					res.Deleted += n
					budget -= int(n)
					spentWork = true
				}
			}
		}

		// Both passes dry AND some unspent budget means nothing is owed anywhere.
		if !spentWork || budget <= 0 {
			break
		}
	}

	// More is honest: whichever bound still bites after the slice decides whether
	// next tick must pick up where this one stopped.
	more, mErr := trimStillOwed(ctx, tx, c.MaxAgeMs, nowMS, c.MaxRows)
	if mErr != nil {
		return res, nil, mErr
	}
	res.More = more
	return res, nil, nil
}

// trimDeleteChunk executes one bounded IN-subquery delete and returns RowsAffected.
func trimDeleteChunk(ctx context.Context, tx *sql.Tx, stmt string, args ...any) (int64, error) {
	qr, eErr := tx.ExecContext(ctx, stmt, args...)
	if eErr != nil {
		return 0, fmt.Errorf("trim chunk: %w", eErr)
	}
	n, rErr := qr.RowsAffected()
	if rErr != nil {
		return 0, fmt.Errorf("trim rows-affected: %w", rErr)
	}
	return n, nil
}

// trimStillOwed reports whether either active bound still holds work after the slice.
func trimStillOwed(ctx context.Context, tx *sql.Tx, maxAgeMs, nowMS, maxRows int64) (bool, error) {
	if maxRows > 0 {
		var total int64
		if qErr := tx.QueryRowContext(ctx,
			`SELECT count(*) FROM events`).Scan(&total); qErr != nil {
			return false, fmt.Errorf("trim owed count: %w", qErr)
		}
		if total > maxRows {
			return true, nil
		}
	}
	if maxAgeMs > 0 {
		var old int64
		if qErr := tx.QueryRowContext(ctx,
			`SELECT EXISTS (SELECT 1 FROM events WHERE ts < ?)`, nowMS-maxAgeMs).Scan(&old); qErr != nil {
			return false, fmt.Errorf("trim owed age: %w", qErr)
		}
		return old == 1, nil
	}
	return false, nil
}

func min64(a, b int64) int64 {
	if b < a {
		return b
	}
	return a
}
