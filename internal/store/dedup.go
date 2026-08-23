// SPDX-License-Identifier: Apache-2.0

package store

import (
	"context"
	"fmt"

	"github.com/a-holm/messq/internal/queue"
)

// Dedup-key expiry (issue §4). The sweep is a bounded command on the writer engine:
// one call NULLs at most sweepBound expired keys of one stream, so an arbitrarily
// large backlog shrinks in capped bites instead of one long lock-up.

// sweepBound caps one call: the serve ticker invokes SweepDedup per stream every
// dedupSweep interval until the backlog is gone.
const sweepBound = 10_000

// SweepDedup NULLs at most sweepBound expired dedup keys of one stream and reports
// how many went. Keys younger than the stream's live dedup_window_ms survive; an
// unknown name is errs.ErrNotFound.
func (s *Store) SweepDedup(ctx context.Context, stream string) (int64, error) {
	if err := queue.ValidateExistingStreamName(stream); err != nil {
		return 0, err
	}
	res, err := s.enqueue(ctx, "store.SweepDedup", sweepDedupCmd{name: stream})
	if err != nil {
		return 0, err
	}
	cleared, ok := res.(sweepResult)
	if !ok {
		return 0, fmt.Errorf("store.SweepDedup: engine returned %T, want sweepResult", res)
	}
	return int64(cleared), nil
}
