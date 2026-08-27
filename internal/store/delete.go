// SPDX-License-Identifier: Apache-2.0

package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/a-holm/messq/internal/errs"
	"github.com/a-holm/messq/internal/obs"
	"github.com/a-holm/messq/internal/queue"
)

// Stream deletion (issue §2, §9 edge cases). The name-confirmed destructive action:
// the metadata disappears in one small transaction so every read path stops seeing
// the stream immediately, then messages go in bounded chunk commands so a multi-GiB
// delete cannot balloon one WAL transaction. The engine serialises both phases with
// everything else; a create that slips in mid-reap hits refuseDuringReap's marker
// check and gets the recreate-during-reap 409 instead of resurrected rows.

const kindReapResume CmdKind = "reap.resume"

// deleteChunkRows bounds each message-delete command; 10 000 rows is the same
// page size the issue prescribes for the later async reaper's chunks.
const deleteChunkRows = 10_000

// metaReapPrefix is the meta key namespace holding one in-progress reap marker per
// stream being deleted. Written by deleteStreamCmd, cleared by the final
// reapChunkCmd (or by finishInterruptedReaps at startup).
const metaReapPrefix = "reap."

// ReapInProgressError refuses a create whose name is still being reaped after a
// delete. It wraps [errs.ErrConflict]: the name is taken until the last chunk lands.
type ReapInProgressError struct {
	Name      string
	Remaining int64 // approximate messages the chunks still have to walk
}

func (e *ReapInProgressError) Error() string {
	return fmt.Sprintf("stream %q is still being deleted (%d messages remain)",
		e.Name, e.Remaining)
}
func (e *ReapInProgressError) Unwrap() error { return errs.ErrConflict }

// DeleteResult reports what a confirmed deletion removed.
type DeleteResult struct {
	Messages  int64 `json:"messages"`
	Bytes     int64 `json:"bytes"`
	Consumers int64 `json:"consumers"`
}

// DeleteStream removes one stream. confirm must equal name verbatim — the API-level
// ?confirm= parameter lands here unvalidated, and a typo must not destroy data
// (errs.ErrConflict otherwise). The sequence high-water mark survives in meta as
// seq_hwm.<name> (P2): re-creating the name resumes above it, so the stream/seq
// consumer dedup key never collides across delete + recreate.
//
// The metadata phase is one engine command; the message chunks are follow-up commands
// on the same queue. Between them no writer can resurrect or repopulate the name:
// publishes find the streams row gone, and creates hit the reap-marker refusal.
func (s *Store) DeleteStream(ctx context.Context, name, confirm, actor string) (DeleteResult, error) {
	if err := queue.ValidateExistingStreamName(name); err != nil {
		return DeleteResult{}, err
	}
	if confirm != name {
		return DeleteResult{}, errs.E(errs.ErrConflict, "store.DeleteStream",
			"confirm parameter %q does not match stream name %q", confirm, name)
	}

	res, err := s.enqueue(ctx, "store.DeleteStream", deleteStreamCmd{name: name, actor: actor})
	if err != nil {
		return DeleteResult{}, err
	}
	deleted, ok := res.(DeleteResult)
	if !ok {
		return DeleteResult{},
			fmt.Errorf("store.DeleteStream: engine returned %T, want DeleteResult", res)
	}

	for {
		chunkRes, cErr := s.enqueue(ctx, "store.DeleteStream.chunk", reapChunkCmd{name: name})
		if cErr != nil {
			return DeleteResult{}, cErr
		}
		chunk, ok := chunkRes.(reapChunkResult)
		if !ok {
			return DeleteResult{}, fmt.Errorf(
				"store.DeleteStream.chunk: engine returned %T, want reapChunkResult", chunkRes)
		}
		if chunk.Cleared || chunk.Removed == 0 {
			break
		}
	}
	return deleted, nil
}

// ReapResumeResult reports one background resume chunk.
type ReapResumeResult struct {
	Removed int64 // message rows deleted by this chunk
	Pending bool  // work for this or another marker remains: reschedule now
}

// reapResumeCmd finishes ONE chunk of ONE pending reap (issue #27 owns the background
// completion of an already-authorised stream deletion). The metadata transaction of a
// crashed DeleteStream is long done; what remains is purely the bounded row-chunk loop.
type reapResumeCmd struct{}

func (c reapResumeCmd) Kind() CmdKind { return kindReapResume }
func (c reapResumeCmd) Bytes() int    { return 0 }

// Store.ReapResume drives one chunk through the writer. Pending tells the caller to
// call again; an empty marker table yields a zero no-op result.
func (s *Store) ReapResume(ctx context.Context) (ReapResumeResult, error) {
	res, err := s.enqueue(ctx, "store.ReapResume", reapResumeCmd{})
	if err != nil {
		return ReapResumeResult{}, err
	}
	rr, ok := res.(ReapResumeResult)
	if !ok {
		return ReapResumeResult{},
			fmt.Errorf("store.ReapResume: engine returned %T, want ReapResumeResult", res)
	}
	return rr, nil
}

func (c reapResumeCmd) Apply(ctx context.Context, tx *sql.Tx, now time.Time) (
	Result, []obs.Event, error,
) {
	var name string
	findErr := tx.QueryRowContext(ctx,
		`SELECT k FROM meta WHERE k LIKE ? ORDER BY k ASC LIMIT 1`,
		metaReapPrefix+"%").Scan(&name)
	switch {
	case errors.Is(findErr, sql.ErrNoRows):
		return ReapResumeResult{}, nil, nil // idle: nothing authorised is unfinished
	case findErr != nil:
		return nil, nil, fmt.Errorf("find reap marker: %w", findErr)
	}
	name = name[len(metaReapPrefix):]

	chunkRes, _, xErr := reapChunkCmd{name: name}.Apply(ctx, tx, now)
	if xErr != nil {
		return nil, nil, fmt.Errorf("resume reap chunk of %q: %w", name, xErr)
	}
	chunk, ok := chunkRes.(reapChunkResult)
	if !ok {
		return nil, nil, fmt.Errorf(
			"resume reap of %q: engine returned %T, want reapChunkResult", name, chunkRes)
	}
	out := ReapResumeResult{Removed: chunk.Removed, Pending: !chunk.Cleared}
	return out, nil, nil
}
