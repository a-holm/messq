// SPDX-License-Identifier: Apache-2.0

package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/a-holm/messq/internal/errs"
	"github.com/a-holm/messq/internal/queue"
)

// Stream deletion (issue §2, §9 edge cases). The name-confirmed destructive action:
// the metadata disappears in one small transaction so every read path stops seeing
// the stream immediately, then messages go in bounded chunks so a multi-GiB delete
// cannot balloon one WAL transaction. The whole operation holds writeMu, so no
// create or publish can interleave — the async reaper with its recreate-during-reap
// 409 arrives only with #6's command queue.

// deleteChunkRows bounds each message-delete transaction; 10 000 rows is the same
// page size the issue prescribes for the later async reaper's chunks.
const deleteChunkRows = 10_000

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
func (s *Store) DeleteStream(ctx context.Context, name, confirm, actor string) (DeleteResult, error) {
	if err := queue.ValidateExistingStreamName(name); err != nil {
		return DeleteResult{}, err
	}
	if confirm != name {
		return DeleteResult{}, errs.E(errs.ErrConflict, "store.DeleteStream",
			"confirm parameter %q does not match stream name %q", confirm, name)
	}

	s.writeMu.Lock()
	defer s.writeMu.Unlock()

	var res DeleteResult
	err := s.runWriteLocked(ctx, "store.DeleteStream", func(tx txLike) error {
		row := tx.QueryRowContext(ctx,
			`SELECT msgs, bytes FROM stream_stats WHERE stream = ?`, name)
		if err := row.Scan(&res.Messages, &res.Bytes); err != nil {
			return errs.E(errs.ErrNotFound, "store.DeleteStream",
				"stream %q does not exist", name)
		}
		if err := tx.QueryRowContext(ctx,
			`SELECT count(*) FROM consumers WHERE stream = ?`, name).Scan(&res.Consumers); err != nil {
			return fmt.Errorf("count consumers of %q: %w", name, err)
		}
		var next int64
		if err := tx.QueryRowContext(ctx,
			`SELECT next FROM stream_seq WHERE stream = ?`, name).Scan(&next); err != nil &&
			!errors.Is(err, sql.ErrNoRows) {
			return fmt.Errorf("read seq counter of %q: %w", name, err)
		}
		hwm := next - 1

		for _, q := range []string{
			`DELETE FROM deliveries WHERE stream = ?`,
			`DELETE FROM consumers   WHERE stream = ?`,
			`DELETE FROM stream_stats WHERE stream = ?`,
			`DELETE FROM stream_seq   WHERE stream = ?`,
		} {
			if _, xErr := tx.ExecContext(ctx, q, name); xErr != nil {
				return fmt.Errorf("delete %q rows (%.24s…): %w", name, q, xErr)
			}
		}
		if _, xErr := tx.ExecContext(ctx,
			`INSERT INTO meta (k, v) VALUES (?, ?)
			 ON CONFLICT (k) DO UPDATE SET v = excluded.v`,
			metaSeqHwmPrefix+name, fmt.Sprintf("%d", hwm)); xErr != nil {
			return fmt.Errorf("record seq high-water mark for %q: %w", name, xErr)
		}
		if _, xErr := tx.ExecContext(ctx, `DELETE FROM streams WHERE name = ?`, name); xErr != nil {
			return fmt.Errorf("delete stream row %q: %w", name, xErr)
		}
		raw, jErr := json.Marshal(map[string]int64{
			"messages": res.Messages, "bytes": res.Bytes, "consumers": res.Consumers,
		})
		if jErr != nil { // unreachable for fixed keys
			raw = []byte(`{}`)
		}
		return insertEvent(ctx, tx, event{
			ts:     nowMS(s.clk),
			name:   "stream.delete",
			stream: nullStr(name),
			actor:  nullStr(actor),
			detail: nullStr(string(raw)),
		})
	})
	if err != nil {
		return DeleteResult{}, err
	}

	// Message chunks run under the still-held writeMu: each transaction stays
	// bounded, and no writer can resurrect or repopulate the name mid-sweep.
	for {
		removed, cErr := s.deleteMessageChunk(ctx, name)
		if cErr != nil {
			return DeleteResult{}, cErr
		}
		if removed == 0 {
			break
		}
	}
	return res, nil
}

// deleteMessageChunk removes up to deleteChunkRows message rows of one stream inside
// its own transaction and reports how many went.
func (s *Store) deleteMessageChunk(ctx context.Context, name string) (int64, error) {
	var removed int64
	err := s.runWriteLocked(ctx, "store.DeleteStream.chunk", func(tx txLike) error {
		r, xErr := tx.ExecContext(ctx,
			`DELETE FROM messages WHERE rowid IN
			   (SELECT rowid FROM messages WHERE stream = ? LIMIT ?)`,
			name, deleteChunkRows)
		if xErr != nil {
			return fmt.Errorf("delete message chunk of %q: %w", name, xErr)
		}
		removed, xErr = r.RowsAffected()
		if xErr != nil {
			return fmt.Errorf("count deleted chunk of %q: %w", name, xErr)
		}
		return nil
	})
	return removed, err
}
