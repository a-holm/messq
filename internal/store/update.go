// SPDX-License-Identifier: Apache-2.0

package store

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/a-holm/messq/internal/errs"
	"github.com/a-holm/messq/internal/queue"
	"github.com/a-holm/messq/internal/subject"
)

// Stream update (issue §1): a sparse PATCH where "absent" and "zero" stay
// distinguishable — max_msgs: 0 means unlimited, omitting it must not mean that, so
// every field rides a pointer. The authoritative data-loss decision runs inside the
// write transaction against the stored row, not against a handler snapshot.

// StreamPatch carries the present fields of a PATCH /v1/streams/{stream} body. A nil
// pointer leaves the stored value untouched; Name has no field because it is
// immutable (queue.ValidateUpdate enforces the same rule for composed configs).
type StreamPatch struct {
	Subjects      *[]string
	Retention     *queue.Retention
	MaxMsgs       *int64
	MaxBytes      *int64
	MaxAgeMS      *int64
	MaxMsgSize    *int64
	Discard       *queue.Discard
	DedupWindowMS *int64
}

// UpdateResult is the outcome of one successful update: the new read shape plus the
// count of stored messages that no longer match a narrowed subject list (reported,
// never refused — narrowing only affects future publishes).
type UpdateResult struct {
	Info         StreamInfo
	NarrowedMsgs int64
	Fields       []string // the patch fields actually applied
}

// UpdateStream applies a sparse patch to one stream. A narrowing change that would
// make the janitor delete data refuses with queue.[WouldLoseDataError] unless
// allowDataLoss; an unknown name is errs.ErrNotFound; an empty patch is a no-op that
// writes no event row. Every applied update commits its stream.update event in the
// same transaction (D11).
func (s *Store) UpdateStream(ctx context.Context, name string, p StreamPatch, allowDataLoss bool, actor string) (UpdateResult, error) {
	if err := queue.ValidateExistingStreamName(name); err != nil {
		return UpdateResult{}, err
	}
	var res UpdateResult
	err := s.runWrite(ctx, "store.UpdateStream", func(tx txLike) error {
		row := tx.QueryRowContext(ctx, `SELECT `+streamCols+` FROM streams WHERE name = ?`, name)
		old, scanErr := scanStreamInfo(row)
		if scanErr != nil {
			return errs.E(errs.ErrNotFound, "store.UpdateStream",
				"stream %q does not exist", name)
		}
		next, fields := applyPatch(old.Config(), p)
		if len(fields) == 0 { // empty patch: nothing to decide, nothing to audit
			stats, statsErr := streamUsage(ctx, tx, name)
			if statsErr != nil {
				return statsErr
			}
			old.Msgs, old.Bytes = stats.msgs, stats.bytes
			res.Info = old
			return nil
		}
		if vErr := queue.ValidateStreamConfig(next, s.limits); vErr != nil {
			return vErr
		}

		u, mErr := measureUsage(ctx, tx, name, next, nowMS(s.clk))
		if mErr != nil {
			return mErr
		}
		if uErr := queue.ValidateUpdate(old.Config(), next, u, allowDataLoss); uErr != nil {
			return uErr
		}

		if _, xErr := tx.ExecContext(ctx, `UPDATE streams SET
			subjects = ?, retention = ?, max_msgs = ?, max_bytes = ?, max_age_ms = ?,
			max_msg_size = ?, discard = ?, dedup_window_ms = ?
			WHERE name = ?`,
			marshalSubjects(next.Subjects), string(next.Retention),
			next.MaxMsgs, next.MaxBytes, next.MaxAge.Milliseconds(), next.MaxMsgSize,
			string(next.Discard), next.DedupWindow.Milliseconds(), name,
		); xErr != nil {
			return fmt.Errorf("update stream row: %w", xErr)
		}
		raw, jErr := json.Marshal(map[string]any{"fields": fields})
		if jErr != nil { // unreachable for []string
			raw = []byte(`{}`)
		}
		if eErr := insertEvent(ctx, tx, event{
			ts:     nowMS(s.clk),
			name:   "stream.update",
			stream: nullStr(name),
			actor:  nullStr(actor),
			detail: nullStr(string(raw)),
		}); eErr != nil {
			return eErr
		}

		res.Fields = fields
		res.Info = old // sequence/stat fields are filled below
		res.Info.Subjects = next.Subjects
		res.Info.Retention = string(next.Retention)
		res.Info.MaxMsgs = next.MaxMsgs
		res.Info.MaxBytes = next.MaxBytes
		res.Info.MaxAgeMS = next.MaxAge.Milliseconds()
		res.Info.MaxMsgSize = next.MaxMsgSize
		res.Info.Discard = string(next.Discard)
		res.Info.DedupWindowMS = next.DedupWindow.Milliseconds()
		stats, statsErr := streamUsage(ctx, tx, name)
		if statsErr != nil {
			return statsErr
		}
		res.Info.Msgs, res.Info.Bytes = stats.msgs, stats.bytes
		if subjectsChanged(old.Config().Subjects, next.Subjects) {
			n, nErr := countUnmatched(ctx, tx, name, next.Subjects)
			if nErr != nil {
				return nErr
			}
			res.NarrowedMsgs = n
		}
		return nil
	})
	if err != nil {
		return UpdateResult{}, err
	}
	return res, nil
}

// applyPatch lays the present patch fields over old and names what moved, in the
// patch's canonical field order (the event detail's vocabulary).
func applyPatch(old queue.StreamConfig, p StreamPatch) (queue.StreamConfig, []string) {
	next := old
	var fields []string
	if p.Subjects != nil {
		next.Subjects = *p.Subjects
		fields = append(fields, "subjects")
	}
	if p.Retention != nil {
		next.Retention = *p.Retention
		fields = append(fields, "retention")
	}
	if p.MaxMsgs != nil {
		next.MaxMsgs = *p.MaxMsgs
		fields = append(fields, "max_msgs")
	}
	if p.MaxBytes != nil {
		next.MaxBytes = *p.MaxBytes
		fields = append(fields, "max_bytes")
	}
	if p.MaxAgeMS != nil {
		next.MaxAge = msDuration(*p.MaxAgeMS)
		fields = append(fields, "max_age")
	}
	if p.MaxMsgSize != nil {
		next.MaxMsgSize = *p.MaxMsgSize
		fields = append(fields, "max_msg_size")
	}
	if p.Discard != nil {
		next.Discard = *p.Discard
		fields = append(fields, "discard")
	}
	if p.DedupWindowMS != nil {
		next.DedupWindow = msDuration(*p.DedupWindowMS)
		fields = append(fields, "dedup_window")
	}
	return next, fields
}

func subjectsChanged(old, next []string) bool {
	a, b := marshalSubjectsSet(old), marshalSubjectsSet(next)
	if len(a) != len(b) {
		return true
	}
	for i := range a {
		if a[i] != b[i] {
			return true
		}
	}
	return false
}

// streamUsage reads the live counters inside a transaction.
func streamUsage(ctx context.Context, tx txLike, name string) (struct{ msgs, bytes int64 }, error) {
	var out struct{ msgs, bytes int64 }
	err := tx.QueryRowContext(ctx,
		`SELECT msgs, bytes FROM stream_stats WHERE stream = ?`, name,
	).Scan(&out.msgs, &out.bytes)
	if err != nil {
		return out, fmt.Errorf("read stream_stats for %q: %w", name, err)
	}
	return out, nil
}

// measureUsage builds queue.Usage for the data-loss decision: current totals plus the
// rows the new limits would delete on the janitor's next pass. One window-function
// pass computes the union of all three cuts (age, count, bytes) exactly: rbytes is the
// running size sum from the newest row backwards (rows that overflow max_bytes), rnk
// their recency rank (rows beyond max_msgs), published_at the age cut.
func measureUsage(ctx context.Context, tx txLike, name string, next queue.StreamConfig, now int64) (queue.Usage, error) {
	stats, err := streamUsage(ctx, tx, name)
	if err != nil {
		return queue.Usage{}, err
	}
	u := queue.Usage{NowMS: now, Msgs: stats.msgs, Bytes: stats.bytes}
	var atRiskMsgs, atRiskBytes *int64
	err = tx.QueryRowContext(ctx, `
		WITH m AS (
		  SELECT size, published_at,
		         sum(size) OVER (ORDER BY seq DESC) AS rbytes,
		         row_number() OVER (ORDER BY seq DESC) AS rnk
		  FROM messages WHERE stream = ?1)
		SELECT count(*), coalesce(sum(size), 0) FROM m
		 WHERE (?2 > 0 AND published_at < ?3)
		    OR (?4 > 0 AND rnk > ?4)
		    OR (?5 > 0 AND rbytes > ?5)`,
		name, next.MaxAge.Milliseconds(), now-next.MaxAge.Milliseconds(),
		next.MaxMsgs, next.MaxBytes,
	).Scan(&atRiskMsgs, &atRiskBytes)
	if err != nil {
		return u, fmt.Errorf("measure at-risk rows for %q: %w", name, err)
	}
	if atRiskMsgs != nil {
		u.AtRiskMsgs, u.AtRiskBytes = *atRiskMsgs, *atRiskBytes
	}
	return u, nil
}

// countUnmatched counts stored messages whose subject no longer matches the narrowed
// pattern list. O(rows) on the admin path, which is the documented cost of telling
// the operator what narrowing means for existing data.
func countUnmatched(ctx context.Context, tx txLike, name string, patterns []string) (mismatched int64, err error) {
	set, pErr := subject.ParseSet(patterns)
	if pErr != nil { // validated before we got here
		return 0, fmt.Errorf("compile narrowed subjects for %q: %w", name, pErr)
	}
	rows, qErr := tx.QueryContext(ctx, `SELECT subject FROM messages WHERE stream = ?`, name)
	if qErr != nil {
		return 0, fmt.Errorf("scan subjects of %q: %w", name, qErr)
	}
	defer func() {
		if cerr := rows.Close(); cerr != nil {
			err = errors.Join(err, fmt.Errorf("close subject scan of %q: %w", name, cerr))
		}
	}()
	for rows.Next() {
		var subj string
		if err := rows.Scan(&subj); err != nil {
			return 0, fmt.Errorf("scan subject of %q: %w", name, err)
		}
		if !set.Match(subj) {
			mismatched++
		}
	}
	if rErr := rows.Err(); rErr != nil {
		return 0, fmt.Errorf("iterate subjects of %q: %w", name, rErr)
	}
	return mismatched, nil
}
