// SPDX-License-Identifier: Apache-2.0

package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/a-holm/messq/internal/errs"
	"github.com/a-holm/messq/internal/queue"
)

// Stream lifecycle (issue #7 §1–§2): create, read, list. Every state change rides the
// writer engine as a [Cmd] submitted through [Store.enqueue], its co-committed event
// row inside the command's Apply; every read runs on the fenced read-only pool. The
// authoritative config check happens inside the transaction, so a stream narrowed or
// deleted earlier in the same commit batch can never be raced by a stale snapshot.

// StreamInfo is the read shape of one stream: its configuration plus the live
// statistics. The JSON field names are the CLI's --output contract (issue §7) and are
// golden-tested from the day they exist.
type StreamInfo struct {
	Name          string   `json:"name"`
	Subjects      []string `json:"subjects"`
	Retention     string   `json:"retention"`
	MaxMsgs       int64    `json:"max_msgs"`
	MaxBytes      int64    `json:"max_bytes"`
	MaxAgeMS      int64    `json:"max_age_ms"`
	MaxMsgSize    int64    `json:"max_msg_size"`
	Discard       string   `json:"discard"`
	DedupWindowMS int64    `json:"dedup_window_ms"`
	CreatedAt     int64    `json:"created_at"` // unix ms
	FirstSeq      int64    `json:"first_seq"`
	LastSeq       int64    `json:"last_seq"`
	Msgs          int64    `json:"msgs"`
	Bytes         int64    `json:"bytes"`
	// DLQ direction (derived, not stored): DLQ is true for a ".dlq"-suffixed stream;
	// Origin is the base stream (the origin a DLQ back-references, or the stream a
	// non-DLQ's dead-letter stream would be). Derived from the D3 naming contract so
	// #21/#24/#29 do not re-derive the suffix rule. DLQ==false and no Origin => not a
	// dead-letter pair.
	DLQ    bool   `json:"dlq,omitempty"`
	Origin string `json:"origin,omitempty"`
}

// Config renders the stored configuration as the pure layer's value type.
func (i StreamInfo) Config() queue.StreamConfig {
	return queue.StreamConfig{
		Name:        i.Name,
		Subjects:    i.Subjects,
		Retention:   queue.Retention(i.Retention),
		MaxMsgs:     i.MaxMsgs,
		MaxBytes:    i.MaxBytes,
		MaxAge:      msDuration(i.MaxAgeMS),
		MaxMsgSize:  i.MaxMsgSize,
		Discard:     queue.Discard(i.Discard),
		DedupWindow: msDuration(i.DedupWindowMS),
	}
}

// msDuration converts a persisted millisecond count back into a Duration.
func msDuration(ms int64) time.Duration { return time.Duration(ms) * time.Millisecond }

// metaSeqHwmPrefix is the meta key namespace holding the highest sequence ever
// assigned to a deleted stream (issue §9, P2): create resumes above it, so the
// stream/seq consumer dedup key never collides across delete + recreate.
const metaSeqHwmPrefix = "seq_hwm."

// StreamExistsError refuses a create whose name is taken by a different configuration.
// Diff names every field that differs, so the response can point at the exact knobs.
type StreamExistsError struct {
	Name     string
	Diff     []string
	Existing StreamInfo
}

func (e *StreamExistsError) Error() string {
	return fmt.Sprintf("stream %q already exists with a different configuration (%s)",
		e.Name, strings.Join(e.Diff, ", "))
}
func (e *StreamExistsError) Unwrap() error { return errs.ErrConflict }

// NameCaseCollisionError refuses a create whose name differs from an existing stream
// only by case. Names are ASCII (rule S11), so SQLite's NOCASE collation is an exact
// case fold here — "Orders" next to "orders" is a support ticket, not a feature.
type NameCaseCollisionError struct {
	Name     string
	Existing string
}

func (e *NameCaseCollisionError) Error() string {
	return fmt.Sprintf("stream %q differs from existing %q only by case", e.Name, e.Existing)
}
func (e *NameCaseCollisionError) Unwrap() error { return errs.ErrConflict }

// CreateStream validates and stores one stream configuration, idempotently: an
// identical re-create returns the existing stream with existed=true, a different
// configuration for a taken name refuses with [StreamExistsError], and a name that
// differs from an existing one only by case refuses with [NameCaseCollisionError].
// The stream_seq counter resumes above the deleted stream high-water mark when the
// name has been used before (P2).
func (s *Store) CreateStream(ctx context.Context, cfg queue.StreamConfig, actor string) (info StreamInfo, existed bool, err error) {
	if verr := queue.ValidateStreamName(cfg.Name); verr != nil {
		return StreamInfo{}, false, verr
	}
	if verr := queue.ValidateStreamConfig(cfg, s.limits); verr != nil {
		return StreamInfo{}, false, verr
	}
	res, err := s.enqueue(ctx, "store.CreateStream", createStreamCmd{cfg: cfg, actor: actor})
	if err != nil {
		return StreamInfo{}, false, err
	}
	cr, ok := res.(createStreamResult)
	if !ok {
		return StreamInfo{}, false,
			fmt.Errorf("store.CreateStream: engine returned %T, want createStreamResult", res)
	}
	info, existed = cr.info, cr.existed
	if !existed {
		// Fill the live statistics of the freshly created row (all zero today, but the
		// read path stays one code path).
		filled, getErr := s.GetStream(ctx, cfg.Name)
		if getErr != nil {
			return StreamInfo{}, false, getErr
		}
		return filled, false, nil
	}
	return info, true, nil
}

// resumeSeq reports the sequence number a new stream starts at: 1 for a fresh name,
// one above the deleted stream's high-water mark otherwise, and it clears the marker
// read (the key stays: ~40 bytes per name ever used, and a later recreate of the same
// name must keep resuming forward even if this creation never publishes).
func resumeSeq(ctx context.Context, tx *sql.Tx, name string) (int64, error) {
	var raw string
	err := tx.QueryRowContext(ctx, `SELECT v FROM meta WHERE k = ?`, metaSeqHwmPrefix+name).Scan(&raw)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		return 1, nil
	case err != nil:
		return 0, fmt.Errorf("read seq high-water mark: %w", err)
	}
	var hwm int64
	if _, parseErr := fmt.Sscanf(raw, "%d", &hwm); parseErr != nil {
		return 0, fmt.Errorf("meta[%s%s] = %q is not an integer", metaSeqHwmPrefix, name, raw)
	}
	return hwm + 1, nil
}

// GetStream reads one stream's configuration and live statistics from the read-only
// pool. A missing name is errs.ErrNotFound.
func (s *Store) GetStream(ctx context.Context, name string) (StreamInfo, error) {
	if err := queue.ValidateExistingStreamName(name); err != nil {
		return StreamInfo{}, err
	}
	ro := s.readPool()
	info, err := scanStreamInfo(ro.QueryRowContext(ctx,
		`SELECT `+streamCols+` FROM streams WHERE name = ?`, name))
	if errors.Is(err, sql.ErrNoRows) {
		return StreamInfo{}, errs.E(errs.ErrNotFound, "store.GetStream",
			"stream %q does not exist", name)
	}
	if err != nil {
		return StreamInfo{}, fmt.Errorf("read stream %q: %w", name, err)
	}
	if err := fillStreamStats(ctx, ro, &info); err != nil {
		return StreamInfo{}, err
	}
	return info, nil
}

// ListStreams reads every stream in name order. The per-stream statistics make this
// O(streams × index depth), which is the documented cost for the admin surface; the
// scrape-time collector (#21) calls GetStream in a loop for exactly the same reason.
func (s *Store) ListStreams(ctx context.Context) ([]StreamInfo, error) {
	ro := s.readPool()
	rows, err := ro.QueryContext(ctx, `SELECT `+streamCols+` FROM streams ORDER BY name`)
	if err != nil {
		return nil, fmt.Errorf("list streams: %w", err)
	}
	defer func() {
		if cerr := rows.Close(); cerr != nil {
			s.logger.Warn("store.list", "error", cerr.Error())
		}
	}()
	out := make([]StreamInfo, 0, 8)
	for rows.Next() {
		info, scanErr := scanStreamInfo(rows)
		if scanErr != nil {
			return nil, fmt.Errorf("scan stream row: %w", scanErr)
		}
		out = append(out, info)
	}
	if rErr := rows.Err(); rErr != nil {
		return nil, fmt.Errorf("iterate streams: %w", rErr)
	}
	for i := range out {
		if err := fillStreamStats(ctx, ro, &out[i]); err != nil {
			return nil, err
		}
	}
	return out, nil
}

// streamCols is the projection every streams-row reader uses, in scan order.
const streamCols = `name, subjects, retention, max_msgs, max_bytes, max_age_ms,` +
	` max_msg_size, discard, dedup_window_ms, created_at`

// scanStreamInfo scans one streams row from any row source, deriving the DLQ-direction
// fields from the D3 naming contract.
func scanStreamInfo(row interface{ Scan(dest ...any) error }) (StreamInfo, error) {
	var info StreamInfo
	var subjects, retention, discard string
	if err := row.Scan(&info.Name, &subjects, &retention, &info.MaxMsgs, &info.MaxBytes,
		&info.MaxAgeMS, &info.MaxMsgSize, &discard, &info.DedupWindowMS, &info.CreatedAt); err != nil {
		return StreamInfo{}, err
	}
	info.Subjects = unmarshalSubjects(subjects)
	info.Retention = retention
	info.Discard = discard
	info.DLQ = queue.IsDLQ(info.Name)
	if origin, ok := queue.OriginOf(info.Name); ok {
		info.Origin = origin
	}
	return info, nil
}

// querier is the read seam shared by pools and transactions: anything with a
// QueryRowContext. GetStream hands it the fenced RO pool; command Applies may pass
// their *sql.Tx.
type querier interface {
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
}

// fillStreamStats loads msgs/bytes from stream_stats and first_seq/last_seq from the
// messages index. Two separate min/max queries: SQLite only optimises a single bare
// min() or max() per query, and combining them would scan the stream. An empty stream
// reports first_seq 0 and last_seq next-1, so numbering continuity after a purge is
// visible rather than mysterious (issue §5).
func fillStreamStats(ctx context.Context, q querier, info *StreamInfo) error {
	var msgs, bytes sql.Null[int64]
	if err := q.QueryRowContext(ctx,
		`SELECT msgs, bytes FROM stream_stats WHERE stream = ?`, info.Name,
	).Scan(&msgs, &bytes); err != nil && !errors.Is(err, sql.ErrNoRows) {
		return fmt.Errorf("read stream_stats for %q: %w", info.Name, err)
	}
	info.Msgs, info.Bytes = msgs.V, bytes.V

	var first, last sql.Null[int64]
	if err := q.QueryRowContext(ctx,
		`SELECT min(seq) FROM messages WHERE stream = ?`, info.Name).Scan(&first); err != nil {
		return fmt.Errorf("read first_seq for %q: %w", info.Name, err)
	}
	if err := q.QueryRowContext(ctx,
		`SELECT max(seq) FROM messages WHERE stream = ?`, info.Name).Scan(&last); err != nil {
		return fmt.Errorf("read last_seq for %q: %w", info.Name, err)
	}
	info.FirstSeq, info.LastSeq = first.V, last.V
	if !first.Valid {
		var next int64
		if err := q.QueryRowContext(ctx,
			`SELECT next FROM stream_seq WHERE stream = ?`, info.Name).Scan(&next); err != nil &&
			!errors.Is(err, sql.ErrNoRows) {
			return fmt.Errorf("read stream_seq for %q: %w", info.Name, err)
		}
		info.LastSeq = next - 1
	}
	return nil
}

// readPool snapshots the read pool under the mutex.
func (s *Store) readPool() *sql.DB {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.ro
}

// marshalSubjects renders the subject list as the stored JSON array, sorted and
// deduplicated, so equal sets compare equal as strings and goldens are stable.
func marshalSubjects(subs []string) string {
	sorted := slices.Clone(subs)
	slices.Sort(sorted)
	sorted = slices.Compact(sorted)
	raw, err := json.Marshal(sorted)
	if err != nil { // unreachable for []string; kept total for the compiler
		return "[]"
	}
	return string(raw)
}

// unmarshalSubjects parses the stored JSON array. A row that fails to parse is a
// corruption the read path reports rather than silently emptying.
func unmarshalSubjects(raw string) []string {
	var out []string
	if err := json.Unmarshal([]byte(raw), &out); err != nil {
		return []string{"<corrupt subjects: " + raw + ">"}
	}
	return out
}

// configDiff names every field where two configurations disagree, for
// StreamExistsError. Subjects compare as sorted sets: order is not configuration.
func configDiff(old, next queue.StreamConfig) []string {
	var diff []string
	if !slices.Equal(marshalSubjectsSet(old.Subjects), marshalSubjectsSet(next.Subjects)) {
		diff = append(diff, "subjects")
	}
	if old.Retention != next.Retention {
		diff = append(diff, "retention")
	}
	if old.MaxMsgs != next.MaxMsgs {
		diff = append(diff, "max_msgs")
	}
	if old.MaxBytes != next.MaxBytes {
		diff = append(diff, "max_bytes")
	}
	if old.MaxAge != next.MaxAge {
		diff = append(diff, "max_age")
	}
	if old.MaxMsgSize != next.MaxMsgSize {
		diff = append(diff, "max_msg_size")
	}
	if old.Discard != next.Discard {
		diff = append(diff, "discard")
	}
	if old.DedupWindow != next.DedupWindow {
		diff = append(diff, "dedup_window")
	}
	return diff
}

func marshalSubjectsSet(subs []string) []string {
	out := slices.Clone(subs)
	slices.Sort(out)
	return slices.Compact(out)
}
