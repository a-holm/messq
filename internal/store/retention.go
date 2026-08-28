// SPDX-License-Identifier: Apache-2.0

package store

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	"github.com/a-holm/messq/internal/obs"
	"github.com/a-holm/messq/internal/queue"
)

// The retention sweep writer command (issue #27, PLAN §4.5): one pass over every
// stream enforcing its configured limits, oldest-first, bounded per stream. The two
// load-bearing design points:
//
//   - G1 lives INSIDE this transaction. Candidates are planned with a delivery-row
//     probe (planning reads), but the deleting statement re-checks NOT EXISTS over
//     deliveries_seq as part of its WHERE — a pin that appears between plan and delete
//     makes that row survive by construction, never by convention.
//   - G2 skip-not-stall. A pinned candidate does not stop the walk; it is counted for
//     retention.blocked blame while the sweep continues to newer deletable rows.
//
// Housekeeping events are written here, inside the same transaction as the deletes
// they describe (D11 via commitEvent): one aggregate retention.expire per stream per
// sweep that actually deleted, and a retention.blocked when a limit remains violated
// after the sweep purely because of pins. Consumers of the fan-out turn those into
// messq_expired_total / messq_retention_blocked_total (#21 projection).

const kindRetention CmdKind = "retention.sweep"

const (
	defaultRetentionBatch = 512
	maxBlameHolders       = 64
)

// retentionDeleteSQL is the guarded delete's fixed skeleton; the seq placeholder list
// is spliced in per batch. It stays a package constant so the guard test can hold this
// exact statement against a mutation that drops NOT EXISTS.
const retentionDeleteSQL = `
DELETE FROM messages
 WHERE stream = ?
   AND seq IN (` + "%s" + `)
   AND NOT EXISTS (
        SELECT 1 FROM deliveries d
         WHERE d.stream = messages.stream AND d.seq = messages.seq)`

// retentionDeleteReturningSQL is the same statement with exact per-row accounting:
// sizes are read back from the rows the statement itself reports dead, so stats can
// never drift ahead of what physically happened. (A single constant keeps gosec's
// G202 away — nothing is concatenated at runtime.)
const retentionDeleteReturningSQL = retentionDeleteSQL + " RETURNING size"

// RetentionResult aggregates one sweep over all streams.
type RetentionResult struct {
	Streams      int   // streams examined
	Deleted      int64 // messages deleted
	FreedBytes   int64 // bytes reclaimed by the deletes
	BlockedCount int64 // candidates pinned by delivery rows (skipped)
	BlockedBytes int64 // size behind the pinned candidates
	More         bool  // at least one saturated window still violates a limit
}

// RetentionCmd is one writer command. Batch bounds the per-stream deletion window;
// <= 0 selects the default.
type RetentionCmd struct {
	Batch int
}

func (c RetentionCmd) Kind() CmdKind { return kindRetention }
func (c RetentionCmd) Bytes() int    { return 0 } // metadata only until it deletes

// Store.Retention applies one bounded retention sweep.
func (s *Store) Retention(ctx context.Context, req RetentionCmd) (RetentionResult, error) {
	if req.Batch <= 0 {
		req.Batch = defaultRetentionBatch
	}
	res, err := s.enqueue(ctx, "store.Retention", req)
	if err != nil {
		return RetentionResult{}, err
	}
	rr, ok := res.(RetentionResult)
	if !ok {
		return RetentionResult{},
			fmt.Errorf("store.Retention: engine returned %T, want RetentionResult", res)
	}
	return rr, nil
}

type retentionStreamCfg struct {
	name     string
	mode     queue.Retention
	maxMsgs  int64
	maxBytes int64
	maxAgeMs int64
	msgs     int64
	bytes    int64
	expSeq   int64 // current expired_seq watermark
}

// Apply runs inside the writer's batch transaction. One transaction sweeps every
// stream: retention work is exactly the mutation class the group-commit engine exists
// to serialize, and folding several streams' deletes into one commit keeps the sweep's
// event rows atomic with the state they describe.
func (c RetentionCmd) Apply(ctx context.Context, tx *sql.Tx, now time.Time) (_ Result, _ []obs.Event, rerr error) {
	nowMS := now.UnixMilli()
	var res RetentionResult
	var events []obs.Event

	cfgs, cfgErr := c.scanConfigs(ctx, tx)
	if cfgErr != nil {
		return nil, nil, cfgErr
	}
	for _, cfg := range cfgs {
		res.Streams++
		del, freed, blockedN, blockedB, moreStream, evs, sweepErr := c.sweepOne(ctx, tx, nowMS, cfg)
		if sweepErr != nil {
			return nil, nil, sweepErr
		}
		res.Deleted += del
		res.FreedBytes += freed
		res.BlockedCount += blockedN
		res.BlockedBytes += blockedB
		res.More = res.More || moreStream
		events = append(events, evs...)
	}
	return res, events, nil
}

// scanConfigs loads every stream's retention-relevant configuration and stats row,
// deterministically ordered by name.
func (c RetentionCmd) scanConfigs(ctx context.Context, tx *sql.Tx) (
	out []retentionStreamCfg, rerr error,
) {
	rows, qErr := tx.QueryContext(ctx, `
		SELECT s.name, s.retention, s.max_msgs, s.max_bytes, s.max_age_ms,
		       coalesce(t.msgs, 0), coalesce(t.bytes, 0), coalesce(t.expired_seq, 0)
		  FROM streams s
		 LEFT JOIN stream_stats t ON t.stream = s.name
		 ORDER BY s.name ASC`)
	if qErr != nil {
		return nil, fmt.Errorf("retention scan streams: %w", qErr)
	}
	defer func() {
		if cErr := rows.Close(); cErr != nil && rerr == nil {
			rerr = fmt.Errorf("close retention streams: %w", cErr)
		}
	}()
	for rows.Next() {
		var (
			g          retentionStreamCfg
			retMode    string
			expSeqNull sql.Null[int64]
		)
		if sErr := rows.Scan(&g.name, &retMode, &g.maxMsgs, &g.maxBytes, &g.maxAgeMs,
			&g.msgs, &g.bytes, &expSeqNull); sErr != nil {
			return nil, fmt.Errorf("scan retention stream config: %w", sErr)
		}
		g.mode = queue.Retention(retMode)
		g.expSeq = expSeqNull.V
		out = append(out, g)
	}
	if eErr := rows.Err(); eErr != nil {
		return nil, fmt.Errorf("iterate retention streams: %w", eErr)
	}
	return out, nil
}

// candidatesFor reads the oldest window of messages for one stream as planner input,
// probing each row for a delivery-row pin. mode=workqueue additionally caps the window
// at everything strictly below the consumers' shared floor.
func (c RetentionCmd) candidatesFor(ctx context.Context, tx *sql.Tx,
	cfg retentionStreamCfg, window int) (
	cands []queue.Candidate, noop bool, rerr error,
) {
	floorFilter := ""
	args := []any{cfg.name}
	floor := sql.NullInt64{}
	if cfg.mode == queue.RetentionWorkQueue {
		fErr := tx.QueryRowContext(ctx,
			`SELECT min(cursor_seq) FROM consumers WHERE stream = ?`, cfg.name).Scan(&floor)
		if fErr != nil {
			return nil, false, fmt.Errorf("retention floor %s: %w", cfg.name, fErr)
		}
		if !floor.Valid {
			// min() over an empty set: no consumer exists at all, so there is no
			// interest signal yet — nothing may be reaped (reason=no_consumers).
			return nil, true, nil
		}
		floorFilter = " AND seq < ?"
		args = append(args, floor.Int64)
	}
	args = append(args, window)
	q := `
		SELECT m.seq, m.size, m.published_at,
		       EXISTS (SELECT 1 FROM deliveries d
		                WHERE d.stream = m.stream AND d.seq = m.seq) AS pinned
		  FROM messages m
		 WHERE m.stream = ?` + floorFilter + `
		 ORDER BY m.seq ASC
		 LIMIT ?`
	rows, qErr := tx.QueryContext(ctx, q, args...)
	if qErr != nil {
		return nil, false, fmt.Errorf("retention candidates %s: %w", cfg.name, qErr)
	}
	defer func() {
		if cErr := rows.Close(); cErr != nil && rerr == nil {
			rerr = fmt.Errorf("close retention candidates %s: %w", cfg.name, cErr)
		}
	}()
	cands = make([]queue.Candidate, 0, window)
	for rows.Next() {
		var cd queue.Candidate
		if sErr := rows.Scan(&cd.Seq, &cd.Size, &cd.PublishedAt, &cd.HasDelivery); sErr != nil {
			return nil, false, fmt.Errorf("scan retention candidate %s: %w", cfg.name, sErr)
		}
		cands = append(cands, cd)
	}
	if eErr := rows.Err(); eErr != nil {
		return nil, false, fmt.Errorf("iterate retention candidates %s: %w", cfg.name, eErr)
	}
	return cands, false, nil
}

// sweepOne enforces one stream's policy inside the shared transaction and returns the
// aggregate deltas, the skipped-blocked accounting, whether this stream's window
// saturated with a limit still violated (More), and any audit carriers committed on
// the stream's behalf.
func (c RetentionCmd) sweepOne(ctx context.Context, tx *sql.Tx, nowMS int64,
	cfg retentionStreamCfg) (del int64, freed int64, blockedN int64, blockedB int64,
	more bool, evs []obs.Event, rerr error,
) {
	batch := c.Batch
	if batch <= 0 {
		batch = defaultRetentionBatch
	}
	view := queue.RetentionView{
		MaxMsgs: cfg.maxMsgs, MaxBytes: cfg.maxBytes, MaxAgeMs: cfg.maxAgeMs,
		Msgs: cfg.msgs, Bytes: cfg.bytes, NowMs: nowMS,
	}
	cands, noop, err := c.candidatesFor(ctx, tx, cfg, batch*2+maxBlameHolders)
	if err != nil {
		return 0, 0, 0, 0, false, nil, err
	}

	var plan queue.EvictionPlan
	switch cfg.mode {
	case queue.RetentionLimits:
		plan = queue.PlanEviction(cands, view, batch)
	case queue.RetentionWorkQueue:
		if noop {
			// reason=no_consumers: interest-based reaping needs interest to exist.
			return 0, 0, 0, 0, false, nil, nil
		}
		plan = planWorkqueue(cands, batch)
	default:
		// Unknown mode strings cannot get past validated writes; treat them like
		// limits so a future enum value can never silently disable enforcement.
		plan = queue.PlanEviction(cands, view, batch)
	}

	del, freed, delErr := c.applyDelete(ctx, tx, cfg.name, plan.Seqs)
	if delErr != nil {
		return 0, 0, 0, 0, false, nil, delErr
	}

	// The watermark only advances when something actually left below it.
	highDel := plan.HighestDeletedSeq
	if del > 0 && highDel > 0 {
		if uErr := updateWatermarks(ctx, tx, cfg.name, highDel, nowMS, del, freed); uErr != nil {
			return 0, 0, 0, 0, false, nil, uErr
		}
	}

	// Post-delete reality check: the guarded statement has the final word about what
	// survived, so the still-violating judgement uses ACTUAL deletions, not planned
	// ones. A limit violated after the sweep purely because pins ate part of the
	// plan is exactly what retention.blocked exists to name (G2).
	postMsgs := view.Msgs - del
	postBytes := view.Bytes - freed

	// More only when THIS window saturated AND reality still violates — a window
	// that came back short means nothing deletable remains in reach and next tick's
	// probe would be wasted work.
	more = plan.More || (del == int64(batch) &&
		queue.StillViolating(postMsgs, postBytes, view))

	stillBlocked := plan.BlockedCount > 0 && queue.StillViolating(postMsgs, postBytes, view)

	if del > 0 {
		evExpire, eErr := emitExpireEvent(ctx, tx, nowMS, cfg, del, freed, postMsgs, postBytes)
		if eErr != nil {
			return 0, 0, 0, 0, false, nil, eErr
		}
		evs = append(evs, evExpire)
	}
	if stillBlocked {
		evBlock, bErr := c.emitBlockedEvent(ctx, tx, nowMS, cfg,
			plan.HighestBlockedSeq, plan.BlockedBytes)
		if bErr != nil {
			return 0, 0, 0, 0, false, nil, bErr
		}
		evs = append(evs, evBlock)
	}
	return del, freed, plan.BlockedCount, plan.BlockedBytes, more, evs, nil
}

// applyDelete runs THE guarded delete via retentionDeleteBatch.
func (c RetentionCmd) applyDelete(ctx context.Context, tx *sql.Tx,
	stream string, seqs []int64,
) (int64, int64, error) {
	return retentionDeleteBatch(ctx, tx, stream, seqs)
}

// retentionDeleteBatch deletes the given seqs with the in-SQL delivery-row guard and
// returns exact accounting read back through RETURNING size. It is a package-level
// function so the guard test can drive the statement directly, pin present or not —
// the two-way anchor for G1.
func retentionDeleteBatch(ctx context.Context, tx *sql.Tx,
	stream string, seqs []int64,
) (n int64, freed int64, rerr error) {
	if len(seqs) == 0 {
		return 0, 0, nil
	}
	ph := strings.TrimSuffix(strings.Repeat("?,", len(seqs)), ",")
	stmt := fmt.Sprintf(retentionDeleteReturningSQL, ph)
	args := make([]any, 0, len(seqs)+1)
	args = append(args, stream)
	for _, sq := range seqs {
		args = append(args, sq)
	}
	rows, qErr := tx.QueryContext(ctx, stmt, args...)
	if qErr != nil {
		return 0, 0, fmt.Errorf("retention guarded delete %s: %w", stream, qErr)
	}
	defer func() {
		if cErr := rows.Close(); cErr != nil && rerr == nil {
			rerr = fmt.Errorf("close retained deletions %s: %w", stream, cErr)
		}
	}()
	for rows.Next() {
		var sz int64
		if sErr := rows.Scan(&sz); sErr != nil {
			return 0, 0, fmt.Errorf("scan retained deletion size %s: %w", stream, sErr)
		}
		n++
		freed += sz
	}
	if eErr := rows.Err(); eErr != nil {
		return 0, 0, fmt.Errorf("iterate retained deletions %s: %w", stream, eErr)
	}
	return n, freed, nil
}

// planWorkqueue builds the workqueue eviction plan from below-floor candidates:
// every unpinned message under the floor leaves, oldest-first, within the batch bound;
// pins count toward the same blame accounting as the limits passes.
func planWorkqueue(cands []queue.Candidate, batch int) queue.EvictionPlan {
	plan := queue.EvictionPlan{}
	for i := range cands {
		cd := &cands[i]
		if cd.HasDelivery {
			plan.BlockedCount++
			plan.BlockedBytes += cd.Size
			if cd.Seq > plan.HighestBlockedSeq {
				plan.HighestBlockedSeq = cd.Seq
			}
			continue
		}
		if len(plan.Seqs) >= batch {
			break
		}
		plan.Seqs = append(plan.Seqs, cd.Seq)
		plan.FreedBytes += cd.Size
		if cd.Seq > plan.HighestDeletedSeq {
			plan.HighestDeletedSeq = cd.Seq
		}
	}
	plan.More = len(plan.Seqs) == batch
	return plan
}

// updateWatermarks advances expired_seq/expired_at monotonically and settles the
// sweep's exact deletion accounting into the maintained counters.
func updateWatermarks(ctx context.Context, tx *sql.Tx, stream string,
	highDel, nowMS int64, deleted, freed int64,
) error {
	if _, uErr := tx.ExecContext(ctx, `
		UPDATE stream_stats
		   SET msgs        = msgs - ?,
		       bytes       = bytes - ?,
		       expired_seq = MAX(expired_seq, ?),
		       expired_at  = CASE WHEN ? > expired_seq THEN ? ELSE expired_at END
		 WHERE stream = ?`,
		deleted, freed, highDel, highDel, nowMS, stream); uErr != nil {
		return fmt.Errorf("retention watermarks %s: %w", stream, uErr)
	}
	return nil
}

// emitExpireEvent writes the aggregate expiry audit row + carrier for one sweep slice.
func emitExpireEvent(ctx context.Context, tx *sql.Tx, nowMS int64, cfg retentionStreamCfg,
	deleted, freed, postMsgs, postBytes int64,
) (obs.Event, error) {
	detail, jErr := jsonMarshal(map[string]any{
		"deleted":    deleted,
		"freed":      freed,
		"msgs_left":  postMsgs,
		"bytes_left": postBytes,
	})
	if jErr != nil { // unreachable map of scalars
		detail = []byte(`{}`)
	}
	return commitEvent(ctx, tx, event{
		ts:     nowMS,
		name:   "retention.expire",
		stream: nullStr(cfg.name),
		detail: nullStr(string(detail)),
	})
}

// emitBlockedEvent blames the OLDEST blocking holder (queue.SelectBlame's contract)
// and names the blocked payload size plus the blocked head's age in ms.
func (c RetentionCmd) emitBlockedEvent(ctx context.Context, tx *sql.Tx, nowMS int64,
	cfg retentionStreamCfg, highestBlockedSeq, wouldFreeBytes int64) (
	ev obs.Event, rerr error,
) {
	// Blame window: delivery rows at or below the pinned frontier. Pins ABOVE it were
	// never blocking anything the pass wanted.
	var holders []queue.Holder
	rows, qErr := tx.QueryContext(ctx, `
		SELECT d.consumer, d.seq
		  FROM deliveries d
		 WHERE d.stream = ? AND d.seq <= ?
		 ORDER BY d.seq ASC
		 LIMIT ?`, cfg.name, highestBlockedSeq, maxBlameHolders)
	if qErr != nil {
		return obs.Event{}, fmt.Errorf("retention blame scan %s: %w", cfg.name, qErr)
	}
	defer func() {
		if cErr := rows.Close(); cErr != nil && rerr == nil {
			rerr = fmt.Errorf("close retention holders %s: %w", cfg.name, cErr)
		}
	}()
	for rows.Next() {
		var h queue.Holder
		if sErr := rows.Scan(&h.Consumer, &h.Seq); sErr != nil {
			return obs.Event{}, fmt.Errorf("scan retention holder %s: %w", cfg.name, sErr)
		}
		holders = append(holders, h)
	}
	if eErr := rows.Err(); eErr != nil {
		return obs.Event{}, fmt.Errorf("iterate retention holders %s: %w", cfg.name, eErr)
	}
	blame, ok := queue.SelectBlame(holders)
	if !ok {
		// Every pin vanished between the candidate probe and this query inside the
		// same transaction — impossible for concurrent writers (they serialize),
		// possible only if the plan misjudged. Report nothing rather than inventing
		// a culprit.
		return obs.Event{}, nil
	}

	ageMS := int64(0)
	var oldest sql.NullInt64
	aErr := tx.QueryRowContext(ctx,
		`SELECT min(published_at) FROM messages WHERE stream = ? AND seq <= ?`,
		cfg.name, highestBlockedSeq).Scan(&oldest)
	if aErr != nil {
		return obs.Event{}, fmt.Errorf("retention blocked age %s: %w", cfg.name, aErr)
	}
	if oldest.Valid && nowMS > oldest.Int64 {
		ageMS = nowMS - oldest.Int64
	}

	detail, jErr := jsonMarshal(map[string]any{
		"blocking_seq":     blame.Seq,
		"would_free_bytes": wouldFreeBytes,
		"age_ms":           ageMS,
	})
	if jErr != nil { // unreachable map of scalars
		detail = []byte(`{}`)
	}
	return commitEvent(ctx, tx, event{
		ts:       nowMS,
		name:     "retention.blocked",
		stream:   nullStr(cfg.name),
		consumer: nullStr(blame.Consumer),
		detail:   nullStr(string(detail)),
	})
}
