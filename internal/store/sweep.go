// SPDX-License-Identifier: Apache-2.0

package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/a-holm/messq/internal/errs"
	"github.com/a-holm/messq/internal/obs"
	"github.com/a-holm/messq/internal/queue"
)

// The sweeper writer command (issue #11): the timeout arm of the backoff schedule. It
// scans the expired INFLIGHT rows, decides each through the pure queue.DecideSweep
// planner, and applies redeliver/dead/skip inside the engine's batch transaction — the
// ONE place a timeout becomes a durable state (S7.3). It never touches attempts (D6):
// a timeout on attempt n releases the row still at n, and the next claim makes it n+1.
// The per-row statement carries the WHERE fence (state/attempts/generation) so a sibling
// command in the same batch that already resolved the row is never written over.

const kindSweep CmdKind = "consumer.sweep"

// Sweeper metric-name constants (issue #11 §9.4). #21 registers collectors against these
// names; this issue counts every outcome through the SweepMetrics seam.
const (
	metricTimeoutsTotal = "messq_timeouts_total"
	//nolint:gosec // G101: the wire metric name is a handover constant handed to #21, not a credential.
	metricRedeliveredTotal  = "messq_redelivered_total"
	metricDeadTotal         = "messq_dead_total"
	metricSweepLatenessSecs = "messq_sweep_lateness_seconds"
	metricSweepDurationSecs = "messq_sweep_duration_seconds"
	metricSweepRows         = "messq_sweep_rows"
	metricSweepBacklog      = "messq_sweep_backlog"
	metricSweepSkippedTotal = "messq_sweep_skipped_total"
)

// SweepMetrics counts every sweep outcome exactly. Runs on the writer goroutine.
type SweepMetrics interface {
	Timeout()     // a lease expired (counted even when the row later skips)
	Redelivered() // an expired row released to READY at a new deadline
	Dead()        // an expired row routed to the dead seam at the bound
}

type nopSweepMetrics struct{}

func (nopSweepMetrics) Timeout()     {}
func (nopSweepMetrics) Redelivered() {}
func (nopSweepMetrics) Dead()        {}

// SweepResult aggregates one expiry-sweep command.
type SweepResult struct {
	Expired       int
	Redelivered   int
	Dead          int
	Skipped       int
	Deferred      int // DEAD rows left for a later tick because the copy budget bound this tx
	Woke          []queue.ConsumerKey
	More          bool
	NextDueMS     int64 // MIN(visible_at) over remaining INFLIGHT rows; 0 = none
	MaxLatenessMS int64
}

// SweepCmd is one writer command. Unexported fields are populated by [Store.Sweep].
type SweepCmd struct {
	Limit    int
	DeadSink DeadSink // #12 seam; nil = DropSink
	metrics  SweepMetrics
	jitter   queue.Jitter
}

func (c SweepCmd) Kind() CmdKind { return kindSweep }
func (c SweepCmd) Bytes() int    { return 0 } // metadata only until #12's DLQ copy lands

// Store.Sweep applies one expiry-sweep batch. The batch is bounded by the Store's
// maxSweepBatch (I11).
func (s *Store) Sweep(ctx context.Context, req SweepCmd) (SweepResult, error) {
	if req.Limit <= 0 {
		req.Limit = s.maxSweepBatch
	}
	if req.Limit > s.maxSweepBatch {
		return SweepResult{}, errs.E(errs.ErrBadRequest, "Store.Sweep",
			"a sweep batch of %d rows exceeds --sweep-max-batch %d", req.Limit, s.maxSweepBatch)
	}
	r := SweepCmd{Limit: req.Limit, DeadSink: req.DeadSink}
	if r.DeadSink == nil {
		r.DeadSink = s.newDeadSink()
	}
	r.metrics = s.sweepMetrics
	r.jitter = s.jitter
	res, err := s.enqueue(ctx, "store.Sweep", r)
	if err != nil {
		return SweepResult{}, err
	}
	sr, ok := res.(SweepResult)
	if !ok {
		return SweepResult{}, fmt.Errorf("store.Sweep: engine returned %T, want SweepResult", res)
	}
	return sr, nil
}

// sweepRow is one expired delivery row, plus the message identity needed for the
// msg.timeout event and the dead path ("" when the origin message was purged).
type sweepRow struct {
	key         queue.ConsumerKey
	seq         int64
	subject     string
	attempts    int32
	generation  int32
	visibleAt   int64
	deliveredAt int64
	lastReason  string
	msgID       string
	traceID     string
}

// sweepPolicy is the slice of the consumers row a sweep fences against.
type sweepPolicy struct {
	found bool
	cfg   queue.SweepPolicy
	// ackWaitMS is cached for the msg.timeout event detail (ack_wait_ms).
	ackWaitMS int64
}

// loadSweepPolicy loads the fence facts of one consumer. Not-found is reported
// (found=false) so the row is skipped rather than mutated against a half-config.
func loadSweepPolicy(ctx context.Context, tx *sql.Tx, key queue.ConsumerKey) (sweepPolicy, error) {
	var sp sweepPolicy
	var ackWaitMS, maxDeliver int64
	var backoffJSON string
	err := tx.QueryRowContext(ctx, `
		SELECT ack_wait_ms, max_deliver, backoff_ms, generation
		  FROM consumers WHERE stream = ? AND name = ?`, key.Stream, key.Consumer).
		Scan(&ackWaitMS, &maxDeliver, &backoffJSON, &sp.cfg.Generation)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		return sp, nil
	case err != nil:
		return sp, fmt.Errorf("read consumer %q/%q: %w", key.Stream, key.Consumer, err)
	}
	sp.found = true
	sp.ackWaitMS = ackWaitMS
	sp.cfg.AckWait = time.Duration(ackWaitMS) * time.Millisecond
	//nolint:gosec // G115: max_deliver is validated <= maxDeliverCap (int32) at create.
	sp.cfg.MaxDeliver = int32(maxDeliver)
	var ms []int64
	if jErr := json.Unmarshal([]byte(backoffJSON), &ms); jErr != nil {
		return sp, fmt.Errorf("parse backoff of %q/%q: %w", key.Stream, key.Consumer, jErr)
	}
	for _, v := range ms {
		sp.cfg.Backoff = append(sp.cfg.Backoff, time.Duration(v)*time.Millisecond)
	}
	return sp, nil
}

func (c SweepCmd) Apply(ctx context.Context, tx *sql.Tx, now time.Time) (_ Result, _ []obs.Event, err error) {
	nowMS := now.UnixMilli()
	var res SweepResult
	var events []obs.Event

	// Phase 1 — the expired set, in total order, bounded. The LEFT JOIN is deliberate: an
	// orphaned delivery row (message purged under it) must still be retirable.
	rows, err := tx.QueryContext(ctx, `
		SELECT d.stream, d.consumer, d.seq, coalesce(d.subject, ''), d.attempts, d.generation,
		       d.visible_at, coalesce(d.delivered_at, 0), coalesce(d.last_reason, ''),
		       coalesce(m.id, ''), coalesce(m.trace_id, '')
		  FROM deliveries d
		  LEFT JOIN messages m ON m.stream = d.stream AND m.seq = d.seq
		 WHERE d.state = 1 AND d.visible_at <= ?
		 ORDER BY d.visible_at, d.stream, d.consumer, d.seq
		 LIMIT ?`, nowMS, c.Limit)
	if err != nil {
		return nil, nil, fmt.Errorf("sweep expired scan: %w", err)
	}
	defer func() {
		if cerr := rows.Close(); cerr != nil && err == nil {
			err = fmt.Errorf("close sweep rows: %w", cerr)
		}
	}()
	var loaded []sweepRow
	for rows.Next() {
		var r sweepRow
		if sErr := rows.Scan(&r.key.Stream, &r.key.Consumer, &r.seq, &r.subject,
			&r.attempts, &r.generation, &r.visibleAt, &r.deliveredAt, &r.lastReason,
			&r.msgID, &r.traceID); sErr != nil {
			return nil, nil, fmt.Errorf("scan sweep row: %w", sErr)
		}
		loaded = append(loaded, r)
	}
	if rErr := rows.Err(); rErr != nil {
		return nil, nil, fmt.Errorf("iterate sweep rows: %w", rErr)
	}
	res.Expired = len(loaded)
	res.More = len(loaded) == c.Limit

	// Phase 2 — consumer policies, once per distinct (stream, consumer) in the batch.
	policies := make(map[queue.ConsumerKey]sweepPolicy)
	for _, r := range loaded {
		if _, ok := policies[r.key]; ok {
			continue
		}
		sp, lErr := loadSweepPolicy(ctx, tx, r.key)
		if lErr != nil {
			return nil, nil, lErr
		}
		policies[r.key] = sp
	}

	// Phase 3 — decide + apply per row.
	woke := make(map[queue.ConsumerKey]bool)
	for _, r := range loaded {
		sp, ok := policies[r.key]
		if !ok || !sp.found {
			res.Skipped++
			continue
		}
		dec := queue.DecideSweep(queue.SweepRow{
			Key: r.key, Seq: r.seq, Subject: r.subject,
			Attempts: r.attempts, Generation: r.generation,
			VisibleAt: r.visibleAt, DeliveredAt: r.deliveredAt,
			LastReason: r.lastReason, MsgID: r.msgID, TraceID: r.traceID,
		}, sp.cfg, nowMS, c.jitter)
		if dec.LatenessMS > res.MaxLatenessMS {
			res.MaxLatenessMS = dec.LatenessMS
		}
		// Every expired lease is a timeout (G10): counted regardless of whether the row
		// is then redelivered, dead-lettered, or skipped by a fence/sibling.
		c.metrics.Timeout()

		switch dec.Action {
		case queue.SweepSkip:
			res.Skipped++

		case queue.SweepRedeliver:
			ev, eErr := c.timeoutEvent(ctx, tx, nowMS, r, sp, dec)
			if eErr != nil {
				return nil, nil, eErr
			}
			events = append(events, ev)
			u, uErr := tx.ExecContext(ctx, `
				UPDATE deliveries SET state = 0, visible_at = ?, last_reason = 'ack_wait'
				 WHERE stream = ? AND consumer = ? AND seq = ? AND state = 1
				   AND attempts = ? AND generation = ?`,
				dec.VisibleAt, r.key.Stream, r.key.Consumer, r.seq, r.attempts, r.generation)
			if uErr != nil {
				return nil, nil, fmt.Errorf("sweep redeliver %q/%q seq %d: %w", r.key.Stream, r.key.Consumer, r.seq, uErr)
			}
			// No changes()==1 assertion here: a sibling SettleCmd in the same batch may
			// have acked the row after the scan ran. A zero change is legal and counted
			// into Skipped (the deliberate divergence from #10's changes()==1 rule).
			n, aErr := u.RowsAffected()
			if aErr != nil {
				return nil, nil, fmt.Errorf("sweep redeliver rows-affected %q/%q seq %d: %w", r.key.Stream, r.key.Consumer, r.seq, aErr)
			}
			if n == 0 {
				res.Skipped++
				continue
			}
			res.Redelivered++
			c.metrics.Redelivered()
			if dec.VisibleAt <= nowMS && !woke[r.key] {
				woke[r.key] = true
				res.Woke = append(res.Woke, r.key)
			}

		case queue.SweepDead:
			ev, eErr := c.timeoutEvent(ctx, tx, nowMS, r, sp, dec)
			if eErr != nil {
				return nil, nil, eErr
			}
			events = append(events, ev)
			//nolint:gosec // G115: seq is bounded by the message sequence space (int64).
			dc := queue.DeadCtx{
				Stream: r.key.Stream, Consumer: r.key.Consumer, Subject: r.subject,
				Seq: uint64(r.seq), MsgID: r.msgID, TraceID: r.traceID,
				Attempts: r.attempts, Cause: dec.Cause, LastReason: r.lastReason,
				Generation: r.generation, MaxDeliver: sp.cfg.MaxDeliver,
				Trigger: queue.DeadTrigger(dec.Trigger),
			}
			evDead, sinkErr := c.DeadSink.Dead(ctx, tx, dc, now)
			if errors.Is(sinkErr, queue.ErrDeadBudget) {
				// The copy budget bound this transaction: leave the row INFLIGHT (it is
				// still expired; the next tick retries the death, I4-bounded). No delete,
				// no msg.dead — a deferred death is not a death yet.
				res.Deferred++
				continue
			}
			if sinkErr != nil {
				return nil, nil, fmt.Errorf("sweep dead sink %q/%q seq %d: %w", r.key.Stream, r.key.Consumer, r.seq, sinkErr)
			}
			events = append(events, evDead)
			d, dErr := tx.ExecContext(ctx, `
				DELETE FROM deliveries
				 WHERE stream = ? AND consumer = ? AND seq = ? AND state = 1
				   AND attempts = ? AND generation = ?`,
				r.key.Stream, r.key.Consumer, r.seq, r.attempts, r.generation)
			if dErr != nil {
				return nil, nil, fmt.Errorf("sweep dead-delete %q/%q seq %d: %w", r.key.Stream, r.key.Consumer, r.seq, dErr)
			}
			n, aErr := d.RowsAffected()
			if aErr != nil {
				return nil, nil, fmt.Errorf("sweep dead rows-affected %q/%q seq %d: %w", r.key.Stream, r.key.Consumer, r.seq, aErr)
			}
			if n == 0 {
				res.Skipped++
				continue
			}
			res.Dead++
			c.metrics.Dead()
		}
	}

	// NextDueMS: MIN(visible_at) over the remaining INFLIGHT rows (0 = none).
	var next sql.NullInt64
	if err := tx.QueryRowContext(ctx,
		`SELECT MIN(visible_at) FROM deliveries WHERE state = 1`).Scan(&next); err != nil {
		return nil, nil, fmt.Errorf("sweep next-due: %w", err)
	}
	if next.Valid {
		res.NextDueMS = next.Int64
	}
	return res, events, nil
}

// timeoutEvent writes the msg.timeout audit row (WARN, never sampleable, NOT
// rate-limited; G10) and returns the carrier. detail carries the full field set:
// held_ms, lateness_ms, ack_wait_ms, schedule_ms, delay_ms, retry_at, attempt,
// max_deliver and cause:"ack_wait". retry_at is the redeliver deadline (0 when dead).
func (c SweepCmd) timeoutEvent(ctx context.Context, tx *sql.Tx, nowMS int64, r sweepRow, sp sweepPolicy, dec queue.SweepDecision) (obs.Event, error) {
	retryAt := int64(0)
	if dec.Action == queue.SweepRedeliver {
		retryAt = dec.VisibleAt
	}
	detail := fmt.Sprintf(`{"held_ms":%d,"lateness_ms":%d,"ack_wait_ms":%d,"schedule_ms":%d,"delay_ms":%d,"retry_at":%d,"attempt":%d,"max_deliver":%d,"cause":"ack_wait"}`,
		dec.HeldMS, dec.LatenessMS, sp.ackWaitMS, dec.ScheduleMS, dec.DelayMS, retryAt,
		r.attempts, sp.cfg.MaxDeliver)
	return commitEvent(ctx, tx, event{
		ts: nowMS, name: "msg.timeout",
		stream: nullStr(r.key.Stream), consumer: nullStr(r.key.Consumer),
		subject: nullStr(r.subject), seq: nullI64(r.seq),
		attempt: nullI64(int64(r.attempts)),
		msgID:   nullStr(r.msgID), traceID: nullStr(r.traceID),
		detail: nullStr(detail),
	})
}
