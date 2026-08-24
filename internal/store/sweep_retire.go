// SPDX-License-Identifier: Apache-2.0

package store

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/a-holm/messq/internal/obs"
	"github.com/a-holm/messq/internal/queue"
)

// The retire writer command (issue #11 §7): lowering max_deliver below current attempts
// strands READY rows at attempts >= max_deliver, which nothing else would ever retire —
// the expiry pass only scans INFLIGHT, and #9's claim has no max_deliver guard (a
// stranded row claimed would reach attempts = max_deliver + 1 and violate I4). RetireCmd
// dead-letters them with Trigger "policy_lowered", Cause DeadMaxDeliver, exactly the
// trigger #12's edge-case table expects. Only READY rows are retired (never mid-flight);
// max_deliver=0 consumers are skipped entirely.

const kindRetire CmdKind = "consumer.retire"

// RetireResult aggregates one retire pass.
type RetireResult struct {
	Retired int
}

// RetireCmd is one writer command. Unexported fields are populated by [Store.Retire].
type RetireCmd struct {
	Limit    int
	DeadSink DeadSink // #12 seam; nil = DropSink
	metrics  SweepMetrics
}

func (c RetireCmd) Kind() CmdKind { return kindRetire }
func (c RetireCmd) Bytes() int    { return 0 } // metadata only

// Store.Retire applies one retire pass. The pass is bounded per consumer by Limit.
func (s *Store) Retire(ctx context.Context, req RetireCmd) (RetireResult, error) {
	if req.Limit <= 0 {
		req.Limit = s.maxSweepBatch
	}
	r := RetireCmd{Limit: req.Limit, DeadSink: req.DeadSink}
	if r.DeadSink == nil {
		r.DeadSink = s.deadSink
	}
	r.metrics = s.sweepMetrics
	res, err := s.enqueue(ctx, "store.Retire", r)
	if err != nil {
		return RetireResult{}, err
	}
	rr, ok := res.(RetireResult)
	if !ok {
		return RetireResult{}, fmt.Errorf("store.Retire: engine returned %T, want RetireResult", res)
	}
	return rr, nil
}

// Apply runs inside the writer's batch transaction.
func (c RetireCmd) Apply(ctx context.Context, tx *sql.Tx, now time.Time) (Result, []obs.Event, error) {
	var res RetireResult
	var events []obs.Event

	// Consumers with max_deliver > 0 (max_deliver=0 never dies by count; skipped).
	cons, err := tx.QueryContext(ctx,
		`SELECT stream, name, max_deliver, generation FROM consumers WHERE max_deliver > 0`)
	if err != nil {
		return nil, nil, fmt.Errorf("retire scan consumers: %w", err)
	}
	type consRow struct {
		stream, name string
		maxDeliver   int32
		generation   int32
	}
	var targets []consRow
	for cons.Next() {
		var cr consRow
		var md int64
		if sErr := cons.Scan(&cr.stream, &cr.name, &md, &cr.generation); sErr != nil {
			cons.Close()
			return nil, nil, fmt.Errorf("scan retire consumer: %w", sErr)
		}
		//nolint:gosec // G115: max_deliver validated <= maxDeliverCap at create.
		cr.maxDeliver = int32(md)
		targets = append(targets, cr)
	}
	if rErr := cons.Err(); rErr != nil {
		cons.Close()
		return nil, nil, fmt.Errorf("iterate retire consumers: %w", rErr)
	}
	if err := cons.Close(); err != nil {
		return nil, nil, fmt.Errorf("close retire consumers: %w", err)
	}

	//nolint:gosec // G115: max_deliver validated to fit int64.
	maxDeliverArg := func(c consRow) int64 { return int64(c.maxDeliver) }

	for _, t := range targets {
		rows, err := tx.QueryContext(ctx, `
			SELECT d.seq, coalesce(d.subject, ''), d.attempts, d.generation,
			       coalesce(d.delivered_at, 0), coalesce(d.last_reason, ''),
			       coalesce(m.id, ''), coalesce(m.trace_id, '')
			  FROM deliveries d
			  LEFT JOIN messages m ON m.stream = d.stream AND m.seq = d.seq
			 WHERE d.stream = ? AND d.consumer = ? AND d.state = 0
			   AND d.attempts >= ? AND d.generation = ?
			 ORDER BY d.seq LIMIT ?`,
			t.stream, t.name, maxDeliverArg(t), t.generation, c.Limit)
		if err != nil {
			return nil, nil, fmt.Errorf("retire scan %q/%q: %w", t.stream, t.name, err)
		}
		var rowsOut []struct {
			seq         int64
			subject     string
			attempts    int32
			generation  int32
			deliveredAt int64
			lastReason  string
			msgID       string
			traceID     string
		}
		for rows.Next() {
			var o struct {
				seq         int64
				subject     string
				attempts    int32
				generation  int32
				deliveredAt int64
				lastReason  string
				msgID       string
				traceID     string
			}
			var att int64
			var gen int64
			if sErr := rows.Scan(&o.seq, &o.subject, &att, &gen, &o.deliveredAt,
				&o.lastReason, &o.msgID, &o.traceID); sErr != nil {
				rows.Close()
				return nil, nil, fmt.Errorf("scan retire row %q/%q: %w", t.stream, t.name, sErr)
			}
			//nolint:gosec // G115: bounded by deliveries.attempts/generation (int64).
			o.attempts = int32(att)
			o.generation = int32(gen)
			rowsOut = append(rowsOut, o)
		}
		if rErr := rows.Err(); rErr != nil {
			rows.Close()
			return nil, nil, fmt.Errorf("iterate retire rows %q/%q: %w", t.stream, t.name, rErr)
		}
		if err := rows.Close(); err != nil {
			return nil, nil, fmt.Errorf("close retire rows %q/%q: %w", t.stream, t.name, err)
		}

		for _, o := range rowsOut {
			//nolint:gosec // G115: seq is bounded by the message sequence space.
			dc := queue.DeadCtx{
				Stream: t.stream, Consumer: t.name, Subject: o.subject,
				Seq: uint64(o.seq), MsgID: o.msgID, TraceID: o.traceID,
				Attempts: o.attempts, Cause: queue.DeadCauseMaxDeliver,
				LastReason: o.lastReason,
			}
			evDead, sinkErr := c.DeadSink.Dead(ctx, tx, dc, now)
			if sinkErr != nil {
				return nil, nil, fmt.Errorf("retire dead sink %q/%q seq %d: %w", t.stream, t.name, o.seq, sinkErr)
			}
			events = append(events, evDead)
			//nolint:gosec // G202: constant fence clause.
			d, dErr := tx.ExecContext(ctx, `
				DELETE FROM deliveries
				 WHERE stream = ? AND consumer = ? AND seq = ? AND state = 0
				   AND attempts = ? AND generation = ?`,
				t.stream, t.name, o.seq, o.attempts, o.generation)
			if dErr != nil {
				return nil, nil, fmt.Errorf("retire delete %q/%q seq %d: %w", t.stream, t.name, o.seq, dErr)
			}
			n, aErr := d.RowsAffected()
			if aErr != nil {
				return nil, nil, fmt.Errorf("retire rows-affected %q/%q seq %d: %w", t.stream, t.name, o.seq, aErr)
			}
			if n == 0 {
				continue // a sibling claimed/acked it; not a bug (never mid-flight)
			}
			res.Retired++
			c.metrics.Dead()
		}
	}
	return res, events, nil
}
