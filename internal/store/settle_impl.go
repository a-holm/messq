// SPDX-License-Identifier: Apache-2.0

package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand/v2"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/a-holm/messq/internal/obs"
	"github.com/a-holm/messq/internal/queue"
)

// The store half of the settle command: the row/consumer loaders, the per-item
// mutation executor (applyAction), the DeadSink seam (DropSink until #12), and the
// rejection-event repeat limiter.

// settleConsumer is the slice of the consumers row a settle fences against.
type settleConsumer struct {
	found      bool
	generation int32
	cursorSeq  int64
	cfg        queue.ConsumerConfig
}

// loadSettleConsumer loads the fence facts of one consumer. Not-found is reported
// (found=false) so the planner can answer unknown (Note A) instead of synthesising an
// idempotent success.
func loadSettleConsumer(ctx context.Context, tx *sql.Tx, stream, name string) (settleConsumer, error) {
	var sc settleConsumer
	var ackWaitMS, maxDeliver int64
	var backoffJSON string
	err := tx.QueryRowContext(ctx, `
		SELECT generation, cursor_seq, ack_wait_ms, max_deliver, backoff_ms
		  FROM consumers WHERE stream = ? AND name = ?`, stream, name).
		Scan(&sc.generation, &sc.cursorSeq, &ackWaitMS, &maxDeliver, &backoffJSON)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		return settleConsumer{}, nil
	case err != nil:
		return sc, fmt.Errorf("read consumer %q/%q: %w", stream, name, err)
	}
	sc.found = true
	sc.cfg = queue.DefaultConsumerConfig(name)
	sc.cfg.AckWait = time.Duration(ackWaitMS) * time.Millisecond
	//nolint:gosec // G115: max_deliver is validated to <= maxDeliverCap (int32) at create.
	sc.cfg.MaxDeliver = int32(maxDeliver)
	var ms []int64
	if err := json.Unmarshal([]byte(backoffJSON), &ms); err != nil {
		return sc, fmt.Errorf("parse backoff of %q/%q: %w", stream, name, err)
	}
	for _, v := range ms {
		sc.cfg.Backoff = append(sc.cfg.Backoff, time.Duration(v)*time.Millisecond)
	}
	return sc, nil
}

// settleRow is one live delivery row plus the message identity needed for events and
// the dead path.
type settleRow struct {
	seq         uint64
	state       int // 0 READY, 1 INFLIGHT
	attempts    int32
	visibleAt   int64
	generation  int32
	deliveredAt int64
	subject     string
	msgID       string
	traceID     string
}

// settleRowChunk bounds the batched LEFT-JOIN row load per query (brief R7).
const settleRowChunk = 200

// loadSettleRows loads the live delivery rows for a group in chunks of at most
// settleRowChunk seqs, LEFT-JOINing messages so an orphaned delivery row (message
// gone) stays settleable — the settle only closes deliveries.
func loadSettleRows(ctx context.Context, tx *sql.Tx, stream, consumer string, seqs []int64) (map[int64]settleRow, error) {
	out := make(map[int64]settleRow, len(seqs))
	for start := 0; start < len(seqs); start += settleRowChunk {
		end := start + settleRowChunk
		if end > len(seqs) {
			end = len(seqs)
		}
		chunk := seqs[start:end]
		ph := strings.TrimSuffix(strings.Repeat("?,", len(chunk)), ",")
		args := []any{stream, consumer}
		for _, s := range chunk {
			args = append(args, s)
		}
		//nolint:gosec // G202: compile-time placeholder list only.
		rows, qErr := tx.QueryContext(ctx, `
			SELECT d.seq, d.state, d.attempts, d.visible_at, d.generation,
			       coalesce(d.delivered_at, 0), coalesce(d.subject, ''), coalesce(m.id, ''), coalesce(m.trace_id, '')
			  FROM deliveries d
			  LEFT JOIN messages m ON m.stream = d.stream AND m.seq = d.seq
			 WHERE d.stream = ? AND d.consumer = ? AND d.seq IN (`+ph+`)`, args...)
		if qErr != nil {
			return nil, fmt.Errorf("load settle rows %q/%q: %w", stream, consumer, qErr)
		}
		defer func() {
			if cErr := rows.Close(); cErr != nil && qErr == nil {
				qErr = fmt.Errorf("close settle rows of %q/%q: %w", stream, consumer, cErr)
			}
		}()
		for rows.Next() {
			var r settleRow
			if sErr := rows.Scan(&r.seq, &r.state, &r.attempts, &r.visibleAt, &r.generation,
				&r.deliveredAt, &r.subject, &r.msgID, &r.traceID); sErr != nil {
				qErr = fmt.Errorf("scan settle row of %q/%q: %w", stream, consumer, sErr)
				break
			}
			//nolint:gosec // G115: the deliveries seq column is signed int64.
			out[int64(r.seq)] = r
		}
		if rErr := rows.Err(); rErr != nil && qErr == nil {
			qErr = fmt.Errorf("iterate settle rows of %q/%q: %w", stream, consumer, rErr)
		}
		if qErr != nil {
			return nil, qErr
		}
	}
	return out, nil
}

// DeadSink is the #12 seam: it runs INSIDE the settle transaction against a fully
// populated DeadCtx, once per DEAD transition. DropSink (the default, dead_policy=drop)
// deletes and records the msg.dead audit row; #12 implements the DLQ copy.
type DeadSink interface {
	Dead(ctx context.Context, tx *sql.Tx, d queue.DeadCtx, now time.Time) (obs.Event, error)
}

// DropSink is the provisional dead policy: it writes the msg.dead audit row and
// returns the carrier; the command deletes the delivery row itself.
type DropSink struct{}

func (DropSink) Dead(ctx context.Context, tx *sql.Tx, d queue.DeadCtx, now time.Time) (obs.Event, error) {
	ev, err := commitEvent(ctx, tx, event{
		ts:       now.UnixMilli(),
		name:     "msg.dead",
		stream:   nullStr(d.Stream),
		consumer: nullStr(d.Consumer),
		subject:  nullStr(d.Subject),
		//nolint:gosec // G115: seq fitted into uint64 at the sink seam; it fits int64 here.
		seq:     nullI64(int64(d.Seq)),
		attempt: nullI64(int64(d.Attempts)),
		msgID:   nullStr(d.MsgID),
		traceID: nullStr(d.TraceID),
		detail:  nullStr(fmt.Sprintf(`{"cause":%q,"attempts":%d,"last_reason":%q}`, d.Cause, d.Attempts, d.LastReason)),
	})
	return ev, err
}

// applyAction executes the mutation a PlanSettle decided (Status OK). Every statement
// repeats the WHERE fence and asserts changes()==1; a zero means the planner and the
// database disagree, which aborts the batch loudly (I7's second line of defence).
func (c SettleCmd) applyAction(ctx context.Context, tx *sql.Tx, now time.Time, nowMS int64,
	plan queue.SettlePlan, it SettleItem, row settleRow, cons settleConsumer,
	reason string, res *ItemResult,
) ([]obs.Event, error) {
	var events []obs.Event
	fence := func(suffix string) string {
		return "stream = ? AND consumer = ? AND seq = ? AND attempts = ? AND generation = ?" + suffix
	}
	fenceArgs := func(base []any) []any {
		return append(base, it.Token.Stream, it.Token.Consumer, it.Token.Seq, it.Token.Attempt, it.Token.Generation)
	}
	changed := func(r sql.Result, what string) error {
		n, err := r.RowsAffected()
		if err != nil {
			return fmt.Errorf("%s: read rows affected: %w", what, err)
		}
		if n != 1 {
			return fmt.Errorf("%s changed %d rows, want exactly 1 (fence held)", what, n)
		}
		return nil
	}

	switch plan.Action {
	case queue.ActionNoop:
		// impossible for a Status-OK plan; guard so exhaustive stays total.
		return nil, nil

	case queue.ActionAck:
		// T3: the terminal state IS the row's absence (D5).
		//nolint:gosec // G202: the interpolated fragment is a constant fence clause.
		r, err := tx.ExecContext(ctx, `DELETE FROM deliveries WHERE `+fence(""), fenceArgs(nil)...)
		if err != nil {
			return nil, fmt.Errorf("ack delete of %q/%q seq %d: %w", it.Token.Stream, it.Token.Consumer, it.Token.Seq, err)
		}
		if aErr := changed(r, "ack delete"); aErr != nil {
			return nil, fmt.Errorf("%w of %q/%q seq %d", aErr, it.Token.Stream, it.Token.Consumer, it.Token.Seq)
		}
		held := plan.HeldMS
		if held < 0 {
			held = 0
		}
		res.HeldFor = time.Duration(held) * time.Millisecond
		res.Late = plan.Late
		detail := fmt.Sprintf(`{"held_ms":%d}`, held)
		if plan.Late {
			detail = fmt.Sprintf(`{"held_ms":%d,"late":true}`, held)
		}
		ev, eErr := commitEvent(ctx, tx, event{
			ts: nowMS, name: "msg.ack",
			stream: nullStr(it.Token.Stream), consumer: nullStr(it.Token.Consumer),
			seq: nullI64(it.Token.Seq), attempt: nullI64(int64(it.Token.Attempt)),
			msgID: nullStr(row.msgID), traceID: nullStr(row.traceID),
			subject: nullStr(row.subject), detail: nullStr(detail),
		})
		if eErr != nil {
			return nil, eErr
		}
		events = append(events, ev)
		c.metrics.Acked()
		c.metrics.AckLatency(res.HeldFor)
		if plan.Late {
			c.metrics.LateAck()
		}

	case queue.ActionRelease:
		// T4/T5 (or READY-min): release to READY at the computed visible_at.
		//nolint:gosec // G202: the interpolated fragment is a constant fence clause.
		r, err := tx.ExecContext(ctx,
			`UPDATE deliveries SET state = 0, visible_at = ?, last_reason = ? WHERE `+fence(""),
			fenceArgs([]any{plan.NewVisibleAt, reason})...)
		if err != nil {
			return nil, fmt.Errorf("release %q/%q seq %d: %w", it.Token.Stream, it.Token.Consumer, it.Token.Seq, err)
		}
		if aErr := changed(r, "release"); aErr != nil {
			return nil, fmt.Errorf("%w of %q/%q seq %d", aErr, it.Token.Stream, it.Token.Consumer, it.Token.Seq)
		}
		res.RetryAt = time.UnixMilli(plan.NewVisibleAt)
		// A READY-min repeat that did not move visible_at produces no state change and
		// must not write a second msg.nak (replay-idempotence: "event only if changed").
		wasReady := row.state == 0
		if !wasReady || row.visibleAt != plan.NewVisibleAt {
			delayMS := plan.NewVisibleAt - nowMS
			if delayMS < 0 {
				delayMS = 0
			}
			ev, eErr := commitEvent(ctx, tx, event{
				ts: nowMS, name: "msg.nak",
				stream: nullStr(it.Token.Stream), consumer: nullStr(it.Token.Consumer),
				seq: nullI64(it.Token.Seq), attempt: nullI64(int64(it.Token.Attempt)),
				msgID: nullStr(row.msgID), traceID: nullStr(row.traceID), subject: nullStr(row.subject),
				detail: nullStr(fmt.Sprintf(`{"delay_ms":%d,"reason":%q,"explicit":%t}`, delayMS, reason, it.Delay != nil)),
			})
			if eErr != nil {
				return nil, eErr
			}
			events = append(events, ev)
			c.metrics.Naked()
		}

	case queue.ActionExtend:
		// T7: push visible_at (attempts untouched), INFLIGHT only.
		//nolint:gosec // G202: the interpolated fragment is a constant fence clause.
		r, err := tx.ExecContext(ctx,
			`UPDATE deliveries SET visible_at = ? WHERE `+fence(" AND state = 1"),
			fenceArgs([]any{plan.NewVisibleAt})...)
		if err != nil {
			return nil, fmt.Errorf("extend delivery %q/%q seq %d: %w", it.Token.Stream, it.Token.Consumer, it.Token.Seq, err)
		}
		if aErr := changed(r, "extend"); aErr != nil {
			return nil, fmt.Errorf("%w of %q/%q seq %d", aErr, it.Token.Stream, it.Token.Consumer, it.Token.Seq)
		}
		res.Deadline = time.UnixMilli(plan.NewVisibleAt)
		ev, eErr := commitEvent(ctx, tx, event{
			ts: nowMS, name: "msg.extend",
			stream: nullStr(it.Token.Stream), consumer: nullStr(it.Token.Consumer),
			seq: nullI64(it.Token.Seq), attempt: nullI64(int64(it.Token.Attempt)),
			msgID: nullStr(row.msgID), traceID: nullStr(row.traceID), subject: nullStr(row.subject),
			detail: nullStr(fmt.Sprintf(`{"deadline_ms":%d,"capped":false}`, plan.NewVisibleAt)),
		})
		if eErr != nil {
			return nil, eErr
		}
		events = append(events, ev)
		c.metrics.Extended()

	case queue.ActionDead: // T5-exhaustion / T6.
		evVerb, verbErr := commitEvent(ctx, tx, event{
			ts: nowMS, name: deadVerbName(plan.Cause),
			stream: nullStr(it.Token.Stream), consumer: nullStr(it.Token.Consumer),
			seq: nullI64(it.Token.Seq), attempt: nullI64(int64(it.Token.Attempt)),
			msgID: nullStr(row.msgID), traceID: nullStr(row.traceID), subject: nullStr(row.subject),
			detail: nullStr(fmt.Sprintf(`{"reason":%q}`, reason)),
		})
		if verbErr != nil {
			return nil, verbErr
		}
		events = append(events, evVerb)
		//nolint:gosec // G115: seq is bounded by the message sequence space (int64).
		dc := queue.DeadCtx{
			Stream: it.Token.Stream, Consumer: it.Token.Consumer, Subject: row.subject,
			Seq: uint64(it.Token.Seq), MsgID: row.msgID, TraceID: row.traceID,
			Attempts: it.Token.Attempt, Cause: plan.Cause, LastReason: reason,
			Generation: it.Token.Generation, MaxDeliver: cons.cfg.MaxDeliver,
			Trigger: settleTriggerFor(plan.Cause),
		}
		evDead, sinkErr := c.DeadSink.Dead(ctx, tx, dc, now)
		if errors.Is(sinkErr, queue.ErrDeadBudget) {
			// Copy budget exhausted mid-batch: stop routing dead transitions (the verb
			// events and any copies already written this batch roll back with the abort —
			// nothing commits, the row stays INFLIGHT/READY, and the client retries,
			// I4-safe). Never abort just for a non-dead sibling already applied.
			return nil, fmt.Errorf("%w (batch aborted; dead rows stay for a later batch)", queue.ErrDeadBudget)
		}
		if sinkErr != nil {
			return nil, fmt.Errorf("dead sink %q/%q seq %d: %w", it.Token.Stream, it.Token.Consumer, it.Token.Seq, sinkErr)
		}
		events = append(events, evDead)
		res.Dead = true
		if plan.Cause == queue.DeadCauseTerminated {
			c.metrics.Termed()
		} else {
			c.metrics.Naked()
		}
		//nolint:gosec // G202: the interpolated fragment is a constant fence clause.
		r, err := tx.ExecContext(ctx, `DELETE FROM deliveries WHERE `+fence(""), fenceArgs(nil)...)
		if err != nil {
			return nil, fmt.Errorf("dead-delete %q/%q seq %d: %w", it.Token.Stream, it.Token.Consumer, it.Token.Seq, err)
		}
		if aErr := changed(r, "dead delete"); aErr != nil {
			return nil, fmt.Errorf("%w of %q/%q seq %d", aErr, it.Token.Stream, it.Token.Consumer, it.Token.Seq)
		}
	}
	return events, nil
}

func deadVerbName(cause queue.DeadCause) string {
	if cause == queue.DeadCauseTerminated {
		return "msg.term"
	}
	return "msg.nak"
}

// settleTriggerFor maps a deadline's cause to the trigger the dead path exposes: a term
// is an explicit terminal error; a max_deliver death from a verb is a nak (the sweeper's
// ack_wait / policy_lowered triggers come from #11's own planner).
func settleTriggerFor(cause queue.DeadCause) queue.DeadTrigger {
	if cause == queue.DeadCauseTerminated {
		return queue.DeadTriggerTerm
	}
	return queue.DeadTriggerNak
}

// rejectionEvent writes a rejection audit row (msg.ack_stale / msg.ack_dup), bounded
// by the per-(consumer,event) repeat limiter — at most one row per interval, carrying
// detail.suppressed. The metric is counted by the caller regardless.
func (c SettleCmd) rejectionEvent(ctx context.Context, tx *sql.Tx, now time.Time, it SettleItem,
	name, reason string, res *ItemResult,
) obs.Event {
	if name == "" {
		return obs.Event{}
	}
	key := it.Token.Stream + "\x00" + it.Token.Consumer + "\x00" + name
	lim := c.limiter[key]
	if lim == nil {
		lim = &rejectionLimiter{}
		c.limiter[key] = lim
	}
	emit, suppressed := lim.due(now.UnixMilli(), c.interval.Milliseconds())
	if !emit {
		return obs.Event{} // suppressed: no row this interval
	}
	resource := map[string]any{
		"token_attempt":   res.Attempt,
		"current_attempt": res.CurrentAttempt,
	}
	if reason != "" {
		resource["reason"] = reason
	}
	if suppressed > 0 {
		resource["suppressed"] = suppressed
	}
	detail, err := json.Marshal(resource)
	if err != nil {
		return obs.Event{} // a rejection row must never abort the batch; the metric counted.
	}
	ev, err := commitEvent(ctx, tx, event{
		ts: now.UnixMilli(), name: name,
		stream: nullStr(it.Token.Stream), consumer: nullStr(it.Token.Consumer),
		seq: nullI64(it.Token.Seq), attempt: nullI64(int64(it.Token.Attempt)),
		detail: nullStr(string(detail)),
	})
	if err != nil {
		return obs.Event{} // a rejection row must never abort the batch; the metric counted.
	}
	return ev
}

// rejectionLimiter bounds rejection audit rows per (consumer, event): the first call
// in an interval emits, later ones accumulate into detail.suppressed (G9).
type rejectionLimiter struct {
	lastMS     int64
	suppressed int64
}

func (r *rejectionLimiter) due(nowMS, intervalMS int64) (emit bool, suppressed int64) {
	if r.lastMS == 0 || nowMS-r.lastMS >= intervalMS {
		sup := r.suppressed
		r.suppressed = 0
		r.lastMS = nowMS
		return true, sup
	}
	r.suppressed++
	return false, 0
}

// truncateUTF8 shortens s to at most max bytes, never splitting a rune (utn8-safe).
func truncateUTF8(s string, max int) string {
	if max <= 0 || len(s) <= max {
		return s
	}
	i := 0
	for i < max {
		_, sz := utf8.DecodeRuneInString(s[i:])
		if i+sz > max {
			break
		}
		i += sz
	}
	return s[:i]
}

// defaultSettleJitter returns the injected seam or the production ±20% uniform jitter
// over its own PCG (never crypto/rand — the writer goroutine must not block on entropy).
func defaultSettleJitter(inj queue.Jitter) queue.Jitter {
	if inj != nil {
		return inj
	}
	return queue.StandardJitter(rand.New(rand.NewPCG(0xC0FFEE, 0xBEEF)))
}
