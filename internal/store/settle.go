// SPDX-License-Identifier: Apache-2.0

package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/a-holm/messq/internal/errs"
	"github.com/a-holm/messq/internal/obs"
	"github.com/a-holm/messq/internal/queue"
)

// The settle writer command of issue #10 §7: acks, naks, terms and extends, batched
// through the group-commit engine with per-token results (never all-or-nothing). The
// planner (internal/queue.PlanSettle) is pure; this package loads a bounded working
// set, calls the planner, and applies what comes back. Every mutating statement repeats
// the WHERE fence (attempts AND generation) so even a planner bug cannot write through
// it — I7 is enforced twice.

const kindSettle CmdKind = "consumer.settle"

// Metric-name constants (issue #10 §9.4). #21 registers collectors against these names;
// this issue counts every outcome through the SettleMetrics seam.
const (
	metricAckedTotal        = "messq_acked_total"
	metricNakedTotal        = "messq_naked_total"
	metricTermedTotal       = "messq_termed_total"
	metricExtendsTotal      = "messq_extends_total"
	metricStaleAcksTotal    = "messq_stale_acks_total"
	metricLateAcksTotal     = "messq_late_acks_total"
	metricAckLatencySeconds = "messq_ack_latency_seconds"
)

// SettleItem is one requested settle.
type SettleItem struct {
	Token  queue.Token
	Verb   queue.Verb
	Delay  *time.Duration // nak only; nil = schedule backoff
	Reason string
}

// ItemStatus aliases the frozen response status set.
type ItemStatus = queue.ItemStatus

// ItemResult is the per-token outcome, in request order.
type ItemResult struct {
	Token          queue.Token
	Status         ItemStatus
	Attempt        int32
	Of             int32
	CurrentAttempt int32 // the row's actual attempt on a stale_ack
	Deadline       time.Time
	RetryAt        time.Time
	Dead           bool
	Capped         bool
	Late           bool
	HeldFor        time.Duration
	Err            error
}

// SettleResult aggregates one batch.
type SettleResult struct {
	Results []ItemResult // request order
	OK      int
	Failed  int
}

// SettleCmd is one writer command. Unexported fields are populated by [Store.Settle].
type SettleCmd struct {
	Items      []SettleItem
	DeadSink   DeadSink // #12 seam; nil = DropSink
	metrics    SettleMetrics
	limiter    map[string]*rejectionLimiter
	interval   time.Duration
	jitter     queue.Jitter
	reasonCap  int
	maxAckWait time.Duration
}

// SettleMetrics counts every settle outcome exactly, even when an event row is
// suppressed by the repeat limiter. Runs on the writer goroutine.
type SettleMetrics interface {
	Acked()                   // an ok ack deleted a row
	AckLatency(time.Duration) // msg.ack held duration
	LateAck()                 // an ack that narrowly avoided a duplicate
	Naked()                   // an ok nak state change
	Termed()                  // an ok term
	Extended()                // an ok extend state change
	StaleAck()                // a stale_ack / wrong_generation outcome
	Noop()                    // a stale / unknown outcome
}

type nopSettleMetrics struct{}

func (nopSettleMetrics) Acked()                   {}
func (nopSettleMetrics) AckLatency(time.Duration) {}
func (nopSettleMetrics) LateAck()                 {}
func (nopSettleMetrics) Naked()                   {}
func (nopSettleMetrics) Termed()                  {}
func (nopSettleMetrics) Extended()                {}
func (nopSettleMetrics) StaleAck()                {}
func (nopSettleMetrics) Noop()                    {}

func (c SettleCmd) Kind() CmdKind { return kindSettle }
func (c SettleCmd) Bytes() int    { return len(c.Items) * 200 }

// rejectEventName maps a rejection status to its audit-event name.
func rejectEventName(status ItemStatus) string {
	switch status {
	case queue.ItemStatusStale:
		return "msg.ack_dup"
	case queue.ItemStatusStaleAck, queue.ItemStatusWrongGen:
		return "msg.ack_stale"
	}
	return ""
}

// Store.Settle applies one settle batch. It refuses a batch above --max-settle-batch
// whole (I11), then enqueues.
func (s *Store) Settle(ctx context.Context, req SettleCmd) (SettleResult, error) {
	if len(req.Items) > s.maxSettleBatch {
		return SettleResult{}, errs.E(errs.ErrBadRequest, "Store.Settle",
			"a settle batch of %d items exceeds --max-settle-batch %d", len(req.Items), s.maxSettleBatch)
	}
	r := SettleCmd{Items: req.Items, DeadSink: req.DeadSink}
	if r.DeadSink == nil {
		r.DeadSink = s.newDeadSink()
	}
	r.metrics = s.settleMetrics
	r.limiter = s.settleBlocked
	r.interval = s.eventRepeatInterval
	r.reasonCap = s.maxReasonBytes
	r.jitter = s.jitter
	r.maxAckWait = s.consumerLimits.MaxAckWait
	res, err := s.enqueue(ctx, "store.Settle", r)
	if err != nil {
		return SettleResult{}, err
	}
	sr, ok := res.(SettleResult)
	if !ok {
		return SettleResult{}, fmt.Errorf("store.Settle: engine returned %T, want SettleResult", res)
	}
	return sr, nil
}

// groupSum is one (stream, consumer) batch group: the seqs to load and the request
// indexes of its non-duplicate members.
type groupSum struct {
	stream, consumer string
	seqs             []int64
	members          []int
}

// Apply runs the batch inside one transaction: group, load consumers and rows
// (chunked), then decide and apply each item in REQUEST order, returning results in the
// same order. Partial success is the defined semantics — business rejections never
// abort the batch.
func (c SettleCmd) Apply(ctx context.Context, tx *sql.Tx, now time.Time) (Result, []obs.Event, error) {
	out := SettleResult{Results: make([]ItemResult, len(c.Items))}
	if len(c.Items) == 0 {
		return out, nil, nil
	}
	var events []obs.Event

	// Build groups, dedupe exact repeats, remember duplicate targets.
	byToken := make(map[string]int, len(c.Items)) // token string -> first request idx
	groups := make([]*groupSum, 0, 4)
	byGroup := make(map[string]*groupSum)
	itemGroup := make([]int, len(c.Items))
	for i, it := range c.Items {
		tk := it.Token.String()
		if first, ok := byToken[tk]; ok {
			out.Results[i] = ItemResult{Token: it.Token, Status: queue.ItemStatusStale}
			_ = first
			out.OK++
			c.metrics.Noop()
			continue
		}
		byToken[tk] = i
		k := it.Token.Stream + "\x00" + it.Token.Consumer
		g := byGroup[k]
		if g == nil {
			g = &groupSum{stream: it.Token.Stream, consumer: it.Token.Consumer}
			byGroup[k] = g
			groups = append(groups, g)
		}
		g.seqs = append(g.seqs, it.Token.Seq)
		g.members = append(g.members, i)
		itemGroup[i] = len(groups) - 1
	}

	// Load consumer + rows per group.
	groupRows := make([]map[int64]settleRow, len(groups))
	groupCons := make([]settleConsumer, len(groups))
	for gi, g := range groups {
		sc, err := loadSettleConsumer(ctx, tx, g.stream, g.consumer)
		if err != nil {
			return nil, nil, err
		}
		rows, err := loadSettleRows(ctx, tx, g.stream, g.consumer, g.seqs)
		if err != nil {
			return nil, nil, err
		}
		groupCons[gi] = sc
		groupRows[gi] = rows
	}

	// Decide and apply each non-duplicate item of the batch in request order.
	for i, it := range c.Items {
		if out.Results[i].Status == queue.ItemStatusStale {
			continue // duplicate: already answered, no work
		}
		gi := itemGroup[i]
		res, evs, applyErr := c.applyOne(ctx, tx, now, it, groupCons[gi], groupRows[gi])
		if applyErr != nil {
			return nil, nil, applyErr // infrastructure damage aborts the batch
		}
		out.Results[i] = res
		events = append(events, evs...)
		switch res.Status {
		case queue.ItemStatusOK, queue.ItemStatusStale:
			out.OK++
		default:
			out.Failed++
		}
	}
	return out, events, nil
}

// applyOne decides and applies a single item, returning its result and the carrier
// events it produced. It never returns a business rejection as an error — rejections
// travel in ItemResult — so partial success is preserved for the whole batch.
func (c SettleCmd) applyOne(ctx context.Context, tx *sql.Tx, now time.Time, it SettleItem,
	cons settleConsumer, rows map[int64]settleRow,
) (ItemResult, []obs.Event, error) {
	reason := truncateUTF8(strings.TrimSpace(it.Reason), c.reasonCap)
	res := ItemResult{Token: it.Token, Attempt: it.Token.Attempt, Of: int32(cons.cfg.MaxDeliver)}

	// Fence step 2 (S3.3): an absent consumer is unknown — never a fabricated
	// idempotent success (Note A).
	if !cons.found {
		res.Status = queue.ItemStatusUnknown
		c.metrics.Noop()
		return res, nil, nil
	}

	row, present := rows[it.Token.Seq]
	req := queue.SettleRequest{
		Verb: it.Verb, Token: it.Token,
		Generation: cons.generation, CursorSeq: cons.cursorSeq,
		Config: cons.cfg, MaxAckWait: c.maxAckWait,
		RowPresent: present,
		NakDelay:   it.Delay, Jitter: c.jitter,
	}
	if present {
		//nolint:gosec // G115: state is a single byte from the deliveries.state column.
		req.RowState = queue.DeliveryState(row.state)
		req.RowAttempts = row.attempts
		req.RowVisibleAt = row.visibleAt
		req.RowDeliveredAt = row.deliveredAt
	} else {
		req.RowState = queue.StateReady
	}

	plan, err := queue.PlanSettle(req, now)
	if err != nil {
		// extend-at-cap / out-of-range nak delay (errs.ErrBadRequest): a per-item
		// rejection, never a batch abort.
		if errors.Is(err, errs.ErrBadRequest) {
			return ItemResult{
				Token: it.Token, Attempt: it.Token.Attempt,
				Of: res.Of, Status: queue.ItemStatusUnknown, Err: err,
			}, nil, nil
		}
		return res, nil, err
	}

	nowMS := now.UnixMilli()
	switch plan.Status {
	case queue.ItemStatusOK:
		res.Status = queue.ItemStatusOK
		// resolve and apply the mutation
		evs, applyErr := c.applyAction(ctx, tx, now, nowMS, plan, it, row, cons, reason, &res)
		if applyErr != nil {
			return res, nil, applyErr
		}
		return res, evs, nil

	case queue.ItemStatusStale:
		res.Status = queue.ItemStatusStale
		c.metrics.Noop()
		// idempotent success: no-row ack lands here; its audit row is a bounded msg.ack_dup.
		ev := c.rejectionEvent(ctx, tx, now, it, rejectEventName(queue.ItemStatusStale), "", &res)
		var evs []obs.Event
		if ev.Event != "" {
			evs = append(evs, ev)
		}
		return res, evs, nil

	case queue.ItemStatusStaleAck, queue.ItemStatusWrongGen:
		// stale_ack / wrong_generation: nothing mutates (I7).
		res.Status = plan.Status
		res.CurrentAttempt = plan.CurrentAttempt
		c.metrics.StaleAck()
		ev := c.rejectionEvent(ctx, tx, now, it, rejectEventName(plan.Status), plan.Reason, &res)
		var evs []obs.Event
		if ev.Event != "" {
			evs = append(evs, ev)
		}
		return res, evs, nil

	case queue.ItemStatusUnknown:
		// unknown (absent row at/after the cursor, or a deleted consumer): no mutation.
		res.Status = queue.ItemStatusUnknown
		c.metrics.Noop()
		return res, nil, nil
	}
	return res, nil, nil
}
