// SPDX-License-Identifier: Apache-2.0

package queue

import (
	"time"

	"github.com/a-holm/messq/internal/errs"
)

// The pure settle planner of issue #10 §2–§3 (SEMANTICS S3.3 fence order, §5.1 T3–T7,
// §5.2 I7). PlanSettle decides, from the consumer state and the live delivery row,
// whether a settle is allowed to touch the row and, when it is, exactly what mutation
// it may perform. It performs no I/O and reads no wall clock: `now` is injected by the
// caller (the writer batch clock), and scheduling jitter flows through the Jitter seam,
// so property tests and synctest runs are fully deterministic. Everything the store's
// SettleCmd needs is in [SettlePlan]; the store only turns the plan into SQL.

// Verb is the closed set of settle verbs.
type Verb string

// The settle verbs (SEMANTICS §7).
const (
	VerbAck    Verb = "ack"
	VerbNak    Verb = "nak"
	VerbTerm   Verb = "term"
	VerbExtend Verb = "extend"
)

// ItemStatus is the frozen response status of one settle item (issue #10 §7, frozen by
// the orchestrator): ok | stale | stale_ack | wrong_generation | unknown. stale and
// stale_ack are opposites — the first is "nothing to do, all is well", the second is
// "a duplicate probably happened" (D7).
type ItemStatus string

// The item statuses.
const (
	ItemStatusOK       ItemStatus = "ok"
	ItemStatusStale    ItemStatus = "stale"
	ItemStatusStaleAck ItemStatus = "stale_ack"
	ItemStatusWrongGen ItemStatus = "wrong_generation"
	ItemStatusUnknown  ItemStatus = "unknown"
)

// String renders the status for the wire enum.
func (s ItemStatus) String() string { return string(s) }

// DeliveryState mirrors the deliveries.state column (0 = READY, 1 = INFLIGHT).
type DeliveryState uint8

const (
	// StateReady is a delivery eligible for (re)claim.
	StateReady DeliveryState = 0
	// StateInflight is a delivery whose lease is currently held by a worker.
	StateInflight DeliveryState = 1
)

// SettleAction is the mutation a plan prescribes.
type SettleAction uint8

const (
	// ActionNoop is a rejection: nothing mutates (invariant I7).
	ActionNoop SettleAction = iota
	// ActionAck deletes the delivery row (the terminal state is the absence of the row, D5).
	ActionAck
	// ActionRelease releases an INFLIGHT row back to READY with a new visible_at (T4).
	ActionRelease
	// ActionExtend pushes an INFLIGHT row's visible_at forward (T7).
	ActionExtend
	// ActionDead routes the row to the DeadSink seam, then deletes it (T5-exhaustion, T6).
	ActionDead
)

// DeadCause names why work reached the dead path (owned here; #12 consumes it).
type DeadCause string

// The dead causes.
const (
	DeadCauseMaxDeliver DeadCause = "max_deliver"
	DeadCauseTerminated DeadCause = "terminated"
)

// DeadCtx is everything a DeadSink copy needs to re-route the message (#12). #10
// defines the type and hands the store's DropSink a fully populated value once per DEAD
// transition (I8's settle-side arm).
type DeadCtx struct {
	Stream     string
	Consumer   string
	Subject    string
	Seq        uint64
	MsgID      string
	TraceID    string
	Attempts   int32
	Cause      DeadCause
	LastReason string
}

// SettleRequest is a single item as the planner sees it: the parsed token's fields, the
// consumer's fence facts, and the live row projection. RowPresent is false when the
// (stream, consumer, seq) has no live delivery row.
type SettleRequest struct {
	Verb           Verb
	Token          Token
	Generation     int32          // consumer.generation — the fence against token.generation
	CursorSeq      int64          // consumer.cursor_seq — absent-row plausibility (Note B)
	Config         ConsumerConfig // AckWait, MaxDeliver, Backoff for the retry arithmetic
	MaxAckWait     time.Duration  // the extend lease cap (C10)
	RowPresent     bool
	RowState       DeliveryState
	RowAttempts    int32
	RowVisibleAt   int64          // unix ms
	RowDeliveredAt int64          // unix ms; 0 when never claimed
	NakDelay       *time.Duration // nak only; nil = schedule backoff
	Jitter         Jitter         // nil = no jitter (deterministic in tests)
}

// SettlePlan is the pure decision: what status to answer and — when OK (and mutated) —
// what to do.
type SettlePlan struct {
	Status         ItemStatus
	Action         SettleAction
	Late           bool // ack on READY: a duplicate was narrowly avoided
	Dead           bool // route to the DeadSink
	Cause          DeadCause
	Reason         string // msg.ack_stale detail reason
	CurrentAttempt int32  // the row's actual attempt on a stale_ack
	NewVisibleAt   int64  // release / extend deadline (unix ms)
	HeldMS         int64  // ack lasting: now - delivered_at
}

// PlanSettle runs the full fence (S3.3) and, when granted, the §5.1 matrix. On the
// extend-at-cap path it returns errs.ErrBadRequest per the spec outcome (the deadline
// unchanged, the delivery times out on schedule — Conflict A). Every rejection returns
// ActionNoop — nothing mutates (I7) — which the store doubles with a WHERE fence.
func PlanSettle(req SettleRequest, now time.Time) (SettlePlan, error) {
	// Fence step 3 (S3.3): generation mismatch — the token outlived the consumer's
	// reset. Wrong_generation wins over everything else and touches nothing.
	if req.Token.Generation != req.Generation {
		return SettlePlan{Status: ItemStatusWrongGen, Reason: "generation"}, nil
	}
	// Fence steps 4–5: the live row (or its absence).
	if !req.RowPresent {
		// Note B: an absent row with seq >= cursor_seq means the message was never
		// delivered to this consumer — "idempotent success" would be a lie (T3a
		// clarification).
		if req.Token.Seq < req.CursorSeq {
			return SettlePlan{Status: ItemStatusStale}, nil
		}
		return SettlePlan{Status: ItemStatusUnknown}, nil
	}
	if req.Token.Attempt != req.RowAttempts {
		reason := ""
		if req.Token.Attempt > req.RowAttempts {
			reason = "future_attempt" // impossible without a bug; the safe direction is no mutation
		}
		return SettlePlan{
			Status: ItemStatusStaleAck, CurrentAttempt: req.RowAttempts,
			Reason: reason,
		}, nil
	}

	// Fenced and on the right attempt: apply the verb.
	switch req.Verb {
	case VerbAck:
		return planAck(req, now), nil
	case VerbNak:
		return planNak(req, now)
	case VerbTerm:
		return planTerm(), nil
	case VerbExtend:
		return planExtend(req, now)
	default:
		return SettlePlan{}, errs.E(errs.ErrBadRequest, "queue.PlanSettle",
			"verb %q is not one of ack/nak/term/extend", req.Verb)
	}
}

// planAck implements T3/T3a/T3b. The terminal state is the row's absence (D5); an ack a
// READY row with the matching attempt is accepted and flagged late (the worker's lease
// expired but the message was not re-claimed — deleting avoids a duplicate).
func planAck(req SettleRequest, now time.Time) SettlePlan {
	plan := SettlePlan{Status: ItemStatusOK, Action: ActionAck}
	if req.RowDeliveredAt != 0 {
		plan.HeldMS = now.UnixMilli() - req.RowDeliveredAt
	}
	if req.RowState == StateReady {
		plan.Late = true
	}
	return plan
}

// planNak implements T4/T5. An INFLIGHT row is released to READY at now + ReleaseDelay
// (deterministic backoff, unjittered explicit per S8.3); one at max_deliver goes to the
// dead path (D6) instead. A READY row is nudged by min(): it never moves later, so a
// retried nak cannot defer a message, only pull it in.
func planNak(req SettleRequest, now time.Time) (SettlePlan, error) {
	delay, err := ReleaseDelay(req.Config, req.RowAttempts, req.NakDelay, req.Jitter)
	if err != nil {
		return SettlePlan{}, err // out-of-range delay (C7)
	}
	dead := req.Config.MaxDeliver > 0 && req.RowAttempts >= req.Config.MaxDeliver
	if req.RowState == StateInflight && dead {
		return SettlePlan{
			Status: ItemStatusOK, Action: ActionDead, Dead: true,
			Cause: DeadCauseMaxDeliver,
		}, nil
	}
	computed := now.Add(delay).UnixMilli()
	if req.RowState == StateReady && computed > req.RowVisibleAt {
		computed = req.RowVisibleAt // min(current, computed): idempotent under replay
	}
	return SettlePlan{Status: ItemStatusOK, Action: ActionRelease, NewVisibleAt: computed}, nil
}

// planTerm implements T6: straight to DEAD, skipping the remaining attempts, on both
// INFLIGHT and READY rows (a permanent error is a property of the payload, not of the
// original state).
func planTerm() SettlePlan {
	return SettlePlan{Status: ItemStatusOK, Action: ActionDead, Dead: true, Cause: DeadCauseTerminated}
}

// planExtend implements T7 (a lease an INFLIGHT row). The deadline adds exactly one
// ack_wait, guarded so the total lease visible_at - delivered_at never exceeds
// max_ack_wait; at the cap it returns errs.ErrBadRequest, deadline unchanged, delivery
// times out on schedule (Conflict A → the normative S6.1 row).
func planExtend(req SettleRequest, now time.Time) (SettlePlan, error) {
	if req.RowState != StateInflight {
		// You cannot extend a lease you no longer hold.
		return SettlePlan{
			Status: ItemStatusStaleAck, CurrentAttempt: req.RowAttempts,
			Reason: "not_inflight",
		}, nil
	}
	nowMS := now.UnixMilli()
	base := req.RowVisibleAt
	if base < nowMS {
		base = nowMS // a post-expiry pre-sweep extend buys a full ack_wait
	}
	newDeadline := base + req.Config.AckWait.Milliseconds()
	// C10: the total lease is visible_at - delivered_at from the claim.
	maxHold := req.RowDeliveredAt + req.MaxAckWait.Milliseconds()
	if newDeadline > maxHold {
		return SettlePlan{}, errs.E(errs.ErrBadRequest, "queue.PlanSettle",
			"extend would push this delivery past --max-ack-wait")
	}
	return SettlePlan{Status: ItemStatusOK, Action: ActionExtend, NewVisibleAt: newDeadline}, nil
}
