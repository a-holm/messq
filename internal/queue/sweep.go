// SPDX-License-Identifier: Apache-2.0

package queue

import "time"

// The sweeper's pure decision function (issue #11, PLAN §5.1 T8/T5). DecideSweep turns
// one expired delivery row plus its consumer's policy into exactly one of three outcomes:
// redeliver at now + jittered-backoff(attempts), dead (at the max_deliver bound), or skip
// (defensively not-expired, or a generation fence mismatch). It performs no I/O, reads no
// wall clock and iterates no maps, so #13's reference model and the fuzzers drive it
// directly — the store only loads the bounded working set and applies the returned
// decision. Like #10's PlanSettle, the *attempt increments at claim* (D6), so the sweeper
// never modifies attempts: a timeout on attempt n releases the row still at n, and the
// next claim makes it n+1 (I4/I9).

// ConsumerKey names one consumer: {Stream, Consumer}. It rides the Waker seam and the
// wake probe sets (#14 consumes the interface this issue declares).
type ConsumerKey struct {
	Stream   string
	Consumer string
}

// SweepRow is the projection of one live delivery row the expiry sweep needs to decide
// on. VisibleAt/DeliveredAt are unix ms; MsgID/TraceID come from the LEFT JOIN and are ""
// when the origin message was purged under the delivery row.
type SweepRow struct {
	Key         ConsumerKey
	Seq         int64
	Subject     string
	Attempts    int32
	Generation  int32
	VisibleAt   int64 // the expired deadline (unix ms)
	DeliveredAt int64 // when this attempt was claimed (unix ms); 0 when never claimed
	LastReason  string
	MsgID       string // "" when the origin message is gone
	TraceID     string
}

// SweepPolicy is the consumer-side facts DecideSweep fences against, loaded once per
// (stream, consumer) per command. AckWait is not needed for the decision but rides along
// so the store can stamp the msg.timeout event's ack_wait_ms without a second query.
type SweepPolicy struct {
	AckWait    time.Duration
	MaxDeliver int32           // 0 = unlimited
	Backoff    []time.Duration // 1..16 entries; last repeats
	Generation int32           // the consumer's live generation — the fence
}

// SweepAction is the closed set of decisions (exhaustive-linted).
type SweepAction uint8

const (
	// SweepSkip leaves the row untouched. Skip names why.
	SweepSkip SweepAction = iota
	// SweepDead routes the row to the dead seam (T5 — max_deliver exhausted).
	SweepDead
	// SweepRedeliver releases the row to READY at now + jittered backoff (T8).
	SweepRedeliver
)

// SweepDecision is one row's outcome.
type SweepDecision struct {
	Action     SweepAction
	VisibleAt  int64     // ms, when Action == SweepRedeliver
	DelayMS    int64     // the jittered delay actually applied (event detail)
	ScheduleMS int64     // the pre-jitter schedule entry (event detail; explains DelayMS)
	Cause      DeadCause // DeadMaxDeliver when Action == SweepDead
	Trigger    string    // "ack_wait" | "policy_lowered"
	HeldMS     int64     // now - DeliveredAt — how long the worker held the lease
	LatenessMS int64     // now - VisibleAt — how late the sweeper was
	Skip       string    // why, when Action == SweepSkip ("not_expired" | "generation")
}

// DecideSweep is total: every input produces exactly one decision, and it never panics.
// j is #10's Jitter seam (nil jitters nothing, deterministic for tests and the model).
//
// The rules, in evaluation order:
//
//  1. r.VisibleAt > nowMS           -> SweepSkip{"not_expired"}  (defensive)
//  2. r.Generation != p.Generation  -> SweepSkip{"generation"}  (fence; never mutate)
//  3. p.MaxDeliver > 0 &&
//     r.Attempts >= p.MaxDeliver    -> SweepDead{Cause: DeadMaxDeliver}
//  4. otherwise                     -> SweepRedeliver at nowMS + jitter(BackoffFor(attempts))
//
// Rule 3 is the whole max_deliver contract. Because attempts was incremented at claim
// (D6), a row in flight on attempt 5 of a max_deliver=5 consumer has already had five
// handler invocations; letting its lease expire is the fifth failure and it dies. The
// trigger distinguishes the ordinary expiry death ("ack_wait") from a row stranded above
// a lowered max_deliver ("policy_lowered") — the retirement pass feeds the same planner
// with the same rule.
func DecideSweep(r SweepRow, p SweepPolicy, nowMS int64, j Jitter) SweepDecision {
	held := nowMS - r.DeliveredAt
	if held < 0 {
		held = 0
	}
	lateness := nowMS - r.VisibleAt
	if lateness < 0 {
		lateness = 0
	}

	// Rule 1 — defensive: the scan should not have handed us a not-yet-expired row.
	if r.VisibleAt > nowMS {
		return SweepDecision{
			Action: SweepSkip, Skip: "not_expired",
			HeldMS: held, LatenessMS: lateness,
		}
	}
	// Rule 2 — generation fence: a survivor with a stale generation is the S3 belt; #28
	// drops rows when it bumps. Never mutate on a fence mismatch.
	if r.Generation != p.Generation {
		return SweepDecision{
			Action: SweepSkip, Skip: "generation",
			HeldMS: held, LatenessMS: lateness,
		}
	}
	// Rule 3 — the delivery bound (T5). Trigger: a row above the cap is a retirement
	// (policy_lowered); one at the cap died of a plain ack_wait timeout.
	if p.MaxDeliver > 0 && r.Attempts >= p.MaxDeliver {
		trigger := "ack_wait"
		if r.Attempts > p.MaxDeliver {
			trigger = "policy_lowered"
		}
		return SweepDecision{
			Action: SweepDead, Cause: DeadCauseMaxDeliver, Trigger: trigger,
			HeldMS: held, LatenessMS: lateness,
		}
	}
	// Rule 4 — redeliver at now + BackoffFor(attempts) jittered ±20% (S8.2).
	base := cForSchedule(p).BackoffFor(r.Attempts)
	scheduleMS := base.Milliseconds()
	delay := base
	if j != nil {
		delay = j(base)
	}
	return SweepDecision{
		Action: SweepRedeliver, VisibleAt: nowMS + delay.Milliseconds(),
		DelayMS: delay.Milliseconds(), ScheduleMS: scheduleMS, Trigger: "ack_wait",
		HeldMS: held, LatenessMS: lateness,
	}
}

// cForSchedule is a bare ConsumerConfig carrying only the backoff array, so DecideSweep
// reuses #10's BackoffFor (which reads only the backoff slice).
func cForSchedule(p SweepPolicy) ConsumerConfig {
	return ConsumerConfig{Backoff: p.Backoff}
}
