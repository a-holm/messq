// SPDX-License-Identifier: Apache-2.0

package queue

import "time"

// The retention planner's pure arithmetic (#27 slice 1, PLAN §4.5, SEMANTICS S15). The
// store loads a bounded oldest-first working set from the read pool plus the stream_stats
// snapshot; PlanEviction turns them into one bounded candidate list that the deleting
// transaction re-checks against the delivery-row guard in SQL — the planner is a candidate
// generator, the transaction is the authority (G1). Blame selection and the clock-jump
// detector live here too, because both are equally pure decisions over small structs.

// Candidate is one message row as the planner sees it, in ascending seq order.
type Candidate struct {
	Seq         int64
	Size        int64
	PublishedAt int64 // unix ms, as stored
	HasDelivery bool  // a deliveries row pins this message right now
}

// RetentionView carries the configured limits plus the stream_stats snapshot the planner
// plans against. Zero means unlimited for each limit, exactly as in StreamConfig.
type RetentionView struct {
	MaxMsgs  int64 // 0 = unlimited
	MaxBytes int64 // 0 = unlimited
	MaxAgeMs int64 // 0 = unlimited
	Msgs     int64 // stream_stats.msgs at planning time
	Bytes    int64 // stream_stats.bytes at planning time
	NowMs    int64 // the tick's wall reading, for the age cutoff
}

// EvictionPlan is one bounded sweep's outcome. Seqs is ascending and never longer than
// the batch bound. The Blocked* fields feed retention.blocked's blame query (which runs
// over delivery rows with seq <= HighestBlockedSeq); HighestDeletedSeq becomes the
// expired_seq watermark #28's seek hands its "no longer exists" answers from.
type EvictionPlan struct {
	Seqs              []int64 // deletable candidates, oldest-first
	FreedBytes        int64   // sum of their sizes
	HighestDeletedSeq int64   // max selected seq; 0 when nothing was selected
	BlockedCount      int64   // candidates seen but pinned by a delivery row (skipped)
	BlockedBytes      int64   // their total size — retention.blocked's would_free_bytes
	HighestBlockedSeq int64   // the blame query window's upper bound
	More              bool    // batch-saturated AND still violating: reschedule now
}

// PlanEviction merges the three passes of retention=limits — age → count → bytes, all
// oldest-first — into one deduplicated, batch-bounded plan.
//
// The rules it implements, each pinned by a test:
//
//   - A limit of 0 is unlimited and selects nothing, ever.
//   - The age pass takes every candidate with published_at < now-max_age_ms. Age may
//     empty the stream entirely; expiry is what age means.
//   - The count pass takes exactly stats.msgs - max_msgs oldest candidates when positive.
//   - The bytes pass accumulates sizes oldest-first until stats.bytes - taken <=
//     max_bytes. Byte pressure NEVER deletes the stream's last remaining message
//     (max_bytes below one message keeps exactly one — no infinite delete-then-refuse).
//   - A pinned candidate (HasDelivery) is SKIPPED, not stopping (G2): one stuck consumer
//     must not fill the disk. It is counted into the Blocked* accounting instead.
//   - At most `batch` candidates are selected. More reports whether this batch saturated
//     AND the projected post-delete stats would still violate an active limit — the
//     janitor reschedules while budget remains; anything less saturating ends the tick
//     (a sweep blocked by consumers cannot be helped by more budget, so More=false).
//
// cands must be in ascending seq order (the store's ORDER BY seq); the walk relies on it
// for the union's ordering and dedup-free single pass. No maps are iterated.
func PlanEviction(cands []Candidate, v RetentionView, batch int) EvictionPlan {
	plan := EvictionPlan{Seqs: make([]int64, 0, min(batch, len(cands)))}
	if batch <= 0 {
		return plan
	}

	cutoff := int64(0)
	ageActive := v.MaxAgeMs > 0
	if ageActive {
		cutoff = v.NowMs - v.MaxAgeMs
	}
	excess := int64(0) // count pass quota remaining
	if v.MaxMsgs > 0 && v.Msgs > v.MaxMsgs {
		excess = v.Msgs - v.MaxMsgs
	}
	need := int64(0) // byte pass target remaining
	if v.MaxBytes > 0 && v.Bytes > v.MaxBytes {
		need = v.Bytes - v.MaxBytes
	}

	for i := range cands {
		c := &cands[i]
		wantAge := ageActive && c.PublishedAt < cutoff
		wantCount := excess > 0
		wantBytes := need > 0
		if !wantAge && !wantCount && !wantBytes {
			continue // passes satisfied; everything newer is safe
		}
		if c.HasDelivery { // skipped, not stalled (G2); the SQL guard re-checks anyway
			plan.BlockedCount++
			plan.BlockedBytes += c.Size
			if c.Seq > plan.HighestBlockedSeq {
				plan.HighestBlockedSeq = c.Seq
			}
			continue
		}
		if wantAge {
			// Age pressure may take any message, including the last one.
		} else if v.Msgs-int64(len(plan.Seqs)) <= 1 {
			// Count/byte pressure stops at exactly one kept message.
			break
		}
		plan.Seqs = append(plan.Seqs, c.Seq)
		plan.FreedBytes += c.Size
		if c.Seq > plan.HighestDeletedSeq {
			plan.HighestDeletedSeq = c.Seq
		}
		if excess > 0 {
			excess--
		}
		need -= c.Size
		if need < 0 {
			need = 0
		}
		if len(plan.Seqs) == batch {
			break
		}
	}

	plan.More = len(plan.Seqs) == batch && violatesAfter(len(plan.Seqs), plan.FreedBytes, v)
	return plan
}

// violatesAfter projects the stats after deleting n messages totalling freed bytes and
// reports whether an active limit would still be violated.
func violatesAfter(n int, freed int64, v RetentionView) bool {
	msgs := v.Msgs - int64(n)
	bytes := v.Bytes - freed
	if v.MaxMsgs > 0 && msgs > v.MaxMsgs {
		return true
	}
	return v.MaxBytes > 0 && bytes > v.MaxBytes
}

// Holder names one delivery row pinning a candidate, as the blame query returns it.
// Paused consumers ride through unchanged: pausing is not abandoning (#27 §6), so
// selection treats them like anyone else.
type Holder struct {
	Consumer string
	Seq      int64
}

// SelectBlame picks the guilty consumer for retention.blocked: the owner of the OLDEST
// blocking seq (§7's ORDER BY blocking_seq LIMIT 1), ties broken by the lexicographically
// smallest name so repeated sweeps over the same rows blame consistently. ok=false when
// there is nobody to blame.
func SelectBlame(holders []Holder) (Holder, bool) {
	var best Holder
	ok := false
	for _, h := range holders {
		if !ok || h.Seq < best.Seq || (h.Seq == best.Seq && h.Consumer < best.Consumer) {
			best = h
			ok = true
		}
	}
	return best, ok
}

// TickSample is one janitor tick's clock pair: the wall reading destined for stored
// timestamps, and a monotonic anchor measured on the same process lifetime.
type TickSample struct {
	Wall time.Time     // Clock.Now()
	Mono time.Duration // e.g. Clock.Since(processStart): immune to NTP steps
}

// ClockJump carries the two deltas DetectClockJump observed across consecutive ticks.
type ClockJump struct {
	WallDelta time.Duration
	MonoDelta time.Duration
}

// DetectClockJump guards the age pass (G7, §11): a forward NTP step of a month must not
// let now-max_age_ms swallow a whole history in one correctly-configured, irreversible
// sweep. It compares how far the wall clock moved between two ticks against how far the
// monotonic anchor moved; divergence strictly greater than tol reports a jump and the
// janitor skips ONE age pass. Backward steps jump too (nothing may be deleted "early");
// gradual drift within tolerance never does. tol < 0 behaves as 0.
func DetectClockJump(prev, cur TickSample, tol time.Duration) (ClockJump, bool) {
	j := ClockJump{
		WallDelta: cur.Wall.Sub(prev.Wall),
		MonoDelta: cur.Mono - prev.Mono,
	}
	d := j.WallDelta - j.MonoDelta
	if d < 0 {
		d = -d
	}
	return j, d > tol
}
