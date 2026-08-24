// SPDX-License-Identifier: Apache-2.0

package queue

import (
	"testing"
	"time"
)

// The DecideSweep planner tests (issue #11 G1): totality over the full cross-product
// (never panics, exactly one decision), the rule order 1->2->3->4, and hardcoded golden
// expectations for the redeliver schedule so the planner can never drift into agreeing
// with a mutant of itself.

// identityJitter returns its input unchanged; ScheduleMS == DelayMS for exact
// redeliver-time assertions.
func identityJitter(d time.Duration) time.Duration { return d }

func defaultSweepPolicy(t *testing.T, maxDeliver int32, backoff []time.Duration) SweepPolicy {
	t.Helper()
	return SweepPolicy{MaxDeliver: maxDeliver, Backoff: backoff, Generation: 1}
}

// TestDecideSweepTotality drives the full cross-product from G1 and asserts each input
// yields exactly one well-formed decision and never panics. A panic or a hole in the
// rule ladder (e.g. a missing default) reds this immediately.
func TestDecideSweepTotality(t *testing.T) {
	expired := []bool{false, true}
	gens := []int32{1, 99} // match, stale
	maxDelivers := []int32{0, 1, 5}
	attempts := []int32{0, 1, 2, 5, 6}
	backoffs := []struct {
		name string
		b    []time.Duration
	}{
		{"empty", nil},
		{"one", []time.Duration{30 * time.Second}},
		{"default", []time.Duration{1 * time.Second, 5 * time.Second, 30 * time.Second, 2 * time.Minute, 10 * time.Minute}},
	}
	const now = int64(1_700_000_000_000)

	cases := 0
	for _, isExpired := range expired {
		for _, gen := range gens {
			for _, md := range maxDelivers {
				for _, att := range attempts {
					for _, bk := range backoffs {
						visibleAt := now - 1 // expired
						if !isExpired {
							visibleAt = now + 1000
						}
						row := SweepRow{
							Key: ConsumerKey{Stream: "s", Consumer: "c"}, Seq: 1,
							Attempts: att, Generation: gen, VisibleAt: visibleAt,
							DeliveredAt: now - 1000, MsgID: "m", TraceID: "t",
						}
						pol := defaultSweepPolicy(t, md, bk.b)
						pol.Generation = 1
						dec := DecideSweep(row, pol, now, identityJitter)
						switch dec.Action {
						case SweepSkip, SweepDead, SweepRedeliver:
						default:
							t.Fatalf("expired=%v gen=%d md=%d att=%d backoff=%s: invalid action %d",
								isExpired, gen, md, att, bk.name, dec.Action)
						}
						switch dec.Action {
						case SweepSkip:
							if dec.Skip == "" {
								t.Fatalf("expired=%v gen=%d md=%d att=%d: skip with no reason", isExpired, gen, md, att)
							}
						case SweepDead:
							if dec.Cause != DeadCauseMaxDeliver {
								t.Fatalf("expired=%v gen=%d md=%d att=%d: dead cause %q", isExpired, gen, md, att, dec.Cause)
							}
							if dec.Trigger != "ack_wait" && dec.Trigger != "policy_lowered" {
								t.Fatalf("expired=%v gen=%d md=%d att=%d: dead trigger %q", isExpired, gen, md, att, dec.Trigger)
							}
						case SweepRedeliver:
							if dec.VisibleAt < now {
								t.Fatalf("expired=%v gen=%d md=%d att=%d: redeliver into the past (%d)", isExpired, gen, md, att, dec.VisibleAt)
							}
						}
						cases++
					}
				}
			}
		}
	}
	if cases < 2*2*3*5*3 {
		t.Fatalf("matrix ran %d cases, want the full %d", cases, 2*2*3*5*3)
	}
}

// TestDecideSweepRules pins the rule order (1-2-3-4) and the exact per-rule behaviour.
func TestDecideSweepRules(t *testing.T) {
	const now = int64(1_700_000_000_000)
	row := func(attempts, gen int32, visibleAt int64) SweepRow {
		return SweepRow{
			Key: ConsumerKey{Stream: "s", Consumer: "c"}, Seq: 7,
			Attempts: attempts, Generation: gen, VisibleAt: visibleAt,
			DeliveredAt: now - 5000, MsgID: "m", TraceID: "tr",
		}
	}
	defBackoff := []time.Duration{1 * time.Second, 5 * time.Second}

	tests := []struct {
		name     string
		row      SweepRow
		pol      SweepPolicy
		wantAct  SweepAction
		wantSkip string
		wantTrig string
		wantAt   int64
	}{
		{
			// Rule 1: not expired wins over everything, even a stale generation.
			name:    "rule1_not_expired_beats_stale_gen",
			row:     row(0, 99, now+1000),
			pol:     defaultSweepPolicy(t, 5, defBackoff),
			wantAct: SweepSkip, wantSkip: "not_expired",
		},
		{
			// Rule 2: generation mismatch skips, regardless of max_deliver.
			name:    "rule2_generation_mismatch_skips",
			row:     row(6, 7, now-1),
			pol:     defaultSweepPolicy(t, 5, defBackoff),
			wantAct: SweepSkip, wantSkip: "generation",
		},
		{
			// Rule 3 + ack_wait: expired at exactly max_deliver dies with trigger ack_wait.
			name:    "rule3_dead_at_bound_ack_wait",
			row:     row(5, 1, now-1),
			pol:     defaultSweepPolicy(t, 5, defBackoff),
			wantAct: SweepDead, wantTrig: "ack_wait",
		},
		{
			// Rule 3 + policy_lowered: expired above the bound (retirement path).
			name:    "rule3_dead_above_bound_policy_lowered",
			row:     row(6, 1, now-1),
			pol:     defaultSweepPolicy(t, 5, defBackoff),
			wantAct: SweepDead, wantTrig: "policy_lowered",
		},
		{
			// Rule 4: not at the bound redelivers at now + backoff[attempt-1].
			// attempt 2 -> backoff[1] = 5s.
			name:    "rule4_redeliver_schedule_attempt2",
			row:     row(2, 1, now-1),
			pol:     defaultSweepPolicy(t, 5, defBackoff),
			wantAct: SweepRedeliver, wantAt: now + 5000,
		},
		{
			// Rule 4 with max_deliver=0 (unlimited): never dies by count.
			name:    "rule4_unlimited_max_deliver",
			row:     row(99, 1, now-1),
			pol:     defaultSweepPolicy(t, 0, defBackoff),
			wantAct: SweepRedeliver, wantAt: now + 5000,
		},
		{
			// Empty backoff -> immediate redelivery (RetryHorizon 0 is warned at create).
			name:    "rule4_empty_backoff_immediate",
			row:     row(1, 1, now-1),
			pol:     defaultSweepPolicy(t, 5, nil),
			wantAct: SweepRedeliver, wantAt: now,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			dec := DecideSweep(tc.row, tc.pol, now, identityJitter)
			if dec.Action != tc.wantAct {
				t.Fatalf("action = %d, want %d", dec.Action, tc.wantAct)
			}
			if tc.wantSkip != "" && dec.Skip != tc.wantSkip {
				t.Fatalf("skip = %q, want %q", dec.Skip, tc.wantSkip)
			}
			if tc.wantTrig != "" && dec.Trigger != tc.wantTrig {
				t.Fatalf("trigger = %q, want %q", dec.Trigger, tc.wantTrig)
			}
			if tc.wantAct == SweepRedeliver && dec.VisibleAt != tc.wantAt {
				t.Fatalf("visible_at = %d, want %d", dec.VisibleAt, tc.wantAt)
			}
		})
	}
}

// TestDecideSweepJitterApplies pins that the jitter seam perturbs the schedule: with a
// deterministic +50% jitter one entry's pre-jitter and post-jitter values are visibly
// different, and ScheduleMS/DelayMS carry both so the msg.timeout event can explain it.
func TestDecideSweepJitterApplies(t *testing.T) {
	const now = int64(1_700_000_000_000)
	half := Jitter(func(d time.Duration) time.Duration { return d + d/2 }) // +50%
	dec := DecideSweep(SweepRow{
		Key: ConsumerKey{Stream: "s", Consumer: "c"}, Seq: 1,
		Attempts: 1, Generation: 1, VisibleAt: now - 1, DeliveredAt: now - 1000,
	}, defaultSweepPolicy(t, 5, []time.Duration{2 * time.Second}), now, half)
	if dec.Action != SweepRedeliver {
		t.Fatalf("action = %d, want redeliver", dec.Action)
	}
	if dec.ScheduleMS != 2000 {
		t.Fatalf("schedule_ms = %d, want 2000", dec.ScheduleMS)
	}
	if dec.DelayMS != 3000 {
		t.Fatalf("delay_ms = %d, want 3000 (2000 + 50%%)", dec.DelayMS)
	}
	if dec.VisibleAt != now+3000 {
		t.Fatalf("visible_at = %d, want %d", dec.VisibleAt, now+3000)
	}
}
