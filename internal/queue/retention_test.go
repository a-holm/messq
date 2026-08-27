// SPDX-License-Identifier: Apache-2.0
package queue

import (
	"slices"
	"testing"
	"time"
)

// The eviction planner's nasty-table tests (#27 slice 1). PlanEviction is pure: the store
// loads a bounded oldest-first working set plus the stream_stats snapshot, and the planner
// turns them into one bounded candidate list. Every rule PLAN §4.5 and SEMANTICS S15 pin is
// asserted here as arithmetic, not against SQLite.

func candidates(c ...Candidate) []Candidate { return c }

// unixms builds the wall reading a tick would carry, in unix milliseconds.
func unixms(ms int64) time.Time { return time.UnixMilli(ms).UTC() }

func TestPlanEvictionAgePass(t *testing.T) {
	// max_age_ms only: every message older than now-1000 goes, newest stays.
	c := candidates(
		Candidate{Seq: 1, Size: 10, PublishedAt: 500},
		Candidate{Seq: 2, Size: 10, PublishedAt: 1200},
		Candidate{Seq: 3, Size: 10, PublishedAt: 2000},
	)
	got := PlanEviction(c, RetentionView{
		MaxAgeMs: 1000, Msgs: 3, Bytes: 30, NowMs: 2000,
	}, 100)

	if want := []int64{1}; !slices.Equal(got.Seqs, want) {
		t.Fatalf("age pass seqs = %v, want %v", got.Seqs, want)
	}
	if got.FreedBytes != 10 {
		t.Fatalf("FreedBytes = %d, want 10", got.FreedBytes)
	}
	if got.HighestDeletedSeq != 1 {
		t.Fatalf("HighestDeletedSeq = %d, want 1", got.HighestDeletedSeq)
	}
	if got.More {
		t.Fatalf("More = true, want false: the plan brought the stream under its limit")
	}
}

func TestPlanEvictionCountPass(t *testing.T) {
	// max_msgs only: exactly the excess, oldest first.
	c := candidates(
		Candidate{Seq: 1, Size: 10, PublishedAt: 1},
		Candidate{Seq: 2, Size: 20, PublishedAt: 2},
		Candidate{Seq: 3, Size: 40, PublishedAt: 3},
		Candidate{Seq: 4, Size: 80, PublishedAt: 4},
	)
	got := PlanEviction(c, RetentionView{MaxMsgs: 2, Msgs: 4, Bytes: 150, NowMs: 10}, 100)

	if want := []int64{1, 2}; !slices.Equal(got.Seqs, want) {
		t.Fatalf("count pass seqs = %v, want %v", got.Seqs, want)
	}
	if got.FreedBytes != 30 {
		t.Fatalf("FreedBytes = %d, want 30", got.FreedBytes)
	}
	if got.More {
		t.Fatalf("More = true, want false")
	}
}

func TestPlanEvictionBytesPass(t *testing.T) {
	// max_bytes only: accumulate sizes oldest-first until stats.bytes - taken <= max_bytes.
	c := candidates(
		Candidate{Seq: 1, Size: 100, PublishedAt: 1},
		Candidate{Seq: 2, Size: 100, PublishedAt: 2},
		Candidate{Seq: 3, Size: 100, PublishedAt: 3},
		Candidate{Seq: 4, Size: 100, PublishedAt: 4},
	)
	// bytes 400, limit 250 → free >= 150 → seqs 1 and 2 (200 freed).
	got := PlanEviction(c, RetentionView{MaxBytes: 250, Msgs: 4, Bytes: 400, NowMs: 10}, 100)

	if want := []int64{1, 2}; !slices.Equal(got.Seqs, want) {
		t.Fatalf("bytes pass seqs = %v, want %v", got.Seqs, want)
	}
	if got.FreedBytes != 200 {
		t.Fatalf("FreedBytes = %d, want 200", got.FreedBytes)
	}
	if got.More {
		t.Fatalf("More = true, want false")
	}
}

func TestPlanEvictionThreeLimitsAtOnce(t *testing.T) {
	// age + count + bytes all violated; the union is deduped and ordered by seq.
	c := candidates(
		Candidate{Seq: 1, Size: 50, PublishedAt: 100}, // selected by all three passes
		Candidate{Seq: 2, Size: 50, PublishedAt: 900},
		Candidate{Seq: 3, Size: 50, PublishedAt: 950},
		Candidate{Seq: 4, Size: 50, PublishedAt: 990},
		Candidate{Seq: 5, Size: 50, PublishedAt: 995},
	)
	// age: cutoff 950-800 = 150 → only seq 1 (published 100 < 150). count: msgs 5,
	// max_msgs 3 → excess 2 → seqs 1,2. bytes: 250 total, max 150 → free 100 → seqs
	// 1,2 (their 100 bytes hit the target exactly). Union: [1 2].
	got := PlanEviction(c, RetentionView{
		MaxMsgs: 3, MaxBytes: 150, MaxAgeMs: 800,
		Msgs: 5, Bytes: 250, NowMs: 950,
	}, 100)

	if want := []int64{1, 2}; !slices.Equal(got.Seqs, want) {
		t.Fatalf("union seqs = %v, want [1 2]", got.Seqs)
	}
	if got.FreedBytes != 100 {
		t.Fatalf("FreedBytes = %d, want 100", got.FreedBytes)
	}
	if got.HighestDeletedSeq != 2 {
		t.Fatalf("HighestDeletedSeq = %d, want 2", got.HighestDeletedSeq)
	}
	if got.BlockedCount != 0 {
		t.Fatalf("BlockedCount = %d, want 0", got.BlockedCount)
	}
}

func TestPlanEvictionZeroMeansUnlimited(t *testing.T) {
	c := candidates(
		Candidate{Seq: 1, Size: 1 << 20, PublishedAt: 0},
		Candidate{Seq: 2, Size: 1 << 20, PublishedAt: 1},
	)
	for name, v := range map[string]RetentionView{
		"no limits at all": {Msgs: 2, Bytes: 2 << 20, NowMs: 5},
	} {
		t.Run(name, func(t *testing.T) {
			got := PlanEviction(c, v, 100)
			if len(got.Seqs) != 0 {
				t.Fatalf("seqs = %v, want none: an unset limit (0) must never delete", got.Seqs)
			}
		})
	}

	// Two limits configured together must still respect each other's arithmetic; spelled
	// out rather than table-driven so each expectation is readable.
	t.Run("count+age without bytes", func(t *testing.T) {
		got := PlanEviction(c, RetentionView{MaxMsgs: 1, MaxAgeMs: 100, Msgs: 2, Bytes: 2 << 20, NowMs: 101}, 100)
		if want := []int64{1}; !slices.Equal(got.Seqs, want) {
			t.Fatalf("seqs = %v, want [1] (count excess 1; age cutoff 1 also selects seq 1)", got.Seqs)
		}
	})
	t.Run("bytes alone with unlimited count", func(t *testing.T) {
		got := PlanEviction(c, RetentionView{MaxBytes: 1 << 20, Msgs: 2, Bytes: 2 << 20, NowMs: 5}, 100)
		if want := []int64{1}; !slices.Equal(got.Seqs, want) {
			t.Fatalf("seqs = %v, want [1]: free 1MiB, keep exactly one message", got.Seqs)
		}
	})
}

func TestPlanEvictionMaxBytesBelowOneMessageKeepsExactlyOne(t *testing.T) {
	// The edge table: max_bytes smaller than a single message keeps exactly one message —
	// never an infinite delete-then-refuse loop, never an empty stream from byte pressure.
	c := candidates(
		Candidate{Seq: 1, Size: 1000, PublishedAt: 1},
		Candidate{Seq: 2, Size: 1000, PublishedAt: 2},
		Candidate{Seq: 3, Size: 1000, PublishedAt: 3},
	)
	got := PlanEviction(c, RetentionView{MaxBytes: 1, Msgs: 3, Bytes: 3000, NowMs: 10}, 100)

	if want := []int64{1, 2}; !slices.Equal(got.Seqs, want) {
		t.Fatalf("seqs = %v, want [1 2]: byte pressure deletes down to exactly one", got.Seqs)
	}
	if got.More {
		t.Fatalf("More = true, want false: one remaining message is the floor, not pending work")
	}
}

func TestPlanEvictionBlockedHeadSkippedNotStalled(t *testing.T) {
	// G2: with the OLDEST message pinned by a delivery row, the sweep continues past it.
	c := candidates(
		Candidate{Seq: 1, Size: 10, PublishedAt: 1, HasDelivery: true},
		Candidate{Seq: 2, Size: 10, PublishedAt: 2},
		Candidate{Seq: 3, Size: 10, PublishedAt: 3},
	)
	got := PlanEviction(c, RetentionView{MaxMsgs: 1, Msgs: 3, Bytes: 30, NowMs: 10}, 100)

	// excess = 3-1 = 2: the sweep continues past the pinned head and takes BOTH
	// unpinned candidates.
	if want := []int64{2, 3}; !slices.Equal(got.Seqs, want) {
		t.Fatalf("seqs = %v, want [2 3]: blocked head skipped, not stopping", got.Seqs)
	}
	if got.BlockedCount != 1 || got.BlockedBytes != 10 {
		t.Fatalf("blocked accounting = (%d,%d), want (1,10)", got.BlockedCount, got.BlockedBytes)
	}
	if got.HighestBlockedSeq != 1 {
		t.Fatalf("HighestBlockedSeq = %d, want 1", got.HighestBlockedSeq)
	}
}

func TestPlanEvictionBlockedInteriorSkipped(t *testing.T) {
	c := candidates(
		Candidate{Seq: 1, Size: 10, PublishedAt: 1},
		Candidate{Seq: 2, Size: 10, PublishedAt: 2, HasDelivery: true},
		Candidate{Seq: 3, Size: 10, PublishedAt: 3},
		Candidate{Seq: 4, Size: 10, PublishedAt: 4},
	)
	got := PlanEviction(c, RetentionView{MaxMsgs: 1, Msgs: 4, Bytes: 40, NowMs: 10}, 100)

	// excess = 4-1 = 3: seqs 1, 3 and 4 go; only the pinned seq 2 survives its turn.
	if want := []int64{1, 3, 4}; !slices.Equal(got.Seqs, want) {
		t.Fatalf("seqs = %v, want [1 3 4]", got.Seqs)
	}
	if got.BlockedCount != 1 || got.HighestBlockedSeq != 2 {
		t.Fatalf("blocked accounting = (%d, seq %d), want (1, seq 2)",
			got.BlockedCount, got.HighestBlockedSeq)
	}
}

func TestPlanEvictionEveryCandidateBlocked(t *testing.T) {
	// Edge table row: zero deleted, blocked accounting set, More=false so the tick ends.
	c := candidates(
		Candidate{Seq: 1, Size: 10, PublishedAt: 1, HasDelivery: true},
		Candidate{Seq: 2, Size: 20, PublishedAt: 2, HasDelivery: true},
	)
	got := PlanEviction(c, RetentionView{MaxBytes: 5, Msgs: 2, Bytes: 30, NowMs: 10}, 100)

	if len(got.Seqs) != 0 {
		t.Fatalf("seqs = %v, want none", got.Seqs)
	}
	if got.BlockedCount != 2 || got.BlockedBytes != 30 || got.HighestBlockedSeq != 2 {
		t.Fatalf("blocked accounting = (%d,%d,seq %d), want (2,30,seq 2)",
			got.BlockedCount, got.BlockedBytes, got.HighestBlockedSeq)
	}
	if got.More {
		t.Fatalf("More = true, want false: nothing was deleted, rescheduling cannot help")
	}
}

func TestPlanEvictionBatchBoundAndMore(t *testing.T) {
	// The store hands the planner a bounded oldest-first working set. When even the
	// whole window cannot satisfy the limits, the saturated batch reports More=true so
	// the janitor reschedules while budget remains; once a sweep satisfies its limit,
	// More=false ends the tick.
	window := []Candidate{
		{Seq: 1, Size: 1, PublishedAt: 1},
		{Seq: 2, Size: 1, PublishedAt: 2},
		{Seq: 3, Size: 1, PublishedAt: 3},
	}
	// Stats say 10 msgs / 10 bytes; max_bytes 4 demands 6 freed; the window holds 3.
	bounded := PlanEviction(window, RetentionView{MaxBytes: 4, Msgs: 10, Bytes: 10, NowMs: 10}, 3)
	if want := []int64{1, 2, 3}; !slices.Equal(bounded.Seqs, want) {
		t.Fatalf("bounded seqs = %v, want [1 2 3]", bounded.Seqs)
	}
	if !bounded.More {
		t.Fatalf("More = false, want true: after this batch the stream still violates max_bytes")
	}

	enough := make([]Candidate, 0, 9)
	for i := int64(1); i <= 9; i++ {
		enough = append(enough, Candidate{Seq: i, Size: 1, PublishedAt: i})
	}
	done := PlanEviction(enough, RetentionView{MaxBytes: 4, Msgs: 10, Bytes: 10, NowMs: 10}, 9)
	if want := []int64{1, 2, 3, 4, 5, 6}; !slices.Equal(done.Seqs, want) {
		t.Fatalf("done seqs = %v, want [1 2 3 4 5 6]: stop once 6 bytes are freed", done.Seqs)
	}
	if done.More {
		t.Fatalf("More = true, want false: the limit is satisfied")
	}
}

func TestSelectBlame(t *testing.T) {
	t.Run("empty holders blame nobody", func(t *testing.T) {
		if _, ok := SelectBlame(nil); ok {
			t.Fatal("ok = true, want false")
		}
	})
	t.Run("oldest blocking seq wins among multiple blockers", func(t *testing.T) {
		got, ok := SelectBlame([]Holder{
			{Consumer: "fast", Seq: 900},
			{Consumer: "slow", Seq: 10493},
			{Consumer: "mid", Seq: 5000},
		})
		if !ok {
			t.Fatal("ok = false, want true")
		}
		if got.Seq != 900 {
			t.Fatalf("blocking_seq = %d, want 900 (the oldest)", got.Seq)
		}
		if got.Consumer != "fast" {
			t.Fatalf("consumer = %q, want %q (the owner of the oldest seq)", got.Consumer, "fast")
		}
	})
	t.Run("tie breaks on consumer name deterministically", func(t *testing.T) {
		got, ok := SelectBlame([]Holder{{Consumer: "zeta", Seq: 5}, {Consumer: "alpha", Seq: 5}})
		if !ok || got.Consumer != "alpha" || got.Seq != 5 {
			t.Fatalf("blame = %+v ok=%v, want alpha@5", got, ok)
		}
	})
	t.Run("a paused consumer is blamed like any other", func(t *testing.T) {
		// Pausing is not abandoning (#27 §6): the caller passes paused holders in
		// unchanged, and selection looks at seq/name only.
		got, ok := SelectBlame([]Holder{{Consumer: "paused-one", Seq: 12}})
		if !ok || got.Consumer != "paused-one" || got.Seq != 12 {
			t.Fatalf("blame = %+v ok=%v, want paused-one@12", got, ok)
		}
	})
}

func TestStillViolating(t *testing.T) {
	v := RetentionView{MaxMsgs: 10, MaxBytes: 1000, Msgs: 12, Bytes: 1200}
	t.Run("both limits broken", func(t *testing.T) {
		if !StillViolating(11, 900, v) {
			t.Fatal("msgs above max must violate")
		}
	})
	t.Run("byte limit alone broken", func(t *testing.T) {
		if !StillViolating(5, 1100, v) {
			t.Fatal("bytes above max with slack msgs must still violate")
		}
	})
	t.Run("exactly at the limit is compliant", func(t *testing.T) {
		if StillViolating(10, 1000, v) {
			t.Fatal("boundary equality is compliance, not violation")
		}
	})
	t.Run("unlimited limits never violate", func(t *testing.T) {
		open := RetentionView{}
		if StillViolating(1<<40, 1<<40, open) {
			t.Fatal("0 = unlimited: nothing can violate it")
		}
	})
}

func TestDetectClockJump(t *testing.T) {
	const tol = time.Minute

	t.Run("steady ticks within tolerance do not jump", func(t *testing.T) {
		prev := TickSample{Wall: unixms(0), Mono: time.Minute}
		cur := TickSample{Wall: unixms((time.Minute + time.Second).Milliseconds()), Mono: 2 * time.Minute}
		if _, jumped := DetectClockJump(prev, cur, tol); jumped {
			t.Fatal("jumped = true, want false: |Δwall−Δmono| = 1s ≤ tolerance")
		}
	})
	t.Run("forward jump beyond tolerance is reported", func(t *testing.T) {
		prev := TickSample{Wall: unixms(0), Mono: time.Minute}
		cur := TickSample{
			Wall: unixms((90 * time.Minute).Milliseconds()),
			Mono: 2 * time.Minute,
		}
		got, jumped := DetectClockJump(prev, cur, tol)
		if !jumped {
			t.Fatal("jumped = false, want true: wall stepped +90m while mono advanced 1m")
		}
		if got.WallDelta != 90*time.Minute {
			t.Fatalf("WallDelta = %v, want %v", got.WallDelta, 90*time.Minute)
		}
		if got.MonoDelta != time.Minute {
			t.Fatalf("MonoDelta = %v, want %v", got.MonoDelta, time.Minute)
		}
	})
	t.Run("backward jump beyond tolerance is reported", func(t *testing.T) {
		prev := TickSample{Wall: unixms(time.Hour.Milliseconds()), Mono: time.Hour}
		cur := TickSample{Wall: unixms(0), Mono: time.Hour + time.Minute}
		got, jumped := DetectClockJump(prev, cur, tol)
		if !jumped {
			t.Fatal("jumped = false, want true: wall stepped -1h while mono advanced 1m")
		}
		want := -time.Hour
		if got.WallDelta != want {
			t.Fatalf("WallDelta = %v, want %v", got.WallDelta, want)
		}
	})
	t.Run("exactly at tolerance is not a jump", func(t *testing.T) {
		prev := TickSample{Wall: unixms(0), Mono: 0}
		cur := TickSample{Wall: unixms(time.Minute.Milliseconds()), Mono: 0}
		if _, jumped := DetectClockJump(prev, cur, tol); jumped {
			t.Fatal("jumped = true, want false: divergence equals tolerance (strictly-greater rule)")
		}
	})
	t.Run("wall and mono moving together never jumps", func(t *testing.T) {
		prev := TickSample{Wall: unixms(0), Mono: 0}
		cur := TickSample{Wall: unixms(time.Hour.Milliseconds()), Mono: time.Hour}
		if _, jumped := DetectClockJump(prev, cur, tol); jumped {
			t.Fatal("jumped = true, want false: consistent clocks")
		}
	})
}
