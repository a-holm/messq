// SPDX-License-Identifier: Apache-2.0

package store

import (
	"context"
	"testing"
	"time"

	"github.com/a-holm/messq/internal/queue"
)

// The RetireCmd tests (issue #11 G7): lowering max_deliver below current attempts
// strands READY rows that nothing else would retire; RetireCmd dead-letters them with
// Trigger "policy_lowered", never touching INFLIGHT rows (never mid-flight).

// TestRetireStrandedReadyRows lowers max_deliver from 5 to 1 after three rows are
// released READY at attempts=1, then retires them all — the row gone, a msg.dead audit
// row present, INFLIGHT rows untouched.
func TestRetireStrandedReadyRows(t *testing.T) {
	st, _, _ := openSweepStore(t)

	// Seed 3 claims (attempts=1, INFLIGHT), then nak them all to READY.
	toks := seedSettle(t, st, 3, 3)
	settles := make([]SettleItem, 0, len(toks))
	for _, tk := range toks {
		settles = append(settles, SettleItem{Token: tk, Verb: queue.VerbNak, Reason: "test"})
	}
	sr, err := st.Settle(context.Background(), settleCmd(settles...))
	if err != nil {
		t.Fatalf("nak: %v", err)
	}
	if sr.OK != 3 {
		t.Fatalf("nak ok = %d, want 3", sr.OK)
	}

	// All three rows are now READY at attempts=1.
	if ready := countRows(t, st, `SELECT count(*) FROM deliveries WHERE state = 0 AND attempts = 1`); ready != 3 {
		t.Fatalf("READY rows after nak = %d, want 3", ready)
	}

	// Lower max_deliver to 1: every READY row at attempts=1 is now stranded.
	if _, upErr := st.UpdateConsumer(context.Background(), "orders", "worker",
		ConsumerPatch{MaxDeliver: int32ptr(1)}, "test"); upErr != nil {
		t.Fatalf("lower max_deliver: %v", upErr)
	}

	rr, err := st.Retire(context.Background(), RetireCmd{Limit: 10})
	if err != nil {
		t.Fatalf("retire: %v", err)
	}
	if rr.Retired != 3 {
		t.Fatalf("retired = %d, want 3", rr.Retired)
	}

	// The stranded rows are gone; a msg.dead row exists for each.
	if remaining := countRows(t, st, `SELECT count(*) FROM deliveries WHERE stream='orders' AND consumer='worker'`); remaining != 0 {
		t.Fatalf("deliveries after retire = %d, want 0", remaining)
	}
	if dead := countRows(t, st, `SELECT count(*) FROM events WHERE event = 'msg.dead'`); dead != 3 {
		t.Fatalf("msg.dead rows = %d, want 3", dead)
	}
}

// TestRetireSkippedForMaxDeliver0 asserts the pass is a no-op for max_deliver=0
// consumers (G7: "skipped entirely for max_deliver = 0") — unlimited consumers never die
// by count, so nothing is ever stranded there.
func TestRetireSkippedForMaxDeliver0(t *testing.T) {
	st, fk, _ := openSweepStore(t)
	seedSweep(t, st, func(c *queue.ConsumerConfig) { c.MaxDeliver = 0 }, 2, 2)
	fk.Advance(31 * time.Second) // deadlines pass, but max_deliver=0: no bound to enforce

	rr, err := st.Retire(context.Background(), RetireCmd{Limit: 10})
	if err != nil {
		t.Fatalf("retire: %v", err)
	}
	if rr.Retired != 0 {
		t.Fatalf("retired = %d, want 0 for max_deliver=0", rr.Retired)
	}
	// Both rows still present (INFLIGHT, untouched — never mid-flight).
	if inflight := countRows(t, st, `SELECT count(*) FROM deliveries WHERE state = 1`); inflight != 2 {
		t.Fatalf("INFLIGHT rows = %d, want 2 (retire skipped for max_deliver=0)", inflight)
	}
}

// TestRetireNeverTouchesInflightRows asserts the "never mid-flight" half of G7: an
// INFLIGHT row stranded above a lowered max_deliver is left alone — a worker may be
// holding it — and is NOT retired by the pass.
func TestRetireNeverTouchesInflightRows(t *testing.T) {
	st, _, _ := openSweepStore(t)
	seedSweep(t, st, nil, 2, 2) // 2 INFLIGHT rows at attempts=1

	// Lower max_deliver below the in-flight attempts: 1 <= 1, but the rows are INFLIGHT.
	if _, err := st.UpdateConsumer(context.Background(), "orders", "worker",
		ConsumerPatch{MaxDeliver: int32ptr(1)}, "test"); err != nil {
		t.Fatalf("lower max_deliver: %v", err)
	}

	rr, err := st.Retire(context.Background(), RetireCmd{Limit: 10})
	if err != nil {
		t.Fatalf("retire: %v", err)
	}
	if rr.Retired != 0 {
		t.Fatalf("retired = %d, want 0 (INFLIGHT rows never mid-flight)", rr.Retired)
	}
	if inflight := countRows(t, st, `SELECT count(*) FROM deliveries WHERE state = 1`); inflight != 2 {
		t.Fatalf("INFLIGHT rows after retire = %d, want 2 (in-progress work untouched)", inflight)
	}
}

func int32ptr(v int32) *int32 { return &v }
