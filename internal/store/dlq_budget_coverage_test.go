// SPDX-License-Identifier: Apache-2.0

package store

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/a-holm/messq/internal/clock"
	"github.com/a-holm/messq/internal/queue"
)

// The DLQ-budget deferral coverage tests (issue #12 restore): they reach the
// queue.ErrDeadBudget handling in the RetireCmd and SettleCmd callers, which the sweep
// budget test (TestDLQBudgetDefers) exercises for SweepCmd only. A tiny
// MaxBytesPerCommit bounds one transaction's copies; the first Dead returns ErrDeadBudget
// and the caller defers the row to a later pass.
//
// These use the public Retire/Settle APIs so the seam wiring (newDeadSink, budget
// refresh per command) is exercised exactly as production builds it.

// TestRetireBudgetDefersThenCompletes: with a budget that fits one copy, a retire of
// three stranded rows rounds-trips through ErrDeadBudget on the first pass (some copies
// written, the rest deferred) and completes on the second.
func TestRetireBudgetDefersThenCompletes(t *testing.T) {
	// 45 bytes fits exactly one 40-byte copy, so the first retire defers the rest.
	st, fk := openDLQStore(t, func(c *queue.DLQConfig) { c.MaxBytesPerCommit = 45 })
	ctx := context.Background()

	// Seed 3 claims with 40-byte bodies (so a 45-byte budget fits exactly one copy), nak
	// them all READY (attempts=1), lower max_deliver to 1 so they are stranded, then retire.
	for i := 0; i < 3; i++ {
		if _, err := st.Publish(ctx, PublishCmd{
			Stream: "orders", Req: queue.PublishReq{Subject: "orders.1", Body: make([]byte, 40)},
		}); err != nil {
			t.Fatalf("publish %d: %v", i, err)
		}
	}
	cfg := queue.DefaultConsumerConfig("worker")
	cfg.Filters = []string{">"}
	if _, err := st.CreateConsumer(ctx, "orders", cfg, queue.StartPosition{Kind: queue.StartFirst}, "test"); err != nil {
		t.Fatalf("create consumer: %v", err)
	}
	res, err := st.Fetch(ctx, FetchReq{Stream: "orders", Consumer: "worker", Batch: 3})
	if err != nil || len(res.Messages) != 3 {
		t.Fatalf("fetch = %d msgs err=%v, want 3", len(res.Messages), err)
	}
	settles := make([]SettleItem, 0, 3)
	for _, m := range res.Messages {
		tok, pErr := queue.ParseToken(m.AckToken)
		if pErr != nil {
			t.Fatalf("parse token %q: %v", m.AckToken, pErr)
		}
		settles = append(settles, SettleItem{Token: tok, Verb: queue.VerbNak, Reason: "test"})
	}
	if sr, srErr := st.Settle(ctx, settleCmd(settles...)); srErr != nil || sr.OK != 3 {
		t.Fatalf("nak ok = %d err=%v, want 3", sr.OK, srErr)
	}
	if _, upErr := st.UpdateConsumer(ctx, "orders", "worker",
		ConsumerPatch{MaxDeliver: int32ptr(1)}, "test"); upErr != nil {
		t.Fatalf("lower max_deliver: %v", upErr)
	}

	// First retire: the 45-byte command budget fits exactly one 40-byte copy, so exactly
	// one row is copied and the other two defer (no error to the caller — ErrDeadBudget is
	// swallowed as a deferral).
	rr, err := st.Retire(ctx, RetireCmd{Limit: 10})
	if err != nil {
		t.Fatalf("retire (bounded): %v", err)
	}
	if rr.Retired != 1 {
		t.Fatalf("retired %d of 3, want exactly 1 (a 45-byte budget fits one 40-byte copy)", rr.Retired)
	}
	if got := countDeliveryRows(t, st); got != 2 {
		t.Fatalf("deliveries left after first retire = %d, want 2 deferred", got)
	}
	if got := len(readDLQRows(t, st, "orders.dlq")); got != 1 {
		t.Fatalf("dlq copies after first retire = %d, want 1", got)
	}

	// Each later pass starts a fresh per-command budget, so it copies another one; loop
	// until the stranded rows are drained (bounded — the budget defers one per pass).
	total := rr.Retired
	for pass := 0; pass < 5 && countDeliveryRows(t, st) > 0; pass++ {
		fk.Advance(time.Second)
		next, err := st.Retire(ctx, RetireCmd{Limit: 10})
		if err != nil {
			t.Fatalf("retire (drain pass %d): %v", pass, err)
		}
		total += next.Retired
	}
	if total != 3 {
		t.Fatalf("total retired = %d, want 3", total)
	}
	if countDeliveryRows(t, st) != 0 {
		t.Fatalf("deliveries after drain = %d, want 0", countDeliveryRows(t, st))
	}
	if got := len(readDLQRows(t, st, "orders.dlq")); got != 3 {
		t.Fatalf("dlq copies after drain = %d, want 3", got)
	}
}

// TestSettleBudgetAbortsDeadBatch: SettleCmd treats ErrDeadBudget as a batch abort — the
// whole settle rolls back (the row stays), the client retries, and nothing commits half a
// death. This pins settle_impl's ErrDeadBudget arm.
func TestSettleBudgetAbortsDeadBatch(t *testing.T) {
	// A 1-byte budget means no 40-byte copy can ever fit, so the DEAD transition refuses.
	st, _ := openDLQStore(t, func(c *queue.DLQConfig) { c.MaxBytesPerCommit = 1 })
	ctx := context.Background()

	cfg := queue.DefaultConsumerConfig("worker")
	cfg.MaxDeliver = 1
	if _, err := st.CreateConsumer(ctx, "orders", cfg, queue.StartPosition{Kind: queue.StartFirst}, "test"); err != nil {
		t.Fatalf("create consumer: %v", err)
	}
	if _, err := st.Publish(ctx, PublishCmd{
		Stream: "orders", Req: queue.PublishReq{Subject: "orders.1", Body: make([]byte, 40)},
	}); err != nil {
		t.Fatalf("publish: %v", err)
	}
	res, err := st.Fetch(ctx, FetchReq{Stream: "orders", Consumer: "worker", Batch: 1})
	if err != nil || len(res.Messages) != 1 {
		t.Fatalf("fetch = %d msgs err=%v, want 1", len(res.Messages), err)
	}
	tok, err := queue.ParseToken(res.Messages[0].AckToken)
	if err != nil {
		t.Fatalf("parse token: %v", err)
	}

	// nak at attempts==max_deliver => DEAD; the 1-byte budget refuses, so the whole settle
	// batch errors with ErrDeadBudget and the row survives.
	_, err = st.Settle(ctx, settleCmd(SettleItem{Token: tok, Verb: queue.VerbNak, Reason: "boom"}))
	if !errors.Is(err, queue.ErrDeadBudget) {
		t.Fatalf("settle DEAD error = %v, want ErrDeadBudget batch abort", err)
	}
	if countDeliveryRows(t, st) != 1 {
		t.Fatalf("deliveries after aborted DEAD = %d, want 1 (the batch rolled back)", countDeliveryRows(t, st))
	}
	if n := countRows(t, st, `SELECT count(*) FROM events WHERE event = 'msg.dead'`); n != 0 {
		t.Fatalf("msg.dead rows = %d, want 0 (nothing committed)", n)
	}
}

var _ = clock.Fake{} // keep the clock import for fk-typed helpers in sibling tests
