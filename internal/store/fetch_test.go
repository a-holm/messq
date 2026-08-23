// SPDX-License-Identifier: Apache-2.0

package store

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/a-holm/messq/internal/clock"
	"github.com/a-holm/messq/internal/errs"
	"github.com/a-holm/messq/internal/queue"
)

// newFetchStore opens an engine-less store with stream "orders" and returns it with
// its fake clock.
func newFetchStore(t *testing.T) (*Store, *clock.Fake) {
	t.Helper()
	st := newConsumerStream(t)
	fk, ok := st.clk.(*clock.Fake)
	if !ok {
		t.Fatal("store clock is not *clock.Fake")
	}
	return st, fk
}

// publishSubjs publishes one message per subject, advancing the clock 1s between each.
func publishSubjs(t *testing.T, st *Store, subjects ...string) {
	t.Helper()
	ctx := context.Background()
	fk, ok := st.clk.(*clock.Fake)
	if !ok {
		t.Fatal("store clock is not *clock.Fake")
	}
	for _, s := range subjects {
		if _, err := st.Publish(ctx, PublishCmd{Stream: "orders", Req: queue.PublishReq{Subject: s, Body: []byte("x")}}); err != nil {
			t.Fatalf("publish %q: %v", s, err)
		}
		fk.Advance(time.Second)
	}
}

func newFetchConsumer(t *testing.T, st *Store, name string, cfg queue.ConsumerConfig) {
	t.Helper()
	if _, err := st.CreateConsumer(context.Background(), "orders", cfg, queue.StartPosition{Kind: queue.StartFirst}, "test"); err != nil {
		t.Fatalf("create consumer: %v", err)
	}
}

func countEvent(t *testing.T, st *Store, name string) int64 {
	t.Helper()
	var n int64
	if err := st.RO().QueryRowContext(context.Background(),
		`SELECT count(*) FROM events WHERE event = ?`, name).Scan(&n); err != nil {
		t.Fatalf("count %s events: %v", name, err)
	}
	return n
}

func TestFetchBasicDelivery(t *testing.T) {
	st, _ := newFetchStore(t)
	publishSubjs(t, st, "orders.1", "orders.2", "orders.3")
	cfg := queue.DefaultConsumerConfig("worker")
	cfg.Filters = []string{">"}
	newFetchConsumer(t, st, "worker", cfg)

	res, err := st.Fetch(context.Background(), FetchReq{Stream: "orders", Consumer: "worker", Batch: 2})
	if err != nil {
		t.Fatalf("Fetch: %v", err)
	}
	if len(res.Messages) != 2 {
		t.Fatalf("messages = %d, want 2", len(res.Messages))
	}
	if res.Messages[0].Seq != 1 || res.Messages[1].Seq != 2 {
		t.Fatalf("claimed seqs = %d,%d, want 1,2", res.Messages[0].Seq, res.Messages[1].Seq)
	}
	if res.Messages[0].Attempt != 1 || res.Messages[1].Attempt != 1 {
		t.Fatalf("attempts = %d,%d, want 1,1", res.Messages[0].Attempt, res.Messages[1].Attempt)
	}
	if got := res.Messages[0].AckToken; got != "orders/worker/1/1/1" {
		t.Fatalf("token = %q, want orders/worker/1/1/1", got)
	}
	if res.CursorSeq != 4 {
		t.Fatalf("cursor_seq = %d, want 4 (advanced past all three)", res.CursorSeq)
	}
	if res.Pending != 3 || res.Inflight != 2 {
		t.Fatalf("pending/inflight = %d/%d, want 3/2", res.Pending, res.Inflight)
	}
	if res.Hold != HoldNone {
		t.Fatalf("hold = %q, want \"\"", res.Hold)
	}
	if res.Messages[0].Body == nil {
		t.Fatal("body should be present")
	}
}

func TestFetchFilterSkipsNonMatching(t *testing.T) {
	st, _ := newFetchStore(t)
	publishSubjs(t, st, "orders.eu.created", "orders.us.created", "orders.eu.shipped")
	cfg := queue.DefaultConsumerConfig("worker")
	cfg.Filters = []string{"orders.eu.>"}
	newFetchConsumer(t, st, "worker", cfg)

	res, err := st.Fetch(context.Background(), FetchReq{Stream: "orders", Consumer: "worker", Batch: 10})
	if err != nil {
		t.Fatalf("Fetch: %v", err)
	}
	if len(res.Messages) != 2 {
		t.Fatalf("messages = %d, want 2 (only orders.eu.* match)", len(res.Messages))
	}
	if res.Messages[0].Subject != "orders.eu.created" || res.Messages[1].Subject != "orders.eu.shipped" {
		t.Fatalf("subjects = %q,%q, want the two orders.eu.*", res.Messages[0].Subject, res.Messages[1].Subject)
	}
	// Cursor advanced past the non-matching orders.us.created (seq 2) too.
	if res.CursorSeq != 4 {
		t.Fatalf("cursor_seq = %d, want 4", res.CursorSeq)
	}
	// C1: every delivery row is below the cursor.
	var maxSeq int64
	if err := st.RO().QueryRowContext(context.Background(),
		`SELECT coalesce(max(seq), 0) FROM deliveries WHERE stream='orders' AND consumer='worker'`).Scan(&maxSeq); err != nil {
		t.Fatalf("max delivery seq: %v", err)
	}
	if maxSeq >= res.CursorSeq {
		t.Fatalf("C1 violated: max delivery seq %d >= cursor %d", maxSeq, res.CursorSeq)
	}
}

func TestFetchClaimPostIncrement(t *testing.T) {
	st, _ := newFetchStore(t)
	publishSubjs(t, st, "orders.1")
	cfg := queue.DefaultConsumerConfig("worker")
	cfg.AckWait = 30 * time.Second
	newFetchConsumer(t, st, "worker", cfg)

	res, err := st.Fetch(context.Background(), FetchReq{Stream: "orders", Consumer: "worker", Batch: 1})
	if err != nil {
		t.Fatalf("Fetch: %v", err)
	}
	var state, attempts int64
	var visibleAt, deliveredAt int64
	if err := st.RO().QueryRowContext(context.Background(),
		`SELECT state, attempts, visible_at, delivered_at FROM deliveries WHERE stream='orders' AND consumer='worker' AND seq=1`).
		Scan(&state, &attempts, &visibleAt, &deliveredAt); err != nil {
		t.Fatalf("read delivery: %v", err)
	}
	if state != 1 || attempts != 1 {
		t.Fatalf("state/attempts = %d/%d, want 1/1", state, attempts)
	}
	if visibleAt != deliveredAt+30000 {
		t.Fatalf("visible_at %d != delivered_at %d + 30000", visibleAt, deliveredAt)
	}
	// Token carries the POST-increment attempt.
	if res.Messages[0].AckToken != "orders/worker/1/1/1" {
		t.Fatalf("token = %q, want post-increment attempt 1", res.Messages[0].AckToken)
	}
	// One msg.deliver event per claimed row.
	if got := countEvent(t, st, "msg.deliver"); got != 1 {
		t.Fatalf("msg.deliver events = %d, want 1", got)
	}
}

func TestFetchByteBudget(t *testing.T) {
	st, _ := newFetchStore(t)
	ctx := context.Background()
	// Three 100-byte bodies.
	for i := 0; i < 3; i++ {
		if _, err := st.Publish(ctx, PublishCmd{
			Stream: "orders",
			Req:    queue.PublishReq{Subject: "orders.1", Body: make([]byte, 100)},
		}); err != nil {
			t.Fatalf("publish: %v", err)
		}
	}
	cfg := queue.DefaultConsumerConfig("worker")
	cfg.Filters = []string{">"}
	newFetchConsumer(t, st, "worker", cfg)

	res, err := st.Fetch(ctx, FetchReq{Stream: "orders", Consumer: "worker", Batch: 10, MaxBytes: 250})
	if err != nil {
		t.Fatalf("Fetch: %v", err)
	}
	if len(res.Messages) != 2 {
		t.Fatalf("messages = %d, want 2 (250-byte budget over 100-byte bodies)", len(res.Messages))
	}

	// Oversized single message is still returned.
	if _, pErr := st.Publish(ctx, PublishCmd{
		Stream: "orders",
		Req:    queue.PublishReq{Subject: "orders.big", Body: make([]byte, 1000)},
	}); pErr != nil {
		t.Fatalf("publish big: %v", pErr)
	}
	res, err = st.Fetch(ctx, FetchReq{Stream: "orders", Consumer: "worker", Batch: 10, MaxBytes: 100})
	if err != nil {
		t.Fatalf("Fetch big: %v", err)
	}
	if len(res.Messages) != 1 {
		t.Fatalf("oversized messages = %d, want 1 (always return at least one)", len(res.Messages))
	}
}

func TestFetchFlowControlPinsCursor(t *testing.T) {
	st, _ := newFetchStore(t)
	publishSubjs(t, st, "orders.1", "orders.2", "orders.3")
	cfg := queue.DefaultConsumerConfig("worker")
	cfg.MaxAckPending = 2
	newFetchConsumer(t, st, "worker", cfg)
	ctx := context.Background()

	// First fetch: top-up admits seqs 1,2 then declines seq 3 at the bound, pinning the
	// cursor at 3. Claims seq 1 (batch=1).
	res, err := st.Fetch(ctx, FetchReq{Stream: "orders", Consumer: "worker", Batch: 1})
	if err != nil {
		t.Fatalf("fetch 1: %v", err)
	}
	if res.CursorSeq != 3 {
		t.Fatalf("cursor_seq = %d, want 3 (pinned at the declined match)", res.CursorSeq)
	}
	if len(res.Messages) != 1 || res.Messages[0].Seq != 1 {
		t.Fatalf("messages = %+v, want [seq 1]", res.Messages)
	}

	// Second fetch: at the bound but a READY row (seq 2) is still claimable.
	res, err = st.Fetch(ctx, FetchReq{Stream: "orders", Consumer: "worker", Batch: 1})
	if err != nil {
		t.Fatalf("fetch 2: %v", err)
	}
	if len(res.Messages) != 1 || res.Messages[0].Seq != 2 {
		t.Fatalf("messages = %+v, want [seq 2]", res.Messages)
	}

	// Third fetch: nothing READY, at the bound → flow_control.
	res, err = st.Fetch(ctx, FetchReq{Stream: "orders", Consumer: "worker", Batch: 1})
	if err != nil {
		t.Fatalf("fetch 3: %v", err)
	}
	if res.Hold != HoldFlowControl || res.MaxAckPending != 2 || res.Pending != 2 {
		t.Fatalf("hold/max/pending = %q/%d/%d, want flow_control/2/2", res.Hold, res.MaxAckPending, res.Pending)
	}
	// The declined message (seq 3) is not lost: it is still >= the cursor.
	var admitted int64
	if err := st.RO().QueryRowContext(ctx,
		`SELECT count(*) FROM deliveries WHERE stream='orders' AND consumer='worker'`).Scan(&admitted); err != nil {
		t.Fatalf("count deliveries: %v", err)
	}
	if admitted != 2 {
		t.Fatalf("admitted = %d, want 2 (seq 3 held back by flow control, not dropped)", admitted)
	}
}

func TestFetchFlowBlockedRateLimited(t *testing.T) {
	st, fk := newFetchStore(t)
	publishSubjs(t, st, "orders.1")
	cfg := queue.DefaultConsumerConfig("worker")
	cfg.MaxAckPending = 1
	newFetchConsumer(t, st, "worker", cfg)
	ctx := context.Background()

	// Claim the only message → pending = 1 = bound.
	if _, err := st.Fetch(ctx, FetchReq{Stream: "orders", Consumer: "worker", Batch: 1}); err != nil {
		t.Fatalf("first fetch: %v", err)
	}
	// Blocked fetch emits one flow.blocked.
	if _, err := st.Fetch(ctx, FetchReq{Stream: "orders", Consumer: "worker", Batch: 1}); err != nil {
		t.Fatalf("blocked fetch: %v", err)
	}
	if got := countEvent(t, st, "flow.blocked"); got != 1 {
		t.Fatalf("flow.blocked events = %d, want 1", got)
	}
	// Same instant: rate-limited, no second event.
	for i := 0; i < 5; i++ {
		if _, err := st.Fetch(ctx, FetchReq{Stream: "orders", Consumer: "worker", Batch: 1}); err != nil {
			t.Fatalf("blocked fetch %d: %v", i, err)
		}
	}
	if got := countEvent(t, st, "flow.blocked"); got != 1 {
		t.Fatalf("flow.blocked events after burst = %d, want still 1", got)
	}
	// After the interval, one more event.
	fk.Advance(11 * time.Second)
	if _, err := st.Fetch(ctx, FetchReq{Stream: "orders", Consumer: "worker", Batch: 1}); err != nil {
		t.Fatalf("blocked fetch post-interval: %v", err)
	}
	if got := countEvent(t, st, "flow.blocked"); got != 2 {
		t.Fatalf("flow.blocked events post-interval = %d, want 2", got)
	}
}

func TestFetchPaused(t *testing.T) {
	st, _ := newFetchStore(t)
	publishSubjs(t, st, "orders.1")
	cfg := queue.DefaultConsumerConfig("worker")
	newFetchConsumer(t, st, "worker", cfg)
	ctx := context.Background()
	if _, err := st.SetPaused(ctx, "orders", "worker", true, "test"); err != nil {
		t.Fatalf("pause: %v", err)
	}
	res, err := st.Fetch(ctx, FetchReq{Stream: "orders", Consumer: "worker", Batch: 1})
	if err != nil {
		t.Fatalf("Fetch paused: %v", err)
	}
	if res.Hold != HoldPaused || len(res.Messages) != 0 {
		t.Fatalf("hold/messages = %q/%d, want paused/0", res.Hold, len(res.Messages))
	}
	// No deliveries were admitted while paused.
	var n int64
	if err := st.RO().QueryRowContext(ctx,
		`SELECT count(*) FROM deliveries WHERE stream='orders' AND consumer='worker'`).Scan(&n); err != nil {
		t.Fatalf("count deliveries: %v", err)
	}
	if n != 0 {
		t.Fatalf("paused fetch admitted %d deliveries, want 0", n)
	}
}

func TestFetchMissingConsumer(t *testing.T) {
	st, _ := newFetchStore(t)
	_, err := st.Fetch(context.Background(), FetchReq{Stream: "orders", Consumer: "ghost", Batch: 1})
	if !errors.Is(err, errs.ErrNotFound) {
		t.Fatalf("Fetch missing consumer = %v, want ErrNotFound", err)
	}
}

func TestFetchInvalidBatch(t *testing.T) {
	st, _ := newFetchStore(t)
	cfg := queue.DefaultConsumerConfig("worker")
	newFetchConsumer(t, st, "worker", cfg)
	if _, err := st.Fetch(context.Background(), FetchReq{Stream: "orders", Consumer: "worker", Batch: 0}); !errors.Is(err, errs.ErrBadRequest) {
		t.Fatalf("Fetch batch=0 = %v, want ErrBadRequest", err)
	}
}

func TestFetchMaxAckPendingOneSerial(t *testing.T) {
	st, _ := newFetchStore(t)
	publishSubjs(t, st, "orders.1", "orders.2")
	cfg := queue.DefaultConsumerConfig("worker")
	cfg.MaxAckPending = 1
	newFetchConsumer(t, st, "worker", cfg)
	ctx := context.Background()

	// Strictly serial: one row at a time.
	res, err := st.Fetch(ctx, FetchReq{Stream: "orders", Consumer: "worker", Batch: 10})
	if err != nil {
		t.Fatalf("fetch 1: %v", err)
	}
	if len(res.Messages) != 1 || res.Messages[0].Seq != 1 {
		t.Fatalf("messages = %+v, want [seq 1] alone", res.Messages)
	}
	if res.CursorSeq != 2 {
		t.Fatalf("cursor_seq = %d, want 2 (pinned at the declined seq 2)", res.CursorSeq)
	}
}
