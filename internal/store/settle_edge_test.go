// SPDX-License-Identifier: Apache-2.0

package store

import (
	"context"
	"sync"
	"testing"

	"github.com/a-holm/messq/internal/queue"
)

// Slice 13: 32-goroutine settle concurrency against the real writer (G13), orphaned
// delivery-row settleability (null msg id flagged, R7), and the chunked row-load bound
// (≤ settleRowChunk per query).

// TestSettleConcurrency32 drives 32 goroutines settling overlapping token sets through
// the group-commit writer: exactly one ok per (seq, attempt), everything else stale,
// no double msg.ack, all results request-ordered, -race clean.
func TestSettleConcurrency32(t *testing.T) {
	st, _ := openWiredCommandPathStore(t, fakeClock(), Config{Durability: DurabilityFull}, hooks{})
	if _, _, err := st.CreateStream(context.Background(), queue.DefaultConfig("orders"), "test"); err != nil {
		t.Fatalf("create stream: %v", err)
	}
	rec := &recordMetrics{}
	st.settleMetrics = rec
	const n = 64
	for i := 0; i < n; i++ {
		if _, err := st.Publish(context.Background(), PublishCmd{
			Stream: "orders", Req: queue.PublishReq{Subject: "orders.1", Body: []byte("x")},
		}); err != nil {
			t.Fatalf("publish %d: %v", i, err)
		}
	}
	cfg := queue.DefaultConsumerConfig("worker")
	if _, err := st.CreateConsumer(context.Background(), "orders", cfg, queue.StartPosition{Kind: queue.StartFirst}, "test"); err != nil {
		t.Fatalf("create consumer: %v", err)
	}
	res, err := st.Fetch(context.Background(), FetchReq{Stream: "orders", Consumer: "worker", Batch: n})
	if err != nil {
		t.Fatalf("fetch: %v", err)
	}
	toks := make([]queue.Token, 0, len(res.Messages))
	for _, m := range res.Messages {
		tk, err := queue.ParseToken(m.AckToken)
		if err != nil {
			t.Fatalf("parse: %v", err)
		}
		toks = append(toks, tk)
	}

	const coroutines = 32
	var wg sync.WaitGroup
	results := make([]SettleResult, coroutines)
	errs := make([]error, coroutines)
	for g := 0; g < coroutines; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			items := make([]SettleItem, len(toks))
			for j, tk := range toks {
				items[j] = SettleItem{Token: tk, Verb: queue.VerbAck}
			}
			sr, e := st.Settle(context.Background(), settleCmd(items...))
			results[g], errs[g] = sr, e
		}(g)
	}
	wg.Wait()

	for g := 0; g < coroutines; g++ {
		if errs[g] != nil {
			t.Fatalf("goroutine %d settle error: %v", g, errs[g])
		}
		if len(results[g].Results) != n {
			t.Fatalf("goroutine %d results = %d, want %d (request order)", g, len(results[g].Results), n)
		}
	}
	okTotal, staleTotal := 0, 0
	for _, sr := range results {
		for _, r := range sr.Results {
			switch r.Status {
			case queue.ItemStatusOK:
				okTotal++
			case queue.ItemStatusStale:
				staleTotal++
			default:
				t.Fatalf("concurrent ack got %s; want ok or stale", r.Status)
			}
		}
	}
	// exactly one ok per (seq, attempt-1): n acks won, the rest stale.
	if okTotal != n {
		t.Fatalf("ok acks = %d, want %d (exactly one per row)", okTotal, n)
	}
	if staleTotal != n*(coroutines-1) {
		t.Fatalf("stale acks = %d, want %d", staleTotal, n*(coroutines-1))
	}
	if ev := countEvent(t, st, "msg.ack"); ev != n {
		t.Fatalf("msg.ack events = %d, want exactly %d (no double ack)", ev, n)
	}
}

// TestSettleOrphanRowSettles verifies a delivery whose message row disappeared still
// settles: the ack deletes it and the event is flagged with a null msg id, never a
// silent drop nor a 500.
func TestSettleOrphanRowSettles(t *testing.T) {
	st, _, _ := openSettleStore(t)
	if _, err := st.Publish(context.Background(), PublishCmd{Stream: "orders", Req: queue.PublishReq{Subject: "orders.1", Body: []byte("x")}}); err != nil {
		t.Fatalf("publish: %v", err)
	}
	cfg := queue.DefaultConsumerConfig("worker")
	if _, err := st.CreateConsumer(context.Background(), "orders", cfg, queue.StartPosition{Kind: queue.StartFirst}, "test"); err != nil {
		t.Fatalf("create consumer: %v", err)
	}
	res, err := st.Fetch(context.Background(), FetchReq{Stream: "orders", Consumer: "worker", Batch: 1})
	if err != nil {
		t.Fatalf("fetch: %v", err)
	}
	tk, perr := queue.ParseToken(res.Messages[0].AckToken)
	if perr != nil {
		t.Fatalf("parse token: %v", perr)
	}
	// out-of-band: the message row vanishes underneath the delivery.
	if _, derr := st.rw.ExecContext(context.Background(), `DELETE FROM messages WHERE stream='orders'`); derr != nil {
		t.Fatalf("orphan the message: %v", derr)
	}
	sr, err := st.Settle(context.Background(), settleCmd(SettleItem{Token: tk, Verb: queue.VerbAck}))
	if err != nil {
		t.Fatalf("settle orphan: %v", err)
	}
	if sr.Results[0].Status != queue.ItemStatusOK {
		t.Fatalf("orphan ack = %s, want ok", sr.Results[0].Status)
	}
	if countDeliveryRows(t, st) != 0 {
		t.Fatal("orphan delivery row not deleted by the ack")
	}
	// the event row exists with a NULL msg id (flagged orphan, never a silent drop).
	var msgID string
	var count int
	if err := st.RO().QueryRowContext(context.Background(),
		`SELECT count(*) FROM events WHERE event = 'msg.ack'`).Scan(&count); err != nil {
		t.Fatalf("count msg.ack: %v", err)
	}
	if count != 1 {
		t.Fatalf("msg.ack events = %d, want 1", count)
	}
	if err := st.RO().QueryRowContext(context.Background(),
		`SELECT COALESCE(msg_id, '') FROM events WHERE event = 'msg.ack'`).Scan(&msgID); err != nil {
		t.Fatalf("read msg_id: %v", err)
	}
	if msgID != "" {
		t.Fatalf("orphan msg.ack carried msg_id=%q, want empty (message gone)", msgID)
	}
}

// TestSettleChunkedRowLoad exercises the ≤ settleRowChunk batched row load by settling
// more than a chunk of deliveries in one batch.
func TestSettleChunkedRowLoad(t *testing.T) {
	st, _, _ := openSettleStore(t)
	const n = 250 // > settleRowChunk = 200
	for i := 0; i < n; i++ {
		if _, err := st.Publish(context.Background(), PublishCmd{
			Stream: "orders", Req: queue.PublishReq{Subject: "orders.1", Body: []byte("x")},
		}); err != nil {
			t.Fatalf("publish %d: %v", i, err)
		}
	}
	cfg := queue.DefaultConsumerConfig("worker")
	if _, err := st.CreateConsumer(context.Background(), "orders", cfg, queue.StartPosition{Kind: queue.StartFirst}, "test"); err != nil {
		t.Fatalf("create consumer: %v", err)
	}
	res, err := st.Fetch(context.Background(), FetchReq{Stream: "orders", Consumer: "worker", Batch: n})
	if err != nil {
		t.Fatalf("fetch: %v", err)
	}
	items := make([]SettleItem, 0, len(res.Messages))
	for _, m := range res.Messages {
		tk, perr := queue.ParseToken(m.AckToken)
		if perr != nil {
			t.Fatalf("parse token: %v", perr)
		}
		items = append(items, SettleItem{Token: tk, Verb: queue.VerbAck})
	}
	sr, err := st.Settle(context.Background(), settleCmd(items...))
	if err != nil {
		t.Fatalf("settle chunked batch: %v", err)
	}
	if len(sr.Results) != n || sr.OK != n || sr.Failed != 0 {
		t.Fatalf("chunked batch ok=%d failed=%d results=%d, want %d/0/%d", sr.OK, sr.Failed, len(sr.Results), n, n)
	}
	if countDeliveryRows(t, st) != 0 {
		t.Fatal("chunked batch left delivery rows")
	}
	if ev := countEvent(t, st, "msg.ack"); ev != n {
		t.Fatalf("msg.ack events = %d, want %d", ev, n)
	}
}
