// SPDX-License-Identifier: Apache-2.0

package store

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/a-holm/messq/internal/errs"
	"github.com/a-holm/messq/internal/queue"
)

// Slice 5: the SettleCmd skeleton — grouping, in-batch dedupe, per-token request-order
// results, the --max-settle-batch whole-batch rejection, and the I7 no-mutation-on-
// rejection rule across deliveries and consumers.

func TestSettleResultsRequestOrder(t *testing.T) {
	st, _, _ := openSettleStore(t)
	// Two consumers, a couple of messages each, all claimed.
	for i := 1; i <= 2; i++ {
		if _, err := st.Publish(context.Background(), PublishCmd{
			Stream: "orders", Req: queue.PublishReq{Subject: "orders.1", Body: []byte("x")},
		}); err != nil {
			t.Fatalf("publish %d: %v", i, err)
		}
	}
	for _, name := range []string{"worker", "extra"} {
		cfg := queue.DefaultConsumerConfig(name)
		if _, err := st.CreateConsumer(context.Background(), "orders", cfg, queue.StartPosition{Kind: queue.StartFirst}, "test"); err != nil {
			t.Fatalf("create consumer %s: %v", name, err)
		}
		if _, err := st.Fetch(context.Background(), FetchReq{Stream: "orders", Consumer: name, Batch: 10}); err != nil {
			t.Fatalf("fetch %s: %v", name, err)
		}
	}
	// A batch deliberately out of consumer order and mixing verbs.
	items := []SettleItem{
		{Token: qtok("orders", "extra", 1, 1, 1), Verb: queue.VerbAck},
		{Token: qtok("orders", "worker", 2, 1, 1), Verb: queue.VerbAck},
		{Token: qtok("orders", "worker", 1, 1, 1), Verb: queue.VerbAck},
		{Token: qtok("orders", "extra", 2, 1, 1), Verb: queue.VerbAck},
	}
	res, err := st.Settle(context.Background(), settleCmd(items...))
	if err != nil {
		t.Fatalf("Settle: %v", err)
	}
	if len(res.Results) != 4 {
		t.Fatalf("results = %d, want 4", len(res.Results))
	}
	for i, want := range items {
		got := res.Results[i].Token
		if got != want.Token {
			t.Fatalf("result[%d] token = %+v, want %+v — results must be in request order", i, got, want.Token)
		}
	}
	if res.OK != 4 || res.Failed != 0 {
		t.Fatalf("ok=%d failed=%d, want 4/0", res.OK, res.Failed)
	}
}

func TestSettleDedupeExactRepeats(t *testing.T) {
	st, _, _ := openSettleStore(t)
	toks := seedSettle(t, st, 1, 1)
	items := []SettleItem{
		{Token: toks[0], Verb: queue.VerbAck},
		{Token: toks[0], Verb: queue.VerbAck},
	}
	res, err := st.Settle(context.Background(), settleCmd(items...))
	if err != nil {
		t.Fatalf("Settle: %v", err)
	}
	if res.Results[0].Status != queue.ItemStatusOK {
		t.Fatalf("first ack = %s, want ok", res.Results[0].Status)
	}
	if res.Results[1].Status != queue.ItemStatusStale {
		t.Fatalf("second ack = %s, want stale (dedup)", res.Results[1].Status)
	}
	if n := countEvent(t, st, "msg.ack"); n != 1 {
		t.Fatalf("msg.ack events = %d, want exactly 1", n)
	}
}

func TestSettleOverLimitRejectedWhole(t *testing.T) {
	st, _, _ := openSettleStore(t)
	st.maxSettleBatch = 2
	items := []SettleItem{
		{Token: qtok("s", "c", 1, 1, 1), Verb: queue.VerbAck},
		{Token: qtok("s", "c", 2, 1, 1), Verb: queue.VerbAck},
		{Token: qtok("s", "c", 3, 1, 1), Verb: queue.VerbAck},
	}
	_, err := st.Settle(context.Background(), settleCmd(items...))
	if err == nil || !errors.Is(err, errs.ErrBadRequest) {
		t.Fatalf("over-limit batch returned %v, want ErrBadRequest", err)
	}
}

// TestSettleRejectionMutatesNothing is the I7 core: a stale_ack / wrong_generation /
// unknown settle leaves deliveries and consumers byte-identical.
func TestSettleRejectionMutatesNothing(t *testing.T) {
	st, _, _ := openSettleStore(t)
	toks := seedSettle(t, st, 1, 1)

	// Build the canonical stale-ack state: nak at delay 0 then re-claim -> attempt 2
	// INFLIGHT, while the token still names attempt 1.
	delayZero := time.Duration(0)
	if _, err := st.Settle(context.Background(), settleCmd(SettleItem{Token: toks[0], Verb: queue.VerbNak, Delay: &delayZero})); err != nil {
		t.Fatalf("nak: %v", err)
	}
	if _, err := st.Fetch(context.Background(), FetchReq{Stream: "orders", Consumer: "worker", Batch: 10}); err != nil {
		t.Fatalf("re-fetch: %v", err)
	}

	// Now snapshot: the rejection below must leave deliveries and consumers identical.
	before := deliveriesSnapshot(t, st)
	consumersBefore := consumersSnapshot(t, st)
	res, err := st.Settle(context.Background(), settleCmd(SettleItem{Token: toks[0], Verb: queue.VerbAck}))
	if err != nil {
		t.Fatalf("settle: %v", err)
	}
	if res.Results[0].Status != queue.ItemStatusStaleAck {
		t.Fatalf("stale settle = %s, want stale_ack", res.Results[0].Status)
	}
	if before != deliveriesSnapshot(t, st) {
		t.Fatalf("I7: a rejected settle mutated deliveries: before=%s after=%s", before, deliveriesSnapshot(t, st))
	}
	if consumersBefore != consumersSnapshot(t, st) {
		t.Fatalf("I7: a rejected settle mutated consumers")
	}
}
