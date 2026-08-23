// SPDX-License-Identifier: Apache-2.0

package store

import (
	"context"
	"testing"
	"time"

	"github.com/a-holm/messq/internal/queue"
)

// Slice 6: the ack path (T3 / T3a / T3b) — ack deletes the row and co-commits msg.ack;
// double ack is ok→stale with one msg.ack; a late ack against a READY row is accepted
// late=true; a stale ack against an INFLIGHT next attempt is 409 stale_ack.

func TestSettleAckDeletesRowAndEvent(t *testing.T) {
	st, _, rec := openSettleStore(t)
	toks := seedSettle(t, st, 1, 1)
	res, err := st.Settle(context.Background(), settleCmd(SettleItem{Token: toks[0], Verb: queue.VerbAck}))
	if err != nil {
		t.Fatalf("ack: %v", err)
	}
	if res.Results[0].Status != queue.ItemStatusOK || res.Results[0].Late {
		t.Fatalf("ack = %+v, want ok not-late", res.Results[0])
	}
	if got := countDeliveryRows(t, st); got != 0 {
		t.Fatalf("deliveries rows after ack = %d, want 0 (terminal = absence, D5)", got)
	}
	if n := countEvent(t, st, "msg.ack"); n != 1 {
		t.Fatalf("msg.ack events = %d, want 1", n)
	}
	acked, _, _, _, _, _, _ := rec.total()
	if acked != 1 {
		t.Fatalf("Acked counter = %d, want 1", acked)
	}
}

func TestSettleDoubleAck(t *testing.T) {
	st, _, _ := openSettleStore(t)
	toks := seedSettle(t, st, 1, 1)
	cmd := settleCmd(
		SettleItem{Token: toks[0], Verb: queue.VerbAck},
		SettleItem{Token: toks[0], Verb: queue.VerbAck},
	)
	res, err := st.Settle(context.Background(), cmd)
	if err != nil {
		t.Fatalf("settle: %v", err)
	}
	if res.Results[0].Status != queue.ItemStatusOK {
		t.Fatalf("first ack = %s, want ok", res.Results[0].Status)
	}
	if res.Results[1].Status != queue.ItemStatusStale {
		t.Fatalf("second (same token) ack = %s, want stale", res.Results[1].Status)
	}
	if n := countEvent(t, st, "msg.ack"); n != 1 {
		t.Fatalf("msg.ack events = %d, want exactly 1", n)
	}
}

func TestSettleStaleAckInflight(t *testing.T) {
	st, _, rec := openSettleStore(t)
	toks := seedSettle(t, st, 2, 2)
	// seq 1 at attempt 1. nak->ready, re-claim to attempt 2.
	delayZero := time.Duration(0)
	if _, err := st.Settle(context.Background(), settleCmd(SettleItem{Token: toks[0], Verb: queue.VerbNak, Delay: &delayZero})); err != nil {
		t.Fatalf("nak: %v", err)
	}
	if _, err := st.Fetch(context.Background(), FetchReq{Stream: "orders", Consumer: "worker", Batch: 10}); err != nil {
		t.Fatalf("re-fetch: %v", err)
	}
	// ack the stale attempt-1 token
	res, err := st.Settle(context.Background(), settleCmd(SettleItem{Token: toks[0], Verb: queue.VerbAck}))
	if err != nil {
		t.Fatalf("ack: %v", err)
	}
	r := res.Results[0]
	if r.Status != queue.ItemStatusStaleAck || r.CurrentAttempt != 2 {
		t.Fatalf("stale ack = status %s current=%d, want stale_ack/2", r.Status, r.CurrentAttempt)
	}
	// the row is untouched and still INFLIGHT at attempt 2.
	if got := attemptsFor(t, st, toks[0].Seq); got != 2 {
		t.Fatalf("delivery attempts after stale ack = %d, want 2", got)
	}
	if n := countEvent(t, st, "msg.ack_stale"); n != 1 {
		t.Fatalf("msg.ack_stale events = %d, want 1", n)
	}
	_, lateAck, _, _, _, staleAck, _ := rec.total()
	if staleAck != 1 || lateAck != 0 {
		t.Fatalf("metrics staleAck=%d lateAck=%d, want 1/0", staleAck, lateAck)
	}
}

func TestSettleLateAckReadyAccepted(t *testing.T) {
	st, _, rec := openSettleStore(t)
	toks := seedSettle(t, st, 1, 1)
	// release attempt 1 back to READY (worker's lease expired, not yet re-claimed).
	delayZero := time.Duration(0)
	if _, err := st.Settle(context.Background(), settleCmd(SettleItem{Token: toks[0], Verb: queue.VerbNak, Delay: &delayZero})); err != nil {
		t.Fatalf("nak: %v", err)
	}
	res, err := st.Settle(context.Background(), settleCmd(SettleItem{Token: toks[0], Verb: queue.VerbAck}))
	if err != nil {
		t.Fatalf("late ack: %v", err)
	}
	if res.Results[0].Status != queue.ItemStatusOK || !res.Results[0].Late {
		t.Fatalf("late ack = %+v, want ok late=true", res.Results[0])
	}
	if got := countDeliveryRows(t, st); got != 0 {
		t.Fatalf("deliveries after accepted late ack = %d, want 0 (a duplicate was avoided)", got)
	}
	acked, lateAck, _, _, _, _, _ := rec.total()
	if acked != 1 || lateAck != 1 {
		t.Fatalf("metrics acked=%d late=%d, want 1/1", acked, lateAck)
	}
}

func TestSettleAckUnknownAbsentConsumer(t *testing.T) {
	st, _, _ := openSettleStore(t)
	// consumer "ghost" was never created → the token resolves to unknown, not stale.
	res, err := st.Settle(context.Background(), settleCmd(SettleItem{Token: qtok("orders", "ghost", 1, 1, 1), Verb: queue.VerbAck}))
	if err != nil {
		t.Fatalf("settle: %v", err)
	}
	if res.Results[0].Status != queue.ItemStatusUnknown {
		t.Fatalf("absent consumer ack = %s, want unknown (Note A)", res.Results[0].Status)
	}
}
