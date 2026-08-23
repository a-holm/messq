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

// Slice 8: term + extend. Term goes straight to DEAD (cause=terminated) from both
// INFLIGHT and READY rows, skipping remaining attempts; extend pushes visible_at by one
// ack_wait with attempts untouched, is rejected on READY (not_inflight), and at the
// --max-ack-wait cap returns errs.ErrBadRequest with an unchanged deadline (Conflict A).

func TestSettleTermDead(t *testing.T) {
	st, _, rec := openSettleStore(t)
	toks := seedSettle(t, st, 1, 1)
	res, err := st.Settle(context.Background(), settleCmd(SettleItem{Token: toks[0], Verb: queue.VerbTerm, Reason: "unparseable payload"}))
	if err != nil {
		t.Fatalf("term: %v", err)
	}
	if !res.Results[0].Dead {
		t.Fatalf("term dead=false, want dead")
	}
	if got := countDeliveryRows(t, st); got != 0 {
		t.Fatalf("deliveries after term = %d, want 0", got)
	}
	if n := countEvent(t, st, "msg.term"); n != 1 {
		t.Fatalf("msg.term events = %d, want 1", n)
	}
	if n := countEvent(t, st, "msg.dead"); n != 1 {
		t.Fatalf("msg.dead events = %d, want 1", n)
	}
	_, _, _, termed, _, _, _ := rec.total()
	if termed != 1 {
		t.Fatalf("term metric = %d, want 1", termed)
	}
}

func TestTermWorksOnReadyRow(t *testing.T) {
	st, _, _ := openSettleStore(t)
	toks := seedSettle(t, st, 1, 1)
	// release to READY, then term — a permanent error is a property of the payload.
	delayZero := time.Duration(0)
	if _, err := st.Settle(context.Background(), settleCmd(SettleItem{Token: toks[0], Verb: queue.VerbNak, Delay: &delayZero})); err != nil {
		t.Fatalf("nak: %v", err)
	}
	res, err := st.Settle(context.Background(), settleCmd(SettleItem{Token: toks[0], Verb: queue.VerbTerm}))
	if err != nil {
		t.Fatalf("term on READY: %v", err)
	}
	if !res.Results[0].Dead {
		t.Fatalf("term on READY dead=false, want dead")
	}
	if got := countDeliveryRows(t, st); got != 0 {
		t.Fatalf("deliveries after READY term = %d, want 0", got)
	}
}

func TestExtendPushesDeadline(t *testing.T) {
	st, _, rec := openSettleStore(t)
	toks := seedSettle(t, st, 1, 1)
	before := visibleAtOf(t, st, toks[0].Seq)
	res, err := st.Settle(context.Background(), settleCmd(SettleItem{Token: toks[0], Verb: queue.VerbExtend}))
	if err != nil {
		t.Fatalf("extend: %v", err)
	}
	if res.Results[0].Status != queue.ItemStatusOK {
		t.Fatalf("extend = %s, want ok", res.Results[0].Status)
	}
	after := visibleAtOf(t, st, toks[0].Seq)
	want := before + int64(30*time.Second/time.Millisecond) // exactly one ack_wait
	if after != want {
		t.Fatalf("visible_at = %d, want %d (before + ack_wait)", after, want)
	}
	if got := attemptsFor(t, st, toks[0].Seq); got != 1 {
		t.Fatalf("attempts after extend = %d, want unchanged 1", got)
	}
	if n := countEvent(t, st, "msg.extend"); n != 1 {
		t.Fatalf("msg.extend events = %d, want 1", n)
	}
	_, _, _, _, extended, _, _ := rec.total()
	if extended != 1 {
		t.Fatalf("extend metric = %d, want 1", extended)
	}
}

func TestExtendOnReadyRejected(t *testing.T) {
	st, _, _ := openSettleStore(t)
	toks := seedSettle(t, st, 1, 1)
	delayZero := time.Duration(0)
	if _, err := st.Settle(context.Background(), settleCmd(SettleItem{Token: toks[0], Verb: queue.VerbNak, Delay: &delayZero})); err != nil {
		t.Fatalf("nak: %v", err)
	}
	res, err := st.Settle(context.Background(), settleCmd(SettleItem{Token: toks[0], Verb: queue.VerbExtend}))
	if err != nil {
		t.Fatalf("extend on READY: %v", err)
	}
	if res.Results[0].Status != queue.ItemStatusStaleAck {
		t.Fatalf("extend on READY = %s, want stale_ack (cannot extend a lease you no longer hold)", res.Results[0].Status)
	}
}

func TestExtendAtCapRejected(t *testing.T) {
	st, _, _ := openSettleStore(t)
	st.consumerLimits.MaxAckWait = 150 * time.Millisecond
	toks := settleWithAckWait(t, st, 100*time.Millisecond)
	before := visibleAtOf(t, st, toks[0].Seq)
	res, err := st.Settle(context.Background(), settleCmd(SettleItem{Token: toks[0], Verb: queue.VerbExtend}))
	if err != nil {
		t.Fatalf("extend: %v", err)
	}
	r := res.Results[0]
	// claim -> visible now+100ms; extend would set now+200ms, beyond now+150ms cap.
	if r.Err == nil || !errors.Is(r.Err, errs.ErrBadRequest) {
		t.Fatalf("extend-at-cap Err = %v, want ErrBadRequest (Conflict A → spec wins)", r.Err)
	}
	if after := visibleAtOf(t, st, toks[0].Seq); after != before {
		t.Fatalf("extend-at-cap changed the deadline: %d -> %d; it must stay unchanged and time out", before, after)
	}
}

// settleWithAckWait opens a store with a window tweak and claims with the given ack_wait.
func settleWithAckWait(t *testing.T, st *Store, wait time.Duration) []queue.Token {
	t.Helper()
	if _, err := st.Publish(context.Background(), PublishCmd{
		Stream: "orders", Req: queue.PublishReq{Subject: "orders.1", Body: []byte("x")},
	}); err != nil {
		t.Fatalf("publish: %v", err)
	}
	cfg := queue.DefaultConsumerConfig("worker")
	cfg.AckWait = wait
	if _, err := st.CreateConsumer(context.Background(), "orders", cfg, queue.StartPosition{Kind: queue.StartFirst}, "test"); err != nil {
		t.Fatalf("create consumer: %v", err)
	}
	res, err := st.Fetch(context.Background(), FetchReq{Stream: "orders", Consumer: "worker", Batch: 5})
	if err != nil {
		t.Fatalf("fetch: %v", err)
	}
	var toks []queue.Token
	for _, m := range res.Messages {
		tok, err := queue.ParseToken(m.AckToken)
		if err != nil {
			t.Fatalf("parse: %v", err)
		}
		toks = append(toks, tok)
	}
	return toks
}
