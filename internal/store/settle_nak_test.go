// SPDX-License-Identifier: Apache-2.0

package store

import (
	"context"
	"testing"
	"time"

	"github.com/a-holm/messq/internal/queue"
)

// Slice 7: the nak path (T4/T5). Release to READY at now + backoff[attempts-1]
// (identity seam — ±20% jitter is the planner's own), last_reason stored, attempts
// unchanged (D6: increment only at claim); exhaustion at attempts == max_deliver goes
// to the dead path with cause=max_deliver; a repeated nak never moves visible_at later.

// seedSettleWith creates consumer "worker" with a customised config before claiming.
func seedSettleWith(t *testing.T, st *Store, mutate func(*queue.ConsumerConfig)) []queue.Token {
	t.Helper()
	if _, err := st.Publish(context.Background(), PublishCmd{
		Stream: "orders", Req: queue.PublishReq{Subject: "orders.1", Body: []byte("x")},
	}); err != nil {
		t.Fatalf("publish: %v", err)
	}
	cfg := queue.DefaultConsumerConfig("worker")
	cfg.Filters = []string{">"}
	mutate(&cfg)
	if _, err := st.CreateConsumer(context.Background(), "orders", cfg, queue.StartPosition{Kind: queue.StartFirst}, "test"); err != nil {
		t.Fatalf("create consumer: %v", err)
	}
	res, err := st.Fetch(context.Background(), FetchReq{Stream: "orders", Consumer: "worker", Batch: 5})
	if err != nil {
		t.Fatalf("fetch: %v", err)
	}
	toks := make([]queue.Token, 0, len(res.Messages))
	for _, m := range res.Messages {
		tok, err := queue.ParseToken(m.AckToken)
		if err != nil {
			t.Fatalf("parse token: %v", err)
		}
		toks = append(toks, tok)
	}
	return toks
}

func TestSettleNakReleasesToReady(t *testing.T) {
	st, fk, _ := openSettleStore(t)
	toks := seedSettleWith(t, st, func(*queue.ConsumerConfig) {})
	nowMS := fk.Now().UnixMilli()
	res, err := st.Settle(context.Background(), settleCmd(SettleItem{
		Token: toks[0], Verb: queue.VerbNak, Reason: "upstream 503",
	}))
	if err != nil {
		t.Fatalf("nak: %v", err)
	}
	r := res.Results[0]
	if r.Status != queue.ItemStatusOK || r.Dead {
		t.Fatalf("nak = %+v, want ok not-dead", r)
	}
	want := nowMS + int64(time.Second/time.Millisecond) // backoff[0] = 1s, identity jitter
	if got := visibleAtOf(t, st, toks[0].Seq); got != want {
		t.Fatalf("visible_at = %d, want %d (now + backoff[0])", got, want)
	}
	if got := attemptsFor(t, st, toks[0].Seq); got != 1 {
		t.Fatalf("attempts after nak = %d, want 1 (D6: only claim increments)", got)
	}
	var reason string
	if err := st.RO().QueryRowContext(context.Background(),
		`SELECT last_reason FROM deliveries WHERE seq = ?`, toks[0].Seq).Scan(&reason); err != nil {
		t.Fatalf("read last_reason: %v", err)
	}
	if reason != "upstream 503" {
		t.Fatalf("last_reason = %q, want %q", reason, "upstream 503")
	}
	if n := countEvent(t, st, "msg.nak"); n != 1 {
		t.Fatalf("msg.nak events = %d, want 1", n)
	}
}

func TestSettleNakExhaustionDead(t *testing.T) {
	st, _, _ := openSettleStore(t)
	toks := seedSettleWith(t, st, func(c *queue.ConsumerConfig) { c.MaxDeliver = 2 })
	delayZero := time.Duration(0)
	// attempt 1 -> nak -> READY
	if res, err := st.Settle(context.Background(), settleCmd(SettleItem{Token: toks[0], Verb: queue.VerbNak, Delay: &delayZero})); err != nil {
		t.Fatalf("nak#1: %v", err)
	} else if res.Results[0].Dead {
		t.Fatalf("nak#1 dead=true, want not (attempts 1 < max_deliver 2)")
	}
	// re-claim -> attempt 2 == max_deliver INFLIGHT; take the NEW attempt-2 token.
	re := mustFetch(t, st)
	if len(re) != 1 || re[0].Attempt != 2 {
		t.Fatalf("re-claim tokens = %+v, want one at attempt 2", re)
	}
	// nak at attempt 2 == max_deliver → DEAD cause=max_deliver.
	res, err := st.Settle(context.Background(), settleCmd(SettleItem{Token: re[0].Tok, Verb: queue.VerbNak}))
	if err != nil {
		t.Fatalf("nak#2: %v", err)
	}
	if !res.Results[0].Dead {
		t.Fatalf("nak at max_deliver dead=false, want dead")
	}
	if got := countDeliveryRows(t, st); got != 0 {
		t.Fatalf("deliveries after dead = %d, want 0", got)
	}
	if n := countEvent(t, st, "msg.nak"); n != 2 {
		t.Fatalf("msg.nak events = %d, want 2 (one per nak)", n)
	}
	if n := countEvent(t, st, "msg.dead"); n != 1 {
		t.Fatalf("msg.dead events = %d, want 1", n)
	}
}

// mustFetch re-fetches orders/worker and returns the claimed tokens' attempts.
func mustFetch(t *testing.T, st *Store) []struct {
	Attempt int32
	Tok     queue.Token
} {
	t.Helper()
	res, err := st.Fetch(context.Background(), FetchReq{Stream: "orders", Consumer: "worker", Batch: 5})
	if err != nil {
		t.Fatalf("fetch: %v", err)
	}
	out := make([]struct {
		Attempt int32
		Tok     queue.Token
	}, 0, len(res.Messages))
	for _, m := range res.Messages {
		tok, err := queue.ParseToken(m.AckToken)
		if err != nil {
			t.Fatalf("parse: %v", err)
		}
		out = append(out, struct {
			Attempt int32
			Tok     queue.Token
		}{m.Attempt, tok})
	}
	return out
}

func TestSettleNakRepeatNeverLater(t *testing.T) {
	st, fk, _ := openSettleStore(t)
	toks := seedSettle(t, st, 1, 1)
	if _, err := st.Settle(context.Background(), settleCmd(SettleItem{Token: toks[0], Verb: queue.VerbNak})); err != nil {
		t.Fatalf("nak#1: %v", err)
	}
	first := visibleAtOf(t, st, toks[0].Seq)
	// advance past the first backoff so the recomputed delay would be smaller; then the
	// second nak must keep visible_at = min(current, computed) — never later.
	fk.Advance(2 * time.Second)
	if _, err := st.Settle(context.Background(), settleCmd(SettleItem{Token: toks[0], Verb: queue.VerbNak})); err != nil {
		t.Fatalf("nak#2: %v", err)
	}
	if got := visibleAtOf(t, st, toks[0].Seq); got > first {
		t.Fatalf("repeated nak moved visible_at later: %d > %d; min() violated", got, first)
	}
	if n := countEvent(t, st, "msg.nak"); n != 1 {
		t.Fatalf("msg.nak events = %d, want 1 (a repeat that did not move emits no event)", n)
	}
}
