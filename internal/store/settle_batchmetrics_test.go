// SPDX-License-Identifier: Apache-2.0

package store

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/a-holm/messq/internal/queue"
)

// Slice 9: batch semantics (mixed outcomes across consumers, request-order results,
// one commit), exact metric counting even under event suppression, and the bounded
// rejection-audit repeat limiter (G9).

func TestSettleMixedBatchAcrossConsumers(t *testing.T) {
	st, _, _ := openSettleStore(t)
	// publish, create two consumers, claim everything.
	for i := 1; i <= 3; i++ {
		if _, err := st.Publish(context.Background(), PublishCmd{
			Stream: "orders", Req: queue.PublishReq{Subject: "orders.1", Body: []byte("x")},
		}); err != nil {
			t.Fatalf("publish %d: %v", i, err)
		}
	}
	all := make([]queue.Token, 0, 6)
	for _, name := range []string{"worker", "extra"} {
		cfg := queue.DefaultConsumerConfig(name)
		if _, err := st.CreateConsumer(context.Background(), "orders", cfg, queue.StartPosition{Kind: queue.StartFirst}, "test"); err != nil {
			t.Fatalf("create %s: %v", name, err)
		}
		res, err := st.Fetch(context.Background(), FetchReq{Stream: "orders", Consumer: name, Batch: 10})
		if err != nil {
			t.Fatalf("fetch %s: %v", name, err)
		}
		for _, m := range res.Messages {
			tok, err := queue.ParseToken(m.AckToken)
			if err != nil {
				t.Fatalf("parse: %v", err)
			}
			all = append(all, tok)
		}
	}
	// 6 claimed across 2 consumers; mix verbs so ack + term delete, extend keeps a row.
	var items []SettleItem
	for i, tok := range all {
		switch i % 3 {
		case 0:
			items = append(items, SettleItem{Token: tok, Verb: queue.VerbAck})
		case 1:
			items = append(items, SettleItem{Token: tok, Verb: queue.VerbTerm})
		default:
			items = append(items, SettleItem{Token: tok, Verb: queue.VerbExtend})
		}
	}
	res, err := st.Settle(context.Background(), settleCmd(items...))
	if err != nil {
		t.Fatalf("mixed batch: %v", err)
	}
	if len(res.Results) != len(items) {
		t.Fatalf("results = %d, want %d", len(res.Results), len(items))
	}
	for i, want := range items {
		if res.Results[i].Token != want.Token {
			t.Fatalf("result[%d] token mismatch (request order)", i)
		}
	}
	if res.OK != 6 || res.Failed != 0 {
		t.Fatalf("ok=%d failed=%d, want 6/0", res.OK, res.Failed)
	}
	// exactly the ok ones applied: 2 acks deletes + 2 terms dead + 2 extends leave 2
	// live rows (the extends) — the two acks and two terms removed 4.
	if n := countDeliveryRows(t, st); n != 2 {
		t.Fatalf("deliveries after mixed batch = %d, want 2", n)
	}
	if n := countEvent(t, st, "msg.ack"); n != 2 {
		t.Fatalf("msg.ack = %d, want 2", n)
	}
	if n := countEvent(t, st, "msg.term"); n != 2 {
		t.Fatalf("msg.term = %d, want 2", n)
	}
	if n := countEvent(t, st, "msg.extend"); n != 2 {
		t.Fatalf("msg.extend = %d, want 2", n)
	}
}

func TestSettleMetricsCountExactDespiteSuppression(t *testing.T) {
	st, fk, rec := openSettleStore(t)
	toks := seedSettle(t, st, 2, 2)
	// Drive seq 1 to attempt 2 so its attempt-1 token is stale, and consume seq 2.
	delayZero := time.Duration(0)
	if _, err := st.Settle(context.Background(), settleCmd(
		SettleItem{Token: toks[0], Verb: queue.VerbNak, Delay: &delayZero},
		SettleItem{Token: toks[1], Verb: queue.VerbAck},
	)); err != nil {
		t.Fatalf("setup: %v", err)
	}
	if _, err := st.Fetch(context.Background(), FetchReq{Stream: "orders", Consumer: "worker", Batch: 10}); err != nil {
		t.Fatalf("refetch: %v", err)
	}
	staleAck := func() {
		t.Helper()
		if _, err := st.Settle(context.Background(), settleCmd(SettleItem{Token: toks[0], Verb: queue.VerbAck})); err != nil {
			t.Fatalf("stale ack: %v", err)
		}
	}
	// 1. one stale ack emits the first bounded audit row (suppressed 0).
	staleAck()
	// 2. 25 more within the same interval: every metric counted, no rows.
	for i := 0; i < 25; i++ {
		staleAck()
	}
	// 3. advance past --event-repeat-interval, then emit: the next row carries suppressed=25.
	fk.Advance(st.eventRepeatInterval + 1*time.Millisecond)
	staleAck()

	_ = rec
	rows := suppressionDetail(t, st, "msg.ack_stale")
	if n := len(rows); n != 2 {
		t.Fatalf("msg.ack_stale rows = %d, want 2 (repeat-limited)", n)
	}
	hadSuppressed := false
	for _, s := range rows {
		if s == 25 {
			hadSuppressed = true
		}
	}
	if !hadSuppressed {
		t.Fatalf("a row carrying detail.suppressed=25 is missing: %v", rows)
	}
	_, _, _, _, _, staleAckCount, _ := rec.total()
	if staleAckCount != 27 {
		t.Fatalf("stale_ack metric = %d, want exactly 27 (every occurrence)", staleAckCount)
	}
}

// suppressionDetail returns the suppressed=N counts from rows of one rejection event.
func suppressionDetail(t *testing.T, st *Store, eventName string) []int {
	t.Helper()
	rows, err := st.RO().QueryContext(context.Background(),
		`SELECT detail FROM events WHERE event = ? AND consumer = 'worker'`, eventName)
	if err != nil {
		t.Fatalf("query suppression: %v", err)
	}
	defer func() {
		if cerr := rows.Close(); cerr != nil {
			t.Errorf("close suppression rows: %v", cerr)
		}
	}()
	var out []int
	for rows.Next() {
		var raw string
		if err := rows.Scan(&raw); err != nil {
			t.Fatalf("scan: %v", err)
		}
		out = append(out, detailSuppressed(raw))
	}
	if rErr := rows.Err(); rErr != nil {
		t.Fatalf("iterate suppression rows: %v", rErr)
	}
	return out
}

// detailSuppressed reads "suppressed" out of a settle-rejection detail JSON row.
func detailSuppressed(raw string) int {
	var d struct {
		Suppressed int `json:"suppressed"`
	}
	if err := json.Unmarshal([]byte(raw), &d); err != nil {
		return 0
	}
	return d.Suppressed
}
