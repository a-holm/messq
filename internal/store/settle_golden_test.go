// SPDX-License-Identifier: Apache-2.0

package store

import (
	"context"
	"testing"
	"time"

	"github.com/a-holm/messq/internal/errs"
	"github.com/a-holm/messq/internal/queue"
)

// Slice 10: verify integration. This hosts the flagship settle event fixture that #20
// renders as the byte-for-byte `messq trace` golden, the settle-sentinel
// exhaustiveness check over errs.All(), and the seedable I7-verify regression.

// TestSettleEventGolden is the flagship event sequence (issue #10 §Test plan §Golden):
// publish → deliver(1) → [naK - a direct row release stubs the #11 timeout] → deliver(2)
// → ack. Asserting the exact sequence here gives #20 a stable input.
func TestSettleEventGolden(t *testing.T) {
	st, _, _ := openSettleStore(t)
	if _, e := st.Publish(context.Background(), PublishCmd{
		Stream: "orders", Req: queue.PublishReq{Subject: "orders.1", Body: []byte("golden")},
	}); e != nil {
		t.Fatalf("publish: %v", e)
	}
	cfg := queue.DefaultConsumerConfig("worker")
	if _, e := st.CreateConsumer(context.Background(), "orders", cfg, queue.StartPosition{Kind: queue.StartFirst}, "test"); e != nil {
		t.Fatalf("create consumer: %v", e)
	}

	// deliver(1): claim the single message.
	f1, e := st.Fetch(context.Background(), FetchReq{Stream: "orders", Consumer: "worker", Batch: 1})
	if e != nil {
		t.Fatalf("fetch: %v", e)
	}
	t1, e := queue.ParseToken(f1.Messages[0].AckToken)
	if e != nil {
		t.Fatalf("parse token: %v", e)
	}
	// the worker waits too long: release seq 1 back to READY (a direct row release
	// stands in for the #11 sweeper's timeout).
	delayZero := time.Duration(0)
	if _, rErr := st.Settle(context.Background(), settleCmd(SettleItem{Token: t1, Verb: queue.VerbNak, Delay: &delayZero})); rErr != nil {
		t.Fatalf("release: %v", rErr)
	}
	// deliver(2): re-claim seq 1 at attempt 2, then ack it.
	f2, e := st.Fetch(context.Background(), FetchReq{Stream: "orders", Consumer: "worker", Batch: 2})
	if e != nil {
		t.Fatalf("refetch: %v", e)
	}
	var second *queue.Token
	for _, m := range f2.Messages {
		tok, e := queue.ParseToken(m.AckToken)
		if e != nil {
			t.Fatalf("parse token: %v", e)
		}
		if tok.Seq == 1 {
			second = &tok
			break
		}
	}
	if second == nil {
		t.Fatal("second claim of seq 1 missing")
	}
	if _, aErr := st.Settle(context.Background(), settleCmd(SettleItem{Token: *second, Verb: queue.VerbAck})); aErr != nil {
		t.Fatalf("ack: %v", aErr)
	}

	// The flagship audit trail, in order: deliver(1) → nak(timeout stub) → deliver(2)
	// → ack. Asserting the exact sequence gives #20 a stable input.
	got := eventSequence(t, st)
	want := []string{"msg.deliver", "msg.nak", "msg.deliver", "msg.ack"}
	if len(got) != len(want) {
		t.Fatalf("event sequence = %v, want %v", got, want)
	}
	for i := range got {
		if got[i] != want[i] {
			t.Fatalf("event[%d] = %q, want %q (whole sequence %v)", i, got[i], want[i], want)
		}
	}
}

// eventSequence reads every events-table row for the orders/worker consumer in commit
// order, returning just the event names.
func eventSequence(t *testing.T, st *Store) []string {
	t.Helper()
	rows, err := st.RO().QueryContext(context.Background(),
		`SELECT event FROM events WHERE stream = 'orders' AND consumer = 'worker' AND event LIKE 'msg.%' ORDER BY id`)
	if err != nil {
		t.Fatalf("read events: %v", err)
	}
	defer func() {
		if cerr := rows.Close(); cerr != nil {
			t.Errorf("close events: %v", cerr)
		}
	}()
	var out []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			t.Fatalf("scan event: %v", err)
		}
		out = append(out, name)
	}
	if rErr := rows.Err(); rErr != nil {
		t.Fatalf("iterate events: %v", rErr)
	}
	return out
}

func TestSettleSentinelExhaustiveness(t *testing.T) {
	// Every settle outcome maps onto a distinct sentinel from errs.All() (#3's rule:
	// a closed set, every deviation is one of these).
	all := errs.All()
	table := map[queue.ItemStatus]error{
		queue.ItemStatusOK:       nil, // ok is not a rejection
		queue.ItemStatusStale:    nil,
		queue.ItemStatusStaleAck: errs.ErrStaleAck,
		queue.ItemStatusWrongGen: errs.ErrWrongGen,
		queue.ItemStatusUnknown:  errs.ErrNotFound,
	}
	seen := map[error]bool{}
	for _, err := range all {
		seen[err] = true
	}
	for status, sent := range table {
		if sent == nil {
			continue
		}
		if !seen[sent] {
			t.Fatalf("outcome %q uses sentinel %v, which is not in errs.All()", status, sent)
		}
	}
}
