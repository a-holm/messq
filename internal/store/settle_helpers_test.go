// SPDX-License-Identifier: Apache-2.0

package store

import (
	"context"
	"fmt"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/a-holm/messq/internal/clock"
	"github.com/a-holm/messq/internal/queue"
)

// Shared settle-test harness: a store with deterministic jitter, a recording metrics
// twin, and read helpers for the deliveries/events tables.

// recordMetrics is the recording SettleMetrics twin for settle tests.
type recordMetrics struct {
	mu       sync.Mutex
	acked    int
	lateAck  int
	ackedDur time.Duration
	naked    int
	termed   int
	extended int
	staleAck int
	noop     int
}

func (r *recordMetrics) Acked() { r.inc(&r.acked) }
func (r *recordMetrics) ackLatency(d time.Duration) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.ackedDur += d
}
func (r *recordMetrics) AckLatency(d time.Duration) { r.ackLatency(d) }
func (r *recordMetrics) LateAck()                   { r.inc(&r.lateAck) }
func (r *recordMetrics) Naked()                     { r.inc(&r.naked) }
func (r *recordMetrics) Termed()                    { r.inc(&r.termed) }
func (r *recordMetrics) Extended()                  { r.inc(&r.extended) }
func (r *recordMetrics) StaleAck()                  { r.inc(&r.staleAck) }
func (r *recordMetrics) Noop()                      { r.inc(&r.noop) }

func (r *recordMetrics) inc(p *int) {
	r.mu.Lock()
	defer r.mu.Unlock()
	*p++
}

func (r *recordMetrics) total() (acked, lateAck, naked, termed, extended, staleAck, noop int) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.acked, r.lateAck, r.naked, r.termed, r.extended, r.staleAck, r.noop
}

// openSettleStore opens an engine-less store with a deterministic identity jitter so
// exact visible_at arithmetic is possible, and a recording metrics twin installed.
func openSettleStore(t testing.TB) (*Store, *clock.Fake, *recordMetrics) {
	t.Helper()
	opt := testOptions(filepath.Join(t.TempDir(), "data"), fakeClock(), &logCapture{})
	opt.Jitter = func(d time.Duration) time.Duration { return d } // deterministic
	st, _, err := Open(context.Background(), opt)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() {
		if closeErr := st.Close(context.Background()); closeErr != nil {
			t.Errorf("close store: %v", closeErr)
		}
	})
	rec := &recordMetrics{}
	st.settleMetrics = rec
	if _, _, err := st.CreateStream(context.Background(), queue.DefaultConfig("orders"), "test"); err != nil {
		t.Fatalf("create stream orders: %v", err)
	}
	fk, ok := st.clk.(*clock.Fake)
	if !ok {
		t.Fatal("store clock is not *clock.Fake")
	}
	return st, fk, rec
}

// qtok mints a token with keyed fields (this package may not use unkeyed literals of
// the queue type).
func qtok(s, c string, seq int64, att, gen int32) queue.Token {
	return queue.Token{Stream: s, Consumer: c, Seq: seq, Attempt: att, Generation: gen}
}

// seedSettle publishes n messages, creates consumer "worker", and claims batch messages,
// returning the parsed ack tokens of the claimed deliveries.
func seedSettle(t testing.TB, st *Store, n, batch int) []queue.Token {
	t.Helper()
	for i := 1; i <= n; i++ {
		if _, err := st.Publish(context.Background(), PublishCmd{
			Stream: "orders", Req: queue.PublishReq{Subject: "orders.1", Body: []byte("x")},
		}); err != nil {
			t.Fatalf("publish %d: %v", i, err)
		}
	}
	cfg := queue.DefaultConsumerConfig("worker")
	cfg.Filters = []string{">"}
	if _, err := st.CreateConsumer(context.Background(), "orders", cfg, queue.StartPosition{Kind: queue.StartFirst}, "test"); err != nil {
		t.Fatalf("create consumer: %v", err)
	}
	res, err := st.Fetch(context.Background(), FetchReq{Stream: "orders", Consumer: "worker", Batch: batch})
	if err != nil {
		t.Fatalf("fetch: %v", err)
	}
	toks := make([]queue.Token, 0, len(res.Messages))
	for _, m := range res.Messages {
		tok, err := queue.ParseToken(m.AckToken)
		if err != nil {
			t.Fatalf("parse minted token %q: %v", m.AckToken, err)
		}
		toks = append(toks, tok)
	}
	return toks
}

// settleCmd builds a SettleCmd for assertions; the store's Settle fills the seams.
func settleCmd(items ...SettleItem) SettleCmd {
	return SettleCmd{Items: items}
}

// countDeliveryRows counts live delivery rows for the "orders/worker" lease.
func countDeliveryRows(t *testing.T, st *Store) int64 {
	t.Helper()
	var n int64
	if err := st.RO().QueryRowContext(context.Background(),
		`SELECT count(*) FROM deliveries`).Scan(&n); err != nil {
		t.Fatalf("count deliveries: %v", err)
	}
	return n
}

// attemptsFor reads the attempts column of one delivery row.
func attemptsFor(t *testing.T, st *Store, seq int64) int32 {
	t.Helper()
	var a int32
	if err := st.RO().QueryRowContext(context.Background(),
		`SELECT attempts FROM deliveries WHERE seq = ?`, seq).Scan(&a); err != nil {
		t.Fatalf("read attempts of seq %d: %v", seq, err)
	}
	return a
}

// visibleAtOf reads the visible_at (unix ms) of one delivery row.
func visibleAtOf(t *testing.T, st *Store, seq int64) int64 {
	t.Helper()
	var v int64
	if err := st.RO().QueryRowContext(context.Background(),
		`SELECT visible_at FROM deliveries WHERE seq = ?`, seq).Scan(&v); err != nil {
		t.Fatalf("read visible_at of seq %d: %v", seq, err)
	}
	return v
}

// deliveriesSnapshot returns a canonical rendering of the deliveries table for the I7
// before/after diff.
func deliveriesSnapshot(t *testing.T, st *Store) string {
	t.Helper()
	rows, err := st.RO().QueryContext(context.Background(),
		`SELECT stream, consumer, seq, state, attempts, visible_at, generation, coalesce(delivered_at,0), coalesce(last_reason,'') FROM deliveries ORDER BY stream, consumer, seq`)
	if err != nil {
		t.Fatalf("snapshot deliveries: %v", err)
	}
	defer func() {
		if cerr := rows.Close(); cerr != nil {
			t.Errorf("close deliveries snapshot: %v", cerr)
		}
	}()
	var out string
	for rows.Next() {
		var s, c, reason string
		var seq, state, attempts, vis, gen, deliv int64
		if err := rows.Scan(&s, &c, &seq, &state, &attempts, &vis, &gen, &deliv, &reason); err != nil {
			t.Fatalf("scan snapshot: %v", err)
		}
		out += fmt.Sprintf("%s/%s/%d/%d/%d/%d/%d/%d/%q;", s, c, seq, state, attempts, vis, gen, deliv, reason)
	}
	if rErr := rows.Err(); rErr != nil {
		t.Fatalf("iterate deliveries snapshot: %v", rErr)
	}
	return out
}

// consumersSnapshot returns a canonical rendering of the consumers table, limited to
// the fence columns, for the I7 before/after diff.
func consumersSnapshot(t *testing.T, st *Store) string {
	t.Helper()
	rows, err := st.RO().QueryContext(context.Background(),
		`SELECT stream, name, cursor_seq, generation, ack_wait_ms, max_deliver, backoff_ms, paused FROM consumers ORDER BY stream, name`)
	if err != nil {
		t.Fatalf("snapshot consumers: %v", err)
	}
	defer func() {
		if cerr := rows.Close(); cerr != nil {
			t.Errorf("close consumers snapshot: %v", cerr)
		}
	}()
	var out string
	for rows.Next() {
		var s, n, backoff string
		var cur, gen, ack, md, paused int64
		if err := rows.Scan(&s, &n, &cur, &gen, &ack, &md, &backoff, &paused); err != nil {
			t.Fatalf("scan consumers snapshot: %v", err)
		}
		out += fmt.Sprintf("%s/%s/%d/%d/%d/%d/%s/%d;", s, n, cur, gen, ack, md, backoff, paused)
	}
	if rErr := rows.Err(); rErr != nil {
		t.Fatalf("iterate consumers snapshot: %v", rErr)
	}
	return out
}
