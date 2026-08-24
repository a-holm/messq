// SPDX-License-Identifier: Apache-2.0

package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"path/filepath"
	"testing"
	"time"

	"github.com/a-holm/messq/internal/clock"
	"github.com/a-holm/messq/internal/queue"
)

// The store DLQ sink tests (issue #12, G1–G9): the public-API observable half of the
// dead path. A max_deliver exhaustion or a term on a dlq-policy consumer auto-creates
// <stream>.dlq and copies the payload with provenance in the SAME transaction the
// delivery row dies in; drop deletes without a copy; the budget defers; origin-missing
// retires the row anyway.

// dlqRow is the projection of one DLQ copy a test asserts on.
type dlqRow struct {
	seq         int64
	id          string
	subject     string
	hdr         string
	size        int64
	publishedAt int64
	traceID     string
	dedupKey    *string
}

// readDLQRows loads every row of <stream>.dlq in seq order.
func readDLQRows(t *testing.T, st *Store, dlq string) []dlqRow {
	t.Helper()
	rows, err := st.RO().QueryContext(context.Background(),
		`SELECT seq, id, subject, coalesce(hdr,''), size, published_at, trace_id, dedup_key FROM messages WHERE stream = ? ORDER BY seq`, dlq)
	if err != nil {
		t.Fatalf("query dlq %q: %v", dlq, err)
	}
	defer func() {
		if cerr := rows.Close(); cerr != nil {
			t.Errorf("close dlq rows: %v", cerr)
		}
	}()
	var out []dlqRow
	for rows.Next() {
		var r dlqRow
		var dedup sql.Null[string] // nullable dedup_key
		if err := rows.Scan(&r.seq, &r.id, &r.subject, &r.hdr, &r.size, &r.publishedAt, &r.traceID, &dedup); err != nil {
			t.Fatalf("scan dlq row: %v", err)
		}
		if dedup.Valid {
			r.dedupKey = &dedup.V
		}
		out = append(out, r)
	}
	if rErr := rows.Err(); rErr != nil {
		t.Fatalf("iterate dlq rows: %v", rErr)
	}
	return out
}

// TestSweepDeadLettersToDLQ is the flagship DLQ admission test: a max_deliver=1 consumer
// whose single in-flight row times out is dead-lettered into the auto-created orders.dlq
// with the full provenance set, a NEW id (C1), preserved trace, NULL dedup_key, and its
// delivery row gone — one transaction (G1/G3/G4).
func TestSweepDeadLettersToDLQ(t *testing.T) {
	st, fk, _ := openSweepStore(t)
	seedSweep(t, st, func(c *queue.ConsumerConfig) { c.MaxDeliver = 1 }, 1, 1)
	fk.Advance(30 * time.Second)

	res, err := st.Sweep(context.Background(), SweepCmd{Limit: 10})
	if err != nil {
		t.Fatalf("sweep: %v", err)
	}
	if res.Dead != 1 {
		t.Fatalf("result = %+v, want 1 dead", res)
	}
	if n := countDeliveryRows(t, st); n != 0 {
		t.Fatalf("deliveries after dead-letter = %d, want 0", n)
	}

	// The DLQ stream was auto-created with the template.
	info, err := st.GetStream(context.Background(), "orders.dlq")
	if err != nil {
		t.Fatalf("get orders.dlq: %v", err)
	}
	if info.DedupWindowMS != 0 {
		t.Fatalf("orders.dlq dedup_window_ms = %d, want 0 (correctness requirement)", info.DedupWindowMS)
	}
	if info.Retention != "limits" {
		t.Fatalf("orders.dlq retention = %q, want limits (a DLQ must not self-delete)", info.Retention)
	}

	rows := readDLQRows(t, st, "orders.dlq")
	if len(rows) != 1 {
		t.Fatalf("dlq has %d copies, want 1", len(rows))
	}
	cp := rows[0]
	if cp.subject != "orders.1" {
		t.Fatalf("copy subject = %q, want the original", cp.subject)
	}
	if cp.traceID == "" {
		t.Fatal("copy trace_id is empty; it must be preserved")
	}
	if cp.dedupKey != nil {
		t.Fatalf("copy dedup_key = %q, want NULL (dedup disabled on a DLQ)", *cp.dedupKey)
	}

	// Provenance: parse the hdr JSON and assert the mandatory set.
	var hdr map[string]string
	if err := json.Unmarshal([]byte(cp.hdr), &hdr); err != nil {
		t.Fatalf("copy hdr not JSON: %v", err)
	}
	for _, k := range []string{
		"Messq-Origin-Stream", "Messq-Origin-Seq", "Messq-Origin-Consumer",
		"Messq-Attempts", "Messq-Cause", "Messq-Dead-At", "Messq-Origin-Id",
	} {
		if v, ok := hdr[k]; !ok || v == "" {
			t.Fatalf("copy hdr missing %q (have %v)", k, hdr)
		}
	}
	if hdr["Messq-Origin-Stream"] != "orders" || hdr["Messq-Origin-Consumer"] != "worker" || hdr["Messq-Cause"] != "max_deliver" {
		t.Fatalf("copy provenance wrong: %v", hdr)
	}

	// The copy mints a NEW id (C1): it differs from the origin's.
	origID := originMsgID(t, st, "orders", 1)
	if cp.id == origID {
		t.Fatalf("copy id %q preserved the origin id; C1 requires a fresh ULID", cp.id)
	}
	if cp.traceID != originTraceID(t, st, "orders", 1) {
		t.Fatal("copy trace_id must equal the origin's (S4.4)")
	}

	// Exactly one msg.dead; no msg.publish for the copy.
	if n := countEvent(t, st, "msg.dead"); n != 1 {
		t.Fatalf("msg.dead events = %d, want 1", n)
	}
	if n := countEvent(t, st, "msg.publish"); n != 1 {
		t.Fatalf("msg.publish events = %d, want 1 (only the origin's; no event for the copy)", n)
	}
}

// TestDLQAutoCreateIdempotent pins that the SECOND death does not recreate the DLQ:
// one stream.create (actor=system, reason=dead_letter), and the second copy reuses it.
func TestDLQAutoCreateIdempotent(t *testing.T) {
	st, fk, _ := openSweepStore(t)
	seedSweep(t, st, func(c *queue.ConsumerConfig) { c.MaxDeliver = 1 }, 2, 2)
	fk.Advance(30 * time.Second)
	if _, err := st.Sweep(context.Background(), SweepCmd{Limit: 10}); err != nil {
		t.Fatalf("sweep: %v", err)
	}
	var creates int
	if err := st.RO().QueryRowContext(context.Background(),
		`SELECT count(*) FROM events WHERE event = 'stream.create' AND coalesce(stream,'') = 'orders.dlq'`).Scan(&creates); err != nil {
		t.Fatalf("count dlq creates: %v", err)
	}
	if creates != 1 {
		t.Fatalf("orders.dlq stream.create events = %d, want 1 (second death must not recreate)", creates)
	}
	rows := readDLQRows(t, st, "orders.dlq")
	if len(rows) != 2 {
		t.Fatalf("dlq copies = %d, want 2", len(rows))
	}
	if rows[0].seq+1 != rows[1].seq {
		t.Fatalf("copy seqs not contiguous: %d then %d", rows[0].seq, rows[1].seq)
	}
	// The system actor is on the create event.
	var actor, detail string
	if err := st.RO().QueryRowContext(context.Background(),
		`SELECT actor, detail FROM events WHERE event = 'stream.create' AND stream = 'orders.dlq'`).Scan(&actor, &detail); err != nil {
		t.Fatalf("read orders.dlq stream.create: %v", err)
	}
	if actor != "system" || detail != `{"reason":"dead_letter","origin":"orders"}` {
		t.Fatalf("stream.create actor=%q detail=%q, want system + dead_letter", actor, detail)
	}
}

// TestDLQTwoConsumersSameSeq: two consumers dead-letter the same origin seq -> two
// copies in the same DLQ, each with a NEW id, the same trace, distinguished by
// Messq-Origin-Consumer (G2/G6).
func TestDLQTwoConsumersSameSeq(t *testing.T) {
	st, fk, _ := openSweepStore(t)
	// Publish one message, create two consumers each max_deliver=1, claim both.
	if _, err := st.Publish(context.Background(), PublishCmd{
		Stream: "orders", Req: queue.PublishReq{Subject: "orders.1", Body: []byte("x")},
	}); err != nil {
		t.Fatalf("publish: %v", err)
	}
	for _, name := range []string{"a", "b"} {
		cfg := queue.DefaultConsumerConfig(name)
		cfg.MaxDeliver = 1
		if _, err := st.CreateConsumer(context.Background(), "orders", cfg, queue.StartPosition{Kind: queue.StartFirst}, "test"); err != nil {
			t.Fatalf("create %s: %v", name, err)
		}
		if _, err := st.Fetch(context.Background(), FetchReq{Stream: "orders", Consumer: name, Batch: 1}); err != nil {
			t.Fatalf("fetch %s: %v", name, err)
		}
	}
	fk.Advance(30 * time.Second)
	if _, err := st.Sweep(context.Background(), SweepCmd{Limit: 10}); err != nil {
		t.Fatalf("sweep: %v", err)
	}
	rows := readDLQRows(t, st, "orders.dlq")
	if len(rows) != 2 {
		t.Fatalf("dlq copies = %d, want 2", len(rows))
	}
	// same trace, distinct (new) ids, distinct consumers.
	traces := map[string]bool{}
	ids := map[string]bool{}
	cons := map[string]bool{}
	for _, r := range rows {
		traces[r.traceID] = true
		ids[r.id] = true
		var hdr map[string]string
		if err := json.Unmarshal([]byte(r.hdr), &hdr); err != nil {
			t.Fatalf("hdr not JSON: %v", err)
		}
		cons[hdr["Messq-Origin-Consumer"]] = true
	}
	if len(traces) != 1 {
		t.Fatalf("two copies of one origin must share a trace_id, got %v", traces)
	}
	if len(ids) != 2 {
		t.Fatalf("two copies must mint distinct new ids (C1), got %v", ids)
	}
	if !cons["a"] || !cons["b"] {
		t.Fatalf("Messq-Origin-Consumer must distinguish the copies: %v", cons)
	}
}

// TestDLQOriginMissing: an origin message deleted behind retention's back (direct SQL)
// still retires the delivery row (never a stuck consumer) and records origin_missing.
func TestDLQOriginMissing(t *testing.T) {
	st, fk, _ := openSweepStore(t)
	seedSweep(t, st, func(c *queue.ConsumerConfig) { c.MaxDeliver = 1 }, 1, 1)
	// Delete the origin message directly (bypassing retention, which never does this).
	if _, err := st.rw.ExecContext(context.Background(),
		`DELETE FROM messages WHERE stream = 'orders' AND seq = 1`); err != nil {
		t.Fatalf("delete origin: %v", err)
	}
	fk.Advance(30 * time.Second)
	res, err := st.Sweep(context.Background(), SweepCmd{Limit: 10})
	if err != nil {
		t.Fatalf("sweep: %v", err)
	}
	if res.Dead != 1 {
		t.Fatalf("dead = %d, want 1 (the row is still retired)", res.Dead)
	}
	if n := countDeliveryRows(t, st); n != 0 {
		t.Fatalf("deliveries after origin_missing = %d, want 0 (never a stuck consumer)", n)
	}
	// origin_missing: NO dlq stream created (nothing to copy).
	if _, err := st.GetStream(context.Background(), "orders.dlq"); err == nil {
		t.Fatal("orders.dlq was created for an origin_missing death; it must not be")
	}
	// The msg.dead event records detail.dlq=origin_missing.
	var detail string
	if err := st.RO().QueryRowContext(context.Background(),
		`SELECT detail FROM events WHERE event = 'msg.dead'`).Scan(&detail); err != nil {
		t.Fatalf("read msg.dead: %v", err)
	}
	if !containsStr(detail, `"dlq":"origin_missing"`) {
		t.Fatalf("msg.dead detail %q lacks origin_missing", detail)
	}
}

// TestDLQDropPolicy: a drop-policy consumer deletes the row and emits msg.dead with
// detail.policy=drop; no DLQ stream is created and no copy exists (G8).
func TestDLQDropPolicy(t *testing.T) {
	st, fk, _ := openSweepStore(t)
	seedSweep(t, st, func(c *queue.ConsumerConfig) { c.MaxDeliver = 1; c.DeadPolicy = queue.DeadPolicyDrop }, 1, 1)
	fk.Advance(30 * time.Second)
	res, err := st.Sweep(context.Background(), SweepCmd{Limit: 10})
	if err != nil {
		t.Fatalf("sweep: %v", err)
	}
	if res.Dead != 1 {
		t.Fatalf("dead = %d, want 1", res.Dead)
	}
	if countDeliveryRows(t, st) != 0 {
		t.Fatal("row not deleted on drop")
	}
	if _, err := st.GetStream(context.Background(), "orders.dlq"); err == nil {
		t.Fatal("orders.dlq created for a drop; it must not be")
	}
	var detail string
	if err := st.RO().QueryRowContext(context.Background(),
		`SELECT detail FROM events WHERE event = 'msg.dead'`).Scan(&detail); err != nil {
		t.Fatalf("read msg.dead: %v", err)
	}
	if !containsStr(detail, `"dlq":"dropped"`) || !containsStr(detail, `"policy":"drop"`) {
		t.Fatalf("msg.dead detail %q lacks dropped/drop", detail)
	}
}

// openDLQStore opens a fresh store whose DLQ config is tweakable before Open.
func openDLQStore(t *testing.T, mutate func(*queue.DLQConfig)) (*Store, *clock.Fake) {
	t.Helper()
	opt := testOptions(filepath.Join(t.TempDir(), "data"), fakeClock(), &logCapture{})
	opt.Jitter = func(d time.Duration) time.Duration { return d }
	opt.DLQ = queue.DefaultDLQConfig(opt.Limits)
	if mutate != nil {
		mutate(&opt.DLQ)
	}
	st, _, err := Open(context.Background(), opt)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() {
		if closeErr := st.Close(context.Background()); closeErr != nil {
			t.Errorf("close dlq store: %v", closeErr)
		}
	})
	if _, _, err := st.CreateStream(context.Background(), queue.DefaultConfig("orders"), "test"); err != nil {
		t.Fatalf("create stream orders: %v", err)
	}
	fk, ok := st.clk.(*clock.Fake)
	if !ok {
		t.Fatal("store clock is not *clock.Fake")
	}
	return st, fk
}

// TestDLQBypassesDLQMaxMsgSize: the copy carries a payload larger than the auto-created
// DLQ's max_msg_size (here driven down to 8 bytes) — a broker migration is never refused
// by a stream limit (PLAN §4.5, G5).
func TestDLQBypassesDLQMaxMsgSize(t *testing.T) {
	st, fk := openDLQStore(t, func(c *queue.DLQConfig) { c.MaxMsgSize = 8 })
	if _, err := st.Publish(context.Background(), PublishCmd{
		Stream: "orders", Req: queue.PublishReq{Subject: "orders.1", Body: make([]byte, 100)},
	}); err != nil {
		t.Fatalf("publish 100-byte body: %v", err)
	}
	cfg := queue.DefaultConsumerConfig("worker")
	cfg.MaxDeliver = 1
	if _, err := st.CreateConsumer(context.Background(), "orders", cfg, queue.StartPosition{Kind: queue.StartFirst}, "test"); err != nil {
		t.Fatalf("create consumer: %v", err)
	}
	if _, err := st.Fetch(context.Background(), FetchReq{Stream: "orders", Consumer: "worker", Batch: 1}); err != nil {
		t.Fatalf("fetch: %v", err)
	}
	fk.Advance(30 * time.Second)
	res, err := st.Sweep(context.Background(), SweepCmd{Limit: 10})
	if err != nil {
		t.Fatalf("sweep: %v", err)
	}
	if res.Dead != 1 {
		t.Fatalf("dead = %d, want 1 (oversized copy must still land)", res.Dead)
	}
	rows := readDLQRows(t, st, "orders.dlq")
	if len(rows) != 1 || rows[0].size != 100 {
		t.Fatalf("dlq copies = %+v, want one 100-byte copy", rows)
	}
}

// TestDLQBudgetDefers: with a tiny per-transaction byte budget, a sweep tick copies
// what fits and defers the rest (SweepResult.Deferred), leaving those INFLIGHT rows for
// a later tick — which then completes them (G7).
func TestDLQBudgetDefers(t *testing.T) {
	st, fk := openDLQStore(t, func(c *queue.DLQConfig) { c.MaxBytesPerCommit = 45 }) // fits exactly one 40-byte copy
	for i := 0; i < 3; i++ {
		if _, err := st.Publish(context.Background(), PublishCmd{
			Stream: "orders", Req: queue.PublishReq{Subject: "orders.1", Body: make([]byte, 40)},
		}); err != nil {
			t.Fatalf("publish %d: %v", i, err)
		}
	}
	cfg := queue.DefaultConsumerConfig("worker")
	cfg.MaxDeliver = 1
	if _, err := st.CreateConsumer(context.Background(), "orders", cfg, queue.StartPosition{Kind: queue.StartFirst}, "test"); err != nil {
		t.Fatalf("create consumer: %v", err)
	}
	if _, err := st.Fetch(context.Background(), FetchReq{Stream: "orders", Consumer: "worker", Batch: 3}); err != nil {
		t.Fatalf("fetch: %v", err)
	}
	fk.Advance(30 * time.Second)

	// First tick: the 45-byte budget fits exactly one 40-byte copy; the rest defer.
	res, err := st.Sweep(context.Background(), SweepCmd{Limit: 10})
	if err != nil {
		t.Fatalf("sweep: %v", err)
	}
	if res.Dead+res.Deferred != 3 {
		t.Fatalf("dead+deferred = %d, want 3 (all three were dead transitions)", res.Dead+res.Deferred)
	}
	if res.Deferred < 1 || res.Dead < 1 {
		t.Fatalf("first tick dead=%d deferred=%d, want both non-zero", res.Dead, res.Deferred)
	}
	if n := countDeliveryRows(t, st); int(n) != res.Deferred {
		t.Fatalf("deferred rows left INFLIGHT = %d, want %d", n, res.Deferred)
	}
	if len(readDLQRows(t, st, "orders.dlq")) != res.Dead {
		t.Fatalf("dlq copies after first tick = %d, want %d", len(readDLQRows(t, st, "orders.dlq")), res.Dead)
	}
	// Subsequent ticks (fresh budget per command) land the rest.
	for ticks := 0; ticks < 3 && countDeliveryRows(t, st) > 0; ticks++ {
		fk.Advance(30 * time.Second)
		if _, err := st.Sweep(context.Background(), SweepCmd{Limit: 10}); err != nil {
			t.Fatalf("sweep completion: %v", err)
		}
	}
	if countDeliveryRows(t, st) != 0 {
		t.Fatalf("deliveries after completion = %d, want 0", countDeliveryRows(t, st))
	}
	if got := len(readDLQRows(t, st, "orders.dlq")); got != 3 {
		t.Fatalf("dlq copies after all ticks = %d, want 3", got)
	}
}

// originMsgID returns the id of the origin message (stream, seq).
func originMsgID(t *testing.T, st *Store, stream string, seq int64) string {
	t.Helper()
	var id string
	if err := st.RO().QueryRowContext(context.Background(),
		`SELECT id FROM messages WHERE stream = ? AND seq = ?`, stream, seq).Scan(&id); err != nil {
		t.Fatalf("read origin id: %v", err)
	}
	return id
}

// originTraceID returns the trace_id of the origin message (stream, seq).
func originTraceID(t *testing.T, st *Store, stream string, seq int64) string {
	t.Helper()
	var tr string
	if err := st.RO().QueryRowContext(context.Background(),
		`SELECT trace_id FROM messages WHERE stream = ? AND seq = ?`, stream, seq).Scan(&tr); err != nil {
		t.Fatalf("read origin trace_id: %v", err)
	}
	return tr
}
