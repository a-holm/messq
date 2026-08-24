// SPDX-License-Identifier: Apache-2.0

package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/a-holm/messq/internal/queue"
)

// The DLQ-sink white-box coverage tests (issue #12 restore): they drive DLQSink.Dead
// directly inside a real transaction against a hand-seeded origin row, so the branches
// the public sweep/retire tests never reach — the origin-header merge loop, the
// provenance degradation/trim ladder, reason truncation, the monotone published_at
// clamp, and the operator-created-DLQ reuse path — are exercised deterministically
// (G1–G9). They also cover the publishOpts (BypassLimits/SuppressEvent/id) the DLQ and
// redrive family shares. Nothing here touches production code.

// dlqSinkFor builds a DLQSink whose limits/DLQ config the test may tweak before one Dead.
func dlqSinkFor(t *testing.T, st *Store, mutate func(*queue.Limits, *queue.DLQConfig)) DLQSink {
	t.Helper()
	lim := queue.DefaultLimits()
	dlqCfg := queue.DefaultDLQConfig(lim)
	if mutate != nil {
		mutate(&lim, &dlqCfg)
	}
	return DLQSink{
		limits: lim,
		dlq:    dlqCfg,
		newID:  st.newID,
		budget: queue.NewDeadBudget(dlqCfg),
	}
}

// runTx commits fn's transaction when it returns nil (so side effects persist for the
// assertion reads on the RO pool), rolling back only on error.
func runTx(t *testing.T, st *Store, fn func(ctx context.Context, tx *sql.Tx) error) {
	t.Helper()
	ctx := context.Background()
	conn, err := st.rw.Conn(ctx)
	if err != nil {
		t.Fatalf("conn: %v", err)
	}
	defer func() {
		if cerr := conn.Close(); cerr != nil {
			t.Errorf("close conn: %v", cerr)
		}
	}()
	tx, err := conn.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	if err := fn(ctx, tx); err != nil {
		_ = tx.Rollback()
		t.Fatalf("fn: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit: %v", err)
	}
}

// deadCtx builds the canonical dead-letter context for orders/worker seq 1.
func deadCtx() queue.DeadCtx {
	return queue.DeadCtx{
		Stream: "orders", Consumer: "worker", Subject: "orders.1",
		Seq: 1, MsgID: "origin0000000000000000000001", TraceID: "deadbeef1234",
		Attempts: 1, Cause: queue.DeadCauseMaxDeliver, LastReason: "exhausted",
		Generation: 1, MaxDeliver: 1, Trigger: queue.DeadTriggerAckWait,
	}
}

// seedOriginRow writes an origin message row (stream orders, seq 1) directly, attaching
// hdrJSON (or NULL when empty).
func seedOriginRow(t *testing.T, tx *sql.Tx, hdrJSON string) {
	t.Helper()
	var hdr any
	if hdrJSON != "" {
		hdr = hdrJSON
	}
	if _, err := tx.ExecContext(context.Background(), `
		INSERT INTO messages (stream, seq, id, subject, hdr, body, size, published_at, trace_id, dedup_key)
		VALUES ('orders', 1, 'origin0000000000000000000001', 'orders.1', ?,
		        x'6f72646572', 6, 1700000000000, 'deadbeef1234', NULL)`, hdr); err != nil {
		t.Fatalf("seed origin row: %v", err)
	}
}

// seedDLQConsumer writes the worker consumer on orders with dead_policy=dlq, so
// PlanDead decides to copy rather than drop.
func seedDLQConsumer(t *testing.T, tx *sql.Tx) {
	t.Helper()
	if _, err := tx.ExecContext(context.Background(), `
		INSERT INTO consumers
			(stream, name, filters, ack_wait_ms, max_deliver, max_ack_pending, backoff_ms,
			 ordered, dead_policy, cursor_seq, generation, paused, created_at)
			VALUES ('orders', 'worker', '["version"]', 30000, 1, 10, '[1000]', 0, 'dlq', 1, 1, 0, 0)`); err != nil {
		t.Fatalf("seed dlq consumer: %v", err)
	}
}

// readDLQJSONHeader returns the parsed header map of the first row of a DLQ stream.
func readDLQJSONHeader(t *testing.T, st *Store, dlq string) map[string]string {
	t.Helper()
	rows := readDLQRows(t, st, dlq)
	if len(rows) != 1 {
		t.Fatalf("dlq %q has %d rows, want 1", dlq, len(rows))
	}
	var hdr map[string]string
	if err := json.Unmarshal([]byte(rows[0].hdr), &hdr); err != nil {
		t.Fatalf("dlq hdr not JSON: %v", err)
	}
	return hdr
}

// deadNow is the writer clock for the direct sink calls.
var deadNow = time.UnixMilli(1700001000000)

// TestDLQSinkMergesOriginUserHeaders: a copy keeps the origin's user headers under the
// provenance set — the mergeCopyHeaders loop that any Valid origin hdr reaches.
func TestDLQSinkMergesOriginUserHeaders(t *testing.T) {
	st, _ := openDLQStore(t, nil)
	runTx(t, st, func(ctx context.Context, tx *sql.Tx) error {
		seedDLQConsumer(t, tx)
		seedOriginRow(t, tx, `{"Tenant":"acme","Region":"eu"}`)
		sink := dlqSinkFor(t, st, nil)
		ev, err := sink.Dead(ctx, tx, deadCtx(), deadNow)
		if err != nil {
			return err
		}
		if ev.Event != "msg.dead" {
			return errors.New("dead event name != msg.dead")
		}
		return nil
	})

	hdr := readDLQJSONHeader(t, st, "orders.dlq")
	if hdr["Tenant"] != "acme" || hdr["Region"] != "eu" {
		t.Fatalf("copy lost origin user headers: %v", hdr)
	}
	for _, k := range []string{
		"Messq-Origin-Stream", "Messq-Origin-Seq", "Messq-Origin-Consumer",
		"Messq-Attempts", "Messq-Cause", "Messq-Dead-At", "Messq-Origin-Id",
	} {
		if v, ok := hdr[k]; !ok || v == "" {
			t.Fatalf("copy hdr missing %q (have %v)", k, hdr)
		}
	}
	if hdr["Messq-Origin-Stream"] != "orders" || hdr["Messq-Origin-Consumer"] != "worker" {
		t.Fatalf("copy provenance wrong: %v", hdr)
	}
}

// deadEventDetail reads the first msg.dead event's detail JSON.
func deadEventDetail(t *testing.T, st *Store) string {
	t.Helper()
	var detail string
	if err := st.RO().QueryRowContext(context.Background(),
		`SELECT detail FROM events WHERE event = 'msg.dead' ORDER BY id LIMIT 1`).Scan(&detail); err != nil {
		t.Fatalf("read msg.dead detail: %v", err)
	}
	return detail
}

// TestDLQSinkTrimsDegradationLadder: when the merged header JSON exceeds the 2x budget,
// the copy drops Messq-Last-Reason then Messq-Origin-Published-At and records
// headers_trimmed=true rather than failing the death.
func TestDLQSinkTrimsDegradationLadder(t *testing.T) {
	st, _ := openDLQStore(t, nil)
	runTx(t, st, func(ctx context.Context, tx *sql.Tx) error {
		// A 120-byte user value plus the ~370-byte provenance set exceeds a 480-byte
		// (2x240) budget, forcing both rungs of the degradation ladder; after the second
		// rung the mandatory set (460 B) still fits, so the death succeeds trimmed.
		seedDLQConsumer(t, tx)
		seedOriginRow(t, tx, `{"alpha":"`+strings.Repeat("x", 120)+`"}`)
		sink := dlqSinkFor(t, st, func(lim *queue.Limits, _ *queue.DLQConfig) {
			lim.MaxHeaderBytes = 240 // 2x budget = 480
		})
		ev, err := sink.Dead(ctx, tx, deadCtx(), deadNow)
		if err != nil {
			return err
		}
		if ev.Event != "msg.dead" {
			return errors.New("dead event name != msg.dead")
		}
		return nil
	})

	hdr := readDLQJSONHeader(t, st, "orders.dlq")
	if _, ok := hdr["Messq-Last-Reason"]; ok {
		t.Fatalf("Messq-Last-Reason survived the trim: %v", hdr)
	}
	if _, ok := hdr["Messq-Origin-Published-At"]; ok {
		t.Fatalf("Messq-Origin-Published-At survived the trim: %v", hdr)
	}
	if hdr["Messq-Origin-Stream"] != "orders" || hdr["Messq-Origin-Seq"] != "1" {
		t.Fatalf("mandatory provenance was trimmed away: %v", hdr)
	}
	detail := deadEventDetail(t, st)
	if !containsStr(detail, `"headers_trimmed":true`) {
		t.Fatalf("msg.dead detail %q lacks headers_trimmed", detail)
	}
}

// TestDLQSinkTrimsDownToError: under an absurdly tiny budget even the mandatory
// provenance set cannot fit — a death is refused, never silently mislabelled.
func TestDLQSinkTrimsDownToError(t *testing.T) {
	st, _ := openDLQStore(t, nil)
	ctx := context.Background()
	conn, err := st.rw.Conn(ctx)
	if err != nil {
		t.Fatalf("conn: %v", err)
	}
	defer func() {
		if cerr := conn.Close(); cerr != nil {
			t.Errorf("close conn: %v", cerr)
		}
	}()
	tx, err := conn.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	defer func() {
		if rbErr := tx.Rollback(); rbErr != nil {
			t.Errorf("rollback: %v", rbErr)
		}
	}()
	seedDLQConsumer(t, tx)
	seedOriginRow(t, tx, "")
	sink := dlqSinkFor(t, st, func(lim *queue.Limits, _ *queue.DLQConfig) {
		lim.MaxHeaderBytes = 16 // 2x budget = 32; the mandatory set is ~300 B
	})
	_, deadErr := sink.Dead(ctx, tx, deadCtx(), deadNow)
	if deadErr == nil || !strings.Contains(deadErr.Error(), "mandatory header set exceeds") {
		t.Fatalf("dead under tiny budget = %v, want the mandatory-set refusal", deadErr)
	}
	_ = tx // kept for the deferred rollback
}

// TestDLQSinkTruncatesReason: a reason wider than the DLQ's ReasonHeaderBytes is cut on a
// rune boundary, its cut flagged as Messq-Last-Reason-Truncated, and the msg.dead detail
// records reason_truncated=true (D9: nothing is lost — the full reason stays in detail).
func TestDLQSinkTruncatesReason(t *testing.T) {
	st, _ := openDLQStore(t, nil)
	runTx(t, st, func(ctx context.Context, tx *sql.Tx) error {
		seedDLQConsumer(t, tx)
		seedOriginRow(t, tx, "")
		dc := deadCtx()
		dc.LastReason = "child exited 1 with a very long diagnostic from stderr"
		sink := dlqSinkFor(t, st, func(_ *queue.Limits, dlq *queue.DLQConfig) {
			dlq.ReasonHeaderBytes = 8
		})
		ev, err := sink.Dead(ctx, tx, dc, deadNow)
		if err != nil {
			return err
		}
		if ev.Event != "msg.dead" {
			return errors.New("dead event name != msg.dead")
		}
		return nil
	})

	hdr := readDLQJSONHeader(t, st, "orders.dlq")
	reason, ok := hdr["Messq-Last-Reason"]
	if !ok {
		t.Fatalf("copy lacks Messq-Last-Reason: %v", hdr)
	}
	if len(reason) != 8 {
		t.Fatalf("truncated reason len = %d (%q), want 8 bytes on a rune boundary", len(reason), reason)
	}
	if hdr["Messq-Last-Reason-Truncated"] != "true" {
		t.Fatalf("copy lacks Messq-Last-Reason-Truncated=true: %v", hdr)
	}
	detail := deadEventDetail(t, st)
	if !containsStr(detail, `"reason_truncated":true`) {
		t.Fatalf("msg.dead detail %q lacks reason_truncated", detail)
	}
}

// TestDLQSinkClampsMonotonePublishedAt: a DLQ already holding a row whose published_at is
// in the future clamps the copy's published_at forward (P4), so a backwards wall-clock
// jump cannot corrupt the messages_age B-tree of the DLQ stream.
func TestDLQSinkClampsMonotonePublishedAt(t *testing.T) {
	st, _ := openDLQStore(t, nil)
	runTx(t, st, func(ctx context.Context, tx *sql.Tx) error {
		if _, err := tx.ExecContext(ctx, `
			INSERT INTO streams (name, subjects, retention, max_age_ms, created_at)
			VALUES ('orders.dlq', '["version"]', 'limits', 0, 1700000000000)`); err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx,
			`INSERT INTO stream_seq (stream, next) VALUES ('orders.dlq', 5)`); err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, `
			INSERT INTO messages (stream, seq, id, subject, body, size, published_at, trace_id)
			VALUES ('orders.dlq', 4, 'dlqpre00000000000000000004', 'orders.1',
			        x'666f6f', 3, 1700002000000, 'future-trace')`); err != nil {
			return err
		}
		return nil
	})

	runTx(t, st, func(ctx context.Context, tx *sql.Tx) error {
		seedDLQConsumer(t, tx)
		seedOriginRow(t, tx, "")
		// ensureDLQStream sees the pre-existing orders.dlq (idempotent: no recreate), so
		// the copy allocates seq 5 and must clamp published_at to the pre-existing max.
		sink := dlqSinkFor(t, st, nil)
		ev, err := sink.Dead(ctx, tx, deadCtx(), deadNow)
		if err != nil {
			return err
		}
		if ev.Event != "msg.dead" {
			return errors.New("dead event name != msg.dead")
		}
		return nil
	})

	rows := readDLQRows(t, st, "orders.dlq")
	if len(rows) != 2 {
		t.Fatalf("orders.dlq rows = %d, want 2 (pre-created + copy)", len(rows))
	}
	copyRow := rows[1]
	if copyRow.seq != 5 {
		t.Fatalf("copy seq = %d, want the next free seq 5", copyRow.seq)
	}
	if copyRow.publishedAt != 1700002000000 {
		t.Fatalf("copy published_at = %d, want clamped forward to 1700002000000 (P4)", copyRow.publishedAt)
	}
	if copyRow.traceID != "deadbeef1234" {
		t.Fatalf("copy trace_id = %q, want the origin's preserved (S4.4)", copyRow.traceID)
	}
}

// TestDLQSinkC1FreshIDPinned: the copy mints a NEW id (C1) even when driven directly,
// never reusing the origin id.
func TestDLQSinkC1FreshIDPinned(t *testing.T) {
	st, _ := openDLQStore(t, nil)
	origID := deadCtx().MsgID
	runTx(t, st, func(ctx context.Context, tx *sql.Tx) error {
		seedDLQConsumer(t, tx)
		seedOriginRow(t, tx, "")
		sink := dlqSinkFor(t, st, nil)
		if _, err := sink.Dead(ctx, tx, deadCtx(), deadNow); err != nil {
			return err
		}
		return nil
	})
	rows := readDLQRows(t, st, "orders.dlq")
	if len(rows) != 1 {
		t.Fatalf("orders.dlq rows = %d, want 1", len(rows))
	}
	if rows[0].id == origID {
		t.Fatal("copy reused the origin id; C1 requires a fresh ULID")
	}
	if len(rows[0].id) != 26 {
		t.Fatalf("copy id %q is not a 26-char ULID", rows[0].id)
	}
}

// TestPublishTxBypassLimitsAndSuppressEvent exercises the publishOpts the DLQ/redrive
// family reuses: BypassLimits lets a broker migration exceed the stream's admission
// limits and bypasses dedup; SuppressEvent omits the audit row; an IDOverride pins the
// copy's id without minting a new one.
func TestPublishTxBypassLimitsAndSuppressEvent(t *testing.T) {
	st, _ := openDLQStore(t, nil)
	ctx := context.Background()
	if _, err := st.Publish(ctx, PublishCmd{
		Stream: "orders", Req: queue.PublishReq{Subject: "orders.1", Body: []byte("x")},
	}); err != nil {
		t.Fatalf("seed publish: %v", err)
	}

	var ack Ack
	runTx(t, st, func(ctx context.Context, tx *sql.Tx) error {
		// Oversize body + non-matching subject + a dedup key the seed could collide with;
		// BypassLimits is precisely what lets a migration land anyway.
		req := queue.PublishReq{
			Subject: "orders.notallowed", Body: make([]byte, 64<<10),
			MsgID: "same-key", Headers: map[string]string{"H": "v"},
		}
		var err error
		ack, _, err = publishTx(ctx, tx, 1700001000000, st.limits, st.newID, "orders", req, publishOpts{
			IDOverride:    "override-id-0000000000000000",
			BypassLimits:  true,
			SuppressEvent: true,
		})
		return err
	})
	if ack.ID != "override-id-0000000000000000" {
		t.Fatalf("ack.ID = %q, want the IDOverride", ack.ID)
	}
	if ack.Duplicate {
		t.Fatalf("BypassLimits publish must bypass dedup, got a duplicate receipt")
	}
	if ack.Seq != 2 {
		t.Fatalf("bypass publish seq = %d, want 2 (contiguous after the seed)", ack.Seq)
	}
	var storedID string
	if err := st.RO().QueryRowContext(ctx,
		`SELECT id FROM messages WHERE stream = 'orders' AND seq = 2`).Scan(&storedID); err != nil {
		t.Fatalf("bypass message row: %v", err)
	}
	if storedID != "override-id-0000000000000000" {
		t.Fatalf("stored id = %q, want the override", storedID)
	}
	// SuppressEvent means exactly one msg.publish for the whole run (the seed's).
	if n := countEvent(t, st, "msg.publish"); n != 1 {
		t.Fatalf("msg.publish events = %d, want just the seed's 1 (SuppressEvent killed the audit row)", n)
	}
}

// TestPublishTxBypassLimitsDoesNotSuppressEvent pins the other half of publishOpts: with
// SuppressEvent false (the default) a bypassed publish still commits its msg.publish audit
// row and bumps stream_stats, both inside the caller's transaction.
func TestPublishTxBypassLimitsDoesNotSuppressEvent(t *testing.T) {
	st, _ := openDLQStore(t, nil)
	ctx := context.Background()

	runTx(t, st, func(ctx context.Context, tx *sql.Tx) error {
		_, ev, err := publishTx(ctx, tx, 1700001000000, st.limits, st.newID, "orders",
			queue.PublishReq{Subject: "orders.1", Body: []byte("hi")}, publishOpts{BypassLimits: true})
		if err != nil {
			return err
		}
		if ev.Event != "msg.publish" {
			return errors.New("bypass publish returned a non-msg.publish carrier")
		}
		return nil
	})
	if n := countEvent(t, st, "msg.publish"); n != 1 {
		t.Fatalf("msg.publish events = %d, want 1 (SuppressEvent is opt-in)", n)
	}
	var msgs, bytes int64
	if err := st.RO().QueryRowContext(ctx,
		`SELECT msgs, bytes FROM stream_stats WHERE stream = 'orders'`).Scan(&msgs, &bytes); err != nil {
		t.Fatalf("read stats: %v", err)
	}
	if msgs != 1 || bytes != 2 {
		t.Fatalf("stream_stats = (%d msgs, %d bytes), want (1, 2)", msgs, bytes)
	}
}
