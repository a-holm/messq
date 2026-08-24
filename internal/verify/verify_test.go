// SPDX-License-Identifier: Apache-2.0

package verify

import (
	"context"
	"database/sql"
	"io"
	"log/slog"
	"path/filepath"
	"testing"

	"github.com/a-holm/messq/internal/queue"
	"github.com/a-holm/messq/internal/store"
)

// migratedDir builds a real migrated data dir with one stream and one published message,
// so a clean Run has a stream and a message to check against.
func migratedDir(t *testing.T) string {
	t.Helper()
	ctx := context.Background()
	dir := filepath.Join(t.TempDir(), "data")
	st, _, err := store.Open(ctx, store.Options{
		DataDir:    dir,
		Durability: store.DurabilityFull,
		Logger:     slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	if _, _, createErr := st.CreateStream(ctx, queue.DefaultConfig("orders"), "test"); createErr != nil {
		t.Fatalf("create stream: %v", createErr)
	}
	if _, pubErr := st.Publish(ctx, store.PublishCmd{Stream: "orders", Req: queue.PublishReq{
		Subject: "orders.a", Body: []byte("hello"),
	}}); pubErr != nil {
		t.Fatalf("publish: %v", pubErr)
	}
	if closeErr := st.Close(ctx); closeErr != nil {
		t.Fatalf("close store: %v", closeErr)
	}
	return dir
}

// writable opens a write-capable handle on the data dir for planting violating rows.
func writable(t *testing.T, dir string) *sql.DB {
	t.Helper()
	db, err := sql.Open("sqlite", "file:"+filepath.Join(dir, "messq.db"))
	if err != nil {
		t.Fatalf("open writable handle: %v", err)
	}
	t.Cleanup(func() {
		if cerr := db.Close(); cerr != nil {
			t.Logf("close writable handle: %v", cerr)
		}
	})
	return db
}

// runVerify opens the dir read-only and runs the registry.
func runVerify(t *testing.T, dir string, opt Options) Report {
	t.Helper()
	db, err := Open(dir)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() {
		if cerr := db.Close(); cerr != nil {
			t.Logf("close verify handle: %v", cerr)
		}
	}()
	rep, err := Run(context.Background(), db, opt)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	return rep
}

// TestRunClean proves a freshly migrated data dir with one stream and one message passes
// every check, deep included.
func TestRunClean(t *testing.T) {
	dir := migratedDir(t)
	if rep := runVerify(t, dir, Options{Deep: true}); rep.Failed() {
		t.Fatalf("clean data dir has violations: %+v", rep.Violations)
	}
}

// TestRegistryCoverage is the registry meta-test: every S15 invariant in I1–I11 is either a
// check here or named in checkedElsewhere, so the register cannot drift.
func TestRegistryCoverage(t *testing.T) {
	if missing := RegistryCoverage(); len(missing) > 0 {
		t.Fatalf("S15 register drift: %v", missing)
	}
	seen := make(map[string]bool)
	for _, c := range Registry() {
		if seen[c.ID] {
			t.Errorf("check ID %q appears twice in the registry", c.ID)
		}
		seen[c.ID] = true
	}
}

// TestV1NewerSchema proves a newer schema version is refused, never interpreted.
func TestV1NewerSchema(t *testing.T) {
	dir := migratedDir(t)
	db := writable(t, dir)
	if _, err := db.ExecContext(context.Background(), `UPDATE meta SET v = '99' WHERE k = 'schema_version'`); err != nil {
		t.Fatalf("plant newer schema: %v", err)
	}
	rep := runVerify(t, dir, Options{})
	if !hasID(rep.Violations, V1) {
		t.Fatalf("V1 did not fire on a newer schema: %+v", rep.Violations)
	}
}

// TestV6SeqRegression proves a stream whose allocator has fallen behind its highest message
// fires V6.
func TestV6SeqRegression(t *testing.T) {
	dir := migratedDir(t)
	db := writable(t, dir)
	// Insert a message at seq 50 while the allocator stays at 1: next (1) <= max (50).
	if _, err := db.ExecContext(context.Background(), `INSERT INTO messages (stream, seq, id, subject, hdr, body, size, published_at, trace_id, dedup_key)
		VALUES ('orders', 50, '01J0BBBBBBBBBBBBBBBBBBBBBBBB', 'orders.a', NULL, ?, 5, 1, 't', NULL)`, []byte("hello")); err != nil {
		t.Fatalf("plant out-of-range message: %v", err)
	}
	rep := runVerify(t, dir, Options{})
	if !hasID(rep.Violations, V6) {
		t.Fatalf("V6 did not fire on a regressed allocator: %+v", rep.Violations)
	}
}

// TestI4AttemptsOverMax proves a delivery row whose attempts exceed its consumer's bound
// fires I4 — written now, vacuously true until #9 lands, but the fixture proves it bites.
func TestI4AttemptsOverMax(t *testing.T) {
	dir := migratedDir(t)
	db := writable(t, dir)
	if _, err := db.ExecContext(context.Background(), `INSERT INTO consumers (stream, name, created_at, max_deliver)
		VALUES ('orders', 'w1', 1, 5)`); err != nil {
		t.Fatalf("plant consumer: %v", err)
	}
	if _, err := db.ExecContext(context.Background(), `INSERT INTO deliveries (stream, consumer, seq, subject, state, attempts, generation)
		VALUES ('orders', 'w1', 1, 'orders.a', 0, 7, 1)`); err != nil {
		t.Fatalf("plant delivery: %v", err)
	}
	rep := runVerify(t, dir, Options{})
	if !hasID(rep.Violations, I4) {
		t.Fatalf("I4 did not fire on attempts > max_deliver: %+v", rep.Violations)
	}
}

// TestI10FoldMismatch proves the event fold detects a state that drifted from its journal:
// deleting a message leaves the journal one publish ahead of the table.
func TestI10FoldMismatch(t *testing.T) {
	dir := migratedDir(t)
	db := writable(t, dir)
	if _, err := db.ExecContext(context.Background(), `DELETE FROM messages WHERE stream = 'orders'`); err != nil {
		t.Fatalf("delete message: %v", err)
	}
	rep := runVerify(t, dir, Options{Deep: true})
	if !hasID(rep.Violations, I10) {
		t.Fatalf("I10 did not fire on a state that drifted from its journal: %+v", rep.Violations)
	}
}

// TestReadOnlyEnforcement proves the verify connection is fenced write-off: a write attempt
// fails loudly, so no check can ever mutate the data dir it inspects.
func TestReadOnlyEnforcement(t *testing.T) {
	dir := migratedDir(t)
	db, err := Open(dir)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() {
		if cerr := db.Close(); cerr != nil {
			t.Logf("close verify handle: %v", cerr)
		}
	}()
	if _, err := db.ExecContext(context.Background(), `INSERT INTO meta (k, v) VALUES ('verify-probe', 'x')`); err == nil {
		t.Fatal("a write through the verify connection succeeded; query_only(1) is not enforced")
	}
}

func hasID(vs []Violation, id string) bool {
	for _, v := range vs {
		if v.ID == id {
			return true
		}
	}
	return false
}

// TestDLQCheckersGreenOnClean proves a dead-heavy fixture -- a .dlq stream carrying a
// provenance-rich DLQ copy plus its msg.dead journal -- passes every DLQ-specific checker
// (P-DLQ1..5, P-ID1). It asserts those specific IDs green rather than the whole deep
// report: the I10 fold's msg.dead arm is #13's, so a hand-planted death trips I10 by
// design until then.
func TestDLQCheckersGreenOnClean(t *testing.T) {
	dir := migratedDir(t)
	db := writable(t, dir)
	// A .dlq stream may be created only by the broker, so plant it directly, with its
	// stream.create journal row so I10 does not flag a missing create.
	if _, err := db.ExecContext(context.Background(), `INSERT INTO streams
		(name, subjects, retention, max_msgs, max_bytes, max_age_ms, max_msg_size, discard, dedup_window_ms, created_at)
		VALUES ('orders.dlq', '[">"]', 'limits', 0, 0, 720000000, 8388608, 'old', 0, 1)`); err != nil {
		t.Fatalf("plant dlq stream: %v", err)
	}
	if _, err := db.ExecContext(context.Background(), `INSERT INTO stream_seq (stream, next) VALUES ('orders.dlq', 2)`); err != nil {
		t.Fatalf("plant dlq seq: %v", err)
	}
	if _, err := db.ExecContext(context.Background(), `INSERT INTO events
		(ts, event, stream, actor, detail) VALUES (1, 'stream.create', 'orders.dlq', 'system', '{}')`); err != nil {
		t.Fatalf("plant dlq stream.create: %v", err)
	}
	// The DLQ copy carrying the full provenance header set.
	if _, err := db.ExecContext(context.Background(), `INSERT INTO messages
		(stream, seq, id, subject, hdr, body, size, published_at, trace_id, dedup_key)
		VALUES ('orders.dlq', 1, 'NEWID', 'orders.a', ?, ?, 5, 1, 'tr', NULL)`,
		`{"Messq-Origin-Id":"ORIG","Messq-Origin-Stream":"orders","Messq-Origin-Seq":"1","Messq-Origin-Consumer":"w","Messq-Attempts":"1","Messq-Cause":"max_deliver","Messq-Dead-At":"2026-01-01T00:00:00.000Z"}`,
		[]byte("dlq-body")); err != nil {
		t.Fatalf("plant dlq copy: %v", err)
	}
	// The msg.dead journal row that the DLQ hop narrates, detail.dlq=written.
	if _, err := db.ExecContext(context.Background(), `INSERT INTO events
		(ts, event, stream, consumer, subject, msg_id, seq, attempt, trace_id, detail)
		VALUES (2, 'msg.dead', 'orders', 'w', 'orders.a', 'ORIG', 1, 1, 'tr', ?)`,
		`{"cause":"max_deliver","policy":"dlq","attempts":1,"generation":1,"dlq":"written","dlq_stream":"orders.dlq","dlq_seq":1}`); err != nil {
		t.Fatalf("plant msg.dead: %v", err)
	}
	rep := runVerify(t, dir, Options{Deep: true})
	for _, want := range []string{PDLQ1, PDLQ2, PDLQ3, PDLQ4, PDLQ5, PID1, I8} {
		if hasID(rep.Violations, want) {
			t.Fatalf("DLQ checker %s fired on a clean fixture: %+v", want, rep.Violations)
		}
	}
}

// TestPDLQ2MissingProvenance proves P-DLQ2 fires when a .dlq row lacks a provenance header.
func TestPDLQ2MissingProvenance(t *testing.T) {
	dir := migratedDir(t)
	db := writable(t, dir)
	if _, err := db.ExecContext(context.Background(), `INSERT INTO streams
		(name, subjects, retention, max_msgs, max_bytes, max_age_ms, max_msg_size, discard, dedup_window_ms, created_at)
		VALUES ('orders.dlq', '[">"]', 'limits', 0, 0, 864000000, 8388608, 'old', 0, 1)`); err != nil {
		t.Fatalf("plant dlq stream: %v", err)
	}
	if _, err := db.ExecContext(context.Background(), `INSERT INTO stream_seq (stream, next) VALUES ('orders.dlq', 2)`); err != nil {
		t.Fatalf("plant dlq seq: %v", err)
	}
	// A DLQ copy with an EMPTY hdr -- provenance is mandatory for a .dlq row.
	if _, err := db.ExecContext(context.Background(), `INSERT INTO messages
		(stream, seq, id, subject, hdr, body, size, published_at, trace_id, dedup_key)
		VALUES ('orders.dlq', 1, 'NEWID', 'orders.a', NULL, ?, 5, 1, 'tr', NULL)`,
		[]byte(`dlq-body`)); err != nil {
		t.Fatalf("plant provenance-free dlq copy: %v", err)
	}
	rep := runVerify(t, dir, Options{Deep: true})
	if !hasID(rep.Violations, PDLQ2) {
		t.Fatalf("P-DLQ2 did not fire on a provenance-free DLQ copy: %+v", rep.Violations)
	}
}

// TestPID1DetectsDuplicateId proves P-ID1 fires for two non-.dlq messages sharing an id.
func TestPID1DuplicateOutsideDLQ(t *testing.T) {
	dir := migratedDir(t)
	db := writable(t, dir)
	var origID string
	if err := db.QueryRowContext(context.Background(),
		`SELECT id FROM messages WHERE stream = 'orders' LIMIT 1`).Scan(&origID); err != nil {
		t.Fatalf("read original id: %v", err)
	}
	// A second non-.dlq message sharing the original's id violates P-ID1 (C1: ids are
	// globally unique across streams).
	// P-ID1 guards the id-uniqueness contract independently of the schema's UNIQUE index
	// (C1 mints fresh ids, so the index is a belt-and-suspenders and a raw write could
	// bypass it). Drop the index to simulate that raw write path, then plant the dup.
	if _, err := db.ExecContext(context.Background(), `DROP INDEX messages_id`); err != nil {
		t.Fatalf("drop messages_id: %v", err)
	}
	if _, err := db.ExecContext(context.Background(), `INSERT INTO messages
		(stream, seq, id, subject, hdr, body, size, published_at, trace_id, dedup_key)
		VALUES ('orders', 99, ?, 'orders.a', NULL, ?, 5, 2, 't2', NULL)`, origID, []byte("dup")); err != nil {
		t.Fatalf("plant dup id: %v", err)
	}
	rep := runVerify(t, dir, Options{Deep: true})
	if !hasID(rep.Violations, PID1) {
		t.Fatalf("P-ID1 did not fire on a duplicated id: %+v", rep.Violations)
	}
}
