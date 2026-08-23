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
