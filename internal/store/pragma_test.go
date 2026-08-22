// SPDX-License-Identifier: Apache-2.0

package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/a-holm/messq/internal/clock"
)

// openForTest builds the DSN for role, registers its expectation set, and opens the handle
// through the production path — tests never hand-roll a DSN or bypass the registry.
func openForTest(t *testing.T, path string, role poolRole, opt Options, maxConns int) *sql.DB {
	t.Helper()
	dsn := buildDSN(path, role, opt)
	registerExpectations(dsn, expectationsFor(role, opt))
	db, err := openSQLite(dsn)
	if err != nil {
		t.Fatalf("open %s: %v", dsn, err)
	}
	t.Cleanup(func() {
		if cerr := db.Close(); cerr != nil {
			t.Errorf("close %s: %v", dsn, cerr)
		}
	})
	db.SetMaxOpenConns(maxConns)
	// No idle retention: every Conn after a Close is a fresh physical connection that ran
	// the hook again, which is exactly what the poisoned-registration legs below rely on.
	db.SetMaxIdleConns(0)
	return db
}

// assertPragmas reads every pragma back from one raw connection and compares against the
// normalized expectation — the same comparison the connection hook performs.
func assertPragmas(t *testing.T, ctx context.Context, conn *sql.Conn, exps []expect) {
	t.Helper()
	for _, e := range exps {
		var got string
		if err := conn.QueryRowContext(ctx, "PRAGMA "+e.name).Scan(&got); err != nil {
			t.Errorf("read PRAGMA %s: %v", e.name, err)
			continue
		}
		if norm := normalizePragmaValue(got); norm != e.want {
			t.Errorf("PRAGMA %s read back %q (normalized %q), want %q", e.name, got, norm, e.want)
		}
	}
}

// TestEveryPooledConnectionHoldsPragmas is the G4 proof: every physical connection of both
// pools carries the full expectation set for its role and durability mode. Readers are
// forced to N distinct connections by holding all of them concurrently; the writer's single
// connection is checked directly.
func TestEveryPooledConnectionHoldsPragmas(t *testing.T) {
	for _, tc := range []struct {
		name string
		opt  Options
	}{
		{"full", Options{}},
		{"relaxed", Options{Durability: DurabilityRelaxed}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			const readers = 4

			rOpt := tc.opt
			rOpt.ReadPoolSize = readers

			dir := t.TempDir()
			wDB := openForTest(t, dir+"/messq.db", poolWriter, tc.opt, 1)
			rDB := openForTest(t, dir+"/messq.db", poolReader, rOpt, readers)

			// Hold every reader-pool connection at once so each one is a distinct
			// physical connection that ran the hook. Acquisitions are sequential:
			// concurrent opens of fresh-file WAL conversions raced SQLITE_BUSY past
			// busy_timeout under -race load, while held connections force exactly the
			// same pool expansion without the open race.
			conns := make([]*sql.Conn, readers)
			for i := 0; i < readers; i++ {
				conn, err := rDB.Conn(ctx)
				if err != nil {
					t.Fatalf("acquire pooled conn %d of %d: %v", i+1, readers, err)
				}
				conns[i] = conn
			}

			exps := expectationsFor(poolReader, rOpt)
			if got := exps[len(exps)-1]; got.name != "query_only" || got.want != "1" {
				t.Errorf("last reader expectation = %s=%s, want query_only=1", got.name, got.want)
			}
			for _, c := range conns {
				assertPragmas(t, ctx, c, exps)
			}
			// Release every held connection before the poisoned legs below: they need the
			// pool empty so the next Conn creates a fresh PHYSICAL connection (with no idle
			// retention and no queued waiters, it runs the hook again).
			for _, c := range conns {
				if cerr := c.Close(); cerr != nil {
					t.Errorf("release pooled conn: %v", cerr)
				}
			}

			// Poisoned registration: flip one expectation wrong and prove the next
			// connection the pool creates is refused — the hook verifies every new
			// pooled connection, not just the first batch. (With no idle retention,
			// this Conn cannot reuse an already-verified connection.)
			readerDSN := buildDSN(dir+"/messq.db", poolReader, rOpt)
			poisoned := append([]expect(nil), exps...)
			poisoned[len(poisoned)-1] = expect{name: "query_only", want: "0"}
			registerExpectations(readerDSN, poisoned)
			if _, err := rDB.Conn(ctx); !errors.Is(err, ErrPragmaMismatch) {
				t.Errorf("poisoned reader expectations did not refuse the new connection: %v", err)
			}

			// The writer's single pooled connection gets the writer set: synchronous per
			// durability mode, and never query_only.
			wConn, err := wDB.Conn(ctx)
			if err != nil {
				t.Fatalf("acquire writer conn: %v", err)
			}
			wExps := expectationsFor(poolWriter, tc.opt)
			assertPragmas(t, ctx, wConn, wExps)
			if cerr := wConn.Close(); cerr != nil {
				t.Errorf("release writer conn: %v", cerr)
			}
			for _, e := range wExps {
				if e.name == "query_only" {
					t.Errorf("writer expectation set must not contain query_only")
				}
			}

			writerDSN := buildDSN(dir+"/messq.db", poolWriter, tc.opt)
			wPoisoned := append([]expect(nil), wExps...)
			for i, e := range wPoisoned {
				if e.name == "cache_size" {
					wPoisoned[i].want = "-1"
				}
			}
			registerExpectations(writerDSN, wPoisoned)
			if _, err := wDB.Conn(ctx); !errors.Is(err, ErrPragmaMismatch) {
				t.Errorf("poisoned writer expectations did not refuse the new connection: %v", err)
			}
		})
	}
}

// TestOpenFailsOnPragmaMismatch is the negative half of G4: expectations demanding FULL
// registered against a DSN whose synchronous parameter says NORMAL must fail the first
// connection with an error wrapping ErrPragmaMismatch naming the pragma.
func TestOpenFailsOnPragmaMismatch(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	dsn := buildDSN(dir+"/messq.db", poolWriter, Options{Durability: DurabilityRelaxed})
	registerExpectations(dsn, expectationsFor(poolWriter, Options{})) // full-mode wants

	db, err := openSQLite(dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() {
		if cerr := db.Close(); cerr != nil {
			t.Errorf("close mismatch-probe handle: %v", cerr)
		}
	}()

	err = db.PingContext(ctx)
	if err == nil {
		t.Fatal("Ping succeeded with FULL expectations against a NORMAL DSN")
	}
	if !errors.Is(err, ErrPragmaMismatch) {
		t.Fatalf("error does not wrap ErrPragmaMismatch: %v", err)
	}
	if !strings.Contains(err.Error(), "synchronous=1") || !strings.Contains(err.Error(), "want 2") {
		t.Errorf("mismatch error does not name the pragma values: %v", err)
	}
}

// TestForeignKeysEnforcementPinnedByLiteral closes the vacuous-suite hole the adversarial
// review found (mutant ADV-FK-EXP): every other pragma test derives its expectations from
// the same truth table under test, so a mutation dropping foreign_keys from that table
// passed the whole suite. Both legs here carry their own hardcoded literals instead of
// deriving anything:
//
//   - the production expectation set registered for the writer DSN must contain
//     foreign_keys want "1" — read out of the registry, compared against literals;
//   - an expectation set hardcoding {foreign_keys, want "0"} against that same writer DSN,
//     whose own parameter says foreign_keys(1), must get the pooled connection refused
//     with ErrPragmaMismatch naming foreign_keys.
func TestForeignKeysEnforcementPinnedByLiteral(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	path := dir + "/messq.db"
	opt := Options{} // defaults; foreign_keys does not vary by option
	writerDSN := buildDSN(path, poolWriter, opt)

	// Production wiring, then a literal-content check on what it registered.
	registerExpectations(writerDSN, expectationsFor(poolWriter, opt))
	v, ok := registry.Load(writerDSN)
	if !ok {
		t.Fatal("writer DSN has no registered expectation set after production registration")
	}
	exps, isSet := v.([]expect)
	if !isSet {
		t.Fatalf("registry entry for writer DSN is %T, not []expect", v)
	}
	found := false
	for _, e := range exps {
		if e.name == "foreign_keys" && e.want == "1" {
			found = true
			break
		}
	}
	if !found {
		t.Errorf("production writer expectations lack hardcoded foreign_keys want \"1\": %+v", exps)
	}

	// Poisoned leg: want "0" cannot be satisfied while the DSN parameter keeps
	// foreign_keys(1) — the hook must refuse the connection and name the pragma.
	registerExpectations(writerDSN, []expect{{name: "foreign_keys", want: "0"}})
	db, err := openSQLite(writerDSN)
	if err != nil {
		t.Fatalf("open poisoned probe handle: %v", err)
	}
	defer func() {
		if cerr := db.Close(); cerr != nil {
			t.Errorf("close poisoned probe handle: %v", cerr)
		}
	}()
	err = db.PingContext(ctx)
	if err == nil {
		t.Fatal("pooled connection accepted {foreign_keys, want 0} against a foreign_keys(1) DSN")
	}
	if !errors.Is(err, ErrPragmaMismatch) {
		t.Fatalf("poisoned foreign_keys error does not wrap ErrPragmaMismatch: %v", err)
	}
	if !strings.Contains(err.Error(), "foreign_keys") {
		t.Errorf("mismatch error does not name foreign_keys: %v", err)
	}
}

// TestUnknownDSNHookIsNoOp proves the registry lookup miss leaves foreign SQLite users in
// the same process untouched.
func TestUnknownDSNHookIsNoOp(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	db, err := openSQLite("file:" + dir + "/plain.db")
	if err != nil {
		t.Fatalf("open unregistered DSN: %v", err)
	}
	defer func() {
		if cerr := db.Close(); cerr != nil {
			t.Errorf("close unregistered-DSN handle: %v", cerr)
		}
	}()
	if err := db.PingContext(ctx); err != nil {
		t.Fatalf("ping unregistered DSN: %v", err)
	}
}

// TestForeignKeysCascade proves foreign_keys=1 is live where it matters: deleting a stream
// row removes its stream_seq companion through ON DELETE CASCADE — the silent-correctness
// regression the hook exists to catch would show up here as an orphaned row.
func TestForeignKeysCascade(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	wDB := openForTest(t, dir+"/messq.db", poolWriter, Options{}, 1)

	if _, _, err := migrate(ctx, wDB, clock.System{}); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	stmts := []string{
		`INSERT INTO streams (name, subjects, created_at) VALUES ('orders', '["orders.*"]', 1)`,
		`INSERT INTO stream_seq (stream, next) VALUES ('orders', 2)`,
	}
	for _, s := range stmts {
		if _, err := wDB.ExecContext(ctx, s); err != nil {
			t.Fatalf("%s: %v", s, err)
		}
	}
	if _, err := wDB.ExecContext(ctx, `DELETE FROM streams WHERE name = 'orders'`); err != nil {
		t.Fatalf("delete stream: %v", err)
	}
	var n int
	if err := wDB.QueryRowContext(ctx, `SELECT count(*) FROM stream_seq`).Scan(&n); err != nil {
		t.Fatalf("count stream_seq: %v", err)
	}
	if n != 0 {
		t.Errorf("stream_seq kept %d orphaned row(s) after cascade delete", n)
	}
}

// TestReaderRejectsWrites proves query_only(1) genuinely fences the read pool at the
// engine level, not just in the DSN.
func TestReaderRejectsWrites(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	path := dir + "/messq.db"

	wDB := openForTest(t, path, poolWriter, Options{}, 1)
	if _, _, err := migrate(ctx, wDB, clock.System{}); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	if err := wDB.Close(); err != nil {
		t.Fatalf("close writer handle: %v", err)
	}

	rDB := openForTest(t, path, poolReader, Options{ReadPoolSize: 1}, 1)
	conn, err := rDB.Conn(ctx)
	if err != nil {
		t.Fatalf("acquire reader conn: %v", err)
	}
	defer func() {
		if cerr := conn.Close(); cerr != nil {
			t.Errorf("release reader conn: %v", cerr)
		}
	}()

	if _, err := conn.ExecContext(ctx, `INSERT INTO meta (k, v) VALUES ('x', 'y')`); err == nil {
		t.Fatal("INSERT via read-pool connection succeeded")
	} else if !strings.Contains(err.Error(), "readonly") {
		t.Errorf("query_only write refusal lacks the readonly wording: %v", err)
	}
}

// TestWALReaderNotBlockedByWriterTransaction proves WAL concurrency on real handles: while
// the writer connection holds an open BEGIN IMMEDIATE transaction, a reader connection
// completes a SELECT. Goroutine plus channel synchronisation only — the writer rolls back
// strictly after the reader reported, so the lock is provably held throughout.
func TestWALReaderNotBlockedByWriterTransaction(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	path := dir + "/messq.db"

	wDB := openForTest(t, path, poolWriter, Options{}, 1)
	if _, _, err := migrate(ctx, wDB, clock.System{}); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	if _, err := wDB.ExecContext(ctx,
		`INSERT INTO streams (name, subjects, created_at) VALUES ('s', '["s"]', 1)`); err != nil {
		t.Fatalf("seed stream: %v", err)
	}

	rDB := openForTest(t, path, poolReader, Options{ReadPoolSize: 1}, 1)

	wConn, err := wDB.Conn(ctx)
	if err != nil {
		t.Fatalf("acquire writer conn: %v", err)
	}
	defer func() {
		if cerr := wConn.Close(); cerr != nil {
			t.Errorf("release writer conn: %v", cerr)
		}
	}()
	if _, err := wConn.ExecContext(ctx, `BEGIN IMMEDIATE`); err != nil {
		t.Fatalf("BEGIN IMMEDIATE: %v", err)
	}

	type result struct {
		n   int
		err error
	}
	done := make(chan result, 1)
	go func() {
		rConn, err := rDB.Conn(ctx)
		if err != nil {
			done <- result{err: fmt.Errorf("acquire reader conn: %w", err)}
			return
		}
		defer func() {
			if cerr := rConn.Close(); cerr != nil {
				t.Errorf("release reader conn: %v", cerr)
			}
		}()
		var n int
		if err := rConn.QueryRowContext(ctx, `SELECT count(*) FROM streams`).Scan(&n); err != nil {
			done <- result{err: fmt.Errorf("select under held writer transaction: %w", err)}
			return
		}
		done <- result{n: n}
	}()

	guard := clock.System{}.NewTimer(10 * time.Second)
	select {
	case res := <-done:
		if res.err != nil {
			t.Fatalf("reader failed while writer transaction held: %v", res.err)
		}
		if res.n != 1 {
			t.Errorf("reader saw %d streams, want 1", res.n)
		}
	case <-guard.C():
		t.Fatal("reader blocked on the writer's open transaction — WAL concurrency broken")
	}

	if _, err := wConn.ExecContext(ctx, `ROLLBACK`); err != nil {
		t.Errorf("rollback writer transaction: %v", err)
	}
}
