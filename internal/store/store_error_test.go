// SPDX-License-Identifier: Apache-2.0

package store

import (
	"context"
	"database/sql"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// TestOpenRefusesForeignDatabaseFile covers the "messq.db exists but is not a SQLite
// database" refusal: every pool open fails its ping, startup stops, and nothing is left
// holding the directory lock.
func TestOpenRefusesForeignDatabaseFile(t *testing.T) {
	ctx := context.Background()
	dir := testDataDir(t)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		t.Fatalf("create data dir: %v", err)
	}
	garbage := append([]byte("this is not a database, it is a recipe for bread\x00\x00"), make([]byte, 4096)...)
	if err := os.WriteFile(filepath.Join(dir, dbFileName), garbage, 0o600); err != nil {
		t.Fatalf("write foreign file: %v", err)
	}

	st, report, err := Open(ctx, testOptions(dir, fakeClock(), &logCapture{}))
	if err == nil {
		if closeErr := st.Close(ctx); closeErr != nil {
			t.Errorf("close accepted store: %v", closeErr)
		}
		t.Fatal("Open accepted a non-SQLite database file")
	}
	if st != nil || report != nil {
		t.Errorf("failed Open returned (%v, %v), want (nil, nil)", st, report)
	}
	if strings.Contains(err.Error(), "locked") {
		t.Errorf("refusal misattributed to locking: %v", err)
	}
}

// TestReadOnlyRefusesBrokenDirectories pins read-only refusals: a file the engine cannot
// open at all, and an impostor meta table whose shape disagrees with the ladder.
func TestReadOnlyRefusesBrokenDirectories(t *testing.T) {
	ctx := context.Background()

	t.Run("unopenable-file", func(t *testing.T) {
		dir := testDataDir(t)
		if err := os.MkdirAll(dir, 0o700); err != nil {
			t.Fatalf("create data dir: %v", err)
		}
		if err := os.WriteFile(filepath.Join(dir, dbFileName), []byte("certainly not sqlite"), 0o600); err != nil {
			t.Fatalf("write foreign file: %v", err)
		}
		opts := testOptions(dir, fakeClock(), &logCapture{})
		opts.ReadOnly = true
		st, _, openErr := Open(ctx, opts)
		if openErr == nil {
			if closeErr := st.Close(ctx); closeErr != nil {
				t.Errorf("close accepted store: %v", closeErr)
			}
			t.Fatal("read-only open accepted a non-SQLite file")
		}
	})

	t.Run("impostor-meta-table", func(t *testing.T) {
		dir := testDataDir(t)

		seed, _, err := Open(ctx, testOptions(dir, fakeClock(), &logCapture{}))
		if err != nil {
			t.Fatalf("seed open: %v", err)
		}
		w, err := seed.TakeWriter()
		if err != nil {
			t.Fatalf("take writer: %v", err)
		}
		if _, err := w.ExecContext(ctx,
			`DROP TABLE meta; CREATE TABLE meta (x INTEGER) STRICT`); err != nil {
			t.Fatalf("plant impostor meta: %v", err)
		}
		if wErr := w.Close(); wErr != nil {
			t.Fatalf("owner close: %v", wErr)
		}
		killSimulate(t, seed, w)

		opts := testOptions(dir, fakeClock(), &logCapture{})
		opts.ReadOnly = true
		st2, _, openErr := Open(ctx, opts)
		if openErr == nil {
			if closeErr := st2.Close(ctx); closeErr != nil {
				t.Errorf("close accepted store: %v", closeErr)
			}
			t.Fatal("read-only open accepted an impostor meta table")
		}
	})
}

// TestOpenRefusesSchemaTooNew walks the forward-only refusal through the full Open path:
// a directory stamped by a future binary is refused before anything else happens.
func TestOpenRefusesSchemaTooNew(t *testing.T) {
	ctx := context.Background()
	dir := testDataDir(t)

	st, _, seedErr := Open(ctx, testOptions(dir, fakeClock(), &logCapture{}))
	if seedErr != nil {
		t.Fatalf("seed open: %v", seedErr)
	}
	w, takeErr := st.TakeWriter()
	if takeErr != nil {
		t.Fatalf("take writer: %v", takeErr)
	}
	if _, stampErr := w.ExecContext(ctx,
		`UPDATE meta SET v = '999' WHERE k = 'schema_version'`); stampErr != nil {
		t.Fatalf("stamp future version: %v", stampErr)
	}
	if _, mirrorErr := w.ExecContext(ctx, `PRAGMA user_version = 999`); mirrorErr != nil {
		t.Fatalf("mirror future version: %v", mirrorErr)
	}
	killSimulate(t, st, w)

	st2, report, err := Open(ctx, testOptions(dir, fakeClock(), &logCapture{}))
	if !errors.Is(err, ErrSchemaTooNew) {
		t.Fatalf("open of future directory = %v, want ErrSchemaTooNew (store=%v report=%v)", err, st2, report)
	}
}

// TestRecoveryHelpersRejectBrokenHandles drives the recovery helpers' failure arms directly
// (in-package): a closed pool cannot even begin, missing tables abort and roll back the
// transaction at each stage, and a checkpoint or integrity check on a dead handle reports
// instead of pretending.
func TestRecoveryHelpersRejectBrokenHandles(t *testing.T) {
	ctx := context.Background()
	dir := testDataDir(t)

	st, _, openErr := Open(ctx, testOptions(dir, fakeClock(), &logCapture{}))
	if openErr != nil {
		t.Fatalf("open: %v", openErr)
	}
	rw := st.rw

	if _, _, err := reclaimLeasesAndTrimDedup(ctx, rw, st.clk, 0); err != nil {
		t.Fatalf("healthy baseline reclaim: %v", err)
	}

	// Closed-handle begin arm: a pool that is already closed cannot start a transaction.
	if _, _, err := reclaimLeasesAndTrimDedup(ctx, deadPool(t, dir), st.clk, 0); err == nil {
		t.Error("reclaim on closed pool succeeded")
	}
	if _, took, err := runStartupCheck(ctx, deadPool(t, dir), checkQuickCheck, st.clk); err == nil || took != 0 {
		t.Errorf("quick_check on closed pool = (%q, %v), want an error", took, err)
	}
	if err := recordUncleanEvent(ctx, deadPool(t, dir), st.clk, "x"); err == nil {
		t.Error("unclean event on closed pool succeeded")
	}

	// Event-co-commit arm: dropping events lets the reclaim UPDATE succeed but aborts the
	// transaction at its audit insert — neither write may survive.
	if _, dropErr := rw.ExecContext(ctx, `DROP TABLE events`); dropErr != nil {
		t.Fatalf("drop events: %v", dropErr)
	}
	if reclaimed, dedup, err := reclaimLeasesAndTrimDedup(ctx, rw, st.clk, 0); err == nil {
		t.Errorf("reclaim without events table reported (%d, %d)", reclaimed, dedup)
	}
	if err := recordUncleanEvent(ctx, rw, st.clk, "no events table"); err == nil {
		t.Error("unclean event succeeded without the events table")
	}

	// Missing-deliveries arm: the very first statement of the transaction now fails.
	if _, dropErr := rw.ExecContext(ctx, `DROP TABLE deliveries`); dropErr != nil {
		t.Fatalf("drop deliveries: %v", dropErr)
	}
	if _, _, err := reclaimLeasesAndTrimDedup(ctx, rw, st.clk, 0); err == nil {
		t.Error("reclaim without deliveries table succeeded")
	}

	// Trim arm on a second database: dropping messages fails the last statement, after a
	// successful reclaim — the rollback must undo the flip.
	dir2 := testDataDir(t)
	st2, _, trimOpenErr := Open(ctx, testOptions(dir2, fakeClock(), &logCapture{}))
	if trimOpenErr != nil {
		t.Fatalf("open trim-target: %v", trimOpenErr)
	}
	rw2 := st2.rw
	if _, dropErr := rw2.ExecContext(ctx, `DROP TABLE messages`); dropErr != nil {
		t.Fatalf("drop messages: %v", dropErr)
	}
	if _, _, err := reclaimLeasesAndTrimDedup(ctx, rw2, st.clk, 0); err == nil {
		t.Error("trim without messages table succeeded")
	}

	// Checkpoint arm.
	if _, _, err := checkpointTruncate(ctx, deadPool(t, dir)); err == nil {
		t.Error("checkpoint on closed pool succeeded")
	}

	// Upsert arm.
	if _, dropErr := rw.ExecContext(ctx, `DROP TABLE meta`); dropErr != nil {
		t.Fatalf("drop meta: %v", dropErr)
	}
	if err := upsertMetaDB(ctx, rw, "k", "v"); err == nil {
		t.Error("meta upsert succeeded without the meta table")
	}
}

// TestCloseStillReleasesEverythingWhenWriterIsDead pins Close's contract under sabotage:
// every rw-dependent step fails because the handle was swapped for a corpse, Close joins the
// errors instead of panicking, and the flock is released regardless — a later open succeeds.
func TestCloseStillReleasesEverythingWhenWriterIsDead(t *testing.T) {
	ctx := context.Background()
	dir := testDataDir(t)

	st, _, openErr := Open(ctx, testOptions(dir, fakeClock(), &logCapture{}))
	if openErr != nil {
		t.Fatalf("open: %v", openErr)
	}
	corpse := deadPool(t, dir)

	st.mu.Lock()
	st.rw = corpse // simulate every writer-side statement failing at once
	st.mu.Unlock()

	if err := st.Close(ctx); err == nil {
		t.Fatal("Close with a dead writer handle reported success")
	}

	// The lock must be gone: this process can take it again right now.
	lock, err := lockDataDir(dir, lockExclusive)
	if err != nil {
		t.Fatalf("flock not released by failed Close: %v", err)
	}
	if err := lock.unlock(); err != nil {
		t.Errorf("release probe lock: %v", err)
	}
}

// TestReadOnlyRefusesNonIntegerVersion covers the tampered-bookkeeping arm of the read-only
// identity read: a meta row that is not an integer refuses inspection too.
func TestReadOnlyRefusesNonIntegerVersion(t *testing.T) {
	ctx := context.Background()
	dir := testDataDir(t)

	st, _, seedErr := Open(ctx, testOptions(dir, fakeClock(), &logCapture{}))
	if seedErr != nil {
		t.Fatalf("seed open: %v", seedErr)
	}
	w, takeErr := st.TakeWriter()
	if takeErr != nil {
		t.Fatalf("take writer: %v", takeErr)
	}
	if _, stampErr := w.ExecContext(ctx,
		`UPDATE meta SET v = 'not-a-number' WHERE k = 'schema_version'`); stampErr != nil {
		t.Fatalf("corrupt version: %v", stampErr)
	}
	killSimulate(t, st, w)

	opts := testOptions(dir, fakeClock(), &logCapture{})
	opts.ReadOnly = true
	st2, _, err := Open(ctx, opts)
	if err == nil {
		if closeErr := st2.Close(ctx); closeErr != nil {
			t.Errorf("close accepted store: %v", closeErr)
		}
		t.Fatal("read-only open accepted a non-integer schema_version")
	}
	if !strings.Contains(err.Error(), "not an integer") {
		t.Errorf("refusal does not name the corruption: %v", err)
	}
}

// deadPool returns a database/sql handle already closed against this driver, for reaching
// the "pool unusable" error arms deterministically.
func deadPool(t *testing.T, dir string) *sql.DB {
	t.Helper()
	db, err := openSQLite(buildDSN(filepath.Join(dir, "dead.db"), poolWriter, Options{}))
	if err != nil {
		t.Fatalf("open dead pool: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close dead pool: %v", err)
	}
	return db
}
