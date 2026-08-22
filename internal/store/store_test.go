// SPDX-License-Identifier: Apache-2.0

package store

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/a-holm/messq/internal/clock"
)

// logCapture is a slog handler collecting every record it receives, so tests can assert on
// the store's structured startup lines (recovery.unclean, storage.fatal, …).
type logCapture struct {
	mu      sync.Mutex
	records []slog.Record
}

func (h *logCapture) Enabled(_ context.Context, _ slog.Level) bool { return true }

func (h *logCapture) Handle(_ context.Context, r slog.Record) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.records = append(h.records, r)
	return nil
}

func (h *logCapture) WithAttrs(attrs []slog.Attr) slog.Handler {
	return h // capture flattens; attribute identity tests read the records directly
}

func (h *logCapture) WithGroup(name string) slog.Handler { return h }

// find returns the messages of every captured record at the given level and message text.
func (h *logCapture) find(level slog.Level, msg string) []slog.Record {
	h.mu.Lock()
	defer h.mu.Unlock()
	var out []slog.Record
	for _, r := range h.records {
		if r.Level == level && r.Message == msg {
			out = append(out, r)
		}
	}
	return out
}

const fakeStartMillis = int64(1_700_000_000_000)

func fakeClock() *clock.Fake { return clock.NewFake(time.UnixMilli(fakeStartMillis)) }

// testOptions builds Options wired to dir, the given clock and a capturing logger.
func testOptions(dir string, clk clock.Clock, handler slog.Handler) Options {
	return Options{
		DataDir: dir,
		Clock:   clk,
		Logger:  slog.New(handler),
	}
}

// TestOpenFreshDirectoryLifecycle walks the core §4.4 acceptance: a fresh directory opens at
// schema v1 without an unclean flag, mints exactly one ULID node_id that survives reopens,
// and idempotent reopen leaves the creation bookkeeping untouched.
func TestOpenFreshDirectoryLifecycle(t *testing.T) {
	ctx := context.Background()
	dir := testDataDir(t)
	clk := fakeClock()

	st, report, err := Open(ctx, testOptions(dir, clk, &logCapture{}))
	if err != nil {
		t.Fatalf("first open: %v", err)
	}
	if got := st.SchemaVersion(); got != 2 {
		t.Errorf("SchemaVersion() = %d, want 2", got)
	}
	if st.Durability() != DurabilityFull {
		t.Errorf("Durability() = %v, want full", st.Durability())
	}
	if report.Unclean {
		t.Error("fresh directory reported Unclean")
	}
	if report.CheckKind != "skipped" {
		t.Errorf("CheckKind = %q, want \"skipped\" on a clean fresh open", report.CheckKind)
	}
	if report.SchemaFrom != 0 || report.SchemaTo != 2 {
		t.Errorf("report schema pair = (%d, %d), want (0, 2)", report.SchemaFrom, report.SchemaTo)
	}
	if !isULID(st.NodeID()) {
		t.Errorf("NodeID() = %q is not a ULID", st.NodeID())
	}
	if report.NodeID != st.NodeID() {
		t.Errorf("report.NodeID = %q, want %q", report.NodeID, st.NodeID())
	}

	type metaRow struct{ K, V string }
	snapshot := func(t *testing.T, s *Store) map[string]string {
		t.Helper()
		rows, qErr := s.RO().QueryContext(ctx, `SELECT k, v FROM meta ORDER BY k`)
		if qErr != nil {
			t.Fatalf("read meta: %v", qErr)
		}
		defer func() {
			if cerr := rows.Close(); cerr != nil {
				t.Errorf("close meta rows: %v", cerr)
			}
		}()
		m := map[string]string{}
		for rows.Next() {
			var r metaRow
			if scanErr := rows.Scan(&r.K, &r.V); scanErr != nil {
				t.Fatalf("scan meta row: %v", scanErr)
			}
			m[r.K] = r.V
		}
		if iterErr := rows.Err(); iterErr != nil {
			t.Fatalf("iterate meta: %v", iterErr)
		}
		return m
	}
	before := snapshot(t, st)
	if closeErr := st.Close(ctx); closeErr != nil {
		t.Fatalf("first close: %v", closeErr)
	}

	st2, report2, err := Open(ctx, testOptions(dir, clk, &logCapture{}))
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer func() {
		if cerr := st2.Close(ctx); cerr != nil {
			t.Errorf("close store: %v", cerr)
		}
	}()
	if report2.Unclean {
		t.Error("reopen after clean close reported Unclean")
	}
	if got := st2.SchemaVersion(); got != 2 {
		t.Errorf("reopen SchemaVersion() = %d, want 2", got)
	}
	if st2.NodeID() != st.NodeID() {
		t.Errorf("node_id changed across reopen: %q -> %q", st.NodeID(), st2.NodeID())
	}
	after := snapshot(t, st2)
	for _, key := range []string{"schema_version", "node_id", "created_at"} {
		if after[key] != before[key] {
			t.Errorf("meta[%s] changed across reopen: %q -> %q", key, before[key], after[key])
		}
	}
	if report2.DBBytes <= 0 {
		t.Errorf("report.DBBytes = %d, want > 0 for an established database", report2.DBBytes)
	}
}

// isULID reports whether s parses as the 26-character Crockford rendering internal/id mints.
func isULID(s string) bool {
	if len(s) != 26 {
		return false
	}
	const crockford = "0123456789ABCDEFGHJKMNPQRSTVWXYZabcdefghjkmnpqrstvwxyz"
	for _, c := range s {
		if !strings.ContainsRune(crockford, c) {
			return false
		}
	}
	return true
}

// TestOpenSecondInstanceRefused pins the single-instance guarantee through Open: a second
// open of a held directory fails with ErrDataDirLocked naming the holder pid, and succeeds
// once the first store closed. The pragma-mismatch leg proves Open refuses before any work
// when the hook would downgrade durability.
func TestOpenSecondInstanceRefused(t *testing.T) {
	ctx := context.Background()
	dir := testDataDir(t)

	first, _, err := Open(ctx, testOptions(dir, fakeClock(), &logCapture{}))
	if err != nil {
		t.Fatalf("first open: %v", err)
	}
	_, _, err = Open(ctx, testOptions(dir, fakeClock(), &logCapture{}))
	if !errors.Is(err, ErrDataDirLocked) {
		t.Fatalf("second open error = %v, want ErrDataDirLocked", err)
	}
	if !strings.Contains(err.Error(), fmt.Sprintf("pid=%d", os.Getpid())) {
		t.Errorf("refusal does not name the holder pid: %v", err)
	}
	if closeErr := first.Close(ctx); closeErr != nil {
		t.Fatalf("close first: %v", closeErr)
	}
	second, _, err := Open(ctx, testOptions(dir, fakeClock(), &logCapture{}))
	if err != nil {
		t.Fatalf("open after close: %v", err)
	}
	if closeErr := second.Close(ctx); closeErr != nil {
		t.Fatalf("close second: %v", closeErr)
	}
}

// TestTakeWriterSingleShotAndConcurrentReads covers the exactly-one-writer rule and that the
// read pool keeps serving while the writer handle is privately owned.
func TestTakeWriterSingleShotAndConcurrentReads(t *testing.T) {
	ctx := context.Background()
	st, _, err := Open(ctx, testOptions(testDataDir(t), fakeClock(), &logCapture{}))
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() {
		if cerr := st.Close(ctx); cerr != nil {
			t.Errorf("close store: %v", cerr)
		}
	}()

	w, err := st.TakeWriter()
	if err != nil {
		t.Fatalf("first TakeWriter: %v", err)
	}
	if w == nil {
		t.Fatal("first TakeWriter returned a nil handle")
	}
	if _, insErr := w.ExecContext(ctx,
		`INSERT INTO meta (k, v) VALUES ('probe', '1')`); insErr != nil {
		t.Fatalf("write via taken writer: %v", insErr)
	}
	_, err = st.TakeWriter()
	if !errors.Is(err, ErrWriterTaken) {
		t.Fatalf("second TakeWriter error = %v, want ErrWriterTaken", err)
	}

	// Reads keep flowing on the shared pool while the writer handle is owned elsewhere.
	var wg sync.WaitGroup
	errs := make(chan error, 8)
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			var n int
			if scanErr := st.RO().QueryRowContext(ctx, `SELECT count(*) FROM meta`).Scan(&n); scanErr != nil {
				errs <- fmt.Errorf("concurrent RO query: %w", scanErr)
				return
			}
			if n < 1 {
				errs <- fmt.Errorf("concurrent RO query saw %d meta rows, want >= 1", n)
			}
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Error(err)
	}
}

// TestCloseIdempotentAndSafeOnPartialStore pins Close's contract: twice is fine, a zero-value
// Store (the shape left behind by a failed Open) closes without panicking, and a closed store
// no longer hands out its writer.
func TestCloseIdempotentAndSafeOnPartialStore(t *testing.T) {
	ctx := context.Background()
	st, _, err := Open(ctx, testOptions(testDataDir(t), fakeClock(), &logCapture{}))
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if err := st.Close(ctx); err != nil {
		t.Fatalf("first close: %v", err)
	}
	if err := st.Close(ctx); err != nil {
		t.Fatalf("second close must be a no-op, got %v", err)
	}
	if _, err := st.TakeWriter(); !errors.Is(err, ErrWriterTaken) {
		t.Errorf("TakeWriter after close = %v, want ErrWriterTaken", err)
	}
	partial := &Store{}
	if err := partial.Close(ctx); err != nil {
		t.Errorf("Close on zero-value Store = %v, want nil", err)
	}
}

// TestReadOnlyMode pins the offline-inspection surface: LOCK_SH (inspectors overlap each
// other but not a daemon), no writer handle, working reads, and byte-for-byte no mutation of
// the database file across the whole session.
func TestReadOnlyMode(t *testing.T) {
	ctx := context.Background()
	dir := testDataDir(t)
	path := filepath.Join(dir, dbFileName)

	st, _, err := Open(ctx, testOptions(dir, fakeClock(), &logCapture{}))
	if err != nil {
		t.Fatalf("seed open: %v", err)
	}
	w, err := st.TakeWriter()
	if err != nil {
		t.Fatalf("take writer: %v", err)
	}
	if _, insErr := w.ExecContext(ctx,
		`INSERT INTO meta (k, v) VALUES ('seeded', 'yes')`); insErr != nil {
		t.Fatalf("seed write: %v", insErr)
	}
	if wErr := w.Close(); wErr != nil {
		t.Fatalf("owner close of writer: %v", wErr)
	}
	if closeErr := st.Close(ctx); closeErr != nil {
		t.Fatalf("seed close: %v", closeErr)
	}

	before, readErr := os.ReadFile(path)
	if readErr != nil {
		t.Fatalf("read db: %v", readErr)
	}

	opts := testOptions(dir, fakeClock(), &logCapture{})
	opts.ReadOnly = true
	ro, report, err := Open(ctx, opts)
	if err != nil {
		t.Fatalf("read-only open: %v", err)
	}
	if got := ro.SchemaVersion(); got != 2 {
		t.Errorf("ReadOnly SchemaVersion() = %d, want 2", got)
	}
	if report.CheckKind != "skipped" || report.Unclean {
		t.Errorf("ReadOnly report = %+v, want skipped check and clean state", report)
	}
	if w2, takeErr := ro.TakeWriter(); takeErr == nil || w2 != nil {
		t.Errorf("ReadOnly TakeWriter = (%v, %v), want (nil, error)", w2, takeErr)
	}
	var v string
	if scanErr := ro.RO().QueryRowContext(ctx,
		`SELECT v FROM meta WHERE k = 'seeded'`).Scan(&v); scanErr != nil {
		t.Fatalf("read-only query: %v", scanErr)
	}
	if v != "yes" {
		t.Errorf("read-only query saw %q, want \"yes\"", v)
	}

	// Shared locks overlap: a second inspector opens concurrently…
	opts2 := opts
	ro2, _, ro2Err := Open(ctx, opts2)
	if ro2Err != nil {
		t.Fatalf("second concurrent read-only open: %v", ro2Err)
	}
	// …while the exclusive daemon lock is refused.
	if _, _, exclErr := Open(ctx, testOptions(dir, fakeClock(), &logCapture{})); !errors.Is(exclErr, ErrDataDirLocked) {
		t.Errorf("exclusive open under read-only hold = %v, want ErrDataDirLocked", exclErr)
	}
	if closeErr := ro2.Close(ctx); closeErr != nil {
		t.Errorf("close second inspector: %v", closeErr)
	}
	if closeErr := ro.Close(ctx); closeErr != nil {
		t.Errorf("close read-only store: %v", closeErr)
	}

	after, rereadErr := os.ReadFile(path)
	if rereadErr != nil {
		t.Fatalf("re-read db: %v", rereadErr)
	}
	if sha256.Sum256(before) != sha256.Sum256(after) {
		t.Error("read-only session mutated messq.db")
	}
}

// TestSizesReportsFileAndWALBytes checks the exported gauges against os.Stat ground truth.
func TestSizesReportsFileAndWALBytes(t *testing.T) {
	ctx := context.Background()
	dir := testDataDir(t)
	st, _, err := Open(ctx, testOptions(dir, fakeClock(), &logCapture{}))
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	dbBytes, walBytes, err := st.Sizes()
	if err != nil {
		t.Fatalf("Sizes: %v", err)
	}
	dbStat, err := os.Stat(filepath.Join(dir, dbFileName))
	if err != nil {
		t.Fatalf("stat db: %v", err)
	}
	if dbBytes != dbStat.Size() {
		t.Errorf("Sizes db = %d, want %d", dbBytes, dbStat.Size())
	}
	if walBytes < 0 {
		t.Errorf("Sizes wal = %d, want >= 0", walBytes)
	}
	if err := st.Close(ctx); err != nil {
		t.Fatalf("close: %v", err)
	}
	if _, _, err := st.Sizes(); err == nil {
		t.Error("Sizes on a closed store succeeded — want a closed-store error")
	}
}
