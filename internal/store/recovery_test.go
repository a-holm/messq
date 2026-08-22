// SPDX-License-Identifier: Apache-2.0

package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
)

// testDataDir returns a fresh per-test directory path that Open may create itself: the
// environment's t.TempDir is group-readable, and §10 wants messq to own the 0700 creation.
func testDataDir(t *testing.T) string {
	t.Helper()
	return filepath.Join(t.TempDir(), "data")
}

// fatalHelper is the slice of the testing/rapid T types the kill simulation needs; both
// *testing.T and rapid's *T satisfy it.
type fatalHelper interface {
	Helper()
	Fatalf(format string, args ...any)
	Errorf(format string, args ...any)
}

// killSimulate drops a live store exactly the way SIGKILL would: database handles are closed
// out from under it and the flock released, with none of Close's clean-shutdown writes — no
// optimize, no checkpoint, no marker flip. taken is the handle returned by TakeWriter when
// the test claimed it; everything else is read off the store itself.
func killSimulate(tb fatalHelper, s *Store, taken *sql.DB) {
	tb.Helper()
	if taken != nil {
		if err := taken.Close(); err != nil {
			tb.Fatalf("kill-simulate writer close: %v", err)
		}
	}
	s.mu.Lock()
	ro, lock := s.ro, s.lock
	s.rw, s.ro, s.lock = nil, nil, nil
	s.closed = true
	s.mu.Unlock()
	if ro != nil {
		if err := ro.Close(); err != nil {
			tb.Fatalf("kill-simulate reader close: %v", err)
		}
	}
	if lock != nil {
		if err := lock.unlock(); err != nil {
			tb.Fatalf("kill-simulate unlock: %v", err)
		}
	}
}

// deliveryRow mirrors one deliveries row for whole-table diffs. Nullability matches the
// schema exactly so go-cmp flags any column drift, not just the columns recovery touches.
type deliveryRow struct {
	Stream      string
	Consumer    string
	Seq         int64
	Subject     string
	State       int64
	Attempts    int64
	VisibleAt   int64
	Generation  int64
	DeliveredAt sql.NullInt64
	LastReason  sql.NullString
}

type deliverySeed struct {
	stream, consumer string
	seq              int64
	subject          string
	state            int64 // 0 READY, 1 INFLIGHT
	attempts         int64
	visibleAt        int64
	generation       int64
	deliveredAt      sql.NullInt64
	lastReason       sql.NullString
}

func seedDeliveries(t *testing.T, ctx context.Context, db *sql.DB, seeds []deliverySeed) {
	t.Helper()
	for _, s := range seeds {
		_, err := db.ExecContext(ctx,
			`INSERT INTO deliveries (stream, consumer, seq, subject, state, attempts, visible_at, generation, delivered_at, last_reason)
			 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
			s.stream, s.consumer, s.seq, s.subject, s.state, s.attempts, s.visibleAt, s.generation, s.deliveredAt, s.lastReason)
		if err != nil {
			t.Fatalf("seed delivery %s/%s/%d: %v", s.stream, s.consumer, s.seq, err)
		}
	}
}

func snapshotDeliveries(t *testing.T, ctx context.Context, db *sql.DB) []deliveryRow {
	t.Helper()
	rows, err := db.QueryContext(ctx,
		`SELECT stream, consumer, seq, subject, state, attempts, visible_at, generation, delivered_at, last_reason
		   FROM deliveries ORDER BY stream, consumer, seq`)
	if err != nil {
		t.Fatalf("read deliveries: %v", err)
	}
	defer func() {
		if cerr := rows.Close(); cerr != nil {
			t.Errorf("close deliveries rows: %v", cerr)
		}
	}()
	var out []deliveryRow
	for rows.Next() {
		var r deliveryRow
		if err := rows.Scan(&r.Stream, &r.Consumer, &r.Seq, &r.Subject, &r.State, &r.Attempts,
			&r.VisibleAt, &r.Generation, &r.DeliveredAt, &r.LastReason); err != nil {
			t.Fatalf("scan delivery: %v", err)
		}
		out = append(out, r)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate deliveries: %v", err)
	}
	return out
}

// TestReclaimFlipsInflightDeterministically is the T9 proof. With ReclaimJitter=0 every
// INFLIGHT row flips to READY at exactly now with attempts untouched and delivered_at
// cleared; READY rows keep their backoff state bit for bit.
func TestReclaimFlipsInflightDeterministically(t *testing.T) {
	ctx := context.Background()
	dir := testDataDir(t)
	clk := fakeClock()

	st, _, err := Open(ctx, testOptions(dir, clk, &logCapture{}))
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	w, err := st.TakeWriter()
	if err != nil {
		t.Fatalf("take writer: %v", err)
	}

	seeds := []deliverySeed{
		{
			stream: "orders", consumer: "c1", seq: 1, subject: "orders.a", state: 1, attempts: 3, visibleAt: fakeStartMillis - 500, generation: 2,
			deliveredAt: sql.NullInt64{Int64: fakeStartMillis - 400, Valid: true}, lastReason: sql.NullString{String: "timeout", Valid: true},
		},
		{stream: "orders", consumer: "c1", seq: 2, subject: "orders.b", state: 1, attempts: 1, visibleAt: fakeStartMillis + 60_000, generation: 7},
		{
			stream: "orders", consumer: "c2", seq: 9, subject: "orders.c", state: 1, attempts: 5, visibleAt: fakeStartMillis + 999, generation: 1,
			lastReason: sql.NullString{String: "nak", Valid: true},
		},
		// READY mid-backoff: must survive untouched, including the past visible_at.
		{
			stream: "orders", consumer: "c2", seq: 4, subject: "orders.d", state: 0, attempts: 2, visibleAt: fakeStartMillis - 10_000, generation: 3,
			lastReason: sql.NullString{String: "nak", Valid: true},
		},
		{stream: "billing", consumer: "c1", seq: 1, subject: "billing.x", state: 0, attempts: 0, visibleAt: 0, generation: 5},
	}
	seedDeliveries(t, ctx, w, seeds)

	// Order matches the snapshot query: ORDER BY stream, consumer, seq. Flipped rows read
	// delivered_at NULL and last_reason='broker_restart'; the READY rows keep everything.
	want := []deliveryRow{
		{Stream: "billing", Consumer: "c1", Seq: 1, Subject: "billing.x", State: 0, Attempts: 0, VisibleAt: 0, Generation: 5},
		{
			Stream: "orders", Consumer: "c1", Seq: 1, Subject: "orders.a", State: 0, Attempts: 3, VisibleAt: fakeStartMillis, Generation: 2,
			LastReason: sql.NullString{String: "broker_restart", Valid: true},
		},
		{
			Stream: "orders", Consumer: "c1", Seq: 2, Subject: "orders.b", State: 0, Attempts: 1, VisibleAt: fakeStartMillis, Generation: 7,
			LastReason: sql.NullString{String: "broker_restart", Valid: true},
		},
		{
			Stream: "orders", Consumer: "c2", Seq: 4, Subject: "orders.d", State: 0, Attempts: 2, VisibleAt: fakeStartMillis - 10_000, Generation: 3,
			LastReason: sql.NullString{String: "nak", Valid: true},
		},
		{
			Stream: "orders", Consumer: "c2", Seq: 9, Subject: "orders.c", State: 0, Attempts: 5, VisibleAt: fakeStartMillis, Generation: 1,
			LastReason: sql.NullString{String: "broker_restart", Valid: true},
		},
	}

	// The reclaim runs at Open, so the crash has to happen before the second open.
	killSimulate(t, st, w)

	opts := testOptions(dir, clk, &logCapture{})
	opts.ReclaimJitter = 0 // deterministic: visible_at lands on exactly now
	st2, report, err := Open(ctx, opts)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer func() {
		if cerr := st2.Close(ctx); cerr != nil {
			t.Errorf("close store: %v", cerr)
		}
	}()

	if report.Reclaimed != 3 {
		t.Errorf("RecoveryReport.Reclaimed = %d, want 3", report.Reclaimed)
	}
	got := snapshotDeliveries(t, ctx, st2.RO())
	if diff := cmp.Diff(want, got); diff != "" {
		t.Errorf("post-recovery deliveries table (-want +got):\n%s", diff)
	}

	// The recovery run contributes exactly one co-committed audit event whose detail.count
	// matches the report. (The fresh open earlier in this test wrote its own count=0 row:
	// every Open narrates its reclaim, per the issue's verbatim step-8 SQL.)
	var detail string
	var nMatching, nTotal int
	if err := st2.RO().QueryRowContext(ctx,
		`SELECT count(*), sum(json_extract(detail, '$.count') = ?), min(CASE WHEN json_extract(detail, '$.count') = ? THEN detail END)
		   FROM events WHERE event = 'recovery.reclaimed'`,
		report.Reclaimed, report.Reclaimed).Scan(&nTotal, &nMatching, &detail); err != nil {
		t.Fatalf("count reclaimed events: %v", err)
	}
	if nMatching != 1 {
		t.Fatalf("recovery.reclaimed rows with count=%d = %d (of %d total), want exactly 1",
			report.Reclaimed, nMatching, nTotal)
	}
	var parsed struct {
		Count    int64  `json:"count"`
		JitterMs int64  `json:"jitter_ms"`
		Reason   string `json:"reason"`
	}
	if err := json.Unmarshal([]byte(detail), &parsed); err != nil {
		t.Fatalf("parse event detail %q: %v", detail, err)
	}
	if parsed.Count != 3 || parsed.JitterMs != 0 || parsed.Reason != "broker_restart" {
		t.Errorf("event detail = %+v, want count=3 jitter_ms=0 reason=broker_restart", parsed)
	}
}

// TestReclaimJitterBoundsVisibleAt spreads reclaim over [now, now+jitter) across many rows:
// every flipped row stays inside the window and the spread is non-degenerate.
func TestReclaimJitterBoundsVisibleAt(t *testing.T) {
	ctx := context.Background()
	dir := testDataDir(t)
	clk := fakeClock()

	st, _, err := Open(ctx, testOptions(dir, clk, &logCapture{}))
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	w, err := st.TakeWriter()
	if err != nil {
		t.Fatalf("take writer: %v", err)
	}
	const rows = 300
	seeds := make([]deliverySeed, rows)
	for i := range seeds {
		seeds[i] = deliverySeed{
			stream: "s", consumer: "c", seq: int64(i + 1), subject: "s.x",
			state: 1, attempts: 1, visibleAt: fakeStartMillis, generation: 1,
		}
	}
	seedDeliveries(t, ctx, w, seeds)
	killSimulate(t, st, w)

	const jitter = 750 * time.Millisecond
	opts := testOptions(dir, clk, &logCapture{})
	opts.ReclaimJitter = jitter
	st2, report, err := Open(ctx, opts)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer func() {
		if cerr := st2.Close(ctx); cerr != nil {
			t.Errorf("close store: %v", cerr)
		}
	}()
	if report.Reclaimed != rows {
		t.Errorf("Reclaimed = %d, want %d", report.Reclaimed, rows)
	}

	jitterMs := jitter.Milliseconds()
	var lo, hi, distinct int64
	if err := st2.RO().QueryRowContext(ctx,
		`SELECT min(visible_at), max(visible_at), count(DISTINCT visible_at) FROM deliveries`).Scan(&lo, &hi, &distinct); err != nil {
		t.Fatalf("aggregate visible_at: %v", err)
	}
	if lo < fakeStartMillis || hi >= fakeStartMillis+jitterMs {
		t.Errorf("visible_at window [%d, %d] escapes [%d, %d)", lo, hi, fakeStartMillis, fakeStartMillis+jitterMs)
	}
	if distinct < 2 {
		t.Errorf("visible_at collapsed to %d distinct value(s) over %d rows — jitter degenerate", distinct, rows)
	}
}

// TestUncleanMarkerMatrix walks the marker protocol: clean close → clean open without a
// check; death between opens → unclean open running quick_check; a deleted marker row reads
// as unclean exactly like a "0".
func TestUncleanMarkerMatrix(t *testing.T) {
	ctx := context.Background()

	t.Run("clean-close-then-open-is-clean-and-skips-check", func(t *testing.T) {
		dir := testDataDir(t)
		st, _, err := Open(ctx, testOptions(dir, fakeClock(), &logCapture{}))
		if err != nil {
			t.Fatalf("open: %v", err)
		}
		if closeErr := st.Close(ctx); closeErr != nil {
			t.Fatalf("close: %v", closeErr)
		}
		cap := &logCapture{}
		st2, report, err := Open(ctx, testOptions(dir, fakeClock(), cap))
		if err != nil {
			t.Fatalf("reopen: %v", err)
		}
		defer func() {
			if cerr := st2.Close(ctx); cerr != nil {
				t.Errorf("close store: %v", cerr)
			}
		}()
		if report.Unclean {
			t.Error("reopen after clean Close reported Unclean")
		}
		if report.CheckKind != "skipped" {
			t.Errorf("CheckKind = %q, want skipped", report.CheckKind)
		}
		if len(cap.find(slog.LevelWarn, "recovery.unclean")) != 0 {
			t.Error("clean reopen logged recovery.unclean")
		}
	})

	t.Run("death-between-opens-is-unclean-and-runs-quick-check", func(t *testing.T) {
		dir := testDataDir(t)
		cap1 := &logCapture{}
		st, _, err := Open(ctx, testOptions(dir, fakeClock(), cap1))
		if err != nil {
			t.Fatalf("open: %v", err)
		}
		killSimulate(t, st, nil) // die before any clean shutdown could happen

		cap2 := &logCapture{}
		st2, report, err := Open(ctx, testOptions(dir, fakeClock(), cap2))
		if err != nil {
			t.Fatalf("reopen after death: %v", err)
		}
		defer func() {
			if cerr := st2.Close(ctx); cerr != nil {
				t.Errorf("close store: %v", cerr)
			}
		}()
		if !report.Unclean {
			t.Fatal("reopen after simulated SIGKILL reported Unclean=false")
		}
		if report.CheckKind != "quick_check" {
			t.Errorf("CheckKind = %q, want quick_check after an unclean stop", report.CheckKind)
		}
		if len(cap2.find(slog.LevelWarn, "recovery.unclean")) == 0 {
			t.Error("unclean reopen did not log recovery.unclean at WARN")
		}
		var n int
		if scanErr := st2.RO().QueryRowContext(ctx,
			`SELECT count(*) FROM events WHERE event = 'recovery.unclean'`).Scan(&n); scanErr != nil {
			t.Fatalf("count unclean events: %v", scanErr)
		}
		if n != 1 {
			t.Errorf("recovery.unclean event rows = %d, want 1 per unclean open", n)
		}
	})

	t.Run("deleted-marker-row-reads-as-unclean", func(t *testing.T) {
		dir := testDataDir(t)
		st, _, err := Open(ctx, testOptions(dir, fakeClock(), &logCapture{}))
		if err != nil {
			t.Fatalf("open: %v", err)
		}
		w, err := st.TakeWriter()
		if err != nil {
			t.Fatalf("take writer: %v", err)
		}
		if _, delErr := w.ExecContext(ctx, `DELETE FROM meta WHERE k = 'clean_shutdown'`); delErr != nil {
			t.Fatalf("delete marker: %v", delErr)
		}
		killSimulate(t, st, w)

		st2, report, err := Open(ctx, testOptions(dir, fakeClock(), &logCapture{}))
		if err != nil {
			t.Fatalf("reopen: %v", err)
		}
		defer func() {
			if cerr := st2.Close(ctx); cerr != nil {
				t.Errorf("close store: %v", cerr)
			}
		}()
		if !report.Unclean {
			t.Error("missing clean_shutdown marker did not trip Unclean")
		}
	})
}

// TestFullCheckForcesCheckEvenWhenClean proves --fsck runs integrity_check on a directory
// nothing is wrong with (and that the kind is reported verbatim).
func TestFullCheckForcesCheckEvenWhenClean(t *testing.T) {
	ctx := context.Background()
	dir := testDataDir(t)
	st, _, err := Open(ctx, testOptions(dir, fakeClock(), &logCapture{}))
	if err != nil {
		t.Fatalf("first open: %v", err)
	}
	if closeErr := st.Close(ctx); closeErr != nil {
		t.Fatalf("close: %v", closeErr)
	}
	opts := testOptions(dir, fakeClock(), &logCapture{})
	opts.FullCheck = true
	st2, report, err := Open(ctx, opts)
	if err != nil {
		t.Fatalf("fsck open: %v", err)
	}
	defer func() {
		if cerr := st2.Close(ctx); cerr != nil {
			t.Errorf("close store: %v", cerr)
		}
	}()
	if report.Unclean {
		t.Error("clean directory reported Unclean")
	}
	if report.CheckKind != "integrity_check" {
		t.Errorf("CheckKind = %q, want integrity_check under FullCheck", report.CheckKind)
	}
}

// TestCorruptionRefusedWithoutRepair flips bytes in a copied database's page-2 header and
// demands the triple contract: ErrCorrupt, a storage.fatal ERROR line, and byte-for-byte no
// repair. The untouched copy opening fine under the same --fsck proves the mutation caused it.
func TestCorruptionRefusedWithoutRepair(t *testing.T) {
	ctx := context.Background()
	src := testDataDir(t)

	st, _, err := Open(ctx, testOptions(src, fakeClock(), &logCapture{}))
	if err != nil {
		t.Fatalf("seed open: %v", err)
	}
	w, err := st.TakeWriter()
	if err != nil {
		t.Fatalf("take writer: %v", err)
	}
	payload := []byte(strings.Repeat("x", 200))
	for i := 0; i < 150; i++ {
		_, err = w.ExecContext(ctx,
			`INSERT INTO messages (stream, seq, id, subject, body, size, published_at, trace_id)
			 VALUES ('s', ?, ?, 'subj', ?, ?, ?, 'tr')`,
			i+1, fmt.Sprintf("%026d", i), payload, len(payload), int64(i))
		if err != nil {
			t.Fatalf("seed message %d: %v", i, err)
		}
	}
	if wErr := w.Close(); wErr != nil {
		t.Fatalf("owner close of writer: %v", wErr)
	}
	if seedCloseErr := st.Close(ctx); seedCloseErr != nil {
		t.Fatalf("seed close: %v", seedCloseErr)
	}

	original, err := os.ReadFile(filepath.Join(src, dbFileName))
	if err != nil {
		t.Fatalf("read seeded db: %v", err)
	}
	if len(original) <= 2*4096 {
		t.Fatalf("seeded db only %d bytes — page-2 corruption target missing", len(original))
	}

	mutate := func(t *testing.T, data []byte) string {
		t.Helper()
		dst := testDataDir(t)
		if mkErr := os.MkdirAll(dst, 0o700); mkErr != nil {
			t.Fatalf("create corrupt copy dir: %v", mkErr)
		}
		mutated := append([]byte(nil), data...)
		for i := 4096; i < 4096+32; i++ { // page 2's b-tree header, probed to fail checks
			mutated[i] ^= 0xFF
		}
		path := filepath.Join(dst, dbFileName)
		if writeErr := os.WriteFile(path, mutated, 0o600); writeErr != nil {
			t.Fatalf("write corrupt copy: %v", writeErr)
		}
		return dst
	}

	corruptDir := mutate(t, original)

	opts := testOptions(corruptDir, fakeClock(), &logCapture{})
	opts.FullCheck = true
	cap := &logCapture{}
	opts.Logger = slog.New(cap)
	st2, report, err := Open(ctx, opts)
	if !errors.Is(err, ErrCorrupt) {
		t.Fatalf("open of corrupt copy error = %v, want ErrCorrupt (store=%v report=%v)", err, st2, report)
	}
	if st2 != nil {
		t.Cleanup(func() {
			if cerr := st2.Close(ctx); cerr != nil {
				t.Errorf("close refused-corruption store: %v", cerr)
			}
		})
	}
	if fatal := cap.find(slog.LevelError, "storage.fatal"); len(fatal) == 0 {
		t.Error("corruption refusal did not log storage.fatal at ERROR")
	}
	after, err := os.ReadFile(filepath.Join(corruptDir, dbFileName))
	if err != nil {
		t.Fatalf("re-read corrupt copy: %v", err)
	}
	if len(after) != len(original) {
		t.Errorf("failed open changed file size %d -> %d", len(original), len(after))
	}
	for i := 4096; i < 4096+32; i++ {
		if after[i] == original[i] {
			t.Fatalf("byte at offset %d was restored — repair attempted", i)
		}
	}

	// Negative control: the identical-but-unmutated copy must pass the very same check.
	cleanDir := testDataDir(t)
	if mkErr := os.MkdirAll(cleanDir, 0o700); mkErr != nil {
		t.Fatalf("create pristine copy dir: %v", mkErr)
	}
	cleanPath := filepath.Join(cleanDir, dbFileName)
	if writeErr := os.WriteFile(cleanPath, original, 0o600); writeErr != nil {
		t.Fatalf("write pristine copy: %v", writeErr)
	}
	opts2 := testOptions(cleanDir, fakeClock(), &logCapture{})
	opts2.FullCheck = true
	st3, _, err := Open(ctx, opts2)
	if err != nil {
		t.Fatalf("pristine copy refused under fsck: %v", err)
	}
	if err := st3.Close(ctx); err != nil {
		t.Errorf("close pristine-copy store: %v", err)
	}
}

// TestDedupTrimRespectsStreamWindows seeds expired and live dedup keys behind two stream
// windows and demands exactly the expired set NULLed, plus the freed unique slot accepting a
// re-publish of the trimmed key.
func TestDedupTrimRespectsStreamWindows(t *testing.T) {
	ctx := context.Background()
	dir := testDataDir(t)
	clk := fakeClock()

	st, _, err := Open(ctx, testOptions(dir, clk, &logCapture{}))
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	w, err := st.TakeWriter()
	if err != nil {
		t.Fatalf("take writer: %v", err)
	}
	seedStream := func(stream string, window int64) {
		t.Helper()
		if _, insErr := w.ExecContext(ctx,
			`INSERT INTO streams (name, subjects, dedup_window_ms, created_at) VALUES (?, ?, ?, ?)`,
			stream, `["`+stream+`"]`, window, fakeStartMillis); insErr != nil {
			t.Fatalf("seed stream %s: %v", stream, insErr)
		}
	}
	seedMessage := func(stream string, seq int64, name string, publishedAt int64) {
		t.Helper()
		if _, insErr := w.ExecContext(ctx,
			`INSERT INTO messages (stream, seq, id, subject, body, size, published_at, trace_id, dedup_key)
			 VALUES (?, ?, ?, 'subj', ?, 1, ?, 'tr', ?)`,
			stream, seq, "m-"+name, []byte("b"), publishedAt, name); insErr != nil {
			t.Fatalf("seed message %s: %v", name, insErr)
		}
	}
	// Reopen clock T1 = T0 + 2000. Expiry: published_at + window < T1.
	seedStream("win", 1_000)
	seedStream("wide", 600_000)
	seedMessage("win", 1, "gone", fakeStartMillis-5_000)      // T0-4000 < T1 → trimmed
	seedMessage("win", 2, "kept-late", fakeStartMillis+1_500) // T0+2500 > T1 → kept

	if _, insErr := w.ExecContext(ctx,
		`INSERT INTO messages (stream, seq, id, subject, body, size, published_at, trace_id, dedup_key)
		 VALUES ('win', 3, 'm-second', 'subj', ?, 1, ?, 'tr', 'also-gone')`, []byte("b"), fakeStartMillis-5_000); insErr != nil {
		t.Fatalf("seed second expired message: %v", insErr)
	}
	seedMessage("wide", 1, "kept-wide", fakeStartMillis-5000) // T0+595000 > T1 → kept
	killSimulate(t, st, w)

	clk.Advance(2 * time.Second)

	st2, report, err := Open(ctx, testOptions(dir, clk, &logCapture{}))
	if err != nil {
		t.Fatalf("trim open: %v", err)
	}
	defer func() {
		if cerr := st2.Close(ctx); cerr != nil {
			t.Errorf("close store: %v", cerr)
		}
	}()
	if report.DedupExpired != 2 {
		t.Errorf("DedupExpired = %d, want 2", report.DedupExpired)
	}
	type keyed struct {
		Key sql.NullString
	}
	readKey := func(t *testing.T, stream string, seq int64) keyed {
		t.Helper()
		var k keyed
		if scanErr := st2.RO().QueryRowContext(ctx,
			`SELECT dedup_key FROM messages WHERE stream = ? AND seq = ?`, stream, seq).Scan(&k.Key); scanErr != nil {
			t.Fatalf("read dedup_key of %s/%d: %v", stream, seq, scanErr)
		}
		return k
	}
	if got := readKey(t, "win", 1); got.Key.Valid {
		t.Errorf("expired key survived trim: %q", got.Key.String)
	}
	if got := readKey(t, "win", 3); got.Key.Valid {
		t.Errorf("second expired key survived trim: %q", got.Key.String)
	}
	if got := readKey(t, "win", 2); !got.Key.Valid || got.Key.String != "kept-late" {
		t.Errorf("unexpired key disturbed by trim: %+v", got.Key)
	}
	if got := readKey(t, "wide", 1); !got.Key.Valid || got.Key.String != "kept-wide" {
		t.Errorf("wide-window key disturbed by trim: %+v", got.Key)
	}

	// The partial unique index accepts a re-publish of the trimmed key…
	w2, err := st2.TakeWriter()
	if err != nil {
		t.Fatalf("take writer on trimmed store: %v", err)
	}
	if _, pubErr := w2.ExecContext(ctx,
		`INSERT INTO messages (stream, seq, id, subject, body, size, published_at, trace_id, dedup_key)
		 VALUES ('win', 4, 'm-new', 'subj', x'62', 1, ?, 'tr', 'gone')`, fakeStartMillis+2_000); pubErr != nil {
		t.Errorf("re-publish of trimmed key failed: %v", pubErr)
	}
	// …but the still-held key collides.
	if _, clashErr := w2.ExecContext(ctx,
		`INSERT INTO messages (stream, seq, id, subject, body, size, published_at, trace_id, dedup_key)
		 VALUES ('win', 5, 'm-clash', 'subj', x'62', 1, ?, 'tr', 'kept-late')`, fakeStartMillis+2_000); clashErr == nil {
		t.Error("duplicate of untrimmed dedup_key accepted — unique index broken")
	}
	if closeErr := w2.Close(); closeErr != nil {
		t.Errorf("close test writer: %v", closeErr)
	}
}

// TestRecoveryIsIdempotentAcrossOpens replays the issue's crash-matrix edge: dying anywhere
// around the reclaim leaves a state the next open handles identically — a second recovery
// run finds nothing to reclaim and writes no duplicate audit event.
func TestRecoveryIsIdempotentAcrossOpens(t *testing.T) {
	ctx := context.Background()
	dir := testDataDir(t)
	clk := fakeClock()

	st, _, err := Open(ctx, testOptions(dir, clk, &logCapture{}))
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	w, err := st.TakeWriter()
	if err != nil {
		t.Fatalf("take writer: %v", err)
	}
	seedDeliveries(t, ctx, w, []deliverySeed{
		{stream: "s", consumer: "c", seq: 1, subject: "s.x", state: 1, attempts: 2, visibleAt: fakeStartMillis, generation: 1},
	})
	killSimulate(t, st, w)

	opts := testOptions(dir, clk, &logCapture{})
	opts.ReclaimJitter = 0
	st2, report, err := Open(ctx, opts)
	if err != nil {
		t.Fatalf("first recovery open: %v", err)
	}
	if report.Reclaimed != 1 {
		t.Fatalf("first recovery Reclaimed = %d, want 1", report.Reclaimed)
	}
	// Die again right after the recovered open — the marker is dirty once more.
	killSimulate(t, st2, nil)

	st3, report3, err := Open(ctx, opts)
	if err != nil {
		t.Fatalf("second recovery open: %v", err)
	}
	defer func() {
		if cerr := st3.Close(ctx); cerr != nil {
			t.Errorf("close store: %v", cerr)
		}
	}()
	if report3.Reclaimed != 0 {
		t.Errorf("second recovery Reclaimed = %d, want 0", report3.Reclaimed)
	}
	var nTotal, nRecovery int
	if err := st3.RO().QueryRowContext(ctx,
		`SELECT count(*), sum(json_extract(detail, '$.count') = 1)
		   FROM events WHERE event = 'recovery.reclaimed'`).Scan(&nTotal, &nRecovery); err != nil {
		t.Fatalf("count events: %v", err)
	}
	if nTotal != 3 { // one narrated reclaim per Open: fresh, first recovery, second recovery
		t.Errorf("recovery.reclaimed rows = %d after three opens, want 3", nTotal)
	}
	if nRecovery != 1 {
		t.Errorf("reclaims that moved a row = %d, want exactly 1 — recovery must be idempotent", nRecovery)
	}
	var attempts int64
	if err := st3.RO().QueryRowContext(ctx,
		`SELECT attempts FROM deliveries WHERE stream='s' AND consumer='c' AND seq=1`).Scan(&attempts); err != nil {
		t.Fatalf("read attempts: %v", err)
	}
	if attempts != 2 {
		t.Errorf("attempts = %d across two recoveries, want unchanged 2", attempts)
	}
}
