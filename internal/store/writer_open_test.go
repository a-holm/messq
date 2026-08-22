package store

import (
	"context"
	"database/sql"
	"errors"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// newTestWriter opens a store and constructs its writer in one step, wiring the capturing
// logger through. It fails the test on any error.
func newTestWriter(t *testing.T, optDurability Durability, handler *logCapture) *Writer {
	t.Helper()
	ctx := context.Background()
	opts := testOptions(testDataDir(t), fakeClock(), handler)
	opts.Durability = optDurability
	st, _, openErr := Open(ctx, opts)
	if openErr != nil {
		t.Fatalf("open store: %v", openErr)
	}
	t.Cleanup(func() {
		if closeErr := st.Close(ctx); closeErr != nil {
			t.Errorf("close store: %v", closeErr)
		}
	})
	w, newErr := st.NewWriter(Config{Durability: optDurability})
	if newErr != nil {
		t.Fatalf("NewWriter: %v", newErr)
	}
	return w
}

// TestStoreNewWriterHandsOffExactlyOnce pins the composition: the store's writer is built
// from the sole rw handle, a second construction is refused with ErrWriterTaken, and the
// read pool keeps answering afterwards.
func TestStoreNewWriterHandsOffExactlyOnce(t *testing.T) {
	ctx := context.Background()
	st, _, openErr := Open(ctx, testOptions(testDataDir(t), fakeClock(), &logCapture{}))
	if openErr != nil {
		t.Fatalf("open: %v", openErr)
	}
	defer func() {
		if closeErr := st.Close(ctx); closeErr != nil {
			t.Errorf("close store: %v", closeErr)
		}
	}()
	first, firstErr := st.NewWriter(Config{})
	if firstErr != nil {
		t.Fatalf("first NewWriter: %v", firstErr)
	}
	defer func() {
		if closeErr := first.Close(ctx); closeErr != nil {
			t.Errorf("close first writer: %v", closeErr)
		}
	}()
	second, secondErr := st.NewWriter(Config{})
	if !errors.Is(secondErr, ErrWriterTaken) || second != nil {
		t.Fatalf("second NewWriter = (%v, %v), want (nil, ErrWriterTaken)", second, secondErr)
	}
	var n int
	if scanErr := st.RO().QueryRowContext(ctx, `SELECT count(*) FROM meta`).Scan(&n); scanErr != nil {
		t.Errorf("read pool unusable after hand-off: %v", scanErr)
	}
}

// TestNewWriterRefusesBadPools pins the cheap structural preconditions: nil handles are
// refused, and a pool allowing more than one connection is refused by name — the whole
// safety story of the engine rests on MaxOpenConns being exactly 1.
func TestNewWriterRefusesBadPools(t *testing.T) {
	cfg := Config{}
	if w, nilErr := NewWriter(nil, fakeClock(), cfg); nilErr == nil || w != nil {
		t.Errorf("NewWriter(nil, …) = (%v, %v), want refusal", w, nilErr)
	}
	if w, clkErr := NewWriter(&sql.DB{}, nil, cfg); clkErr == nil || w != nil {
		t.Errorf("NewWriter(…, nil clock) = (%v, %v), want refusal", w, clkErr)
	}

	// A lazily-opened pool over a throwaway file with MaxOpenConns raised: the limit check
	// must fire before any query runs, so no pragma work happens on this handle at all.
	path := dbPath(testDataDir(t))
	if mkdirErr := os.MkdirAll(filepath.Dir(path), 0o700); mkdirErr != nil {
		t.Fatalf("create probe dir: %v", mkdirErr)
	}
	overcommitted, openErr := sql.Open(driverName, "file:"+path)
	if openErr != nil {
		t.Fatalf("open probe pool: %v", openErr)
	}
	defer func() {
		if closeErr := overcommitted.Close(); closeErr != nil {
			t.Errorf("close probe pool: %v", closeErr)
		}
	}()
	overcommitted.SetMaxOpenConns(4)
	_, limitErr := NewWriter(overcommitted, fakeClock(), Config{})
	if limitErr == nil {
		t.Fatal("pool with MaxOpenConns=4 accepted")
	}
	if !strings.Contains(limitErr.Error(), "4") {
		t.Errorf("refusal does not name the observed limit: %v", limitErr)
	}
}

// TestNewWriterVerifiesPragmas pins the read-back verification on the live rw connection:
// a journal_mode that is not wal and a synchronous that does not match the configured
// durability are startup refusals whose message names demanded and observed values —
// D1's answer to "durability lives in a DSN string is a trap".
func TestNewWriterVerifiesPragmas(t *testing.T) {
	ctx := context.Background()

	// Leg 1: a fresh file stamped journal_mode=DELETE through its own minimal DSN. The
	// expectation registry is keyed by exact DSN, so nothing else verifies this handle —
	// what refuses here must be NewWriter's own read-back. The directory is created by hand
	// because Open's own dir creation is deliberately bypassed here.
	path := dbPath(testDataDir(t))
	if mkdirErr := os.MkdirAll(filepath.Dir(path), 0o700); mkdirErr != nil {
		t.Fatalf("create probe dir: %v", mkdirErr)
	}
	stamper, stampOpenErr := sql.Open(driverName, "file:"+path+"?_pragma=journal_mode(DELETE)")
	if stampOpenErr != nil {
		t.Fatalf("open non-wal stamper: %v", stampOpenErr)
	}
	stamper.SetMaxOpenConns(1)
	if pingErr := stamper.PingContext(ctx); pingErr != nil {
		t.Fatalf("ping non-wal stamper: %v", pingErr)
	}
	if _, execErr := stamper.ExecContext(ctx, `CREATE TABLE meta (k TEXT PRIMARY KEY, v TEXT)`); execErr != nil {
		t.Fatalf("seed non-wal file: %v", execErr)
	}
	if closeErr := stamper.Close(); closeErr != nil {
		t.Fatalf("close stamper: %v", closeErr)
	}

	// Reopen WITHOUT any journal pragma, so the file keeps its delete-mode journal, and let
	// NewWriter discover it.
	rw, rwOpenErr := sql.Open(driverName, "file:"+path+"?_txlock=immediate")
	if rwOpenErr != nil {
		t.Fatalf("open rw over non-wal file: %v", rwOpenErr)
	}
	rw.SetMaxOpenConns(1)
	defer func() {
		if closeErr := rw.Close(); closeErr != nil {
			t.Errorf("close rw: %v", closeErr)
		}
	}()
	if pingErr := rw.PingContext(ctx); pingErr != nil {
		t.Fatalf("ping rw: %v", pingErr)
	}
	var journal string
	if scanErr := rw.QueryRowContext(ctx, `PRAGMA journal_mode`).Scan(&journal); scanErr != nil {
		t.Fatalf("read journal back: %v", scanErr)
	}
	if normalizePragmaValue(journal) == "wal" {
		t.Fatalf("setup failed: file stamped DELETE reads back %q", journal)
	}
	_, refuseErr := NewWriter(rw, fakeClock(), Config{})
	if refuseErr == nil {
		t.Fatal("non-WAL file accepted")
	}
	if !errors.Is(refuseErr, ErrPragmaMismatch) || !strings.Contains(refuseErr.Error(), "journal") {
		t.Errorf("non-wal refusal = %v, want ErrPragmaMismatch naming journal_mode", refuseErr)
	}

	// Leg 2: a pool opened demanding FULL, asked to run relaxed — the refusal names both
	// the demanded and the observed value.
	st, _, openErr := Open(ctx, testOptions(testDataDir(t), fakeClock(), &logCapture{}))
	if openErr != nil {
		t.Fatalf("open full store: %v", openErr)
	}
	defer func() {
		if closeErr := st.Close(ctx); closeErr != nil {
			t.Errorf("close full store: %v", closeErr)
		}
	}()
	rwFull, takeErr := st.TakeWriter()
	if takeErr != nil {
		t.Fatalf("take full writer: %v", takeErr)
	}
	defer func() {
		if closeErr := rwFull.Close(); closeErr != nil && !errors.Is(closeErr, sql.ErrConnDone) {
			t.Errorf("close taken rw: %v", closeErr)
		}
	}()
	_, mismatchErr := NewWriter(rwFull, fakeClock(), Config{Durability: DurabilityRelaxed})
	if mismatchErr == nil {
		t.Fatal("relaxed demand on a FULL pool accepted")
	}
	for _, want := range []string{"FULL", "2", "NORMAL", "1"} {
		if !strings.Contains(mismatchErr.Error(), want) {
			t.Errorf("mismatch error %q does not name %q", mismatchErr, want)
		}
	}
}

// TestNewWriterEmitsRelaxedBannerOnce pins the loud-mode rule: constructing a writer over a
// relaxed store logs exactly one WARN banner naming durability=relaxed, and a full writer
// logs none. The banner rides whatever logger the store carries — never suppressible.
func TestNewWriterEmitsRelaxedBannerOnce(t *testing.T) {
	fullHandler := &logCapture{}
	full := newTestWriter(t, DurabilityFull, fullHandler)
	if closeErr := full.Close(context.Background()); closeErr != nil {
		t.Errorf("close full writer: %v", closeErr)
	}
	if got := len(fullHandler.find(slog.LevelWarn, warnRelaxedDurability)); got != 0 {
		t.Errorf("full mode logged %d relaxed banners, want 0", got)
	}

	relaxedHandler := &logCapture{}
	relaxed := newTestWriter(t, DurabilityRelaxed, relaxedHandler)
	if closeErr := relaxed.Close(context.Background()); closeErr != nil {
		t.Errorf("close relaxed writer: %v", closeErr)
	}
	got := relaxedHandler.find(slog.LevelWarn, warnRelaxedDurability)
	if len(got) != 1 {
		t.Fatalf("relaxed banner logged %d times, want exactly 1", len(got))
	}
	var attrs []string
	for _, r := range got {
		r.Attrs(func(a slog.Attr) bool {
			attrs = append(attrs, a.Key+"="+a.Value.String())
			return true
		})
	}
	text := strings.Join(attrs, " ")
	for _, want := range []string{"power loss", "SIGKILL"} {
		if !strings.Contains(text, want) {
			t.Errorf("banner attributes missing %q: %s", want, text)
		}
	}
}

// TestNewWriterRejectsConfigAndModeConflict covers the remaining construction refusals: a
// negative commit window, and a durability demand that contradicts the store it was opened
// with — caught before any pragma is even consulted.
func TestNewWriterRejectsConfigAndModeConflict(t *testing.T) {
	ctx := context.Background()
	opts := testOptions(testDataDir(t), fakeClock(), &logCapture{})
	opts.Durability = DurabilityFull
	st, _, openErr := Open(ctx, opts)
	if openErr != nil {
		t.Fatalf("open: %v", openErr)
	}
	defer func() {
		if closeErr := st.Close(ctx); closeErr != nil {
			t.Errorf("close store: %v", closeErr)
		}
	}()

	if _, windowErr := st.NewWriter(Config{CommitWindow: -time.Second}); windowErr == nil {
		t.Error("negative commit window accepted")
	}
	if _, modeErr := st.NewWriter(Config{Durability: DurabilityRelaxed}); modeErr == nil {
		t.Error("writer demanding relaxed over a full-opened store accepted")
	}
}
