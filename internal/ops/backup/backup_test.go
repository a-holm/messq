// SPDX-License-Identifier: Apache-2.0

package backup

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/a-holm/messq/internal/clock"
	"github.com/a-holm/messq/internal/queue"
	"github.com/a-holm/messq/internal/store"
)

// seedSource builds a real, closed data directory holding one stream with a
// few published messages — the smallest thing VACUUM INTO can snapshot
// meaningfully. Real SQLite in t.TempDir(), never :memory: (§11 ground rules).
// The store enforces a 0700 data dir and t.TempDir() is not guaranteed to be
// one, so the data dir is a subdirectory the store creates (house pattern).
func seedSource(t *testing.T, msgs int) string {
	t.Helper()
	dir := filepath.Join(t.TempDir(), "data")
	st, _, openErr := store.Open(context.Background(), store.Options{DataDir: dir})
	if openErr != nil {
		t.Fatalf("open seed store: %v", openErr)
	}
	if _, _, crErr := st.CreateStream(context.Background(),
		queue.DefaultConfig("orders"), "orders"); crErr != nil {
		t.Fatalf("create stream: %v", crErr)
	}
	for from := 0; from < msgs; from += 10 {
		n := min(10, msgs-from)
		reqs := make([]queue.PublishReq, n)
		for i := range reqs {
			reqs[i] = queue.PublishReq{Subject: "orders.created", Body: []byte("payload")}
		}
		if _, pubErr := st.PublishBatch(context.Background(),
			store.BatchCmd{Stream: "orders", Reqs: reqs}); pubErr != nil {
			t.Fatalf("publish: %v", pubErr)
		}
	}
	if closeErr := st.Close(context.Background()); closeErr != nil {
		t.Fatalf("close seed store: %v", closeErr)
	}
	return dir
}

func dbHash(t *testing.T, dataDir string) []byte {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join(dataDir, "messq.db"))
	if err != nil {
		t.Fatalf("read source db: %v", err)
	}
	return raw
}

func TestPlanAndRunProducesSnapshot(t *testing.T) {
	ctx := context.Background()
	// Pin the umask so the 0600 assertion bites on any host: with a permissive
	// umask the un-chmod'd VACUUM INTO output would leak group/other bits.
	oldUmask := syscall.Umask(0o022)
	defer syscall.Umask(oldUmask)

	src := seedSource(t, 20)
	dest := filepath.Join(t.TempDir(), "snap.db")

	plan, err := Plan(ctx, Options{DataDir: src, Dest: dest})
	if err != nil {
		t.Fatalf("Plan: %v", err)
	}
	if plan.EstimateBytes <= 0 {
		t.Fatalf("plan estimate = %d, want pages×page_size > 0 for a seeded database", plan.EstimateBytes)
	}
	res, runErr := Run(ctx, plan)
	if runErr != nil {
		t.Fatalf("Run: %v", runErr)
	}
	if res.Dest != dest {
		t.Fatalf("result dest = %q, want %q", res.Dest, dest)
	}

	info, statErr := os.Stat(dest)
	if statErr != nil {
		t.Fatalf("snapshot missing after Run: %v", statErr)
	}
	if got := info.Mode().Perm(); got != 0o600 {
		t.Fatalf("snapshot mode = %o, want 600 (payloads are cleartext, D12)", got)
	}
	if res.Bytes != info.Size() || res.Bytes <= 0 {
		t.Fatalf("result bytes = %d, stat = %d, want equal and positive", res.Bytes, info.Size())
	}
	if res.Pages <= 0 {
		t.Fatalf("result pages = %d, want the copied page count", res.Pages)
	}
	if res.TakenAt.IsZero() {
		t.Fatal("result TakenAt is zero; provenance needs a timestamp")
	}

	// The snapshot is a real SQLite database holding the seeded messages.
	snapDB, openErr := os.Open(dest)
	if openErr != nil {
		t.Fatalf("reopen snapshot: %v", openErr)
	}
	header := make([]byte, 16)
	if _, readErr := snapDB.Read(header); readErr != nil {
		t.Fatalf("read snapshot header: %v", readErr)
	}
	if closeErr := snapDB.Close(); closeErr != nil {
		t.Fatalf("close snapshot: %v", closeErr)
	}
	if string(header) != "SQLite format 3\x00" {
		t.Fatalf("snapshot header %q, want a SQLite database", header)
	}
}

// TestBackupLeavesSourceUntouched is G2: the source is never modified by a
// backup — byte-for-byte and mtime.
func TestBackupLeavesSourceUntouched(t *testing.T) {
	ctx := context.Background()
	src := seedSource(t, 10)
	dest := filepath.Join(t.TempDir(), "snap.db")

	before := dbHash(t, src)
	infoBefore, statErr := os.Stat(filepath.Join(src, "messq.db"))
	if statErr != nil {
		t.Fatalf("stat source: %v", statErr)
	}

	plan, planErr := Plan(ctx, Options{DataDir: src, Dest: dest})
	if planErr != nil {
		t.Fatalf("Plan: %v", planErr)
	}
	if _, runErr := Run(ctx, plan); runErr != nil {
		t.Fatalf("Run: %v", runErr)
	}

	after := dbHash(t, src)
	infoAfter, statErr := os.Stat(filepath.Join(src, "messq.db"))
	if statErr != nil {
		t.Fatalf("stat source after backup: %v", statErr)
	}
	if string(before) != string(after) {
		t.Fatal("the backup modified the source database bytes")
	}
	if !infoAfter.ModTime().Equal(infoBefore.ModTime()) {
		t.Fatalf("source mtime moved %v -> %v during a backup",
			infoBefore.ModTime(), infoAfter.ModTime())
	}
}

func TestSnapshotConnectionPragmas(t *testing.T) {
	dsn := snapshotDSN("/data/messq.db", 16<<20)
	for _, want := range []string{
		"_pragma=query_only(0)",    // VACUUM may refuse under the pooled query_only(1)
		"_pragma=temp_store(FILE)", // index-rebuild scratch must not OOM (§4.1)
		"cache_size(-16384)",       // 16 MiB bounded RSS
	} {
		if !strings.Contains(dsn, want) {
			t.Fatalf("snapshot DSN %q lacks %q", dsn, want)
		}
	}
	if strings.Contains(dsn, "mode=ro") {
		t.Fatalf("snapshot DSN %q must not be mode=ro: VACUUM INTO writes the OUTPUT file", dsn)
	}
}

func TestRefusalOrdering(t *testing.T) {
	ctx := context.Background()

	t.Run("existing destination without force refuses with exit-4 type", func(t *testing.T) {
		src := seedSource(t, 5)
		destDir := t.TempDir()
		dest := filepath.Join(destDir, "snap.db")
		if writeErr := os.WriteFile(dest, []byte("previous backup"), 0o600); writeErr != nil {
			t.Fatalf("seed dest: %v", writeErr)
		}
		_, err := Plan(ctx, Options{DataDir: src, Dest: dest})
		var exists *DestinationExistsError
		if !errors.As(err, &exists) {
			t.Fatalf("Plan = %v (%T), want *DestinationExistsError", err, err)
		}
		if exists.Path != dest {
			t.Fatalf("refusal names %q, want %q", exists.Path, dest)
		}
	})

	t.Run("--force overwrites an existing destination", func(t *testing.T) {
		src := seedSource(t, 5)
		dest := filepath.Join(t.TempDir(), "snap.db")
		if writeErr := os.WriteFile(dest, []byte("previous"), 0o600); writeErr != nil {
			t.Fatalf("seed dest: %v", writeErr)
		}
		plan, planErr := Plan(ctx, Options{DataDir: src, Dest: dest, Force: true})
		if planErr != nil {
			t.Fatalf("Plan with Force: %v", planErr)
		}
		if _, runErr := Run(context.Background(), plan); runErr != nil {
			t.Fatalf("Run with Force: %v", runErr)
		}
		got, readErr := os.ReadFile(dest)
		if readErr != nil || string(got)[:7] == "previou" {
			t.Fatalf("destination still holds the old backup (%q, %v)", got, readErr)
		}
	})

	t.Run("destination inside the data dir refuses with exit-2 type", func(t *testing.T) {
		src := seedSource(t, 5)
		_, err := Plan(ctx, Options{
			DataDir: src,
			Dest:    filepath.Join(src, "snap.db"),
		})
		var inside *InsideDataDirError
		if !errors.As(err, &inside) {
			t.Fatalf("Plan = %v (%T), want *InsideDataDirError", err, inside)
		}
	})

	t.Run("inside-data-dir is caught even below sibling-named dirs", func(t *testing.T) {
		src := seedSource(t, 5)
		_, err := Plan(ctx, Options{
			DataDir: src,
			Dest:    filepath.Join(src, "sub", "snap.db"),
		})
		var inside *InsideDataDirError
		if !errors.As(err, &inside) {
			t.Fatalf("Plan = %v, want *InsideDataDirError for a nested path", err)
		}
	})

	t.Run("a lookalike sibling directory is NOT inside the data dir", func(t *testing.T) {
		src := seedSource(t, 5)
		sibling := filepath.Dir(src) // different name guaranteed by TempDir
		if sibling == src {
			t.Skip("tempdir layout degenerate")
		}
		if _, planErr := Plan(ctx, Options{
			DataDir: src,
			Dest:    filepath.Join(sibling, "elsewhere-snap.db"),
		}); planErr != nil {
			t.Fatalf("Plan refused a legitimate sibling destination: %v", planErr)
		}
	})

	t.Run("unwritable destination directory refuses with exit-7 type", func(t *testing.T) {
		if os.Geteuid() == 0 {
			t.Skip("root writes anywhere; the permission refusal cannot fire")
		}
		src := seedSource(t, 5)
		destDir := t.TempDir()
		if chErr := os.Chmod(destDir, 0o500); chErr != nil {
			t.Fatalf("ro-mode dest dir: %v", chErr)
		}
		t.Cleanup(func() { _ = os.Chmod(destDir, 0o700) }) //nolint:errcheck // restore so TempDir cleanup works
		_, err := Plan(ctx, Options{DataDir: src, Dest: filepath.Join(destDir, "snap.db")})
		var denied *NotWritableError
		if !errors.As(err, &denied) {
			t.Fatalf("Plan = %v (%T), want *NotWritableError", err, denied)
		}
	})

	t.Run("source failure wins: nothing about the destination is consulted", func(t *testing.T) {
		missing := filepath.Join(t.TempDir(), "no-such-datadir")
		destDir := t.TempDir()
		dest := filepath.Join(destDir, "snap.db")
		if writeErr := os.WriteFile(dest, []byte("x"), 0o600); writeErr != nil {
			t.Fatalf("seed dest: %v", writeErr)
		}
		_, err := Plan(ctx, Options{DataDir: missing, Dest: dest})
		var exists *DestinationExistsError
		if errors.As(err, &exists) {
			t.Fatalf("destination refusal fired before the source was even opened: %v", err)
		}
		if err == nil {
			t.Fatal("Plan accepted a nonexistent source data dir")
		}
	})

	t.Run("dash destination is a usage error", func(t *testing.T) {
		src := seedSource(t, 5)
		_, err := Plan(ctx, Options{DataDir: src, Dest: "-"})
		if err == nil || !errors.Is(err, ErrUsage) {
			t.Fatalf("Plan(-) = %v, want an ErrUsage-wrapped refusal: stdout streaming is not a backup", err)
		}
	})

	t.Run("empty destination is a usage error", func(t *testing.T) {
		src := seedSource(t, 5)
		_, err := Plan(ctx, Options{DataDir: src, Dest: ""})
		if err == nil || !errors.Is(err, ErrUsage) {
			t.Fatalf("Plan(\"\") = %v, want ErrUsage", err)
		}
	})
}

func TestRunCleansTempOnCanceledContext(t *testing.T) {
	src := seedSource(t, 5)
	destDir := t.TempDir()

	plan, planErr := Plan(context.Background(), Options{
		DataDir: src, Dest: filepath.Join(destDir, "snap.db"),
	})
	if planErr != nil {
		t.Fatalf("Plan: %v", planErr)
	}

	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	_, runErr := Run(canceled, plan)
	if runErr == nil {
		t.Fatal("Run on a canceled context succeeded; the vacuum must honour cancellation")
	}
	if !errors.Is(runErr, context.Canceled) {
		t.Fatalf("Run error = %v, want context.Canceled preserved for the CLI", runErr)
	}
	entries, readErr := os.ReadDir(destDir)
	if readErr != nil {
		t.Fatalf("read dest dir: %v", readErr)
	}
	for _, e := range entries {
		if len(e.Name()) > len(tempPrefix) && e.Name()[:len(tempPrefix)] == tempPrefix {
			t.Fatalf("canceled run left %s behind; every failure path removes the temp dir", e.Name())
		}
	}
	if _, statErr := os.Stat(filepath.Join(destDir, "snap.db")); statErr == nil {
		t.Fatal("canceled run produced a destination; rename must never run on failure")
	}
}

func TestRunSweepsStaleTempsOnSuccess(t *testing.T) {
	ctx := context.Background()
	src := seedSource(t, 5)
	destDir := t.TempDir()

	stale := filepath.Join(destDir, tempPrefix+"killedrun")
	if mkErr := os.MkdirAll(stale, 0o700); mkErr != nil {
		t.Fatalf("seed stale temp: %v", mkErr)
	}
	old := clock.System{}.Now().Add(-2 * time.Hour)
	if chtErr := os.Chtimes(stale, old, old); chtErr != nil {
		t.Fatalf("age the stale temp: %v", chtErr)
	}

	plan, planErr := Plan(ctx, Options{DataDir: src, Dest: filepath.Join(destDir, "snap.db")})
	if planErr != nil {
		t.Fatalf("Plan: %v", planErr)
	}
	res, runErr := Run(ctx, plan)
	if runErr != nil {
		t.Fatalf("Run: %v", runErr)
	}
	if res.Swept != 1 {
		t.Fatalf("result.Swept = %d, want 1 stale temp removed by the successful run", res.Swept)
	}
	if _, statErr := os.Stat(stale); !errors.Is(statErr, os.ErrNotExist) {
		t.Fatalf("stale temp survived a successful run (stat err = %v)", statErr)
	}
}

func TestPlanEstimateReflectsFreelistExclusion(t *testing.T) {
	ctx := context.Background()
	src := seedSource(t, 50)
	dest := filepath.Join(t.TempDir(), "snap.db")

	ro, _, openErr := store.Open(ctx, store.Options{DataDir: src, ReadOnly: true})
	if openErr != nil {
		t.Fatalf("open source ro: %v", openErr)
	}
	var pageCount, freelist, pageSize int64
	if scanErr := ro.RO().QueryRowContext(ctx, "SELECT (SELECT page_count FROM pragma_page_count), (SELECT freelist_count FROM pragma_freelist_count), (SELECT page_size FROM pragma_page_size)").Scan(&pageCount, &freelist, &pageSize); scanErr != nil {
		t.Fatalf("read source pragmas: %v", scanErr)
	}
	if closeErr := ro.Close(ctx); closeErr != nil {
		t.Fatalf("close ro: %v", closeErr)
	}

	plan, planErr := Plan(ctx, Options{DataDir: src, Dest: dest})
	if planErr != nil {
		t.Fatalf("Plan: %v", planErr)
	}
	want := EstimateBytes(pageCount, freelist, pageSize)
	if plan.EstimateBytes != want {
		t.Fatalf("plan estimate = %d, want (page_count−freelist)×page_size = %d",
			plan.EstimateBytes, want)
	}
	// The destination filesystem is shared with everything else on the box, so
	// the two statfs readings cannot be required to match exactly; what must
	// hold is that Plan reported a real reading close to a fresh one.
	free2, freeErr := FreeBytes(filepath.Dir(dest))
	if freeErr != nil {
		t.Fatalf("statfs dest dir again: %v", freeErr)
	}
	drift := plan.FreeBytes - free2
	if drift < 0 {
		drift = -drift
	}
	const slack = int64(256) << 20 // 256 MiB of concurrent-write tolerance
	if drift > slack {
		t.Fatalf("plan free bytes %d vs fresh reading %d differ by more than %d",
			plan.FreeBytes, free2, slack)
	}
}
