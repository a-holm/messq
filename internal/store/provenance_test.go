// SPDX-License-Identifier: Apache-2.0

package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/a-holm/messq/internal/clock"
)

// openWithStoreDir creates a data directory with an initialized (migrated,
// closed) messq.db inside and returns its path.
func openWithStoreDir(t *testing.T) string {
	t.Helper()
	dir := filepath.Join(t.TempDir(), "data")
	st, _, err := Open(context.Background(), Options{DataDir: dir})
	if err != nil {
		t.Fatalf("initialize seed store: %v", err)
	}
	if closeErr := st.Close(context.Background()); closeErr != nil {
		t.Fatalf("close seed store: %v", closeErr)
	}
	return dir
}

// mustWriter takes the store's single writer handle.
func mustWriter(t *testing.T, st *Store) *sql.DB {
	t.Helper()
	rw, err := st.TakeWriter()
	if err != nil {
		t.Fatalf("take writer: %v", err)
	}
	return rw
}

// plantSnapshotProvenance writes the snapshot_* rows a messq backup stamps
// (issue #30 §4) into a CLOSED data directory's meta table via a raw handle.
func plantSnapshotProvenance(t *testing.T, dataDir string, takenAt time.Time, heads map[string]int64) {
	t.Helper()
	path := filepath.Join(dataDir, "messq.db")
	db, err := sql.Open("sqlite", "file:"+path+"?_pragma=journal_mode(DELETE)")
	if err != nil {
		t.Fatalf("open %s raw: %v", path, err)
	}
	defer func() {
		if closeErr := db.Close(); closeErr != nil {
			t.Fatalf("close raw handle: %v", closeErr)
		}
	}()
	raw, marshalErr := json.Marshal(heads)
	if marshalErr != nil {
		t.Fatalf("marshal heads: %v", marshalErr)
	}
	sets := [][2]string{
		{"snapshot_taken_at", strconv.FormatInt(takenAt.UnixMilli(), 10)},
		{"snapshot_source_node", "01HTESTNODEID0000000000TEST"},
		{"snapshot_source_path", "/var/backups/snap.db"},
		{"snapshot_tool_version", "v9.9.9+deadbee"},
		{"snapshot_source_live", "1"},
		{"snapshot_stream_heads", string(raw)},
	}
	for _, kv := range sets {
		if _, execErr := db.ExecContext(context.Background(),
			`INSERT INTO meta (k, v) VALUES (?, ?)
			 ON CONFLICT (k) DO UPDATE SET v = excluded.v`, kv[0], kv[1]); execErr != nil {
			t.Fatalf("plant %s: %v", kv[0], execErr)
		}
	}
}

func TestOpenConvertsSnapshotProvenance(t *testing.T) {
	ctx := context.Background()
	dir := openWithStoreDir(t)
	snapTime := time.Date(2026, 11, 4, 2, 0, 11, 0, time.UTC)
	heads := map[string]int64{"orders": 1057201, "jobs": 88214}

	plantSnapshotProvenance(t, dir, snapTime, heads)

	fake := clock.NewFake(time.Date(2026, 11, 5, 9, 0, 0, 0, time.UTC))
	st, report, openErr := Open(ctx, Options{DataDir: dir, Clock: fake})
	if openErr != nil {
		t.Fatalf("Open with planted snapshot provenance: %v", openErr)
	}
	t.Cleanup(func() {
		if closeErr := st.Close(ctx); closeErr != nil {
			t.Logf("cleanup close: %v", closeErr)
		}
	})

	if report.Restored == nil {
		t.Fatal("RecoveryReport.Restored is nil; a restored data dir must announce itself")
	}
	got := st.Provenance()
	if got == nil {
		t.Fatal("Store.Provenance() = nil after restore detection")
	}
	if !got.SnapshotAt.Equal(snapTime.Truncate(time.Millisecond)) &&
		got.SnapshotAt.UnixMilli() != snapTime.UnixMilli() {
		t.Fatalf("Provenance.SnapshotAt = %v, want %v", got.SnapshotAt, snapTime)
	}
	if got.SourceNodeID != "01HTESTNODEID0000000000TEST" {
		t.Fatalf("Provenance.SourceNodeID = %q", got.SourceNodeID)
	}
	if got.StreamHeads["orders"] != 1057201 || got.StreamHeads["jobs"] != 88214 {
		t.Fatalf("Provenance.StreamHeads = %v", got.StreamHeads)
	}
	if got.ToolVersion != "v9.9.9+deadbee" {
		t.Fatalf("Provenance.ToolVersion = %q", got.ToolVersion)
	}
	if got.RestoredAt.IsZero() {
		t.Fatal("Provenance.RestoredAt is zero")
	}

	// The conversion rewrote the keys: restored_* present, snapshot_* gone.
	rw := mustWriter(t, st)
	for _, key := range []string{
		"restored_at", "restored_from_node", "restored_snapshot_at",
		"restored_stream_heads", "restored_tool_version",
	} {
		if _, ok, readErr := readMeta(ctx, rw, key); readErr != nil || !ok {
			t.Fatalf("meta[%s] missing after conversion (err=%v)", key, readErr)
		}
	}
	for _, key := range []string{
		"snapshot_taken_at", "snapshot_source_node", "snapshot_stream_heads",
	} {
		if _, ok, readErr := readMeta(ctx, rw, key); readErr != nil || ok {
			t.Fatalf("meta[%s] still present after conversion (ok=%v err=%v)", key, ok, readErr)
		}
	}

	// One admin.action row narrates the detection — no new event verb (§9.2).
	var detail string
	evErr := rw.QueryRowContext(ctx,
		`SELECT detail FROM events WHERE event = 'admin.action'
		 ORDER BY id DESC LIMIT 1`).Scan(&detail)
	if evErr != nil {
		t.Fatalf("read admin.action event: %v", evErr)
	}
	if !strings.Contains(detail, "restore_detected") {
		t.Fatalf("admin.action detail = %q, want action restore_detected", detail)
	}
}

func TestSecondStartDoesNotReannounceRestore(t *testing.T) {
	ctx := context.Background()
	dir := openWithStoreDir(t)
	plantSnapshotProvenance(t, dir, time.Date(2026, 11, 4, 2, 0, 11, 0, time.UTC),
		map[string]int64{"orders": 7})

	st, report, openErr := Open(ctx, Options{DataDir: dir})
	if openErr != nil {
		t.Fatalf("first Open: %v", openErr)
	}
	if report.Restored == nil {
		t.Fatal("first start did not detect the restore")
	}
	if closeErr := st.Close(ctx); closeErr != nil {
		t.Fatalf("first Close: %v", closeErr)
	}

	// Second restart: nothing left to convert, so no re-announcement — but the
	// permanent record still answers Provenance() ("survives forever").
	st2, report2, secondErr := Open(ctx, Options{DataDir: dir})
	if secondErr != nil {
		t.Fatalf("second Open: %v", secondErr)
	}
	t.Cleanup(func() {
		if closeErr := st2.Close(ctx); closeErr != nil {
			t.Logf("cleanup close: %v", closeErr)
		}
	})
	if report2.Restored != nil {
		t.Fatalf("second start re-announced the restore (%+v); conversion must be one-shot",
			report2.Restored)
	}
	if st2.Provenance() == nil {
		t.Fatal("Provenance() lost after restart; restored_* rows must survive forever")
	}
}

func TestProvenanceNilWhenNeverRestored(t *testing.T) {
	dir := openWithStoreDir(t)
	st, report, openErr := Open(context.Background(), Options{DataDir: dir})
	if openErr != nil {
		t.Fatalf("Open: %v", openErr)
	}
	t.Cleanup(func() {
		if closeErr := st.Close(context.Background()); closeErr != nil {
			t.Logf("cleanup close: %v", closeErr)
		}
	})
	if report.Restored != nil || st.Provenance() != nil {
		t.Fatalf("fresh dir reported restoration (%+v / %+v)", report.Restored, st.Provenance())
	}
}

func TestCorruptSnapshotProvenanceRefusesStartup(t *testing.T) {
	dir := openWithStoreDir(t)
	plantSnapshotProvenance(t, dir, time.Now(), map[string]int64{"orders": 1})

	// Sabotage the timestamp: not a number.
	path := filepath.Join(dir, "messq.db")
	db, openErr := sql.Open("sqlite", "file:"+path)
	if openErr != nil {
		t.Fatalf("raw open: %v", openErr)
	}
	if _, execErr := db.ExecContext(context.Background(),
		`UPDATE meta SET v = 'not-a-number' WHERE k = 'snapshot_taken_at'`); execErr != nil {
		t.Fatalf("sabotage: %v", execErr)
	}
	if closeErr := db.Close(); closeErr != nil {
		t.Fatalf("close saboteur: %v", closeErr)
	}

	_, _, openErr2 := Open(context.Background(), Options{DataDir: dir})
	if openErr2 == nil {
		t.Fatal("Open accepted garbage snapshot_taken_at; a mystery tail is what this feature forbids")
	}
	if !errors.Is(openErr2, ErrCorrupt) && filepath.Dir(path) == "" {
		t.Fatalf("refusal should be a corruption-class error, got %v", openErr2)
	}
}
