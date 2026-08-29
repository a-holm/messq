// SPDX-License-Identifier: Apache-2.0

package backup

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"

	"github.com/a-holm/messq/internal/queue"
	"github.com/a-holm/messq/internal/store"
)

// openSnapshotRO reopens a finished snapshot read-only for assertions.
func openSnapshotRO(t *testing.T, path string) *sql.DB {
	t.Helper()
	db, err := sql.Open("sqlite", "file:"+path+"?_pragma=query_only(1)")
	if err != nil {
		t.Fatalf("open snapshot ro: %v", err)
	}
	t.Cleanup(func() {
		if closeErr := db.Close(); closeErr != nil {
			t.Logf("close snapshot ro: %v", closeErr)
		}
	})
	if pingErr := db.PingContext(context.Background()); pingErr != nil {
		t.Fatalf("ping snapshot: %v", pingErr)
	}
	return db
}

func snapMeta(t *testing.T, db *sql.DB, key string) (string, bool) {
	t.Helper()
	var v string
	scanErr := db.QueryRowContext(context.Background(),
		`SELECT v FROM meta WHERE k = ?`, key).Scan(&v)
	switch {
	case errors.Is(scanErr, sql.ErrNoRows):
		return "", false
	case scanErr != nil:
		t.Fatalf("read meta[%s]: %v", key, scanErr)
	}
	return v, true
}

func TestRunStampsProvenance(t *testing.T) {
	ctx := context.Background()
	src := seedSource(t, 15)
	dest := filepath.Join(t.TempDir(), "snap.db")

	roSrc, _, openErr := store.Open(ctx, store.Options{DataDir: src, ReadOnly: true})
	if openErr != nil {
		t.Fatalf("open source ro: %v", openErr)
	}
	nodeID := roSrc.NodeID()
	schemaVersion := roSrc.SchemaVersion()
	if nodeID == "" || schemaVersion == 0 {
		t.Fatalf("seed source identity incomplete: node=%q schema=%d", nodeID, schemaVersion)
	}
	if closeErr := roSrc.Close(ctx); closeErr != nil {
		t.Fatalf("close source ro: %v", closeErr)
	}

	res := mustBackup(t, src, dest, Options{})
	db := openSnapshotRO(t, dest)

	if got, ok := snapMeta(t, db, MetaSnapshotSourceNode); !ok || got != nodeID {
		t.Fatalf("meta[%s] = %q,%v want %q", MetaSnapshotSourceNode, got, ok, nodeID)
	}
	if got, ok := snapMeta(t, db, MetaSnapshotTakenAt); !ok || got == "0" || got == "" {
		t.Fatalf("meta[%s] = %q,%v want a unix-ms timestamp", MetaSnapshotTakenAt, got, ok)
	}
	if got, ok := snapMeta(t, db, MetaSnapshotSourcePath); !ok ||
		got != filepath.Join(src, "messq.db") {
		t.Fatalf("meta[%s] = %q,%v want %q", MetaSnapshotSourcePath, got, ok,
			filepath.Join(src, "messq.db"))
	}
	if got, ok := snapMeta(t, db, MetaSnapshotToolVersion); !ok || got == "" {
		t.Fatalf("meta[%s] missing or empty (%v)", MetaSnapshotToolVersion, ok)
	}
	if got, ok := snapMeta(t, db, MetaSnapshotSourceLive); !ok || got != "0" {
		t.Fatalf("meta[%s] = %q,%v want \"0\" with no daemon holding the flock",
			MetaSnapshotSourceLive, got, ok)
	}
	if got, ok := snapMeta(t, db, "clean_shutdown"); !ok || got != "1" {
		t.Fatalf("clean_shutdown = %q,%v — a VACUUM INTO output is consistent by construction "+
			"and must not restore as an unclean crash", got, ok)
	}

	// The result mirrors what was stamped.
	if res.SourceNodeID != nodeID {
		t.Fatalf("result.SourceNodeID = %q, want %q", res.SourceNodeID, nodeID)
	}
	if res.SchemaVersion != schemaVersion {
		t.Fatalf("result.SchemaVersion = %d, want %d", res.SchemaVersion, schemaVersion)
	}
	if len(res.StreamHeads) != 1 || res.StreamHeads["orders"] != 15 {
		t.Fatalf("result.StreamHeads = %v, want orders=15 (15 messages published)", res.StreamHeads)
	}

	// Heads survive as JSON in the snapshot's own meta.
	raw, ok := snapMeta(t, db, MetaSnapshotStreamHeads)
	if !ok {
		t.Fatal("meta[snapshot_stream_heads] missing")
	}
	heads := map[string]int64{}
	if unmarshalErr := json.Unmarshal([]byte(raw), &heads); unmarshalErr != nil {
		t.Fatalf("heads JSON %q: %v", raw, unmarshalErr)
	}
	if heads["orders"] != 15 {
		t.Fatalf("stamped heads = %v, want orders=15", heads)
	}
}

func TestRunStampsTruncatedHeadsBeyondCap(t *testing.T) {
	src := seedSourceNStreams(t, maxStampedStreams+5)
	dest := filepath.Join(t.TempDir(), "snap.db")
	mustBackup(t, src, dest, Options{})

	db := openSnapshotRO(t, dest)
	if _, ok := snapMeta(t, db, MetaSnapshotStreamHeads); ok {
		t.Fatal("snapshot_stream_heads present above the cap; it must be omitted instead")
	}
	if got, ok := snapMeta(t, db, MetaSnapshotHeadsTruncated); !ok || got != "1" {
		t.Fatalf("meta[%s] = %q,%v want \"1\"", MetaSnapshotHeadsTruncated, got, ok)
	}
}

func TestRunStampsHeadsAtExactCap(t *testing.T) {
	src := seedSourceNStreams(t, maxStampedStreams)
	dest := filepath.Join(t.TempDir(), "snap.db")
	mustBackup(t, src, dest, Options{})

	db := openSnapshotRO(t, dest)
	if _, ok := snapMeta(t, db, MetaSnapshotStreamHeads); !ok {
		t.Fatalf("snapshot_stream_heads omitted AT the cap of %d; only above it is omitted", maxStampedStreams)
	}
	if got, ok := snapMeta(t, db, MetaSnapshotHeadsTruncated); ok {
		t.Fatalf("meta[%s] = %q at the cap, want absent", MetaSnapshotHeadsTruncated, got)
	}
}

func TestRunStampsSourceLiveUnderFlockHolder(t *testing.T) {
	src := seedSource(t, 5)
	dest := filepath.Join(t.TempDir(), "snap.db")

	// Simulate the running daemon: hold LOCK_EX on <data-dir>/LOCK for the
	// duration of the backup. Deterministic — no processes, no sleeps.
	lock, openErr := os.OpenFile(filepath.Join(src, "LOCK"), os.O_RDWR, 0o600)
	if openErr != nil {
		t.Fatalf("open LOCK: %v", openErr)
	}
	if lockErr := syscall.Flock(int(lock.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); lockErr != nil {
		t.Fatalf("take exclusive flock: %v", lockErr)
	}
	defer func() {
		_ = syscall.Flock(int(lock.Fd()), syscall.LOCK_UN) //nolint:errcheck // teardown of our own lock
		_ = lock.Close()                                   //nolint:errcheck // teardown
	}()

	mustBackup(t, src, dest, Options{})
	db := openSnapshotRO(t, dest)
	if got, ok := snapMeta(t, db, MetaSnapshotSourceLive); !ok || got != "1" {
		t.Fatalf("meta[%s] = %q,%v want \"1\" while an exclusive holder exists",
			MetaSnapshotSourceLive, got, ok)
	}
}

// mustBackup runs a full Plan+Run and fails the test on any error.
func mustBackup(t *testing.T, src, dest string, o Options) *Result {
	t.Helper()
	o.DataDir = src
	o.Dest = dest
	plan, planErr := Plan(context.Background(), o)
	if planErr != nil {
		t.Fatalf("Plan: %v", planErr)
	}
	res, runErr := Run(context.Background(), plan)
	if runErr != nil {
		t.Fatalf("Run: %v", runErr)
	}
	return res
}

func TestSelfCheckVerifiesFreshSnapshot(t *testing.T) {
	src := seedSource(t, 10)
	dest := filepath.Join(t.TempDir(), "snap.db")
	for _, mode := range []VerifyMode{VerifyQuick, VerifyFull} {
		res := mustBackup(t, src, dest, Options{Force: true, Verify: mode})
		want := "quick_check"
		if mode == VerifyFull {
			want = "integrity_check"
		}
		if res.Verified != want {
			t.Fatalf("Verify mode %d → result.Verified = %q, want %q", mode, res.Verified, want)
		}
	}
	res := mustBackup(t, src, dest, Options{Force: true, Verify: VerifyNone})
	if res.Verified != "skipped" {
		t.Fatalf("VerifyNone → result.Verified = %q, want \"skipped\"", res.Verified)
	}
}

func TestSelfCheckCatchesCorruption(t *testing.T) {
	src := seedSource(t, 30)
	destDir := t.TempDir()

	plan, planErr := Plan(context.Background(), Options{
		DataDir: src, Dest: filepath.Join(destDir, "snap.db"),
	})
	if planErr != nil {
		t.Fatalf("Plan: %v", planErr)
	}
	tmp, mkErr := NewTempDir(destDir)
	if mkErr != nil {
		t.Fatalf("NewTempDir: %v", mkErr)
	}
	t.Cleanup(func() { _ = RemoveTemp(tmp) }) //nolint:errcheck // test cleanup
	snap := filepath.Join(tmp, "snap.db")
	conn, connErr := newSnapshotConn(context.Background(),
		filepath.Join(src, "messq.db"), defaultCacheBytes)
	if connErr != nil {
		t.Fatalf("snapshot conn: %v", connErr)
	}
	if _, execErr := conn.ExecContext(context.Background(), `VACUUM INTO ?`, snap); execErr != nil {
		t.Fatalf("VACUUM INTO: %v", execErr)
	}
	if closeErr := conn.Close(); closeErr != nil {
		t.Fatalf("close conn: %v", closeErr)
	}

	// Sabotage the 100-byte header's page-size field: a parse-breaking flip.
	// (A byte flip deep in the file can land in free/freelist-leaf space, which
	// quick_check deliberately does not verify — the deterministic member of
	// the "corrupted snapshot" family is one the checker cannot open cleanly.)
	raw, readErr := os.ReadFile(snap)
	if readErr != nil {
		t.Fatalf("read snapshot: %v", readErr)
	}
	if len(raw) < 32 {
		t.Fatalf("snapshot implausibly small: %d bytes", len(raw))
	}
	raw[16] = 0x01 // page size becomes 0x0100 = 256 < 512 → invalid
	raw[17] = 0x00
	if writeErr := os.WriteFile(snap, raw, 0o600); writeErr != nil {
		t.Fatalf("write sabotaged snapshot: %v", writeErr)
	}

	expectations := SelfExpectations{
		Verify:       VerifyQuick,
		SchemaVer:    plan.SourceSchemaVersion,
		UserVersion:  plan.SourceUserVersion,
		PageSize:     plan.PageSize,
		AutoVacuum:   plan.SourceAutoVacuum,
		RecordedHead: map[string]int64{"orders": 30},
	}
	err := selfCheck(context.Background(), snap, expectations)
	var failed *SelfCheckError
	if !errors.As(err, &failed) {
		t.Fatalf("selfCheck on a corrupted snapshot = %v, want *SelfCheckError", err)
	}
	if len(failed.Failures) == 0 {
		t.Fatal("SelfCheckError carries no failure lines")
	}
}

func TestSelfCheckCatchesQuickCheckReportedDamage(t *testing.T) {
	src := seedSource(t, 30)
	destDir := t.TempDir()
	tmp, mkErr := NewTempDir(destDir)
	if mkErr != nil {
		t.Fatalf("NewTempDir: %v", mkErr)
	}
	t.Cleanup(func() { _ = RemoveTemp(tmp) }) //nolint:errcheck // test cleanup
	snap := filepath.Join(tmp, "snap.db")
	conn, connErr := newSnapshotConn(context.Background(),
		filepath.Join(src, "messq.db"), defaultCacheBytes)
	if connErr != nil {
		t.Fatalf("snapshot conn: %v", connErr)
	}
	if _, execErr := conn.ExecContext(context.Background(), `VACUUM INTO ?`, snap); execErr != nil {
		t.Fatalf("VACUUM INTO: %v", execErr)
	}
	if closeErr := conn.Close(); closeErr != nil {
		t.Fatalf("close conn: %v", closeErr)
	}

	// Corrupt page 2's first cell-pointer entry (page 2 holds the meta table's
	// root leaf; its cell-pointer array starts right after the 8-byte page
	// header). The header stays valid, so the database OPENS and quick_check
	// must report the damage as output rows, not as a query error.
	raw, readErr := os.ReadFile(snap)
	if readErr != nil {
		t.Fatalf("read snapshot: %v", readErr)
	}
	const pageTwoFirstCellPtr = 4096 + 8 // page 2 starts at 0x1000; +8 skips its leaf-page header
	if len(raw) <= pageTwoFirstCellPtr+1 {
		t.Fatalf("snapshot too small for two pages: %d bytes", len(raw))
	}
	raw[pageTwoFirstCellPtr] = 0xFF // cell offset now points far past the page
	if writeErr := os.WriteFile(snap, raw, 0o600); writeErr != nil {
		t.Fatalf("write sabotaged snapshot: %v", writeErr)
	}

	err := selfCheck(context.Background(), snap, SelfExpectations{Verify: VerifyQuick})
	var failed *SelfCheckError
	if !errors.As(err, &failed) {
		t.Fatalf("selfCheck on structurally damaged snapshot = %v, want *SelfCheckError", err)
	}
	joined := strings.Join(failed.Failures, "; ")
	if !strings.Contains(joined, "quick_check") {
		t.Fatalf("failures %q do not name quick_check", joined)
	}
}

func TestSelfCheckCatchesProvenanceDrift(t *testing.T) {
	src := seedSource(t, 5)
	dest := filepath.Join(t.TempDir(), "snap.db")
	mustBackup(t, src, dest, Options{})

	// Sabotage the user_version mirror: schema drift between source and copy.
	drifty, openErr := sql.Open("sqlite", "file:"+dest)
	if openErr != nil {
		t.Fatalf("open drifty: %v", openErr)
	}
	if _, execErr := drifty.ExecContext(context.Background(), `PRAGMA user_version = 99`); execErr != nil {
		t.Fatalf("sabotage user_version: %v", execErr)
	}
	if closeErr := drifty.Close(); closeErr != nil {
		t.Fatalf("close drifty: %v", closeErr)
	}

	err := selfCheck(context.Background(), dest, SelfExpectations{
		Verify:       VerifyQuick,
		SchemaVer:    2,
		UserVersion:  2,
		PageSize:     4096,
		AutoVacuum:   2,
		RecordedHead: map[string]int64{},
	})
	var failed *SelfCheckError
	if !errors.As(err, &failed) {
		t.Fatalf("selfCheck with drifted user_version = %v, want *SelfCheckError", err)
	}
	joined := strings.Join(failed.Failures, "; ")
	if !strings.Contains(joined, "user_version") {
		t.Fatalf("failure names %q, want it to name user_version", joined)
	}
}

func TestSelfCheckCatchesMissingTail(t *testing.T) {
	src := seedSource(t, 20)
	dest := filepath.Join(t.TempDir(), "snap.db")
	mustBackup(t, src, dest, Options{})

	err := selfCheck(context.Background(), dest, SelfExpectations{
		Verify:       VerifyQuick,
		SchemaVer:    2,
		UserVersion:  2,
		PageSize:     4096,
		AutoVacuum:   2,
		RecordedHead: map[string]int64{"orders": 999}, // head from AFTER the snapshot
	})
	var failed *SelfCheckError
	if !errors.As(err, &failed) {
		t.Fatalf("selfCheck with an impossible head = %v, want *SelfCheckError", err)
	}
	joined := strings.Join(failed.Failures, "; ")
	if !strings.Contains(joined, "orders") {
		t.Fatalf("failure names %q, want the offending stream", joined)
	}
}

func TestRunRemovesTempWhenSelfCheckFails(t *testing.T) {
	src := seedSource(t, 5)
	destDir := t.TempDir()

	// A recorded head the snapshot cannot possibly satisfy forces the
	// self-check red. Injected through the seam Run uses for expectations.
	srcPath := filepath.Join(src, "messq.db")
	tmp, mkErr := NewTempDir(destDir)
	if mkErr != nil {
		t.Fatalf("NewTempDir: %v", mkErr)
	}
	conn, connErr := newSnapshotConn(context.Background(), srcPath, defaultCacheBytes)
	if connErr != nil {
		t.Fatalf("conn: %v", connErr)
	}
	snap := filepath.Join(tmp, "snap.db")
	if _, execErr := conn.ExecContext(context.Background(), `VACUUM INTO ?`, snap); execErr != nil {
		t.Fatalf("vacuum: %v", execErr)
	}
	if closeErr := conn.Close(); closeErr != nil {
		t.Fatalf("close: %v", closeErr)
	}
	expectations := SelfExpectations{Verify: VerifyQuick, RecordedHead: map[string]int64{"orders": 10_000}}
	err := selfCheck(context.Background(), snap, expectations)
	if err == nil {
		t.Fatal("selfCheck passed an impossible expectation")
	}
}

func seedSourceNStreams(t *testing.T, n int) string {
	t.Helper()
	dir := filepath.Join(t.TempDir(), "data")
	st, _, openErr := store.Open(context.Background(), store.Options{DataDir: dir})
	if openErr != nil {
		t.Fatalf("open seed store: %v", openErr)
	}
	for i := 0; i < n; i++ {
		name := fmt.Sprintf("stream%03d", i)
		if _, _, crErr := st.CreateStream(context.Background(),
			queue.DefaultConfig(name), name); crErr != nil {
			t.Fatalf("create %s: %v", name, crErr)
		}
		if _, pubErr := st.PublishBatch(context.Background(),
			store.BatchCmd{Stream: name, Reqs: []queue.PublishReq{
				{Subject: name + ".evt", Body: []byte("x")},
			}}); pubErr != nil {
			t.Fatalf("publish %s: %v", name, pubErr)
		}
	}
	if closeErr := st.Close(context.Background()); closeErr != nil {
		t.Fatalf("close: %v", closeErr)
	}
	return dir
}
