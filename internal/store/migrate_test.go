// SPDX-License-Identifier: Apache-2.0

package store

import (
	"context"
	"database/sql"
	"errors"
	"flag"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"testing/fstest"
	"time"

	"github.com/a-holm/messq/internal/clock"
	"github.com/a-holm/messq/internal/errs"
	"github.com/google/go-cmp/cmp"
	_ "modernc.org/sqlite"
)

// updateGolden regenerates testdata/schema_v1.golden from a fresh migration instead of
// comparing against it:
//
//	go test ./internal/store -run TestSchemaGolden -update-golden
//
// The golden file is the frozen shape of schema v1 (PLAN §4.2); regenerate only when a
// reviewed schema change is intentional, never to make a failing test pass.
var updateGolden = flag.Bool("update-golden", false, "rewrite testdata/schema_v1.golden from a fresh migration")

// stepClock is a fake clock whose Now advances a fixed step on every call. The advance is
// the point: a timestamp that would change if migrate rewrote schema_applied_at provably
// differs from the one recorded on the previous run, so a no-op reopen cannot hide behind a
// coarse real-time clock.
type stepClock struct {
	clock.System
	mu  sync.Mutex
	now time.Time
}

func newStepClock(start time.Time) *stepClock {
	return &stepClock{now: start.UTC()}
}

func (c *stepClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	t := c.now
	c.now = c.now.Add(time.Second)
	return t
}

// openTestDB opens a real database file (never :memory: — the migration ladder has to run
// against the same on-disk format production uses) and closes it at cleanup. The handle
// goes through the package's registered driver so the suite runs unchanged under both
// build tags (driver_modernc.go and driver_cgo.go).
func openTestDB(t *testing.T, path string) *sql.DB {
	t.Helper()
	db, err := sql.Open(driverName, path)
	if err != nil {
		t.Fatalf("open %s: %v", path, err)
	}
	t.Cleanup(func() {
		if cerr := db.Close(); cerr != nil {
			t.Errorf("close %s: %v", path, cerr)
		}
	})
	return db
}

// migrateFresh runs the ladder against a brand-new file and asserts the fresh-open shape so
// every other test can assume a migrated v1 database.
func migrateFresh(t *testing.T, path string, clk clock.Clock) (from, to int) {
	t.Helper()
	db := openTestDB(t, path)
	from, to, err := migrate(context.Background(), db, clk)
	if err != nil {
		t.Fatalf("migrate fresh %s: %v", path, err)
	}
	return from, to
}

// queryInt runs a single-value query that must yield an integer.
func queryInt(t *testing.T, db *sql.DB, q string) int {
	t.Helper()
	var n int
	if err := db.QueryRowContext(context.Background(), q).Scan(&n); err != nil {
		t.Fatalf("query %q: %v", q, err)
	}
	return n
}

// metaValue reads one meta row as text, failing if the key is absent.
func metaValue(t *testing.T, db *sql.DB, k string) string {
	t.Helper()
	var v string
	if err := db.QueryRowContext(context.Background(), `SELECT v FROM meta WHERE k = ?`, k).Scan(&v); err != nil {
		t.Fatalf("meta[%q]: %v", k, err)
	}
	return v
}

// dumpSchema renders sqlite_schema as deterministic text: one `type|name|sql` line per
// object, ordered by name (then type for stability), NULL sql rendered as <NULL>. The
// auto-indexes SQLite creates for rowid-table primary keys are part of the frozen shape and
// appear alongside the hand-written objects.
func dumpSchema(t *testing.T, db *sql.DB) string {
	t.Helper()
	rows, err := db.QueryContext(context.Background(),
		`SELECT type, name, sql FROM sqlite_schema ORDER BY name, type`)
	if err != nil {
		t.Fatalf("query sqlite_schema: %v", err)
	}
	defer func() {
		if cerr := rows.Close(); cerr != nil {
			t.Errorf("close schema rows: %v", cerr)
		}
	}()
	var b strings.Builder
	for rows.Next() {
		var typ, name string
		var create sql.NullString
		if err := rows.Scan(&typ, &name, &create); err != nil {
			t.Fatalf("scan sqlite_schema row: %v", err)
		}
		sqlText := "<NULL>"
		if create.Valid {
			sqlText = create.String
		}
		b.WriteString(typ + "|" + name + "|" + sqlText + "\n")
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate sqlite_schema: %v", err)
	}
	return b.String()
}

// TestSchemaGolden pins the exact schema a fresh migration produces against
// testdata/schema_v1.golden. This is the byte-level guarantee behind the frozen-artefact
// rule: any edit to 0001_init.sql — a dropped index, a changed column, a reformat — changes
// the dump and fails here until the golden is deliberately regenerated.
func TestSchemaGolden(t *testing.T) {
	dir := t.TempDir()
	migrateFresh(t, filepath.Join(dir, dbFileName), newStepClock(time.Unix(1700000000, 0)))
	got := dumpSchema(t, openTestDB(t, filepath.Join(dir, dbFileName)))

	const goldenPath = "testdata" + string(os.PathSeparator) + "schema_v1.golden"
	if *updateGolden {
		if err := os.MkdirAll(filepath.Dir(goldenPath), 0o755); err != nil {
			t.Fatalf("create testdata dir: %v", err)
		}
		if err := os.WriteFile(goldenPath, []byte(got), 0o644); err != nil {
			t.Fatalf("write golden: %v", err)
		}
		return
	}
	want, err := os.ReadFile(goldenPath)
	if err != nil {
		t.Fatalf("read golden (run with -update-golden to create it): %v", err)
	}
	if diff := cmp.Diff(string(want), got); diff != "" {
		t.Fatalf("schema differs from golden (-golden +live):\n%s", diff)
	}
}

// TestMigrateFreshCreatesSchemaV1 covers the fresh path end to end: the ladder runs from 0
// to the top, exactly one migration is embedded, and the version pair reads (0, 1).
func TestMigrateFreshCreatesSchemaV1(t *testing.T) {
	from, to := migrateFresh(t, filepath.Join(t.TempDir(), dbFileName), newStepClock(time.Unix(1700000000, 0)))
	if from != 0 || to != 1 {
		t.Fatalf("migrate fresh = (%d, %d), want (0, 1)", from, to)
	}
	if len(embeddedMigrations) != 1 {
		t.Fatalf("len(embeddedMigrations) = %d, want 1", len(embeddedMigrations))
	}
}

// TestMigrateFreshBookkeeping asserts the meta rows and the user_version mirror on the very
// file migrateFresh produced.
func TestMigrateFreshBookkeeping(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, dbFileName)
	base := time.Unix(1700000000, 0)
	migrateFresh(t, path, newStepClock(base))

	db := openTestDB(t, path)
	if got, want := metaValue(t, db, "schema_version"), "1"; got != want {
		t.Fatalf("meta[schema_version] = %q, want %q", got, want)
	}
	if got := metaValue(t, db, "schema_applied_at"); got == "" {
		t.Fatal("meta[schema_applied_at] missing after fresh migrate")
	}
	sha := metaValue(t, db, "migration_1_sha256")
	if len(sha) != 64 {
		t.Fatalf("meta[migration_1_sha256] = %q, want a 64-char sha256 hex digest", sha)
	}
	if got, want := embeddedMigrations[0].sha, sha; got != want {
		t.Fatalf("embedded sha %q != recorded sha %q", got, want)
	}
	if got := queryInt(t, db, `PRAGMA user_version`); got != 1 {
		t.Fatalf("PRAGMA user_version = %d, want 1", got)
	}
}

// TestMigrateReopenIsNoop covers the steady state: reopening an already-current directory
// runs the ladder again (validating checksums and the mirror) but writes nothing —
// schema_applied_at stays at the first run's value even though the fake clock has advanced
// far past it, and the version pair reads (1, 1).
func TestMigrateReopenIsNoop(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, dbFileName)
	base := time.Unix(1700000000, 0)
	migrateFresh(t, path, newStepClock(base))

	db := openTestDB(t, path)
	appliedAt := metaValue(t, db, "schema_applied_at")

	// The fake clock is minutes past base by now; any rewrite of schema_applied_at would
	// produce a visibly different value.
	from, to, err := migrate(context.Background(), db, newStepClock(base.Add(10*time.Minute)))
	if err != nil {
		t.Fatalf("migrate reopen: %v", err)
	}
	if from != 1 || to != 1 {
		t.Fatalf("migrate reopen = (%d, %d), want (1, 1)", from, to)
	}
	if got := metaValue(t, db, "schema_applied_at"); got != appliedAt {
		t.Fatalf("schema_applied_at changed on no-op reopen: %q -> %q", appliedAt, got)
	}
	if got := queryInt(t, db, `PRAGMA user_version`); got != 1 {
		t.Fatalf("PRAGMA user_version = %d after reopen, want 1", got)
	}
}

// TestSchemaStrictRejectsWrongTypes proves the STRICT table declarations are live: storing a
// TEXT value into messages.size must be refused by the engine, not coerced.
func TestSchemaStrictRejectsWrongTypes(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, dbFileName)
	migrateFresh(t, path, newStepClock(time.Unix(1700000000, 0)))
	db := openTestDB(t, path)

	const insert = `INSERT INTO messages (stream, seq, id, subject, body, size, published_at, trace_id)
		VALUES ('s', 1, 'id', 'subj', x'00', 'not-a-number', 1, 'trace')`
	_, err := db.ExecContext(context.Background(), insert)
	if err == nil {
		t.Fatal("inserted TEXT into STRICT INTEGER column messages.size without an error")
	}
	if !strings.Contains(err.Error(), "cannot store TEXT value in INTEGER") {
		t.Fatalf("insert error %v, want a STRICT type-mismatch complaint", err)
	}
}

// TestMigrateRefusesSchemaTooNew covers the downgrade refusal: a directory stamped with a
// schema version this binary does not ship is refused, errors map to both the store and the
// core sentinel, the database file is left byte-identical (the refusal rolled back), and the
// user_version mirror still reads the directory's own version.
func TestMigrateRefusesSchemaTooNew(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, dbFileName)
	migrateFresh(t, path, newStepClock(time.Unix(1700000000, 0)))

	// Hand-stamp a future version, as a binary from the future would have.
	db := openTestDB(t, path)
	if _, err := db.ExecContext(context.Background(),
		`UPDATE meta SET v = '999' WHERE k = 'schema_version'`); err != nil {
		t.Fatalf("stamp schema_version=999: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close before refusal probe: %v", err)
	}

	before, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read db before refusal: %v", err)
	}

	db = openTestDB(t, path)
	from, to, err := migrate(context.Background(), db, newStepClock(time.Unix(1700000000, 0)))
	if !errors.Is(err, ErrSchemaTooNew) {
		t.Fatalf("migrate too-new = (%d, %d, %v), want ErrSchemaTooNew", from, to, err)
	}
	if !errors.Is(err, errs.ErrSchemaNewer) {
		t.Fatalf("ErrSchemaTooNew does not map to errs.ErrSchemaNewer: %v", err)
	}

	// #5 acceptance: the refusal must NAME the required action, in the issue's transcript
	// wording — a downgrade victim has to be told what to do, not just what happened.
	const wantAction = "next: upgrade messq to >= the version that wrote it, or restore a backup"
	if !strings.Contains(err.Error(), wantAction) {
		t.Errorf("too-new refusal does not name the required action\n  got:  %v\n  want: %q", err, wantAction)
	}

	after, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read db after refusal: %v", err)
	}
	if string(before) != string(after) {
		t.Fatalf("refused migrate modified the database file (%d -> %d bytes)", len(before), len(after))
	}
	if got := queryInt(t, db, `PRAGMA user_version`); got != 1 {
		t.Fatalf("PRAGMA user_version = %d after refusal, want the directory's own 1", got)
	}
}

// TestMigrateDetectsChecksumDrift covers the edited-file guard: when the recorded checksum
// of an applied migration no longer matches the file this binary ships, the ladder refuses
// with ErrMigrationDrift instead of silently building on a schema it did not write.
func TestMigrateDetectsChecksumDrift(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, dbFileName)
	migrateFresh(t, path, newStepClock(time.Unix(1700000000, 0)))

	db := openTestDB(t, path)
	// Tampering the recorded checksum is exactly what an edited migration file produces on
	// the next open: the file's digest no longer matches meta.
	if _, err := db.ExecContext(context.Background(),
		`UPDATE meta SET v = 'deadbeef' WHERE k = 'migration_1_sha256'`); err != nil {
		t.Fatalf("tamper checksum: %v", err)
	}
	_, _, err := migrate(context.Background(), db, newStepClock(time.Unix(1700000000, 0)))
	if !errors.Is(err, ErrMigrationDrift) {
		t.Fatalf("migrate with drifted checksum = %v, want ErrMigrationDrift", err)
	}
}

// TestMigrateRefusesUserVersionMirrorMismatch covers the tampering tripwire: when
// PRAGMA user_version and meta.schema_version disagree, the directory has been edited
// behind the ladder's back and startup refuses rather than guessing which one is true.
func TestMigrateRefusesUserVersionMirrorMismatch(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, dbFileName)
	migrateFresh(t, path, newStepClock(time.Unix(1700000000, 0)))

	db := openTestDB(t, path)
	if _, err := db.ExecContext(context.Background(), `PRAGMA user_version = 42`); err != nil {
		t.Fatalf("hand-edit user_version: %v", err)
	}
	_, _, err := migrate(context.Background(), db, newStepClock(time.Unix(1700000000, 0)))
	if !errors.Is(err, ErrMigrationDrift) {
		t.Fatalf("migrate with mirror mismatch = %v, want ErrMigrationDrift", err)
	}
	if got, want := metaValue(t, db, "schema_version"), "1"; got != want {
		t.Fatalf("meta[schema_version] = %q after refused migrate, want untouched %q", got, want)
	}
}

// TestExecScriptSplitsStatementsWithEmbeddedSemicolons is the regression guard for the
// multi-statement execution path the runner depends on: semicolons inside string literals
// must be left to the real SQL parser. A hand-rolled splitter would break the second
// statement or mangle the value; the driver must apply both inserts verbatim.
func TestExecScriptSplitsStatementsWithEmbeddedSemicolons(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, dbFileName)
	migrateFresh(t, path, newStepClock(time.Unix(1700000000, 0)))
	db := openTestDB(t, path)
	ctx := context.Background()

	conn, connErr := db.Conn(ctx)
	if connErr != nil {
		t.Fatalf("grab conn: %v", connErr)
	}
	defer func() {
		if cerr := conn.Close(); cerr != nil {
			t.Errorf("close probe conn: %v", cerr)
		}
	}()

	script := `INSERT INTO events (ts, event, detail) VALUES (1, 'probe', 'semi ; colon');
INSERT INTO events (ts, event, detail) VALUES (2, 'probe2', 'plain');`
	if err := execScript(ctx, conn, script); err != nil {
		t.Fatalf("execScript with embedded semicolon: %v", err)
	}
	var detail string
	if err := db.QueryRowContext(ctx, `SELECT detail FROM events WHERE event = 'probe'`).Scan(&detail); err != nil {
		t.Fatalf("select probe row: %v", err)
	}
	if detail != "semi ; colon" {
		t.Fatalf("detail = %q, want %q", detail, "semi ; colon")
	}
	var n int
	if err := db.QueryRowContext(ctx, `SELECT count(*) FROM events`).Scan(&n); err != nil {
		t.Fatalf("count events: %v", err)
	}
	if n != 2 {
		t.Fatalf("events count = %d, want 2 (both statements applied)", n)
	}

	// The same path must surface a genuine SQL failure as an error, not swallow the tail of
	// a script that half-applied.
	err := execScript(ctx, conn, `CREATE TABLE split_probe (v INTEGER);
THIS IS NOT SQL;`)
	if err == nil {
		t.Fatal("execScript accepted a script containing invalid SQL")
	}
}

// TestLoadMigrationsRejectsMalformedLadder exercises the packaging guards on an in-memory
// FS: an empty ladder, files not named <ordinal>_<name>.sql, unparseable ordinals and
// duplicate ordinals must all be load errors — a silently partial or reordered ladder would
// migrate directories with the wrong SQL.
func TestLoadMigrationsRejectsMalformedLadder(t *testing.T) {
	good := []byte(`CREATE TABLE t (v INTEGER) STRICT;`)
	for _, tc := range []struct {
		name string
		fs   map[string]string
		want string // substring of the expected error
	}{
		{"empty", map[string]string{}, "no embedded migrations"},
		{"missing ordinal", map[string]string{"migrations/init.sql": string(good)}, "not <ordinal>_<name>.sql"},
		{"bad ordinal", map[string]string{"migrations/x001_init.sql": string(good)}, "bad ordinal"},
		{"duplicate ordinal", map[string]string{
			"migrations/0001_a.sql": string(good),
			"migrations/0001_b.sql": string(good),
		}, "duplicate ordinal 1"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			files := map[string]*fstest.MapFile{}
			for p, content := range tc.fs {
				files[p] = &fstest.MapFile{Data: []byte(content)}
			}
			_, err := loadMigrations(fstest.MapFS(files))
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("loadMigrations(%v) = %v, want error containing %q", tc.fs, err, tc.want)
			}
		})
	}
}

// TestMigrateRefusesMetaWithoutVersionRow covers a meta table stripped of its version row:
// that is not a fresh directory (meta exists) and not a readable state either — bookkeeping
// was removed behind the ladder's back, so it refuses rather than restarting from 0.
func TestMigrateRefusesMetaWithoutVersionRow(t *testing.T) {
	db := openTestDB(t, filepath.Join(t.TempDir(), dbFileName))
	if _, err := db.ExecContext(context.Background(), metaTableDDL); err != nil {
		t.Fatalf("hand-create meta: %v", err)
	}
	_, _, err := migrate(context.Background(), db, newStepClock(time.Unix(1700000000, 0)))
	if !errors.Is(err, ErrMigrationDrift) {
		t.Fatalf("migrate with gutted meta = %v, want ErrMigrationDrift", err)
	}
}

// TestMigrateRefusesNonIntegerSchemaVersion covers a hand-mangled version value: the ladder
// reads meta values as integers it owns, and a non-integer is tampering, not a version.
func TestMigrateRefusesNonIntegerSchemaVersion(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, dbFileName)
	migrateFresh(t, path, newStepClock(time.Unix(1700000000, 0)))

	db := openTestDB(t, path)
	if _, err := db.ExecContext(context.Background(),
		`UPDATE meta SET v = 'soon(tm)' WHERE k = 'schema_version'`); err != nil {
		t.Fatalf("corrupt schema_version: %v", err)
	}
	_, _, err := migrate(context.Background(), db, newStepClock(time.Unix(1700000000, 0)))
	if err == nil || !strings.Contains(err.Error(), "is not an integer") {
		t.Fatalf("migrate with corrupt schema_version = %v, want a parse refusal", err)
	}
}

// TestMigrateDetectsMissingChecksum covers the deleted-checksum variant of drift: a
// directory claiming v1 but carrying no recorded digest for migration 1 cannot prove which
// SQL shaped it.
func TestMigrateDetectsMissingChecksum(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, dbFileName)
	migrateFresh(t, path, newStepClock(time.Unix(1700000000, 0)))

	db := openTestDB(t, path)
	if _, err := db.ExecContext(context.Background(),
		`DELETE FROM meta WHERE k = 'migration_1_sha256'`); err != nil {
		t.Fatalf("delete checksum: %v", err)
	}
	_, _, err := migrate(context.Background(), db, newStepClock(time.Unix(1700000000, 0)))
	if !errors.Is(err, ErrMigrationDrift) {
		t.Fatalf("migrate without recorded checksum = %v, want ErrMigrationDrift", err)
	}
}

// TestMigrateRefusesReadOnlyHandle covers the write-guard of the ladder against a read-only
// connection: on a query_only-fenced connection the refusal is a write-time
// SQLITE_READONLY, never a silent no-op, and the ladder surfaces it as a startup error.
// _query_only is the portable fence — a DSN key on both drivers, where mode=ro exists only
// in the modernc dialect. Which ladder step raises first is a driver/handle detail (with an
// OS-level ro handle the BEGIN succeeds and the meta upsert fails; with query_only fencing
// the WAL files, BEGIN IMMEDIATE itself refuses), so this pins SQLite's contract rather
// than either driver's wrapping.
func TestMigrateRefusesReadOnlyHandle(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, dbFileName)
	migrateFresh(t, path, newStepClock(time.Unix(1700000000, 0)))

	ro, err := sql.Open(driverName, "file:"+path+"?_query_only=1")
	if err != nil {
		t.Fatalf("open read-only: %v", err)
	}
	defer func() {
		if cerr := ro.Close(); cerr != nil {
			t.Errorf("close read-only handle: %v", cerr)
		}
	}()
	from, to, err := migrate(context.Background(), ro, newStepClock(time.Unix(1700000000, 0)))
	if err == nil || !strings.Contains(err.Error(), "attempt to write a readonly database") {
		t.Fatalf("migrate over read-only handle = (%d, %d, %v), want a readonly-database refusal", from, to, err)
	}
}

// TestMigrateRefusesGhostV0Directory covers a stamped-but-unmigrated directory: meta claims
// version 0 while the tables 0001 would create already exist. Replaying 0001 into it must
// collide loudly (the verbatim CREATE TABLE carries no IF NOT EXISTS) rather than silently
// adopting whatever shape someone built by hand.
func TestMigrateRefusesGhostV0Directory(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, dbFileName)
	migrateFresh(t, path, newStepClock(time.Unix(1700000000, 0)))

	db := openTestDB(t, path)
	if _, err := db.ExecContext(context.Background(),
		`UPDATE meta SET v = '0' WHERE k = 'schema_version'`); err != nil {
		t.Fatalf("stamp schema_version=0: %v", err)
	}
	_, _, err := migrate(context.Background(), db, newStepClock(time.Unix(1700000000, 0)))
	if err == nil || !strings.Contains(err.Error(), "apply migration 0001_init") {
		t.Fatalf("migrate into ghost v0 directory = %v, want an apply failure naming 0001_init", err)
	}
}

// TestMigrateReportsLadderLoadFailure pins the guard for a ladder that failed to parse at
// init: migrate must report the packaging error rather than run against a partial ladder.
func TestMigrateReportsLadderLoadFailure(t *testing.T) {
	origMigs, origErr := embeddedMigrations, embeddedMigrationsErr
	t.Cleanup(func() { embeddedMigrations, embeddedMigrationsErr = origMigs, origErr })
	embeddedMigrations, embeddedMigrationsErr = nil, errors.New("ladder did not parse")

	db := openTestDB(t, filepath.Join(t.TempDir(), dbFileName))
	from, to, err := migrate(context.Background(), db, newStepClock(time.Unix(1700000000, 0)))
	if err == nil || err.Error() != "ladder did not parse" {
		t.Fatalf("migrate with unloadable ladder = (%d, %d, %v), want the load error verbatim", from, to, err)
	}
}

// TestMigrateRefusesImpostorMetaTable covers a meta-shaped table with the wrong columns:
// the version query cannot even run, which is corruption by definition, and the error names
// the step ("read applied schema version") rather than leaking raw driver text alone.
func TestMigrateRefusesImpostorMetaTable(t *testing.T) {
	db := openTestDB(t, filepath.Join(t.TempDir(), dbFileName))
	if _, err := db.ExecContext(context.Background(), `CREATE TABLE meta (x INTEGER) STRICT`); err != nil {
		t.Fatalf("create impostor meta: %v", err)
	}
	_, _, err := migrate(context.Background(), db, newStepClock(time.Unix(1700000000, 0)))
	if err == nil || !strings.Contains(err.Error(), "read applied schema version") {
		t.Fatalf("migrate against impostor meta = %v, want a version-read refusal", err)
	}
}
