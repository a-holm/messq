// SPDX-License-Identifier: Apache-2.0

package store

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"embed"
	"encoding/hex"
	"errors"
	"fmt"
	"io/fs"
	"path"
	"slices"
	"strconv"
	"strings"

	"github.com/a-holm/messq/internal/clock"
)

// The migration ladder owns every durable shape change of messq.db. Its contract (PLAN §4.2)
// has four load-bearing properties, each enforced here rather than documented away:
//
//   - Embedded: the binary carries exactly the schema it was built with; a data directory can
//     never be migrated by code that disagrees with the SQL that shaped it.
//   - One transaction: each open applies everything between the directory's version and the
//     binary's inside a single BEGIN IMMEDIATE … COMMIT on one pinned connection, so a crash
//     mid-ladder leaves the previous schema fully intact — SQLite rolls the whole thing back.
//   - Checksummed: the sha256 of every migration file is recorded in meta as it is applied
//     and re-verified on every later open. A binary whose copy of an already-applied file
//     differs from the one that shaped the directory refuses with [ErrMigrationDrift].
//   - Forward-only with a mirror: meta.schema_version is authoritative, PRAGMA user_version
//     mirrors it for tooling that reads the header without SQL; a disagreement between the
//     two is treated as out-of-band tampering and refused.
//
// Ordering subtlety worth remembering: the runner never creates meta ahead of the pending
// migrations even though it needs meta to know what is pending. On a fresh directory it
// detects meta's absence through sqlite_schema instead, starts from 0, and lets migration
// 0001 — whose verbatim CREATE TABLE meta carries no IF NOT EXISTS — bring the table into
// being itself. Pre-creating a look-alike would make 0001 fail with "table meta already
// exists".

//go:embed migrations/*.sql
var migrationsFS embed.FS

// migration is one step of the ladder parsed from an embedded file named
// <n>_<name>.sql (e.g. 0001_init.sql).
type migration struct {
	n    int    // ordinal; matches the zero-padded filename prefix
	name string // descriptive stem after the ordinal
	sql  string // the file verbatim, comments included
	sha  string // hex sha256 of the raw file bytes, as recorded in meta
}

// migrationChecksumKey is the meta key under which migration n's digest is recorded.
func migrationChecksumKey(n int) string { return fmt.Sprintf("migration_%d_sha256", n) }

// Meta bookkeeping keys and the bootstrap DDL for the key/value table they live in. The DDL
// mirrors §4.2's meta table plus IF NOT EXISTS; it only fires when the embedded ladder holds
// no schema-owning migration, because 0001 creates the real table on every fresh directory.
const (
	metaSchemaVersion   = "schema_version"
	metaSchemaAppliedAt = "schema_applied_at"
	metaTableDDL        = `CREATE TABLE IF NOT EXISTS meta (k TEXT PRIMARY KEY, v TEXT NOT NULL) STRICT`
)

// embeddedMigrations is the ladder this binary ships, sorted by ordinal. A load failure is
// kept (not panicked on) so migrate can report it through the ordinary error path.
var embeddedMigrations, embeddedMigrationsErr = loadMigrations(migrationsFS)

// loadMigrations parses and digests every embedded migrations/*.sql file. Ordinals must be
// unique and strictly increasing after sorting; anything else is a packaging bug caught at
// first migrate rather than a silent partial ladder.
func loadMigrations(fsys fs.FS) ([]migration, error) {
	matches, err := fs.Glob(fsys, "migrations/*.sql")
	if err != nil {
		return nil, fmt.Errorf("glob embedded migrations: %w", err)
	}
	if len(matches) == 0 {
		return nil, errors.New("no embedded migrations found")
	}
	out := make([]migration, 0, len(matches))
	for _, p := range matches {
		stem := strings.TrimSuffix(path.Base(p), ".sql")
		num, name, ok := strings.Cut(stem, "_")
		if !ok {
			return nil, fmt.Errorf("embedded migration %s: file name is not <ordinal>_<name>.sql", p)
		}
		n, err := strconv.Atoi(num)
		if err != nil {
			return nil, fmt.Errorf("embedded migration %s: bad ordinal %q: %w", p, num, err)
		}
		raw, err := fs.ReadFile(fsys, p)
		if err != nil {
			return nil, fmt.Errorf("read embedded migration %s: %w", p, err)
		}
		sum := sha256.Sum256(raw)
		out = append(out, migration{
			n:    n,
			name: name,
			sql:  string(raw),
			sha:  hex.EncodeToString(sum[:]),
		})
	}
	slices.SortFunc(out, func(a, b migration) int { return a.n - b.n })
	for i := 1; i < len(out); i++ {
		if out[i].n == out[i-1].n {
			return nil, fmt.Errorf("embedded migrations: duplicate ordinal %d", out[i].n)
		}
	}
	return out, nil
}

// migrate brings the database from its applied schema version up to the version this binary
// ships, in one transaction, and returns the version pair (from, to). An already-current
// directory takes the no-op path: checksums and the user_version mirror are still verified,
// but nothing is written and schema_applied_at keeps its original value.
//
// clk supplies the schema_applied_at timestamp; tests inject a fake to prove that the no-op
// path leaves it untouched.
func migrate(ctx context.Context, db *sql.DB, clk clock.Clock) (from, to int, err error) {
	if embeddedMigrationsErr != nil {
		return 0, 0, embeddedMigrationsErr
	}
	to = len(embeddedMigrations)

	// Pin one connection for the whole ladder. Statements issued through *sql.DB land on
	// arbitrary pooled connections, which would scatter BEGIN/COMMIT across several; a held
	// *sql.Conn makes the transaction real.
	conn, err := db.Conn(ctx)
	if err != nil {
		return 0, to, fmt.Errorf("acquire connection for migrations: %w", err)
	}
	defer func() {
		if cerr := conn.Close(); cerr != nil {
			err = errors.Join(err, fmt.Errorf("release migration connection: %w", cerr))
		}
	}()

	if _, beginErr := conn.ExecContext(ctx, `BEGIN IMMEDIATE`); beginErr != nil {
		return 0, to, fmt.Errorf("begin migration transaction: %w", beginErr)
	}
	committed := false
	defer func() {
		if committed {
			return
		}
		// Roll back with a context that survives cancellation of ctx: a caller cancelling
		// mid-migrate must still get the schema-restoring rollback, not a second failure.
		if rbErr := rollbackMigrate(context.WithoutCancel(ctx), conn); rbErr != nil {
			err = errors.Join(err, rbErr)
		}
	}()

	from, err = readAppliedVersion(ctx, conn)
	if err != nil {
		return 0, to, fmt.Errorf("read applied schema version: %w", err)
	}

	if from > to {
		return from, to, fmt.Errorf("%w: directory is at schema v%d, this binary ships v%d;"+
			" next: upgrade messq to >= the version that wrote it, or restore a backup",
			ErrSchemaTooNew, from, to)
	}

	if from > 0 {
		if err := verifyLadderState(ctx, conn, from); err != nil {
			return from, to, err
		}
	}

	applied := false
	for _, m := range embeddedMigrations {
		if m.n <= from {
			continue
		}
		if err := execScript(ctx, conn, m.sql); err != nil {
			return from, to, fmt.Errorf("apply migration %04d_%s: %w", m.n, m.name, err)
		}
		if err := upsertMeta(ctx, conn, migrationChecksumKey(m.n), m.sha); err != nil {
			return from, to, fmt.Errorf("record checksum of migration %04d: %w", m.n, err)
		}
		applied = true
	}

	// Belt-and-braces for a future ladder whose first entry does not own meta creation;
	// on every current fresh path 0001 has already made the table and this is a no-op.
	if _, err := conn.ExecContext(ctx, metaTableDDL); err != nil {
		return from, to, fmt.Errorf("ensure %s table: %w", "meta", err)
	}

	if err := upsertMeta(ctx, conn, metaSchemaVersion, strconv.Itoa(to)); err != nil {
		return from, to, fmt.Errorf("record schema version %d: %w", to, err)
	}
	if applied {
		if err := upsertMeta(ctx, conn, metaSchemaAppliedAt, strconv.FormatInt(clk.Now().UnixMilli(), 10)); err != nil {
			return from, to, fmt.Errorf("record schema_applied_at: %w", err)
		}
	}

	// Mirror the authoritative meta value into the file header. user_version changes are
	// transactional in SQLite, so a rolled-back ladder cannot leave it half-bumped. The
	// value is a formatted integer, never user input, so Sprintf is injection-safe.
	if _, err := conn.ExecContext(ctx, fmt.Sprintf(`PRAGMA user_version = %d`, to)); err != nil {
		return from, to, fmt.Errorf("mirror schema version into user_version: %w", err)
	}

	if _, err := conn.ExecContext(ctx, `COMMIT`); err != nil {
		return from, to, fmt.Errorf("commit migration transaction: %w", err)
	}
	committed = true
	return from, to, nil
}

// verifyLadderState re-checks everything the directory claims about already-applied
// migrations: the user_version mirror must agree with meta, and every shipped migration with
// an ordinal ≤ from must still hash to the digest recorded when it was applied.
func verifyLadderState(ctx context.Context, conn *sql.Conn, from int) error {
	uv, err := pragmaUserVersion(ctx, conn)
	if err != nil {
		return fmt.Errorf("read user_version mirror: %w", err)
	}
	if uv != from {
		return fmt.Errorf("%w: PRAGMA user_version is %d but meta.%s is %d; the database was modified outside the migration ladder",
			ErrMigrationDrift, uv, metaSchemaVersion, from)
	}
	for _, m := range embeddedMigrations {
		if m.n > from {
			break
		}
		var recorded string
		err := conn.QueryRowContext(ctx, `SELECT v FROM meta WHERE k = ?`, migrationChecksumKey(m.n)).Scan(&recorded)
		switch {
		case errors.Is(err, sql.ErrNoRows):
			return fmt.Errorf("%w: migration %04d is below the applied version but records no checksum in meta",
				ErrMigrationDrift, m.n)
		case err != nil:
			return fmt.Errorf("read checksum of applied migration %04d: %w", m.n, err)
		}
		if recorded != m.sha {
			return fmt.Errorf("%w: migration %04d_%s hashes to %s but meta records %s",
				ErrMigrationDrift, m.n, m.name, m.sha, recorded)
		}
	}
	return nil
}

// readAppliedVersion reports the directory's applied schema version without creating any
// bookkeeping: a fresh directory (no meta table) reads as 0 so migration 0001 itself can
// create meta verbatim. A meta table missing its schema_version row is not a fresh
// directory — something removed bookkeeping behind the ladder's back — and is refused.
func readAppliedVersion(ctx context.Context, conn *sql.Conn) (int, error) {
	var name string
	err := conn.QueryRowContext(ctx,
		`SELECT name FROM sqlite_schema WHERE type = 'table' AND name = 'meta'`).Scan(&name)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		return 0, nil
	case err != nil:
		return 0, err
	}
	var raw string
	err = conn.QueryRowContext(ctx, `SELECT v FROM meta WHERE k = ?`, metaSchemaVersion).Scan(&raw)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		return 0, fmt.Errorf("%w: meta table exists but records no %s row",
			ErrMigrationDrift, metaSchemaVersion)
	case err != nil:
		return 0, err
	}
	v, err := strconv.Atoi(raw)
	if err != nil {
		return 0, fmt.Errorf("meta[%s] = %q is not an integer", metaSchemaVersion, raw)
	}
	return v, nil
}

// pragmaUserVersion reads the file-header mirror of the schema version.
func pragmaUserVersion(ctx context.Context, conn *sql.Conn) (int, error) {
	var v int
	if err := conn.QueryRowContext(ctx, `PRAGMA user_version`).Scan(&v); err != nil {
		return 0, err
	}
	return v, nil
}

// upsertMeta writes one meta row, inserting or overwriting by key.
func upsertMeta(ctx context.Context, conn *sql.Conn, key, val string) error {
	if _, err := conn.ExecContext(ctx,
		`INSERT INTO meta (k, v) VALUES (?, ?) ON CONFLICT (k) DO UPDATE SET v = excluded.v`,
		key, val); err != nil {
		return fmt.Errorf("upsert meta[%s]: %w", key, err)
	}
	return nil
}

// execScript runs a multi-statement SQL script on one connection. The modernc driver walks
// the prepare tail iteratively through the real SQLite parser, so semicolons inside string
// literals are handled correctly and no hand-rolled splitter is involved.
func execScript(ctx context.Context, conn *sql.Conn, script string) error {
	if _, err := conn.ExecContext(ctx, script); err != nil {
		return fmt.Errorf("execute migration script: %w", err)
	}
	return nil
}

// rollbackMigrate undoes the open's transaction; it runs only when the transaction is known
// not to have committed.
func rollbackMigrate(ctx context.Context, conn *sql.Conn) error {
	if _, err := conn.ExecContext(ctx, `ROLLBACK`); err != nil {
		return fmt.Errorf("rollback migration transaction: %w", err)
	}
	return nil
}
