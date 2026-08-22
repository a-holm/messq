// SPDX-License-Identifier: Apache-2.0

//go:build cgosqlite

package store

import (
	"context"
	"database/sql"
	"path/filepath"
	"strings"
	"testing"
)

// TestWireDSNConvertsCanonicalToMattn pins the DSN dialect conversion: mapped pragmas
// become mattn shorthand keys in order, unmapped pragmas are dropped (the hook applies
// them from the registry), and non-pragma parameters pass through untouched.
func TestWireDSNConvertsCanonicalToMattn(t *testing.T) {
	writer := buildDSN("/var/lib/messq/messq.db", poolWriter, Options{})
	got := wireDSN(writer)
	want := "file:/var/lib/messq/messq.db" +
		"?_txlock=immediate" +
		"&_busy_timeout=5000" +
		"&_journal_mode=WAL" +
		"&_synchronous=FULL" +
		"&_foreign_keys=1" +
		"&_cache_size=-65536"
	if got != want {
		t.Errorf("wireDSN(writer) =\n\t%s\nwant\n\t%s", got, want)
	}

	reader := buildDSN("/var/lib/messq/messq.db", poolReader, Options{ReadPoolSize: 2})
	if g := wireDSN(reader); !strings.Contains(g, "_query_only=1") || strings.Contains(g, "_pragma=") {
		t.Errorf("wireDSN(reader) = %s, want query_only carried as _query_only and no _pragma left", g)
	}

	ro := buildDSN("/var/lib/messq/messq.db", poolReadOnly, Options{ReadPoolSize: 2})
	if g := wireDSN(ro); !strings.HasPrefix(g, "file:/var/lib/messq/messq.db?mode=ro&") {
		t.Errorf("wireDSN(read-only) = %s, want mode=ro passed through first", g)
	}

	if g := wireDSN("/plain/path.db"); g != "/plain/path.db" {
		t.Errorf("wireDSN(bare path) = %s, want unchanged", g)
	}
}

// TestCgoDriverRegistrationIsLive proves the escape hatch is wired, not merely compilable:
// the registered messq driver name opens a real file through mattn/go-sqlite3, and the
// pragma hook verifies against the registry exactly as the pure-Go build does. The full
// store suite running under this tag (nightly cgosqlite lane) is the deeper proof; this
// test names the registration contract itself so a silent re-register or rename fails here
// first, with a message about the driver and not three layers into Open.
func TestCgoDriverRegistrationIsLive(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	path := filepath.Join(dir, "probe.db")

	if err := initEmptyDatabase(ctx, path); err != nil {
		t.Fatalf("initEmptyDatabase through %q: %v", driverName, err)
	}

	dsn := buildDSN(path, poolWriter, Options{})
	registerExpectations(dsn, expectationsFor(poolWriter, Options{}))
	db, err := sql.Open(driverName, dsn)
	if err != nil {
		t.Fatalf("sql.Open(%q): %v", driverName, err)
	}
	defer func() {
		if cerr := db.Close(); cerr != nil {
			t.Errorf("close probe db: %v", cerr)
		}
	}()
	if err := db.PingContext(ctx); err != nil {
		t.Fatalf("ping through %q: %v", driverName, err)
	}
	var journal string
	if err := db.QueryRowContext(ctx, `PRAGMA journal_mode`).Scan(&journal); err != nil {
		t.Fatalf("read journal_mode: %v", err)
	}
	if normalizePragmaValue(journal) != "wal" {
		t.Errorf("journal_mode read back %q, want wal", journal)
	}
}
