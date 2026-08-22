// SPDX-License-Identifier: Apache-2.0

//go:build cgosqlite

package store

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"strings"

	sqlite3 "github.com/mattn/go-sqlite3"
)

// This file is the entire driver-specific surface of the cgo build (the D1 escape hatch,
// exercised by the nightly cgosqlite lane): the registration name, the DSN spelling
// conversion from modernc's `_pragma=name(value)` wire format to mattn's shorthand keys,
// and the connect hook that applies the two pragmas mattn has no DSN key for before running
// the SAME read-back verification as the pure-Go build (pragma.go). Everything above the
// two driver files speaks plain database/sql and shares buildDSN, pragmaSettings, and the
// registry; swapping engines is this file versus its twin, nothing else.
//
// Verified against github.com/mattn/go-sqlite3 v1.14.50 source (sqlite3.go), which fixes
// three behaviours this file relies on:
//
//  1. Shorthand keys exist for busy_timeout (_busy_timeout), journal_mode (_journal_mode),
//     synchronous (_synchronous), foreign_keys (_foreign_keys), query_only (_query_only)
//     and cache_size (_cache_size) — but NOT for temp_store or wal_autocheckpoint. Those
//     two are applied by the connect hook below, derived from the registry's expectation
//     set so the values can never drift from the shared truth table in dsn.go.
//  2. Parsing runs in a fixed source order, with `_journal_mode=WAL` forcing synchronous
//     to NORMAL as a documented side effect and `_synchronous` parsed afterwards — so
//     carrying both on one DSN ends at WAL plus the requested mode. The apply phase
//     executes journal_mode before query_only before synchronous, so query_only never
//     fences a preceding write-capable pragma.
//  3. The ConnectHook receives only the *SQLiteConn (no DSN argument), unlike modernc's
//     hook. The wrapper driver below therefore captures the canonical DSN per Open call
//     into the hook closure, keeping the registry keyed on the exact buildDSN output both
//     builds share.
//
// Known, deliberate limitation: mattn ignores unknown DSN parameters, `mode=ro` included,
// so poolReadOnly connections under this build hold an OS-level read-write handle fenced by
// query_only=1 like every other reader. ADR-0002's deviation note designates query_only —
// not the OS handle — as the enforcement layer; nothing in the package depends on the
// read-only pool's file descriptor being read-only.

// driverName is the registration this package installs for the cgo build. sql.Open on this
// name yields mattn-backed connections that run the hook installed below.
const driverName = "sqlite3_messq"

// freshFileDSN is the mattn spelling of initEmptyDatabase's pre-hook connection: stamp
// auto_vacuum (immutable once a table exists) and journal_mode=WAL on an empty database.
// Like the pure-Go twin it is deliberately unregistered — initEmptyDatabase reads both
// values back itself.
func freshFileDSN(path string) string {
	return "file:" + path + "?_auto_vacuum=INCREMENTAL&_journal_mode=WAL"
}

// mattnShorthand maps a PRAGMA name to the DSN key mattn natively applies. Pragmas in this
// table ride the converted DSN; everything else in an expectation set is applied by the
// connect hook from its registered want value.
var mattnShorthand = map[string]string{
	"busy_timeout": "_busy_timeout",
	"journal_mode": "_journal_mode",
	"synchronous":  "_synchronous",
	"foreign_keys": "_foreign_keys",
	"query_only":   "_query_only",
	"cache_size":   "_cache_size",
}

// wireDSN converts a canonical messq DSN (buildDSN output, `_pragma=name(value)` spellings)
// into the mattn dialect: mapped pragmas become their shorthand keys, unmapped pragmas are
// dropped (the hook applies them), and non-pragma parameters (`mode=ro`, `_txlock`) pass
// through verbatim in their original order. A DSN without a query part is returned as-is;
// mattn opens bare paths directly.
func wireDSN(canonical string) string {
	q := strings.IndexByte(canonical, '?')
	if q < 0 {
		return canonical
	}
	parts := strings.Split(canonical[q+1:], "&")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		name, value, found := strings.Cut(p, "_pragma=")
		if !found || name != "" {
			out = append(out, p) // not a _pragma parameter: pass through
			continue
		}
		prag, rest, hasParens := strings.Cut(value, "(")
		if key, ok := mattnShorthand[prag]; ok && hasParens {
			out = append(out, key+"="+strings.TrimSuffix(rest, ")"))
		}
		// Unmapped pragma: dropped here, applied and verified by the hook.
	}
	if len(out) == 0 {
		return canonical[:q]
	}
	return canonical[:q] + "?" + strings.Join(out, "&")
}

// messqDriver adapts mattn/go-sqlite3 to the registry-keyed verification contract: Open
// receives the canonical DSN database/sql was handed, converts it to the mattn dialect, and
// builds a per-connection inner driver whose hook closure captures that canonical string
// (mattn's ConnectHook carries no DSN of its own).
type messqDriver struct{}

// Open implements driver.Driver. Each call constructs a fresh inner SQLiteDriver because
// the hook must close over this connection's DSN; connections are long-lived, so the
// allocation is noise next to opening a SQLite file.
func (messqDriver) Open(canonical string) (driver.Conn, error) {
	inner := &sqlite3.SQLiteDriver{
		ConnectHook: func(conn *sqlite3.SQLiteConn) error {
			return verifyCgoConnection(conn, canonical)
		},
	}
	return inner.Open(wireDSN(canonical))
}

// verifyCgoConnection applies every registry expectation the converted DSN could not carry
// (the pragmas without a mattn shorthand), then hands the connection to the shared
// read-back verification. An unregistered DSN does neither: other SQLite users in the
// process are never affected, exactly as in the pure-Go build.
func verifyCgoConnection(conn pragmaConn, canonical string) error {
	v, ok := registry.Load(canonical)
	if !ok {
		return nil
	}
	if exps, isExpectSet := v.([]expect); isExpectSet {
		ctx := context.Background()
		for _, e := range exps {
			if _, covered := mattnShorthand[e.name]; covered {
				continue
			}
			if _, err := conn.ExecContext(ctx, "PRAGMA "+e.name+" = "+e.want, nil); err != nil {
				return err
			}
		}
	}
	return verifyConnectionPragmas(conn, canonical)
}

func init() { sql.Register(driverName, messqDriver{}) }

// openSQLite opens a database/sql handle on the cgo registration. No global hook
// registration needs guarding here — each Open builds its own closure — so the sync.Once
// the pure-Go twin requires has no counterpart.
func openSQLite(dsn string) (*sql.DB, error) {
	return sql.Open(driverName, dsn)
}
