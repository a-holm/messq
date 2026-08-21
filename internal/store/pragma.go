// SPDX-License-Identifier: Apache-2.0

package store

import (
	"context"
	"database/sql/driver"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"
	"sync"
)

// Pragma enforcement is D1's answer to "durability lives in a DSN string, and a DSN is a
// trap" (ADR-0002): a pooled connection silently running synchronous=NORMAL in WAL mode
// would not fsync on commit, every §6 promise would be false, and nothing would look wrong.
// The defence has two halves. The DSN applies the pragmas (the driver's apply phase, before
// any statement of ours runs); the connection hook then re-reads every pragma from the live
// connection and compares against the expectation set registered for that exact DSN. A
// mismatch fails the connection, which fails the Open — a startup refusal, never a silent
// downgrade.
//
// The hook deliberately applies nothing: everything is already on the DSN, and re-applying
// would blur which layer owns a value. It is verify-only, so a pragma the driver silently
// ignored is still caught — on the re-read.
//
// Deliberately absent from the expectation sets: auto_vacuum. It is immutable once the
// first table exists, so it is set once at fresh-file creation before migration 0001, not
// asserted per connection (PLAN §4.1).

// expect is one read-back assertion: the PRAGMA name and the normalized value every
// connection opened on the registered DSN must report.
type expect struct {
	name string
	want string
}

// registry maps an exact DSN string to its expectation set. Open populates it before the
// first sql.Open; the hook looks up by the DSN the driver reports, which is the same
// string byte for byte. A miss is a no-op, so other SQLite users in the same process are
// never affected by messq's hook.
var registry sync.Map // dsn string -> []expect

// registerExpectations records the expectation set for one exact DSN. Must run before the
// first connection on that DSN; re-registering a DSN replaces its set.
func registerExpectations(dsn string, exps []expect) {
	registry.Store(dsn, exps)
}

// expectationsFor derives the read-back assertions for role under opt. Order is the hook's
// read order and is load-bearing: journal_mode first (it is the persistent file property
// everything else presumes), query_only last (readers only — once read it fences the
// remaining reads' meaning as "post-fence" state).
func expectationsFor(role poolRole, opt Options) []expect {
	settings := pragmaSettings(role, opt)
	exps := make([]expect, 0, len(settings))
	for _, s := range settings {
		exps = append(exps, expect{name: s.name, want: s.want})
	}
	return exps
}

// pragmaConn is the minimal connection surface verification needs. It is satisfied by the
// modernc driver's connection type (driver.ExecerContext + driver.QueryerContext); naming
// it here keeps this file free of any driver import, per the D1 escape-hatch rule that
// driver-specific code lives only in the driver_*.go files.
type pragmaConn interface {
	ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error)
	QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error)
}

// verifyConnectionPragmas is the connection-hook body: look up the DSN's expectation set,
// read every pragma back from the live connection, and refuse the connection on the first
// mismatch. Unknown DSNs verify nothing — a registry miss is an explicit no-op.
func verifyConnectionPragmas(conn pragmaConn, dsn string) error {
	v, ok := registry.Load(dsn)
	if !ok {
		return nil
	}
	exps := v.([]expect)
	ctx := context.Background()
	for _, e := range exps {
		got, err := readPragma(ctx, conn, e.name)
		if err != nil {
			return fmt.Errorf("verify %s on %s: %w", e.name, redactedDSN(dsn), err)
		}
		if normalizePragmaValue(got) != e.want {
			return fmt.Errorf("%w: %s=%s, want %s", ErrPragmaMismatch, e.name, got, e.want)
		}
	}
	return nil
}

// readPragma reads one pragma's single-row single-column result over the raw driver.Rows
// contract: integer pragmas arrive as int64, textual ones as string.
func readPragma(ctx context.Context, conn pragmaConn, name string) (string, error) {
	rows, err := conn.QueryContext(ctx, "PRAGMA "+name, nil)
	if err != nil {
		return "", err
	}
	defer rows.Close()
	dest := make([]driver.Value, 1)
	switch err := rows.Next(dest); {
	case errors.Is(err, io.EOF):
		return "", fmt.Errorf("PRAGMA %s returned no rows", name)
	case err != nil:
		return "", err
	}
	switch v := dest[0].(type) {
	case string:
		return v, nil
	case []byte:
		return string(v), nil
	case int64:
		return strconv.FormatInt(v, 10), nil
	default:
		return fmt.Sprintf("%v", v), nil
	}
}

// normalizePragmaValue canonicalizes a read-back for comparison: numeric pragmas compare
// as integers (the driver may spell them differently across versions), textual ones as
// lower-case.
func normalizePragmaValue(s string) string {
	v := strings.ToLower(strings.TrimSpace(s))
	if n, err := strconv.ParseInt(v, 10, 64); err == nil {
		return strconv.FormatInt(n, 10)
	}
	return v
}

// redactedDSN keeps connection errors diagnosable without echoing the full DSN into logs
// that may end up in support tickets; the path alone identifies the database.
func redactedDSN(dsn string) string {
	if i := strings.IndexByte(dsn, '?'); i >= 0 {
		return dsn[:i]
	}
	return dsn
}
