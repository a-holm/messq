// SPDX-License-Identifier: Apache-2.0

package store

import (
	"database/sql"
	"sync"

	"modernc.org/sqlite"
)

// This file is the entire driver-specific surface of the default (pure-Go) build: the
// driver name, the open path, and the hook registration that arms pragma verification.
// Everything above it speaks plain database/sql, which is the D1 escape hatch — swapping
// the engine for the cgo twin (slice 8) touches this file and its twin, nothing else.

// driverName is the registration modernc.org/sqlite installs in its own init; sql.Open on
// this name yields driver connections that run every registered connection hook.
const driverName = "sqlite"

// hookOnce guards hook registration. sqlite.RegisterConnectionHook appends to the driver's
// hook slice, so a second registration would run verification twice per connection —
// harmless in outcome (the second pass re-reads the same pragmas) but double the syscalls
// on every open, forever. Several stores may open in one process (tests, messq verify
// against a copy), so the guard is not optional.
var hookOnce sync.Once

// openSQLite opens a database/sql handle on the messq driver, arming the connection-hook
// verification first: hooks only fire for connections opened after registration, so the
// registration must precede the first sql.Open. The returned handle is not yet connected;
// the first Ping (or query) creates the physical connection and runs the hook, whose
// failure surfaces from there wrapped as "connection hook: …".
func openSQLite(dsn string) (*sql.DB, error) {
	hookOnce.Do(func() {
		sqlite.RegisterConnectionHook(func(conn sqlite.ExecQuerierContext, dsn string) error {
			return verifyConnectionPragmas(conn, dsn)
		})
	})
	return sql.Open(driverName, dsn)
}
