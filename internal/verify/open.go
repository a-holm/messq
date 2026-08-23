// SPDX-License-Identifier: Apache-2.0

// Package verify is the invariant-checker registry behind `messq verify`: the structural
// checks (V1–V6) and the §5.2 predicates expressible over the schema (I2/I4/I5/I6, and I8
// with the I10 event fold under --deep). Every check runs inside one read transaction, and
// no check can write. The read-only open below is shared by the crash harness's reconciler,
// so the two can never drift in access mode.
package verify

import (
	"database/sql"
	"fmt"
	"path/filepath"

	_ "modernc.org/sqlite" // registers the "sqlite" driver (the pure-Go engine)
)

// Open opens the data dir's messq.db for read-only inspection: the connection is permitted
// to run WAL recovery (which writes -shm) but is fenced write-off by query_only(1), so no
// check can mutate a row. A truly immutable open (immutable=1) is deliberately not used —
// it would ignore the WAL tail and report every recent message as lost (edge case 15).
func Open(dataDir string) (*sql.DB, error) {
	path := filepath.Join(dataDir, "messq.db")
	db, err := sql.Open("sqlite", "file:"+path+"?_pragma=query_only(1)")
	if err != nil {
		return nil, fmt.Errorf("open %s read-only: %w", path, err)
	}
	return db, nil
}
