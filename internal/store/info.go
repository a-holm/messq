// SPDX-License-Identifier: Apache-2.0

package store

import (
	"context"
	"database/sql"
	"fmt"

	"golang.org/x/sys/unix"
)

// The /v1/info read paths (issue #15 §2). Counts ride the shared read pool like every
// inspection query; the durability synchronous value is re-read from a LIVE pooled
// connection rather than trusted from the open-time expectation set, so "a 201 survives
// power loss: YES" (#30) stays an observation instead of a claim.

// LiveSynchronous reads PRAGMA synchronous from a connection handed out by the read
// pool right now. This is the honest answer: the pragma hook verifies at connect time,
// and this re-read lets an operator see the durable-promise the pool is currently
// keeping (2 = FULL, 1 = NORMAL) without any trust in flags or DSNs.
func (s *Store) LiveSynchronous(ctx context.Context) (int, error) {
	ro := s.readPool()
	conn, err := ro.Conn(ctx)
	if err != nil {
		return 0, fmt.Errorf("live synchronous: acquire pooled conn: %w", err)
	}
	defer func() {
		if cerr := conn.Close(); cerr != nil {
			_ = cerr // pool return is best-effort; the caller's error carries the story
		}
	}()
	rows, err := conn.QueryContext(ctx, `PRAGMA synchronous`)
	if err != nil {
		return 0, fmt.Errorf("live synchronous: %w", err)
	}
	defer func() {
		if cerr := rows.Close(); cerr != nil {
			_ = cerr
		}
	}()
	if !rows.Next() {
		if rerr := rows.Err(); rerr != nil {
			return 0, fmt.Errorf("live synchronous: %w", rerr)
		}
		return 0, fmt.Errorf("live synchronous: no row")
	}
	var v sql.Null[int64]
	if err := rows.Scan(&v); err != nil {
		return 0, fmt.Errorf("live synchronous: scan: %w", err)
	}
	if !v.Valid || v.V < 0 || v.V > 3 {
		return 0, fmt.Errorf("live synchronous: implausible value %v", v)
	}
	return int(v.V), nil
}

// DataDirPath returns the directory this store keeps its database in — the statfs
// subject for /v1/info's disk_free_bytes.
func (s *Store) DataDirPath() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.dir
}

// InfoCounts is the row census /v1/info reports: streams, consumers, and the size of
// the audit trail.
type InfoCounts struct {
	Streams    int64 `json:"streams"`
	Consumers  int64 `json:"consumers"`
	EventsRows int64 `json:"events_rows"`
}

// InfoCounts reads the census from the read pool. events rows are append-only (D11:
// the audit trail is never rewritten), so the exact count is one indexed max(id);
// streams and consumers are bounded tables whose COUNT(*) scans stay cheap.
func (s *Store) InfoCounts(ctx context.Context) (InfoCounts, error) {
	ro := s.readPool()
	var out InfoCounts
	var maxID sql.Null[int64]
	if err := ro.QueryRowContext(ctx,
		`SELECT coalesce((SELECT count(*) FROM streams), 0),
		        coalesce((SELECT count(*) FROM consumers), 0),
		        coalesce(max(id), 0) FROM events`).Scan(&out.Streams, &out.Consumers, &maxID); err != nil {
		return InfoCounts{}, fmt.Errorf("info counts: %w", err)
	}
	out.EventsRows = maxID.V // append-only trail: max id IS the row count
	return out, nil
}

// DiskFreeBytes reports free bytes on the filesystem holding the data directory via
// statfs. A store opened on a directory that has disappeared reports the error; callers
// render it as zero rather than failing the endpoint.
func (s *Store) DiskFreeBytes() (int64, error) {
	dir := s.DataDirPath()
	var st unix.Statfs_t
	if err := unix.Statfs(dir, &st); err != nil {
		return 0, fmt.Errorf("statfs %q: %w", dir, err)
	}
	// Bavail is the unprivileged-free block count (reserved blocks excluded), which is
	// what "can this node still write?" means for the operator reading the number. The
	// product overflows int64 only past 8 EiB, which saturates instead of lying negative.
	const maxI64 = ^uint64(0) >> 1
	//nolint:gosec // G115: st.Bsize is a filesystem block size (512..16 MiB); positivity checked.
	bsize := uint64(st.Bsize)
	if bsize == 0 || st.Bavail > maxI64/bsize {
		return int64(maxI64), nil // absurd fs geometry or past-8-EiB free space: saturate
	}
	free := st.Bavail * bsize // bounded above by maxI64 by the guard
	//nolint:gosec // G115: the guard above bounds free <= maxI64, so the cast is exact.
	return int64(free), nil
}
