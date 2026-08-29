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
	"strconv"
	"syscall"
	"time"

	"github.com/a-holm/messq/internal/buildinfo"
)

// The snapshot provenance keys stamped into the SNAPSHOT's own meta table
// (issue #30 §4 — a frozen contract consumed by store.Open step 4.5, doctor's
// backup checks, and #34's fixtures). Never written to the source: the source
// is opened read-only and owned by another process.
const (
	MetaSnapshotTakenAt        = "snapshot_taken_at"               // unix ms of the snapshot read transaction
	MetaSnapshotSourceNode     = "snapshot_source_node"            // source meta.node_id
	MetaSnapshotSourcePath     = "snapshot_source_path"            // absolute path of the source messq.db
	MetaSnapshotToolVersion    = "snapshot_tool_version"           // messq version (+commit) that wrote it
	MetaSnapshotSourceLive     = "snapshot_source_live"            // "1" when a daemon held the data-dir flock
	MetaSnapshotStreamHeads    = "snapshot_stream_heads"           // JSON {"orders":1057201,…}, ≤ 64 streams
	MetaSnapshotHeadsTruncated = "snapshot_stream_heads_truncated" // "1" when heads omitted

	maxStampedStreams = 64 // above this the heads JSON is omitted, not truncated mid-map
)

// provenance is what one run knows about itself at stamp time.
type provenance struct {
	TakenAt      time.Time
	DataDir      string
	SourceDB     string
	SourceNodeID string
	Live         bool
	Heads        map[string]int64
}

// stamp writes the provenance rows into the snapshot's meta table. The write
// runs with synchronous=FULL so the stamp survives the power loss the rename
// just survived; a snapshot without provenance would restore as a mystery.
func stamp(ctx context.Context, snapPath string, p provenance) error {
	db, openErr := sql.Open("sqlite",
		"file:"+snapPath+"?_pragma=synchronous(2)")
	if openErr != nil {
		return fmt.Errorf("open snapshot for stamping: %w", openErr)
	}
	defer func() {
		if closeErr := db.Close(); closeErr != nil {
			openErr = errors.Join(openErr, fmt.Errorf("close stamped snapshot: %w", closeErr))
		}
	}()

	tx, txErr := db.BeginTx(ctx, nil)
	if txErr != nil {
		return fmt.Errorf("begin stamp transaction: %w", txErr)
	}

	put := func(key, value string) error {
		if _, execErr := tx.ExecContext(ctx,
			`INSERT INTO meta (k, v) VALUES (?, ?)
			 ON CONFLICT (k) DO UPDATE SET v = excluded.v`, key, value); execErr != nil {
			return fmt.Errorf("stamp %s: %w", key, execErr)
		}
		return nil
	}

	info := buildinfo.Get()
	version := info.Version
	if info.Commit != "" {
		version += "+" + info.Commit
	}

	sets := [][2]string{
		{MetaSnapshotTakenAt, strconv.FormatInt(p.TakenAt.UnixMilli(), 10)},
		{MetaSnapshotSourceNode, p.SourceNodeID},
		{MetaSnapshotSourcePath, p.SourceDB},
		{MetaSnapshotToolVersion, version},
		{"clean_shutdown", "1"}, // VACUUM INTO output is consistent by construction
	}
	if p.Live {
		sets = append(sets, [2]string{MetaSnapshotSourceLive, "1"})
	} else {
		sets = append(sets, [2]string{MetaSnapshotSourceLive, "0"})
	}
	if len(p.Heads) > 0 && len(p.Heads) <= maxStampedStreams {
		raw, marshalErr := json.Marshal(p.Heads)
		if marshalErr != nil {
			return stampAbort(tx, fmt.Errorf("marshal stream heads: %w", marshalErr))
		}
		sets = append(sets, [2]string{MetaSnapshotStreamHeads, string(raw)})
	} else if len(p.Heads) > maxStampedStreams {
		sets = append(sets, [2]string{MetaSnapshotHeadsTruncated, "1"})
	}
	for _, kv := range sets {
		if putErr := put(kv[0], kv[1]); putErr != nil {
			return stampAbort(tx, putErr)
		}
	}
	if commitErr := tx.Commit(); commitErr != nil {
		return fmt.Errorf("commit stamp transaction: %w", commitErr)
	}
	return openErr
}

func stampAbort(tx *sql.Tx, cause error) error {
	if rbErr := tx.Rollback(); rbErr != nil && !errors.Is(rbErr, sql.ErrTxDone) {
		return errors.Join(cause, fmt.Errorf("rollback stamp: %w", rbErr))
	}
	return cause
}

// sourceIsLive reports whether something currently holds the data directory's
// exclusive flock — in practice, a running daemon (the daemon locks LOCK_EX,
// inspectors LOCK_SH, #5's datadir contract). Best-effort by design: an
// unreadable LOCK reports not-live rather than failing the whole backup, and
// the flag is provenance metadata, not a safety decision.
func sourceIsLive(dataDir string) bool {
	lock, openErr := os.OpenFile(filepath.Join(dataDir, "LOCK"), os.O_RDWR, 0o600)
	if openErr != nil {
		return false
	}
	defer func() {
		if closeErr := lock.Close(); closeErr != nil {
			_ = closeErr // probe only: a close error cannot change the answer already taken
		}
	}()
	if flockErr := syscall.Flock(int(lock.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); flockErr != nil {
		return true // held by someone else
	}
	_ = syscall.Flock(int(lock.Fd()), syscall.LOCK_UN) //nolint:errcheck // releasing our own successful probe
	return false
}
