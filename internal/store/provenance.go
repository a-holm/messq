// SPDX-License-Identifier: Apache-2.0

package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/a-holm/messq/internal/clock"
)

// Snapshot provenance meta keys (issue #30 §4). The backup stamps snapshot_*
// into the snapshot's own meta table; this file owns the restore side of that
// contract: on the first writable start, the keys are rewritten to permanent
// restored_* rows — rewriting rather than keeping means a second restart does
// not re-announce a restore, and the record survives forever (five rows).
const (
	metaSnapshotTakenAt     = "snapshot_taken_at"     // unix ms of the backup read transaction
	metaSnapshotSourceNode  = "snapshot_source_node"  // source node_id
	metaSnapshotToolVersion = "snapshot_tool_version" // messq version (+commit) that wrote it
	metaSnapshotStreamHeads = "snapshot_stream_heads" // JSON {"orders":1057201,…}
	metaRestoredAt          = "restored_at"           // unix ms of the detecting start
	metaRestoredFromNode    = "restored_from_node"    // was snapshot_source_node
	metaRestoredSnapshotAt  = "restored_snapshot_at"  // was snapshot_taken_at
	metaRestoredStreamHeads = "restored_stream_heads" // was snapshot_stream_heads
	metaRestoredToolVersion = "restored_tool_version" // was snapshot_tool_version

	restoreDetectedEvent = "admin.action" // existing closed-vocabulary verb, detail.action=restore_detected
)

// Provenance describes where a data directory came from when it was restored
// from a messq backup. Nil from [Store.Provenance] means never restored.
type Provenance struct {
	RestoredAt   time.Time        // when the detecting start ran
	SnapshotAt   time.Time        // when the backup's read transaction began
	SourceNodeID string           // node_id of the source directory
	StreamHeads  map[string]int64 // stream → last seq at snapshot time
	ToolVersion  string           // messq version that took the snapshot
}

// provenanceSQL sweeps the snapshot_* keys and narrates the detection as one
// admin.action event row. No new event verb is added (§9.2 is closed);
// server.start's detail fields are derived downstream from
// RecoveryReport.Restored.
const (
	provenanceSweepSQL = `DELETE FROM meta WHERE k LIKE 'snapshot_%'`
	provenanceEventSQL = `
INSERT INTO events (ts, event, detail)
VALUES (?1, 'admin.action',
        json_object('action', 'restore_detected',
                    'snapshot_at_ms', ?2,
                    'source_node_id', ?3))`
)

// detectRestoredProvenance runs Open step 4.5. Two outcomes:
//
//   - A stamped snapshot is present → rewrite it to permanent restored_*
//     provenance inside a single write transaction and return
//     (provenance, true): the caller announces this start as a restore.
//   - The restored_* rows alone are present → load them and return
//     (provenance, false): the record survives every later restart, but a
//     second start does not re-announce a restore.
//
// It returns (nil, false) when the directory shows neither stamp set — the
// overwhelmingly common case. A present but unparseable stamp refuses startup
// with ErrCorrupt: a restored dir whose own history cannot be read is exactly
// the "missing tail as mystery" failure this feature exists to prevent.
func detectRestoredProvenance(ctx context.Context, rw *sql.DB, clk clock.Clock) (*Provenance, bool, error) {
	takenRaw, ok, err := readMeta(ctx, rw, metaSnapshotTakenAt)
	if err != nil {
		return nil, false, fmt.Errorf("read meta[%s]: %w", metaSnapshotTakenAt, err)
	}

	if !ok {
		return loadRestoredProvenance(ctx, rw, clk)
	}

	takenMs, parseErr := strconv.ParseInt(strings.TrimSpace(takenRaw), 10, 64)
	if parseErr != nil {
		return nil, false, fmt.Errorf("%w: meta[%s] = %q is not unix ms: %w",
			ErrCorrupt, metaSnapshotTakenAt, takenRaw, parseErr)
	}
	snapshotAt := time.UnixMilli(takenMs)

	sourceNode := readMetaOrEmpty(ctx, rw, metaSnapshotSourceNode)
	toolVersion := readMetaOrEmpty(ctx, rw, metaSnapshotToolVersion)
	headsRaw := readMetaOrEmpty(ctx, rw, metaSnapshotStreamHeads)

	heads := map[string]int64{}
	if headsRaw != "" {
		if unmarshalErr := json.Unmarshal([]byte(headsRaw), &heads); unmarshalErr != nil {
			return nil, false, fmt.Errorf("%w: meta[%s] is not stream-head JSON: %w",
				ErrCorrupt, metaSnapshotStreamHeads, unmarshalErr)
		}
	}

	now := clk.Now()
	tx, txErr := rw.BeginTx(ctx, nil)
	if txErr != nil {
		return nil, false, fmt.Errorf("begin restore-provenance transaction: %w", txErr)
	}
	fail := func(cause error) (*Provenance, bool, error) {
		if rbErr := tx.Rollback(); rbErr != nil && !errors.Is(rbErr, sql.ErrTxDone) {
			return nil, false, errors.Join(cause, fmt.Errorf("rollback restore-provenance: %w", rbErr))
		}
		return nil, false, cause
	}

	for _, kv := range [][2]string{
		{metaRestoredAt, strconv.FormatInt(now.UnixMilli(), 10)},
		{metaRestoredFromNode, sourceNode},
		{metaRestoredSnapshotAt, strconv.FormatInt(takenMs, 10)},
		{metaRestoredStreamHeads, headsRaw},
		{metaRestoredToolVersion, toolVersion},
	} {
		if _, execErr := tx.ExecContext(ctx,
			`INSERT INTO meta (k, v) VALUES (?, ?)
			 ON CONFLICT (k) DO UPDATE SET v = excluded.v`, kv[0], kv[1]); execErr != nil {
			return fail(fmt.Errorf("rewrite %s: %w", kv[0], execErr))
		}
	}
	if _, execErr := tx.ExecContext(ctx, provenanceSweepSQL); execErr != nil {
		return fail(fmt.Errorf("sweep snapshot_ keys: %w", execErr))
	}
	if _, execErr := tx.ExecContext(ctx, provenanceEventSQL,
		now.UnixMilli(), takenMs, sourceNode); execErr != nil {
		return fail(fmt.Errorf("record %s detection: %w", restoreDetectedEvent, execErr))
	}
	if commitErr := tx.Commit(); commitErr != nil {
		return nil, false, fmt.Errorf("commit restore-provenance transaction: %w", commitErr)
	}

	return &Provenance{
		RestoredAt:   now,
		SnapshotAt:   snapshotAt,
		SourceNodeID: sourceNode,
		StreamHeads:  heads,
		ToolVersion:  toolVersion,
	}, true, nil
}

// loadRestoredProvenance reconstructs the permanent provenance rows for a
// restart that finds no snapshot stamp left to convert.
func loadRestoredProvenance(ctx context.Context, rw *sql.DB, clk clock.Clock) (*Provenance, bool, error) {
	atRaw, ok, err := readMeta(ctx, rw, metaRestoredAt)
	if err != nil {
		return nil, false, fmt.Errorf("read meta[%s]: %w", metaRestoredAt, err)
	}
	if !ok {
		return nil, false, nil // never restored
	}
	atMs, parseErr := strconv.ParseInt(strings.TrimSpace(atRaw), 10, 64)
	if parseErr != nil {
		return nil, false, fmt.Errorf("%w: meta[%s] = %q is not unix ms: %w",
			ErrCorrupt, metaRestoredAt, atRaw, parseErr)
	}

	snapMs := atMs
	if snapRaw := readMetaOrEmpty(ctx, rw, metaRestoredSnapshotAt); snapRaw != "" {
		if v, vErr := strconv.ParseInt(strings.TrimSpace(snapRaw), 10, 64); vErr == nil {
			snapMs = v
		}
	}

	heads := map[string]int64{}
	if headsRaw := readMetaOrEmpty(ctx, rw, metaRestoredStreamHeads); headsRaw != "" {
		if unmarshalErr := json.Unmarshal([]byte(headsRaw), &heads); unmarshalErr != nil {
			return nil, false, fmt.Errorf("%w: meta[%s] is not stream-head JSON: %w",
				ErrCorrupt, metaRestoredStreamHeads, unmarshalErr)
		}
	}

	return &Provenance{
		RestoredAt:   time.UnixMilli(atMs),
		SnapshotAt:   time.UnixMilli(snapMs),
		SourceNodeID: readMetaOrEmpty(ctx, rw, metaRestoredFromNode),
		StreamHeads:  heads,
		ToolVersion:  readMetaOrEmpty(ctx, rw, metaRestoredToolVersion),
	}, false, nil
}

// readMetaOrEmpty reads one meta value, treating absence and read errors as "".
// Provenance fields are best-effort annotations around the load-bearing
// snapshot_taken_at check, which has its own strict handling.
func readMetaOrEmpty(ctx context.Context, rw *sql.DB, key string) string {
	v, ok, err := readMeta(ctx, rw, key)
	if err != nil || !ok {
		return ""
	}
	return v
}
