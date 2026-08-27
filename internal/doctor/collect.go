// SPDX-License-Identifier: Apache-2.0

package doctor

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"time"

	// The driver registration is borrowed from internal/store, exactly like
	// internal/verify/open.go: the engine import stays inside the store package
	// and this file opens its own read-only handle over that registration.
	_ "github.com/a-holm/messq/internal/store"
)

// StreamState is one stream's config and counters as doctor sees them.
type StreamState struct {
	Name        string
	Msgs        int64
	Bytes       int64
	MaxAgeMS    int64 // 0 = unlimited retention
	MaxBytes    int64 // 0 = unlimited size
	CreatedAtMS int64
}

// ConsumerState is one consumer's config as doctor sees it.
type ConsumerState struct {
	Stream      string
	Name        string
	AckWaitMS   int64
	MaxDeliver  int32  // 0 = unlimited
	DeadPolicy  string // "dlq" | "drop"
	Paused      bool
	CreatedAtMS int64
}

// RestoredProvenance mirrors store's restored_* meta rows (issue #30 §4) so
// the offline collector never needs a second open mode.
type RestoredProvenance struct {
	SnapshotAtMS int64
	SourceNodeID string
	StreamHeads  map[string]int64
	ToolVersion  string
}

// Snapshot is everything one doctor run knows. Checks read it; collectors fill it.
type Snapshot struct {
	Now time.Time

	Source Source `json:"source"`

	Streams   []StreamState
	Consumers []ConsumerState

	// Restored is non-nil when the data directory carries restore provenance.
	Restored *RestoredProvenance

	// Server carries /v1/info facts in live mode; nil offline.
	Server *ServerFacts
	// Unreachable is "" when live collection succeeded or the source is
	// offline; otherwise it records WHY no daemon answered. It feeds
	// server.unreachable — an unreachable daemon is a finding, never a
	// transport failure escaping to exit 6 (§10).
	Unreachable string
	// CollectErrors records partial collection failures (a family endpoint
	// failing while Info succeeded). Checks missing exactly that data emit
	// SevSkipped naming it; the run never dies over one bad endpoint.
	CollectErrors []string
}

// ServerFacts is what the live collector learns from /v1/info.
type ServerFacts struct {
	Version        string
	NodeID         string
	DurabilityMode string // "full" | "relaxed" — the daemon's configured mode
	Synchronous    int    // pragma read-back from a live pooled connection (#15)
	UptimeMS       int64
}

// OfflineCollector collects from a data directory via a read-only open — no
// data-dir flock, so it runs against a live broker's directory too (SQLite's
// WAL protocol coordinates the processes).
type OfflineCollector struct {
	DataDir string
}

// Collect gathers the snapshot. Collection errors refuse the run (the caller
// cannot diagnose anything without state); per-check gaps inside a collected
// snapshot degrade checks to SevSkipped instead.
func (o OfflineCollector) Collect(ctx context.Context) (*Snapshot, error) {
	db, err := sql.Open("sqlite", "file:"+o.DataDir+"/messq.db?_pragma=query_only(1)")
	if err != nil {
		return nil, fmt.Errorf("open %s read-only: %w", o.DataDir, err)
	}
	defer func() {
		if closeErr := db.Close(); closeErr != nil {
			err = fmt.Errorf("close %s: %w", o.DataDir, closeErr)
		}
	}()
	// Force the lazy driver to actually open NOW so every later failure keeps
	// the data directory in its message — a refusal that cannot name the path
	// it refused teaches nothing.
	if pingErr := db.PingContext(ctx); pingErr != nil {
		return nil, fmt.Errorf("open %s read-only: %w", o.DataDir, pingErr)
	}

	snap := &Snapshot{Source: SourceDataDir}

	rows, qErr := db.QueryContext(ctx, `
		SELECT s.name,
		       COALESCE(st.msgs, 0),
		       COALESCE(st.bytes, 0),
		       s.max_age_ms,
		       s.max_bytes,
		       s.created_at
		  FROM streams s
		  LEFT JOIN stream_stats st ON st.stream = s.name
		 ORDER BY s.name`)
	if qErr != nil {
		return nil, fmt.Errorf("read streams: %w", qErr)
	}
	defer func() {
		if closeErr := rows.Close(); closeErr != nil {
			err = fmt.Errorf("close streams rows: %w", closeErr)
		}
	}()
	for rows.Next() {
		var s StreamState
		if scanErr := rows.Scan(&s.Name, &s.Msgs, &s.Bytes, &s.MaxAgeMS, &s.MaxBytes, &s.CreatedAtMS); scanErr != nil {
			return nil, fmt.Errorf("scan streams: %w", scanErr)
		}
		snap.Streams = append(snap.Streams, s)
	}
	if errErr := rows.Err(); errErr != nil {
		return nil, fmt.Errorf("iterate streams: %w", errErr)
	}

	crows, cErr := db.QueryContext(ctx, `
		SELECT stream, name, ack_wait_ms, max_deliver, dead_policy, paused, created_at
		  FROM consumers
		 ORDER BY stream, name`)
	if cErr != nil {
		return nil, fmt.Errorf("read consumers: %w", cErr)
	}
	defer func() {
		if closeErr := crows.Close(); closeErr != nil {
			err = fmt.Errorf("close consumers rows: %w", closeErr)
		}
	}()
	for crows.Next() {
		var c ConsumerState
		if scanErr := crows.Scan(&c.Stream, &c.Name, &c.AckWaitMS, &c.MaxDeliver,
			&c.DeadPolicy, &c.Paused, &c.CreatedAtMS); scanErr != nil {
			return nil, fmt.Errorf("scan consumers: %w", scanErr)
		}
		snap.Consumers = append(snap.Consumers, c)
	}
	if errErr := crows.Err(); errErr != nil {
		return nil, fmt.Errorf("iterate consumers: %w", errErr)
	}

	snap.Restored = collectProvenance(ctx, db)
	return snap, err
}

// collectProvenance reads the restored_* meta rows when present.
func collectProvenance(ctx context.Context, db *sql.DB) *RestoredProvenance {
	var atMS int64
	scanErr := db.QueryRowContext(ctx,
		`SELECT v FROM meta WHERE k = 'restored_snapshot_at'`).Scan(&atMS)
	if scanErr != nil {
		return nil // absent or unreadable: not our finding to make here
	}
	prov := &RestoredProvenance{SnapshotAtMS: atMS}
	if v := metaString(ctx, db, "restored_from_node"); v != "" {
		prov.SourceNodeID = v
	}
	if v := metaString(ctx, db, "restored_tool_version"); v != "" {
		prov.ToolVersion = v
	}
	if raw := metaString(ctx, db, "restored_stream_heads"); raw != "" {
		heads := map[string]int64{}
		if jsonErr := json.Unmarshal([]byte(raw), &heads); jsonErr == nil {
			prov.StreamHeads = heads
		}
	}
	return prov
}

func metaString(ctx context.Context, db *sql.DB, key string) string {
	var v string
	if scanErr := db.QueryRowContext(ctx,
		`SELECT v FROM meta WHERE k = ?`, key).Scan(&v); scanErr != nil {
		return ""
	}
	return v
}
