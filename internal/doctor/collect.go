// SPDX-License-Identifier: Apache-2.0

package doctor

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
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

	// SampleSubjects holds recent publish subjects (bounded probe for the
	// filter-matching check); nil means "not sampled", never "empty".
	SampleSubjects []string
}

// ConsumerState is one consumer's config as doctor sees it.
type ConsumerState struct {
	Stream        string
	Name          string
	Filters       []string // subject filters; [">"] means everything
	BackoffMS     []int64  // nak backoff ladder; drives TimeToDead approximations
	MaxAckPending int64    // flow-control cap; 0 unset means store default 1000
	AckWaitMS     int64
	MaxDeliver    int32  // 0 = unlimited
	DeadPolicy    string // "dlq" | "drop"
	Paused        bool
	CreatedAtMS   int64
}

// PendingFacts aggregates the deliveries table for ONE consumer without any
// unbounded scan (D5 keeps the pending set small by construction).
type PendingFacts struct {
	PendingCount    int64 // rows in deliveries right now
	InflightCount   int64
	OldestReadyMS   int64 // age of the oldest READY delivery, 0 = none
	LastDeliveredMS int64 // newest delivered_at; 0 = never delivered
	PausedAtMS      int64 // when this consumer paused, from its latest event
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

	// Storage/Durability/Fsync are the disk-side fact bundles. A nil bundle
	// means "not collected"; consuming checks then skip with a reason.
	Storage    *StorageFacts
	Durability *DurabilityFacts
	Fsync      *FsyncFacts

	// Pending maps "stream\x00consumer" to the deliveries aggregate.
	Pending map[string]PendingFacts

	// Metrics arrives from /metrics in live mode; nil offline.
	Metrics *MetricFacts

	// Security holds permission + listener observations when collected.
	Security *SecurityFacts
	// ServerKnobs expose sweep/janitor/reserve configuration from a daemon
	// that publishes them; zero values mean unknown.
	ServerKnobs ListenerConfigFacts

	// Backup family: dir configured plus its observed listing (bounded).
	BackupDir    string
	BackupMaxAge time.Duration
	Backups      []BackupFile

	// Events carries windowed event aggregates when collected.
	Events EventStats

	// Analysis knobs the CLI passes through (§9 flags); zero means a check's
	// documented default applies.
	Window    time.Duration
	IdleAfter time.Duration

	MinFreeBytes int64
	WalMaxBytes  int64
}

// MetricFacts is the slice of /metrics doctor reads (§9.4); the promtext
// scanner lands with its own issue and feeds this shape.
type MetricFacts struct {
	StaleAcksTotal       int64
	DroppedSeries        int64
	PausedConsumers      int64
	StaleAckTopConsumers map[string]int64 // "stream/consumer" -> count
}

// EventStats carries bounded event aggregates in window terms.
type EventStats struct {
	RetentionMS int64 // --event-retention as configured; 0 = unknown here

	DeadGrowthKnown bool             // DeadByOrigin is trustworthy
	DeadByOrigin    map[string]int64 // origin stream -> new dead letters in window
	RedriveCounts   map[string]int64 // origin stream -> dlq.redrive events in window
	DLQDriftList    []DLQDrift       // template-drift reports when #12 lands facts

	StartHistoryKnown bool // starts/unclean facts were collected
	LastStartUnclean  bool
	RecentStarts      int64 // server.start events inside the window
	ClockJumpMS       int64 // backwards wall-clock jump recorded since start
}

// DLQDrift is one reported template mismatch.
type DLQDrift struct {
	Stream string
	Diff   string
}

// Snapshot gains ops-family facts (knobs, security, backups).

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

	// Disk-side facts ride the same read-only handle: file sizes and statfs
	// need no lock, and the synchronous value we report honestly as OUR OWN
	// connection's (offline cannot see the daemon's writer, per §6).
	if storage, sErr := collectStorageFacts(o.DataDir); sErr == nil {
		snap.Storage = storage
	} else {
		err = errors.Join(err, fmt.Errorf("collect storage facts: %w", sErr))
	}
	var sync int
	if pErr := db.QueryRowContext(ctx,
		`SELECT synchronous FROM pragma_synchronous`).Scan(&sync); pErr == nil {
		snap.Durability = &DurabilityFacts{Synchronous: sync, OwnConnection: true}
	}

	snap.Pending = collectPendingFacts(ctx, db)
	collectPausedAges(ctx, db, snap)
	collectSubjectSamples(ctx, db, snap)
	collectEventAggregates(ctx, db, snap)

	snap.Security = collectPermFacts(o.DataDir)
	return snap, err
}

// collectOpsFacts covers the families that live OUTSIDE the database: backup
// directory listings ride the filesystem directly and are wired per-run by
// Collect when configured.

// collectPendingFacts aggregates deliveries once, grouped per consumer pair;
// the pending set is small by construction (D5) so the group scan stays cheap.
// The oldest-ready AGE deliberately rides zero here: deriving it needs #27's
// oldest-pending projection, and doctor would rather skip than guess.
func collectPendingFacts(ctx context.Context, db *sql.DB) map[string]PendingFacts {
	out := map[string]PendingFacts{}
	rows, qErr := db.QueryContext(ctx, `
		SELECT stream, consumer,
		       COUNT(*),
		       COALESCE(SUM(CASE WHEN state = 1 THEN 1 ELSE 0 END), 0),
		       COALESCE(MAX(delivered_at), 0)
		  FROM deliveries
		 GROUP BY stream, consumer`)
	if qErr != nil {
		return nil // facts unavailable: checks skip rather than guess
	}
	defer func() {
		if cErr := rows.Close(); cErr != nil {
			// iterated fully already; the deferred close failing can only mean
			// the driver is wedged — nothing left to do but let the caller's
			// rows.Err-style checks have seen the honest picture above.
			_ = cErr
		}
	}()
	for rows.Next() {
		var stream, consumer string
		var pf PendingFacts
		if sErr := rows.Scan(&stream, &consumer, &pf.PendingCount,
			&pf.InflightCount, &pf.LastDeliveredMS); sErr == nil {
			out[stream+"\x00"+consumer] = pf
		}
	}
	if rErr := rows.Err(); rErr != nil {
		return nil // partial rows would be lies; checks skip instead
	}
	return out
}

// collectPausedAges reads each paused consumer's most recent pause event so
// "paused since when" answers from history instead of guesses.
func collectPausedAges(ctx context.Context, db *sql.DB, snap *Snapshot) {
	for _, c := range snap.Consumers {
		if !c.Paused {
			continue
		}
		var at sql.NullInt64
		row := db.QueryRowContext(ctx, `
			SELECT MAX(ts) FROM events
			 WHERE event = 'consumer.pause' AND stream = ? AND consumer = ?`,
			c.Stream, c.Name)
		if row.Scan(&at) == nil && at.Valid {
			key := c.Stream + "\x00" + c.Name
			pf := snap.Pending[key]
			pf.PausedAtMS = at.Int64
			snap.Pending[key] = pf
		}
	}
}

// maxSampleStreams caps the fan-out of the subject-sampling probe.
const maxSampleStreams = 64

func collectSubjectSamples(ctx context.Context, db *sql.DB, snap *Snapshot) {
	filtered := 0
	for _, st := range snap.Streams {
		for _, c := range snap.Consumers {
			if c.Stream == st.Name && !hasFanOutFilter(c.Filters) {
				filtered++
				break
			}
		}
	}
	if filtered == 0 || filtered > maxSampleStreams {
		return // nothing to match against, or too many probes to be cheap
	}
	for i := range snap.Streams {
		st := &snap.Streams[i]
		if st.Msgs == 0 || !needsSampling(snap.Consumers, st.Name) {
			continue
		}
		st.SampleSubjects = sampleSubjects(ctx, db, st.Name)
	}
}

// sampleSubjects reads one stream's most recent subjects; any failure along
// the way returns nil, which checks treat as "not sampled" rather than data.
func sampleSubjects(ctx context.Context, db *sql.DB, stream string) []string {
	rows, qErr := db.QueryContext(ctx, `
		SELECT subject FROM messages
		 WHERE stream = ?
		 ORDER BY seq DESC LIMIT ?`, stream, sampleSubjectsLookBack)
	if qErr != nil {
		return nil
	}
	defer func() {
		if cErr := rows.Close(); cErr != nil {
			// Same wedge case as above: nothing beyond honesty to do here.
			_ = cErr
		}
	}()
	var subjects []string
	for rows.Next() {
		var subj string
		if sErr := rows.Scan(&subj); sErr != nil {
			return nil
		}
		subjects = append(subjects, subj)
	}
	if rErr := rows.Err(); rErr != nil {
		return nil
	}
	return subjects
}

func hasFanOutFilter(filters []string) bool {
	return len(filters) == 0 || (len(filters) == 1 && filters[0] == ">")
}

func needsSampling(consumers []ConsumerState, stream string) bool {
	for _, c := range consumers {
		if c.Stream == stream && !hasFanOutFilter(c.Filters) {
			return true
		}
	}
	return false
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

// collectEventAggregates fills the windowed ops facts from the events table:
// dead/redrive counts per origin, daemon starts in window, and whether the
// newest start followed an unclean shutdown. Everything is a bounded grouped
// scan riding events_ts; failures degrade to "not collected".
func collectEventAggregates(ctx context.Context, db *sql.DB, snap *Snapshot) {
	snap.Events.DeadByOrigin = map[string]int64{}
	snap.Events.RedriveCounts = map[string]int64{}

	rows, qErr := db.QueryContext(ctx, `
		SELECT event, COALESCE(stream, ''), COUNT(*)
		  FROM events
		 WHERE event IN ('msg.dead', 'dlq.redrive', 'server.start')
		   AND ts >= ?
		 GROUP BY event, stream`, sinceFloorMS(snap))
	if qErr != nil {
		return
	}
	defer func() {
		if cErr := rows.Close(); cErr != nil {
			_ = cErr // close-after-full-iteration wedge; nothing further to do
		}
	}()
	sawAny := false
	for rows.Next() {
		var evName, stream string
		var count int64
		if sErr := rows.Scan(&evName, &stream, &count); sErr != nil {
			return
		}
		sawAny = true
		switch evName {
		case "msg.dead":
			snap.Events.DeadByOrigin[stream] += count
		case "dlq.redrive":
			snap.Events.RedriveCounts[stream] += count
		case "server.start":
			snap.Events.RecentStarts += count
		}
	}
	if rErr := rows.Err(); rErr != nil {
		return
	}
	snap.Events.DeadGrowthKnown = sawAny || true // empty table is truth too

	row := db.QueryRowContext(ctx, `
		SELECT event FROM events
		 WHERE event IN ('server.start', 'recovery.unclean')
		 ORDER BY id DESC LIMIT 1`)
	var last string
	if row.Scan(&last) == nil {
		snap.Events.StartHistoryKnown = true
		snap.Events.LastStartUnclean = last == "recovery.unclean"
	}
}

// sinceFloorMS clamps the analysis window to what this snapshot knows: a wall
// now plus one day lookback is enough for doctor's aggregates without any
// whole-table scan. Precise --since clamping evidence lands with #27.
func sinceFloorMS(snap *Snapshot) int64 {
	if snap.Now.IsZero() {
		return 0
	}
	return snap.Now.Add(-24 * time.Hour).UnixMilli()
}

// collectPermFacts stats the data dir, database and token file exactly as
// #16's preflight would; zeroed mode means "could not stat".
func collectPermFacts(dir string) *SecurityFacts {
	sec := &SecurityFacts{}
	if info, err := os.Stat(dir); err == nil {
		sec.DataDirMode = uint32(info.Mode().Perm())
	}
	if info, err := os.Stat(filepath.Join(dir, "messq.db")); err == nil {
		sec.DBFileMode = uint32(info.Mode().Perm())
	}
	for _, name := range []string{"token", "auth.json", "token.json"} {
		info, err := os.Stat(filepath.Join(dir, name))
		if err == nil {
			sec.TokenFilePath = name
			sec.TokenFileMode = uint32(info.Mode().Perm())
			break
		}
	}
	return sec
}

// maxBackupListingFiles caps the backup-dir walk so a runabout directory can
// never make doctor slow or unbounded.
const maxBackupListingFiles = 20

// collectBackupFacts lists the newest snapshots in dir with their modes and
// stamp visibility (name-shape based: .db file with .messq-backup provenance
// markers is verified by `backup` itself at write time; the deep quick_check
// re-run arrives with the disk-budget work and rides StampState "unknown").
func collectBackupFacts(dir string) []BackupFile {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil
	}
	type dated struct {
		path string
		mod  int64
		mode uint32
		size int64
	}
	var files []dated
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".db") {
			continue
		}
		info, sErr := e.Info()
		if sErr != nil {
			continue
		}
		files = append(files, dated{
			path: filepath.Join(dir, e.Name()),
			mod:  info.ModTime().UnixMilli(),
			mode: uint32(info.Mode().Perm()),
			size: info.Size(),
		})
	}
	sort.Slice(files, func(i, j int) bool { return files[i].mod > files[j].mod })
	if len(files) > maxBackupListingFiles {
		files = files[:maxBackupListingFiles]
	}
	out := make([]BackupFile, 0, len(files))
	for _, f := range files {
		stamp := "missing"
		if probeStampMarker(f.path) {
			stamp = "ok"
		}
		out = append(out, BackupFile{
			Path:       f.path,
			ModTimeMS:  f.mod,
			Bytes:      f.size,
			Mode:       f.mode,
			StampState: stamp,
		})
	}
	return out
}

// probeStampMarker does a cheap presence check of the snapshot_* key prefix by
// reading the first pages' strings — full meta opens are the backup command's
// job; here we only distinguish "stamped" from "plain SQLite file".
func probeStampMarker(path string) bool {
	f, err := os.Open(path)
	if err != nil {
		return false
	}
	defer func() {
		if cErr := f.Close(); cErr != nil {
			_ = cErr // read-only handle; close failure cannot corrupt anything
		}
	}()
	var buf [4096]byte
	n, rErr := io.ReadFull(f, buf[:])
	if rErr != nil && rErr != io.ErrUnexpectedEOF && rErr != io.EOF {
		return false
	}
	return strings.Contains(string(buf[:max(0, n)]), "snapshot_taken_at")
}
