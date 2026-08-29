// SPDX-License-Identifier: Apache-2.0

package backup

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/a-holm/messq/internal/clock"
)

// defaultCacheBytes bounds the snapshot connection's page cache (issue #30 §2
// flag table): 16 MiB — a fraction of the store's 64 MiB pool default, because
// the vacuum's RSS must stay predictable on the operator's box.
const defaultCacheBytes = int64(16) << 20

// ErrUsage marks refusals that are the caller's mistake (empty or "-" as the
// destination): the CLI layer renders them at exit code 2, not as runtime
// failures. VACUUM INTO needs a seekable file; streaming to stdout would be a
// lie about what a backup is.
var ErrUsage = errors.New("usage")

// VerifyMode selects the self-check strength; slice 3 consumes it.
type VerifyMode uint8

const (
	VerifyQuick VerifyMode = iota // PRAGMA quick_check (default)
	VerifyFull                    // PRAGMA integrity_check (--verify full)
	VerifyNone                    // skip; the CLI prints a WARN line
)

// Options configures one backup of DataDir into Dest.
type Options struct {
	// DataDir is the source data directory holding messq.db.
	DataDir string
	// Dest is the destination file path. It must be absolute and must not
	// live inside the data directory (it would join the next snapshot).
	Dest string
	// Force overwrites an existing destination — still via temp+rename, so a
	// failed forced run leaves the old file untouched.
	Force bool
	// Verify selects the self-check run before the rename lands.
	Verify VerifyMode
	// CacheBytes bounds the snapshot connection's page cache;
	// <= 0 means 16 MiB.
	CacheBytes int64
	// Clock is the time seam (#3); nil means clock.System{}.
	Clock clock.Clock
}

func (o *Options) applyDefaults() {
	if o.CacheBytes <= 0 {
		o.CacheBytes = defaultCacheBytes
	}
	if o.Clock == nil {
		o.Clock = clock.System{}
	}
}

// DestinationExistsError refuses to overwrite an existing backup without
// --force: overwriting in place destroys the only good copy if this run fails.
type DestinationExistsError struct {
	Path    string
	Size    int64
	ModTime time.Time
}

func (e *DestinationExistsError) Error() string {
	return fmt.Sprintf("destination already exists: %s (%d bytes)", e.Path, e.Size)
}

// InsideDataDirError refuses a destination under the data directory: it would
// be included in the next snapshot and confuses messq verify.
type InsideDataDirError struct {
	Dest    string
	DataDir string
}

func (e *InsideDataDirError) Error() string {
	return fmt.Sprintf("destination %s is inside the data directory %s", e.Dest, e.DataDir)
}

// NotWritableError refuses an unwritable destination directory.
type NotWritableError struct {
	Dir   string
	Cause error
}

func (e *NotWritableError) Error() string {
	return fmt.Sprintf("destination directory %s is not writable: %v", e.Dir, e.Cause)
}

func (e *NotWritableError) Unwrap() error { return e.Cause }

// Plan validates one backup and performs every refusal BEFORE a single page is
// copied (G4), then sizes the work. On success the caller must hand the plan
// to exactly one Run; on error nothing was touched anywhere. (The issue sketch
// names the type "Plan" too; Go cannot overload the identifier, so the type is
// SnapshotPlan and the verb keeps the short name.)
func Plan(ctx context.Context, o Options) (*SnapshotPlan, error) {
	o.applyDefaults()

	switch {
	case o.Dest == "", o.Dest == "-":
		return nil, fmt.Errorf("%w: destination must be a file path (- would stream, but VACUUM INTO needs a seekable file)", ErrUsage)
	case !filepath.IsAbs(o.Dest):
		return nil, fmt.Errorf("%w: destination %q must be an absolute path", ErrUsage, o.Dest)
	}
	destDir := filepath.Dir(o.Dest)

	// Step 1: open the source read-only first — its failure outranks every
	// destination refusal because nothing else can even be judged without it.
	//
	// The access mode is verify.Open's, not store.Open(ReadOnly): the daemon
	// holds the data-dir flock EXCLUSIVE for its whole life (#5), so a
	// flock-taking read-only open could never run against a live broker. A raw
	// query_only(1) connection needs no data-dir lock — SQLite's own WAL
	// protocol coordinates the two processes — which is what makes the backup
	// usable while the daemon runs AND after it stops (issue §2 step 1). The
	// snapshot connection still never writes: query_only fences it.
	srcPath := filepath.Join(o.DataDir, "messq.db")
	src, identity, openErr := openSourceFacts(ctx, srcPath)
	if openErr != nil {
		return nil, fmt.Errorf("open source data dir read-only: %w", openErr)
	}
	defer func() {
		if closeErr := src.Close(); closeErr != nil {
			openErr = errors.Join(openErr, fmt.Errorf("close source: %w", closeErr))
		}
	}()

	// Step 2a: existing destination without --force → exit 4 territory.
	if info, statErr := os.Stat(o.Dest); statErr == nil {
		if !o.Force {
			mod := time.Time{}
			if info.Mode().IsRegular() {
				mod = info.ModTime()
			}
			return nil, &DestinationExistsError{Path: o.Dest, Size: info.Size(), ModTime: mod}
		}
	} else if !errors.Is(statErr, os.ErrNotExist) {
		return nil, fmt.Errorf("stat destination %s: %w", o.Dest, statErr)
	}

	// Step 2b: destination inside the data dir → exit 2 territory.
	if insideDataDir(o.Dest, o.DataDir) {
		return nil, &InsideDataDirError{Dest: o.Dest, DataDir: o.DataDir}
	}

	// Step 2c: destination directory writable → exit 7 territory.
	if probeErr := probeWritable(destDir); probeErr != nil {
		return nil, &NotWritableError{Dir: destDir, Cause: probeErr}
	}

	// Step 3: size estimate from the source's own pragmas, and step 2d's
	// free-space precheck against the ×1.1 requirement.
	var pageCount, freelist, pageSize int64
	scanErr := src.QueryRowContext(ctx, `SELECT
			(SELECT page_count FROM pragma_page_count),
			(SELECT freelist_count FROM pragma_freelist_count),
			(SELECT page_size FROM pragma_page_size)`).
		Scan(&pageCount, &freelist, &pageSize)
	if scanErr != nil {
		return nil, fmt.Errorf("read source sizing pragmas: %w", scanErr)
	}
	var userVersion, autoVacuum int64
	if uvErr := src.QueryRowContext(ctx,
		`SELECT (SELECT user_version FROM pragma_user_version),
		        (SELECT auto_vacuum FROM pragma_auto_vacuum)`).
		Scan(&userVersion, &autoVacuum); uvErr != nil {
		return nil, fmt.Errorf("read source identity pragmas: %w", uvErr)
	}

	free, freeErr := FreeBytes(destDir)
	if freeErr != nil {
		return nil, freeErr
	}
	estimate := EstimateBytes(pageCount, freelist, pageSize)
	required := RequiredBytes(estimate)
	if spaceErr := CheckSpace(free, required); spaceErr != nil {
		return nil, spaceErr
	}

	return &SnapshotPlan{
		opts:          o,
		sourceDB:      srcPath,
		EstimateBytes: estimate,
		RequiredBytes: required,
		FreeBytes:     free,
		Pages:         pageCount,
		Freelist:      freelist,
		PageSize:      pageSize,

		SourceNodeID:        identity.nodeID,
		SourceSchemaVersion: identity.schemaVersion,
		SourceUserVersion:   userVersion,
		SourceAutoVacuum:    autoVacuum,
	}, nil
}

// sourceIdentity is what Plan learns about the source from its meta table.
type sourceIdentity struct {
	schemaVersion int
	nodeID        string
}

// openSourceFacts opens messq.db fenced read-only (no data-dir flock — see
// Plan's step-1 comment) and reads the on-disk identity. A database without a
// meta table is not a messq data dir and is refused with a teaching error
// rather than snapshotted as garbage.
func openSourceFacts(ctx context.Context, srcDB string) (*sql.DB, sourceIdentity, error) {
	db, err := sql.Open("sqlite", "file:"+srcDB+"?_pragma=query_only(1)")
	if err != nil {
		return nil, sourceIdentity{}, fmt.Errorf("open %s: %w", srcDB, err)
	}
	db.SetMaxOpenConns(1)
	if pingErr := db.PingContext(ctx); pingErr != nil {
		if closeErr := db.Close(); closeErr != nil {
			pingErr = errors.Join(pingErr, closeErr)
		}
		return nil, sourceIdentity{}, fmt.Errorf("open %s: %w", srcDB, pingErr)
	}

	var identity sourceIdentity
	var tableName string
	metaErr := db.QueryRowContext(ctx,
		`SELECT name FROM sqlite_schema WHERE type = 'table' AND name = 'meta'`).Scan(&tableName)
	switch {
	case errors.Is(metaErr, sql.ErrNoRows):
		if closeErr := db.Close(); closeErr != nil {
			_ = closeErr // reporting the real refusal matters more
		}
		return nil, sourceIdentity{}, fmt.Errorf(
			"%s is not a messq data dir (no meta table); point --data-dir at the directory holding messq.db", srcDB)
	case metaErr != nil:
		if closeErr := db.Close(); closeErr != nil {
			_ = closeErr
		}
		return nil, sourceIdentity{}, fmt.Errorf("inspect %s schema: %w", srcDB, metaErr)
	}

	var rawVersion string
	if scanErr := db.QueryRowContext(ctx,
		`SELECT v FROM meta WHERE k = 'schema_version'`).Scan(&rawVersion); scanErr == nil {
		v, parseErr := strconv.Atoi(rawVersion)
		if parseErr != nil {
			if closeErr := db.Close(); closeErr != nil {
				_ = closeErr
			}
			return nil, sourceIdentity{}, fmt.Errorf("meta[schema_version] = %q is not an integer", rawVersion)
		}
		identity.schemaVersion = v
	}
	var nodeID string
	if scanErr := db.QueryRowContext(ctx,
		`SELECT v FROM meta WHERE k = 'node_id'`).Scan(&nodeID); scanErr == nil {
		identity.nodeID = nodeID
	}
	return db, identity, nil
}

// SnapshotPlan is the outcome of a successful Plan call: everything checked,
// nothing copied.
type SnapshotPlan struct {
	opts     Options
	sourceDB string // absolute path of the source messq.db

	// EstimateBytes is (page_count − freelist_count) × page_size at plan time.
	EstimateBytes int64
	// RequiredBytes is EstimateBytes with the ×1.1 headroom applied.
	RequiredBytes int64
	// FreeBytes was free on the destination filesystem at plan time.
	FreeBytes int64
	// Pages, Freelist and PageSize are the source pragmas behind the estimate.
	Pages, Freelist, PageSize int64

	// Source identity read at Plan time; the self-check holds the snapshot to them.
	SourceNodeID        string
	SourceSchemaVersion int
	// SourceUserVersion and SourceAutoVacuum are the file-header mirrors the
	// self-check reads back from the copy (§11: never assume, always read).
	SourceUserVersion int64
	SourceAutoVacuum  int64
}

// insideDataDir reports whether dest lies under dataDir, symlink-aware where
// both sides resolve. A sibling whose name merely shares a prefix is not inside.
func insideDataDir(dest, dataDir string) bool {
	realDest, destErr := filepath.EvalSymlinks(dest)
	if destErr != nil {
		realDest = filepath.Clean(dest)
	}
	realDir, dirErr := filepath.EvalSymlinks(dataDir)
	if dirErr != nil {
		realDir = filepath.Clean(dataDir)
	}
	rel, relErr := filepath.Rel(realDir, realDest)
	return relErr == nil && rel != ".." && !strings.HasPrefix(rel, ".."+string(filepath.Separator))
}

// probeWritable answers "can this process create a file here?" honestly: by
// creating one. Access(2) lies for root and on root-squashed NFS mounts.
func probeWritable(dir string) error {
	probe := filepath.Join(dir, ".messq-backup-probe-"+strconv.Itoa(os.Getpid()))
	f, err := os.OpenFile(probe, os.O_CREATE|os.O_WRONLY, 0o600)
	if err != nil {
		return err
	}
	if closeErr := f.Close(); closeErr != nil {
		return closeErr
	}
	if rmErr := os.Remove(probe); rmErr != nil {
		return rmErr
	}
	return nil
}

// Result narrates one completed backup.
type Result struct {
	// Dest is the file the snapshot was renamed onto.
	Dest string
	// Bytes is the snapshot's size on disk after the rename.
	Bytes int64
	// Pages are the pages VACUUM INTO wrote (the output's page_count).
	Pages int64
	// TakenAt is the moment the snapshot pipeline started.
	TakenAt time.Time
	// Duration covers temp-dir creation through the final parent fsync.
	Duration time.Duration
	// Swept counts stale .messq-backup-* directories removed by this run.
	Swept int

	// SourceNodeID is the source's meta.node_id, stamped into the snapshot.
	SourceNodeID string
	// SchemaVersion is the source meta.schema_version at plan time.
	SchemaVersion int
	// StreamHeads maps stream → last assigned seq at snapshot time.
	StreamHeads map[string]int64
	// InflightAtSnapshot counts deliveries INFLIGHT when the snapshot began;
	// every one of them redelivers after a restore (§4.4 step 3).
	InflightAtSnapshot int64
	// Verified names the self-check that ran: "quick_check",
	// "integrity_check", or "skipped".
	Verified string
}

// Run executes the plan's pipeline: private temp dir → dedicated-connection
// VACUUM INTO → 0600 + fsync → rename into place → parent fsync → sweep.
// Any failure removes the temp dir and never touches the destination.
// Run consumes the plan: exactly one Run per Plan.
func Run(ctx context.Context, p *SnapshotPlan) (*Result, error) {
	start := p.opts.Clock.Now()
	destDir := filepath.Dir(p.opts.Dest)

	tmp, mkErr := NewTempDir(destDir)
	if mkErr != nil {
		return nil, mkErr
	}
	snap := filepath.Join(tmp, "snap.db")
	ok := false
	defer func() {
		if !ok {
			if rmErr := RemoveTemp(tmp); rmErr != nil {
				mkErr = errors.Join(mkErr, rmErr)
			}
		}
	}()

	conn, openErr := newSnapshotConn(ctx, p.sourceDB, p.opts.CacheBytes)
	if openErr != nil {
		return nil, openErr
	}

	// Step 3's spot counts, read on the very connection that will run the
	// VACUUM INTO: stream heads and the INFLIGHT count at snapshot time.
	heads, headErr := readStreamHeads(ctx, conn)
	if headErr != nil {
		return nil, headErr
	}
	var inflight int64
	if scanErr := conn.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM deliveries WHERE state = 1`).Scan(&inflight); scanErr != nil {
		if closeErr := conn.Close(); closeErr != nil {
			scanErr = errors.Join(scanErr, closeErr)
		}
		return nil, fmt.Errorf("count inflight deliveries: %w", scanErr)
	}

	if _, execErr := conn.ExecContext(ctx, `VACUUM INTO ?`, snap); execErr != nil {
		if closeErr := conn.Close(); closeErr != nil {
			execErr = errors.Join(execErr, fmt.Errorf("close snapshot connection: %w", closeErr))
		}
		return nil, fmt.Errorf("VACUUM INTO %s: %w", snap, execErr)
	}
	if closeErr := conn.Close(); closeErr != nil {
		return nil, fmt.Errorf("close snapshot connection: %w", closeErr)
	}

	// Step 6: stamp provenance into the SNAPSHOT (never the source), then
	// step 7: prove the copy is restorable before it gets its real name.
	if stampErr := stamp(ctx, snap, provenance{
		TakenAt:      start,
		DataDir:      p.opts.DataDir,
		SourceDB:     p.sourceDB,
		SourceNodeID: p.SourceNodeID,
		Live:         sourceIsLive(p.opts.DataDir),
		Heads:        heads,
	}); stampErr != nil {
		return nil, stampErr
	}
	checkErr := selfCheck(ctx, snap, SelfExpectations{
		Verify:       p.opts.Verify,
		SchemaVer:    p.SourceSchemaVersion,
		UserVersion:  p.SourceUserVersion,
		PageSize:     p.PageSize,
		AutoVacuum:   p.SourceAutoVacuum,
		RecordedHead: heads,
	})
	if checkErr != nil {
		return nil, checkErr
	}

	// The payload is cleartext (D12): force 0600 whatever the creating
	// process's umask did, flush the file, THEN publish it under its final
	// name. rename(2) is the last visible step; the parent fsync makes the
	// placement survive power loss.
	if chErr := os.Chmod(snap, 0o600); chErr != nil {
		return nil, fmt.Errorf("chmod snapshot 0600: %w", chErr)
	}
	if syncErr := FsyncFile(snap); syncErr != nil {
		return nil, syncErr
	}
	if renameErr := RenameIntoPlace(snap, p.opts.Dest); renameErr != nil {
		return nil, renameErr
	}
	if syncErr := FsyncDir(destDir); syncErr != nil {
		return nil, syncErr
	}

	info, statErr := os.Stat(p.opts.Dest)
	if statErr != nil {
		return nil, fmt.Errorf("stat renamed snapshot: %w", statErr)
	}

	now := p.opts.Clock.Now()
	swept, sweepErr := SweepStale(destDir, now, time.Hour)
	if sweepErr != nil {
		return nil, sweepErr
	}

	ok = true
	return &Result{
		Dest:     p.opts.Dest,
		Bytes:    info.Size(),
		Pages:    pageCountOf(info.Size(), p.PageSize),
		TakenAt:  start,
		Duration: now.Sub(start),
		Swept:    swept,

		SourceNodeID:       p.SourceNodeID,
		SchemaVersion:      p.SourceSchemaVersion,
		StreamHeads:        heads,
		InflightAtSnapshot: inflight,
		Verified:           verifiedWord(p.opts.Verify),
	}, nil
}

// verifiedWord names the self-check that ran (Result.Verified).
func verifiedWord(mode VerifyMode) string {
	switch mode {
	case VerifyQuick:
		return "quick_check"
	case VerifyFull:
		return "integrity_check"
	case VerifyNone:
		return "skipped"
	default:
		return "quick_check"
	}
}

// readStreamHeads reads stream → last assigned seq from the snapshot
// connection. stream_seq.next is the next sequence to assign, so the head is
// next−1; streams with no messages yet carry next=1 and are omitted.
func readStreamHeads(ctx context.Context, conn *sql.DB) (map[string]int64, error) {
	rows, err := conn.QueryContext(ctx, `SELECT stream, next FROM stream_seq`)
	if err != nil {
		return nil, fmt.Errorf("read stream heads: %w", err)
	}
	defer func() {
		if closeErr := rows.Close(); closeErr != nil {
			err = fmt.Errorf("close stream heads rows: %w", closeErr)
		}
	}()
	heads := make(map[string]int64)
	for rows.Next() {
		var stream string
		var next int64
		if scanErr := rows.Scan(&stream, &next); scanErr != nil {
			return nil, fmt.Errorf("scan stream heads: %w", scanErr)
		}
		if next > 1 {
			heads[stream] = next - 1
		}
	}
	if rowErr := rows.Err(); rowErr != nil {
		return nil, fmt.Errorf("iterate stream heads: %w", rowErr)
	}
	return heads, err
}

// pageCountOf derives the snapshot's page count from its final size — honest,
// driver-independent, and exact while the page size is unchanged.
func pageCountOf(bytes, pageSize int64) int64 {
	if pageSize <= 0 {
		return 0
	}
	return bytes / pageSize
}

// snapshotDSN builds the dedicated snapshot connection string. It is NOT the
// pooled-reader DSN: query_only is off (the pooled readers keep query_only(1);
// SQLite may refuse VACUUM under it), temp_store is FILE (§4.1's MEMORY default
// would push the index-rebuild sort scratch into RAM — an OOM on multi-GiB
// databases), and cache_size bounds the connection's RSS. The handle still
// never writes the source: VACUUM INTO only reads it and creates the target.
func snapshotDSN(srcDB string, cacheBytes int64) string {
	kib := cacheKiB(cacheBytes)
	return fmt.Sprintf("file:%s?_pragma=query_only(0)&_pragma=temp_store(FILE)&_pragma=cache_size(-%d)",
		srcDB, kib)
}

// cacheKiB converts a byte budget into SQLite's negative-kibibyte cache_size spelling.
func cacheKiB(cacheBytes int64) int64 {
	if cacheBytes <= 0 {
		cacheBytes = defaultCacheBytes
	}
	return cacheBytes / 1024
}

// newSnapshotConn opens the dedicated single-handle connection the whole
// pipeline runs on. One connection, never pooled: a minutes-long VACUUM INTO
// parked on a pooled reader would starve peek/trace/lag under
// SetMaxOpenConns(NumCPU) — the third load-bearing reason for this handle.
func newSnapshotConn(ctx context.Context, srcDB string, cacheBytes int64) (*sql.DB, error) {
	db, err := sql.Open("sqlite", snapshotDSN(srcDB, cacheBytes))
	if err != nil {
		return nil, fmt.Errorf("open snapshot connection: %w", err)
	}
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	if pingErr := db.PingContext(ctx); pingErr != nil {
		if closeErr := db.Close(); closeErr != nil {
			pingErr = errors.Join(pingErr, closeErr)
		}
		return nil, fmt.Errorf("open snapshot connection: %w", pingErr)
	}
	return db, nil
}
