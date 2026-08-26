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
	"github.com/a-holm/messq/internal/store"
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
	srcPath := filepath.Join(o.DataDir, "messq.db")
	src, _, openErr := store.Open(ctx, store.Options{DataDir: o.DataDir, ReadOnly: true})
	if openErr != nil {
		return nil, fmt.Errorf("open source data dir read-only: %w", openErr)
	}
	defer func() {
		if closeErr := src.Close(context.WithoutCancel(ctx)); closeErr != nil {
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
	scanErr := src.RO().QueryRowContext(ctx, `SELECT
			(SELECT page_count FROM pragma_page_count),
			(SELECT freelist_count FROM pragma_freelist_count),
			(SELECT page_size FROM pragma_page_size)`).
		Scan(&pageCount, &freelist, &pageSize)
	if scanErr != nil {
		return nil, fmt.Errorf("read source sizing pragmas: %w", scanErr)
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
	}, nil
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

// Result narrates one completed backup. Fields grow with the pipeline:
// provenance/verification land with stamp.go/selfcheck.go (slice 3).
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
	if _, execErr := conn.ExecContext(ctx, `VACUUM INTO ?`, snap); execErr != nil {
		if closeErr := conn.Close(); closeErr != nil {
			execErr = errors.Join(execErr, fmt.Errorf("close snapshot connection: %w", closeErr))
		}
		return nil, fmt.Errorf("VACUUM INTO %s: %w", snap, execErr)
	}
	if closeErr := conn.Close(); closeErr != nil {
		return nil, fmt.Errorf("close snapshot connection: %w", closeErr)
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
	}, nil
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
