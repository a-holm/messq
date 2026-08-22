// SPDX-License-Identifier: Apache-2.0

package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"

	"github.com/a-holm/messq/internal/clock"
	"github.com/a-holm/messq/internal/errs"
	"github.com/a-holm/messq/internal/id"
)

// The Store owns <data-dir>/messq.db for one process: the sole read-write handle (handed
// exactly once to the writer goroutine, #6), a shared-fenced read pool, the data-directory
// flock, and the §4.4 startup procedure that binds them together. Its rules are the issue's
// rules:
//
//   - Open runs the whole recovery sequence before returning — no listeners, no goroutines,
//     no retries of failed fsyncs. A failure refuses startup; it never repairs.
//   - TakeWriter hands out the rw handle once. The "exactly one writer" rule is enforced by
//     ErrWriterTaken, not documented away.
//   - Close is idempotent and safe on a partially built store: whatever Open managed to open,
//     Close (or Open's own failure path) releases — handles first, flock last.
//
// Immutable-after-Open fields (nodeID, schemaVersion, durability, …) are written only before
// the store becomes reachable; the mutex guards the mutable handle set and closed flag.

// Meta bookkeeping keys this file owns. migrate.go owns schema_version/schema_applied_at.
const (
	metaCleanShutdown = "clean_shutdown"
	metaNodeID        = "node_id"
	metaCreatedAt     = "created_at"
)

// Store is a live connection set to one data directory. Create it with [Open]; all methods
// are safe for concurrent use.
type Store struct {
	mu     sync.Mutex
	closed bool
	// handedOff marks a successful TakeWriter: closing the returned handle then belongs to
	// its owner, so Close skips every rw-dependent step instead of fighting over the fd.
	handedOff bool

	rw   *sql.DB // sole writer handle; nil in ReadOnly mode or once handed off/closed
	ro   *sql.DB // shared read pool
	lock *dirLock

	dir           string
	nodeID        string
	schemaVersion int
	durability    Durability
	clk           clock.Clock
	logger        *slog.Logger
}

// dbPath renders <dir>/messq.db.
func dbPath(dir string) string { return filepath.Join(dir, dbFileName) }

// readMeta reads one meta row through any database/sql handle; ok is false when the key is
// absent. Both *sql.DB and *sql.Conn satisfy the parameter shape.
func readMeta(ctx context.Context, q interface {
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
}, key string,
) (val string, ok bool, err error) {
	err = q.QueryRowContext(ctx, `SELECT v FROM meta WHERE k = ?`, key).Scan(&val)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		return "", false, nil
	case err != nil:
		return "", false, err
	}
	return val, true, nil
}

// databaseHasUserTables probes the database file with a bare connection: a plain DSN with
// no registered expectations and, crucially, none of the file-property pragmas. Looking is
// all the probe may do — applying auto_vacuum or journal_mode through a DSN writes the file
// header (the change counter at offset 27) even when the values already match, and the
// caller decides BETWEEN this probe and any stamping precisely so an established database
// is never touched by a refused open.
func databaseHasUserTables(ctx context.Context, path string) (established bool, err error) {
	dsn := "file:" + path
	db, err := sql.Open(driverName, dsn)
	if err != nil {
		return false, fmt.Errorf("open emptiness probe: %w", err)
	}
	defer func() {
		if cerr := db.Close(); cerr != nil {
			fmt.Fprintf(os.Stderr, "store: close emptiness-probe handle: %v\n", cerr)
		}
	}()
	if pingErr := db.PingContext(ctx); pingErr != nil {
		return false, fmt.Errorf("probe emptiness on %s: %w", redactedDSN(dsn), pingErr)
	}
	var tables int
	if scanErr := db.QueryRowContext(ctx,
		`SELECT count(*) FROM sqlite_schema WHERE type = 'table' AND name NOT LIKE 'sqlite_%'`,
	).Scan(&tables); scanErr != nil {
		return false, fmt.Errorf("inspect sqlite_schema: %w", scanErr)
	}
	return tables > 0, nil
}

// initEmptyDatabase stamps the two file-property pragmas that must exist before the first
// table does: auto_vacuum=INCREMENTAL (immutable once tables exist) and journal_mode=WAL
// (the persistent property every later connection verifies). The gate runs FIRST, on a bare
// connection that carries no pragmas at all: only when sqlite_schema holds no user tables
// is the pragma-bearing DSN opened. An established database is therefore left strictly
// alone — byte for byte — no matter how Open ends (a too-new schema, corruption, …), while
// a crash between file creation and this step replays safely: the probe still sees no user
// tables and the stamp re-runs on the empty file.
func initEmptyDatabase(ctx context.Context, path string) error {
	established, err := databaseHasUserTables(ctx, path)
	if err != nil {
		return err
	}
	if established {
		return nil // established database: never open the pragma DSN against it
	}

	dsn := freshFileDSN(path)
	db, err := sql.Open(driverName, dsn)
	if err != nil {
		return fmt.Errorf("open fresh-database pragmas: %w", err)
	}
	return func() error {
		defer func() {
			if cerr := db.Close(); cerr != nil {
				fmt.Fprintf(os.Stderr, "store: close fresh-database handle: %v\n", cerr)
			}
		}()
		if pingErr := db.PingContext(ctx); pingErr != nil {
			return fmt.Errorf("apply fresh-database pragmas on %s: %w", redactedDSN(dsn), pingErr)
		}
		var journal, vacuum string
		if jmErr := db.QueryRowContext(ctx, `PRAGMA journal_mode`).Scan(&journal); jmErr != nil {
			return fmt.Errorf("read back journal_mode: %w", jmErr)
		}
		if normalizePragmaValue(journal) != "wal" {
			return fmt.Errorf("%w: fresh file journal_mode=%s, want wal", ErrPragmaMismatch, journal)
		}
		if avErr := db.QueryRowContext(ctx, `PRAGMA auto_vacuum`).Scan(&vacuum); avErr != nil {
			return fmt.Errorf("read back auto_vacuum: %w", avErr)
		}
		// INCREMENTAL reads back as 2 (0 none / 1 full / 2 incremental).
		if normalizePragmaValue(vacuum) != "2" {
			return fmt.Errorf("%w: fresh file auto_vacuum=%s, want 2 (incremental)", ErrPragmaMismatch, vacuum)
		}
		return nil
	}()
}

// openPool builds and verifies one pool for role: register the expectation set before the
// first sql.Open (hooks fire per new physical connection), size the pool per PLAN §4.1, and
// Ping so the very first connection proves its pragmas now.
func openPool(ctx context.Context, path string, role poolRole, opt Options) (*sql.DB, error) {
	dsn := buildDSN(path, role, opt)
	registerExpectations(dsn, expectationsFor(role, opt))
	db, err := openSQLite(dsn)
	if err != nil {
		return nil, fmt.Errorf("open %s pool: %w", role, err)
	}
	maxConns := 1
	if role != poolWriter {
		maxConns = opt.ReadPoolSize
	}
	db.SetMaxOpenConns(maxConns)
	db.SetMaxIdleConns(maxConns)
	db.SetConnMaxLifetime(0)
	if pingErr := db.PingContext(ctx); pingErr != nil {
		if cerr := db.Close(); cerr != nil {
			pingErr = errors.Join(pingErr, fmt.Errorf("close refused pool: %w", cerr))
		}
		return nil, fmt.Errorf("verify %s pool pragmas: %w", role, pingErr)
	}
	return db, nil
}

// Open performs the whole §4.4 startup sequence, in order, before any listener exists:
//
//  1. resolve/create the data dir (§10 permissions), create messq.db 0600, fsync the entry;
//  2. flock LOCK — exclusive for the daemon, shared under [Options.ReadOnly];
//  3. probe emptiness on a bare pragma-free connection; stamp auto_vacuum/journal_mode
//     only when no user tables exist (an established file is never opened on the pragma
//     DSN, so refused opens leave it byte-identical);
//  4. open the writer pool and ping it (the pragma hook verifies here);
//  5. migrate;
//  6. open the read pool;
//  7. detect an unclean stop from the clean_shutdown marker;
//  8. run quick_check/integrity_check when unclean or FullCheck — refuse on damage, never repair;
//  9. reclaim INFLIGHT leases (attempts untouched) + trim expired dedup keys, co-committed
//     with their audit event;
//
// 10. checkpoint the WAL TRUNCATE and mark the directory dirty (clean_shutdown="0").
//
// On failure the returned Store and report are nil and everything opened on the way is
// released, including the flock. ReadOnly mode stops after step 6 with a mode=ro reader as
// the only handle: inspection reports the on-disk state as-is and performs no migrations,
// checks, reclaim, or marker writes.
func Open(ctx context.Context, opt Options) (*Store, *RecoveryReport, error) {
	opt.applyDefaults()
	start := opt.Clock.Now()

	st := &Store{
		dir:        opt.DataDir,
		durability: opt.Durability,
		clk:        opt.Clock,
		logger:     opt.Logger,
	}
	fail := func(err error) (*Store, *RecoveryReport, error) {
		st.cleanup(context.WithoutCancel(ctx))
		return nil, nil, err
	}

	if err := ensureDataDir(opt.DataDir); err != nil {
		return fail(err)
	}
	if _, err := initDBFile(opt.DataDir); err != nil {
		return fail(err)
	}
	lockMode := lockExclusive
	if opt.ReadOnly {
		lockMode = lockShared
	}
	lock, err := lockDataDir(opt.DataDir, lockMode)
	if err != nil {
		return fail(err)
	}
	st.lock = lock

	path := dbPath(opt.DataDir)
	if !opt.ReadOnly {
		// ReadOnly sessions must not touch the file at all — not even through a momentary
		// writable connection (whose close-time checkpoint would merge a leftover WAL and
		// mutate bytes under an "inspection" banner).
		if initErr := initEmptyDatabase(ctx, path); initErr != nil {
			return fail(initErr)
		}
	}

	report := &RecoveryReport{CheckKind: checkSkipped}

	if opt.ReadOnly {
		ro, roErr := openPool(ctx, path, poolReadOnly, opt)
		if roErr != nil {
			return fail(roErr)
		}
		st.ro = ro
		version, nodeID, identErr := readOnDiskIdentity(ctx, ro)
		if identErr != nil {
			return fail(identErr)
		}
		st.schemaVersion = version
		st.nodeID = nodeID
		report.NodeID = nodeID
		report.SchemaFrom, report.SchemaTo = version, version
		report.DBBytes, report.WALBytes, err = st.Sizes()
		if err != nil {
			return fail(fmt.Errorf("measure read-only sizes: %w", err))
		}
		report.Duration = st.clk.Since(start)
		return st, report, nil
	}

	rw, err := openPool(ctx, path, poolWriter, opt)
	if err != nil {
		return fail(err)
	}
	st.rw = rw

	from, to, err := migrate(ctx, rw, st.clk)
	if err != nil {
		return fail(err)
	}
	st.schemaVersion = to
	report.SchemaFrom, report.SchemaTo = from, to

	ro, err := openPool(ctx, path, poolReader, opt)
	if err != nil {
		return fail(err)
	}
	st.ro = ro

	nodeID, err := ensureCreationBookkeeping(ctx, rw, st.clk, opt.NewID)
	if err != nil {
		return fail(err)
	}
	st.nodeID = nodeID

	// Unclean detection distinguishes bookkeeping this Open created (from == 0 — a fresh
	// directory is not "unclean") from a marker left behind by a previous life.
	unclean := false
	if from > 0 {
		marker, ok, markerErr := readMeta(ctx, rw, metaCleanShutdown)
		if markerErr != nil {
			return fail(fmt.Errorf("read clean_shutdown marker: %w", markerErr))
		}
		var reason string
		switch {
		case !ok:
			unclean = true
			reason = "clean_shutdown marker absent"
		case marker == "0":
			unclean = true
			reason = "clean_shutdown marker is 0"
		}
		if unclean {
			st.logger.Warn("recovery.unclean", "node", nodeID, "reason", reason)
			if eventErr := recordUncleanEvent(ctx, rw, st.clk, reason); eventErr != nil {
				return fail(eventErr)
			}
		}
	}
	report.Unclean = unclean

	if unclean || opt.FullCheck {
		kind := checkQuickCheck
		if opt.FullCheck {
			kind = checkIntegrityCheck
		}
		problems, took, checkErr := runStartupCheck(ctx, rw, kind, st.clk)
		report.CheckKind = kind
		report.CheckDuration = took
		if checkErr != nil {
			return fail(checkErr)
		}
		if len(problems) > 0 {
			joined := strings.Join(problems, "; ")
			st.logger.Error("storage.fatal",
				"node", nodeID,
				"kind", kind,
				"problems", joined,
				"hint", "restore this data directory from backup; messq never repairs damage")
			return fail(fmt.Errorf("%w: PRAGMA %s reported: %s", ErrCorrupt, kind, joined))
		}
		st.logger.Info("recovery.check", "node", nodeID, "kind", kind, "result", "ok")
	}

	reclaimed, dedupExpired, err := reclaimLeasesAndTrimDedup(ctx, rw, st.clk, opt.ReclaimJitter)
	if err != nil {
		return fail(err)
	}
	report.Reclaimed = reclaimed
	report.DedupExpired = dedupExpired

	busy, pages, err := checkpointTruncate(ctx, rw)
	if err != nil {
		return fail(err)
	}
	report.CheckpointPages = pages
	if busy != 0 {
		st.logger.Warn("recovery.checkpoint", "node", nodeID, "busy", busy,
			"hint", "a reader held the WAL during the startup checkpoint; baseline is partial")
	}

	if markErr := upsertMetaDB(ctx, rw, metaCleanShutdown, "0"); markErr != nil {
		return fail(fmt.Errorf("mark directory dirty: %w", markErr))
	}

	report.NodeID = nodeID
	report.DBBytes, report.WALBytes, err = st.Sizes()
	if err != nil {
		return fail(fmt.Errorf("measure sizes: %w", err))
	}
	report.Duration = st.clk.Since(start)
	logRecoveryLines(st.logger, nodeID, from, to, opt.ReclaimJitter.Milliseconds(), report)
	return st, report, nil
}

// ensureCreationBookkeeping mints node_id once (via the Options.NewID seam) and stamps
// created_at once, both stable across every later open. Two independent upserts: a crash
// between them leaves the next open to finish the job, and neither value ever changes after.
func ensureCreationBookkeeping(ctx context.Context, rw *sql.DB, clk clock.Clock, newID func() id.MsgID) (string, error) {
	stored, ok, err := readMeta(ctx, rw, metaNodeID)
	if err != nil {
		return "", fmt.Errorf("read meta[%s]: %w", metaNodeID, err)
	}
	if !ok {
		stored = newID().String()
		if writeErr := upsertMetaDB(ctx, rw, metaNodeID, stored); writeErr != nil {
			return "", writeErr
		}
	}
	_, ok, err = readMeta(ctx, rw, metaCreatedAt)
	if err != nil {
		return "", fmt.Errorf("read meta[%s]: %w", metaCreatedAt, err)
	}
	if !ok {
		stamp := strconv.FormatInt(clk.Now().UnixMilli(), 10)
		if writeErr := upsertMetaDB(ctx, rw, metaCreatedAt, stamp); writeErr != nil {
			return "", writeErr
		}
	}
	return stored, nil
}

// readOnDiskIdentity reports the schema version and node_id a read-only session sees, with
// no writes anywhere: an absent meta table simply reads as version 0.
func readOnDiskIdentity(ctx context.Context, ro *sql.DB) (version int, nodeID string, err error) {
	var name string
	err = ro.QueryRowContext(ctx,
		`SELECT name FROM sqlite_schema WHERE type = 'table' AND name = 'meta'`).Scan(&name)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		return 0, "", nil
	case err != nil:
		return 0, "", fmt.Errorf("inspect sqlite_schema for meta: %w", err)
	}
	raw, ok, err := readMeta(ctx, ro, metaSchemaVersion)
	switch {
	case err != nil:
		return 0, "", fmt.Errorf("read meta[%s]: %w", metaSchemaVersion, err)
	case ok:
		v, err := strconv.Atoi(raw)
		if err != nil {
			return 0, "", fmt.Errorf("meta[%s] = %q is not an integer", metaSchemaVersion, raw)
		}
		version = v
	}
	if found, ok, idErr := readMeta(ctx, ro, metaNodeID); idErr == nil && ok {
		nodeID = found
	}
	return version, nodeID, nil
}

// upsertMetaDB writes one meta row through a *sql.DB handle (the single-connection writer
// pool makes the statement effectively exclusive).
func upsertMetaDB(ctx context.Context, db *sql.DB, key, val string) error {
	if _, err := db.ExecContext(ctx,
		`INSERT INTO meta (k, v) VALUES (?, ?) ON CONFLICT (k) DO UPDATE SET v = excluded.v`,
		key, val); err != nil {
		return fmt.Errorf("upsert meta[%s]: %w", key, err)
	}
	return nil
}

// cleanup releases whatever this store holds: pools first, flock last. It is the failure
// path of Open and the tail of Close; every field is tolerated nil.
func (s *Store) cleanup(ctx context.Context) {
	var errs []error
	s.mu.Lock()
	rw, ro, lock := s.rw, s.ro, s.lock
	s.rw, s.ro, s.lock = nil, nil, nil
	s.closed = true
	s.mu.Unlock()
	if ro != nil {
		if err := ro.Close(); err != nil {
			errs = append(errs, fmt.Errorf("close read pool: %w", err))
		}
	}
	if rw != nil {
		if err := rw.Close(); err != nil {
			errs = append(errs, fmt.Errorf("close writer handle: %w", err))
		}
	}
	if lock != nil {
		if err := lock.unlock(); err != nil {
			errs = append(errs, err)
		}
	}
	for _, err := range errs {
		s.logger.WarnContext(ctx, "cleanup after failed open", "error", err)
	}
}

// TakeWriter hands the sole read-write handle to its single owner — the #6 writer goroutine.
// The second call returns [ErrWriterTaken], as does any call after Close: the rule is
// enforced, not documented. A store opened ReadOnly has no writer handle at all and returns
// a plain error saying so.
//
// Once taken, closing the returned handle is the owner's responsibility; [Store.Close] then
// deliberately skips every rw-dependent step rather than close the fd out from under its
// owner (which also means the clean_shutdown marker stays untouched — a directory whose
// writer was handed off cannot be gracefully closed by anyone else).
func (s *Store) TakeWriter() (*sql.DB, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	switch {
	case s.closed || s.handedOff:
		return nil, fmt.Errorf("%w", ErrWriterTaken)
	case s.rw == nil:
		return nil, errors.New("no read-write handle: store was opened ReadOnly")
	}
	db := s.rw
	s.handedOff = true
	return db, nil
}

// NewWriter constructs the group-commit engine over this store's sole read-write handle,
// which it takes internally via [TakeWriter]: callers never touch the rw *sql.DB, which is
// how the "exactly one writer" rule stays structural (PLAN §3.2). The writer inherits the
// store's clock, logger, node identity and durability mode; a Config demanding a different
// durability is refused before anything is constructed — the pragma the pool was opened with
// and the pragma the engine verifies must be one decision, not two.
//
// If construction fails after the hand-off (the pool failed its read-back), the rw handle is
// closed here: a store whose writer cannot start has no further use for it.
func (s *Store) NewWriter(cfg Config, opts ...WriterOption) (*Writer, error) {
	s.mu.Lock()
	durability, clk, logger, nodeID := s.durability, s.clk, s.logger, s.nodeID
	s.mu.Unlock()

	if cfg.Durability != durability {
		return nil, fmt.Errorf("%w: store opened with --durability=%s, writer configured durability=%s",
			errs.ErrBadRequest, durability, cfg.Durability)
	}
	rw, err := s.TakeWriter()
	if err != nil {
		return nil, err
	}
	opts = append(opts, withLogger(logger), withNodeID(nodeID))
	w, err := NewWriter(rw, clk, cfg, opts...)
	if err != nil {
		if cerr := rw.Close(); cerr != nil {
			logger.Warn("writer.construct", "error", fmt.Sprintf("close refused rw handle: %v", cerr))
		}
		return nil, err
	}
	return w, nil
}

// RO exposes the shared read pool for peek, trace, list, lag, and metrics. Every pooled
// connection is fenced write-off by query_only=1, verified on creation by the hook.
func (s *Store) RO() *sql.DB {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.ro
}

// SchemaVersion returns the applied schema version this Open established (ReadOnly: the
// version found on disk).
func (s *Store) SchemaVersion() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.schemaVersion
}

// NodeID returns the identity minted once at database creation and stable forever.
func (s *Store) NodeID() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.nodeID
}

// Durability returns the crash-promise this store was opened with.
func (s *Store) Durability() Durability {
	return s.durability
}

// Sizes measures the database and WAL files on disk (WAL absent ⇒ 0). A closed store no
// longer speaks for its directory and reports that instead of stale numbers.
func (s *Store) Sizes() (dbBytes, walBytes int64, err error) {
	s.mu.Lock()
	closed := s.closed
	dir := s.dir
	s.mu.Unlock()
	if closed {
		return 0, 0, errors.New("store is closed")
	}
	stDb, err := os.Stat(dbPath(dir))
	if err != nil {
		return 0, 0, fmt.Errorf("stat %s: %w", dbFileName, err)
	}
	walBytes = 0
	if stWal, err := os.Stat(dbPath(dir) + "-wal"); err == nil {
		walBytes = stWal.Size()
	}
	return stDb.Size(), walBytes, nil
}

// Close runs the graceful-shutdown path in PLAN §4.4's order — analysis_limit, optimize,
// wal_checkpoint(TRUNCATE), the clean_shutdown="1" commit, then handles and finally the
// flock. It is idempotent and safe on the partial shape a failed Open leaves (all-nil
// internals close as a no-op). When the writer handle was already taken via [TakeWriter],
// the rw-dependent steps belong to its owner and are skipped here.
func (s *Store) Close(ctx context.Context) error {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil
	}
	s.closed = true
	rw, ro, lock, handedOff := s.rw, s.ro, s.lock, s.handedOff
	s.rw, s.ro, s.lock = nil, nil, nil
	s.mu.Unlock()

	var errs []error
	writeShutdown := rw != nil && !handedOff
	if writeShutdown {
		if _, err := rw.ExecContext(ctx, `PRAGMA analysis_limit = 400`); err != nil {
			errs = append(errs, fmt.Errorf("analysis_limit: %w", err))
		}
		if _, err := rw.ExecContext(ctx, `PRAGMA optimize`); err != nil {
			errs = append(errs, fmt.Errorf("optimize: %w", err))
		}
		if _, _, err := checkpointTruncate(ctx, rw); err != nil {
			errs = append(errs, err)
		}
		if err := upsertMetaDB(ctx, rw, metaCleanShutdown, "1"); err != nil {
			errs = append(errs, fmt.Errorf("record clean shutdown: %w", err))
		}
	}
	if ro != nil {
		if err := ro.Close(); err != nil {
			errs = append(errs, fmt.Errorf("close read pool: %w", err))
		}
	}
	if writeShutdown {
		if err := rw.Close(); err != nil {
			errs = append(errs, fmt.Errorf("close writer handle: %w", err))
		}
	}
	if lock != nil {
		if err := lock.unlock(); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}
