// SPDX-License-Identifier: Apache-2.0

package api

import (
	"context"
	"crypto/rand"
	"errors"
	"log/slog"
	"net"
	"net/http"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"github.com/a-holm/messq/internal/buildinfo"
	"github.com/a-holm/messq/internal/clock"
	"github.com/a-holm/messq/internal/id"
	"github.com/a-holm/messq/internal/queue"
	"github.com/a-holm/messq/internal/store"
)

// Server is the HTTP surface of the daemon: the ServeMux route table plus the
// dedup-sweep ticker. It owns the net/http server and the sweep loop; the store's
// open/close lifecycle belongs to the caller (the serve command), so Serve never closes
// the store.
type Server struct {
	store     *store.Store
	clk       clock.Clock
	logger    *slog.Logger
	startedAt time.Time
	reqGen    *id.Gen
	conns     connLimiter
	// compiled is the wildcard matcher index over routes(); routesOnce builds it.
	compiled   []*compiledRoute
	routesOnce sync.Once
	// health is the HealthState the probes read; New installs a tracker that mirrors
	// the store's fsyncgate unless an implementation was injected.
	health HealthState
	// infoCache holds the --info-cache-cached measurements of /v1/info; guarded by
	// infoMu so concurrent GETs share one refresh instead of stampeding syscalls.
	infoMu    sync.Mutex
	infoCache *infoSnapshot
	// waiters is the bounded long-poll park/wake fabric: store.Waker for the sweeper
	// and obs.Sink for committed publishes.
	waiters *Registry
	// closing is closed ONCE when Serve's context ends, before ReleaseAll, so parked
	// handlers can answer hold_reason=shutting_down.
	closing   chan struct{}
	closeOnce sync.Once
	// cfg carries every §9 knob with its default already applied; handlers read the
	// effective values from here so clamps echo what the server actually enforces.
	cfg Config
	// limits are the process-wide validation ceilings the handlers use for fast-path
	// rejection; the store re-validates the same numbers inside the transaction.
	limits  queue.Limits
	httpSrv atomic.Pointer[http.Server]
}

// Config is the full §9 server configuration. Every numeric/duration field may arrive
// zero, meaning "use the documented default"; New fills them in so handlers always see
// effective values. The defaults are the issue's flag table, with --max-waiters at the
// A1 register's 4096 (orchestrator ruling 2026-08-24, §8 Q2), not 10000.
type Config struct {
	Store  *store.Store
	Clock  clock.Clock
	Logger *slog.Logger
	// SweepEvery is the dedup sweep period (--dedup-sweep-interval); must be positive.
	SweepEvery time.Duration
	// Limits seeds the process-wide publish-validation ceilings; zero means
	// queue.DefaultLimits().
	Limits queue.Limits
	// MaxBatchBytes is the NDJSON body ceiling for messages:batch (--max-batch-bytes).
	MaxBatchBytes int64

	MaxWaiters            int           // --max-waiters; parked long polls process-wide (4096)
	MaxWaitersPerConsumer int           // --max-waiters-per-consumer (256)
	MaxFetchWait          time.Duration // --max-fetch-wait; ceiling on wait_ms (5m)
	FetchEmptyDamper      time.Duration // --fetch-empty-damper; empty-wake coalesce window (5ms)
	MaxRequestBytes       int64         // --max-request-bytes; JSON control bodies (1 MiB)
	ReadHeaderTimeout     time.Duration // --read-header-timeout; slowloris bound (10s)
	IdleTimeout           time.Duration // --idle-timeout; keep-alive idle (120s)
	MaxRequestHeaderBytes int           // --max-request-header-bytes; http.Server cap (16 KiB)
	WriterSubmitTimeout   time.Duration // --writer-submit-timeout; full cmdCh → busy (5s)
	MaxConns              int           // --max-conns; accept-side semaphore (1024)

	// HealthState overrides the probes' state source (#15). nil means the built-in
	// healthTracker: degraded kinds recorded via RecordDegraded, draining for #17,
	// and read-only mirrored from the store's writer latch (when Store is set).
	HealthState HealthState
	// Listeners names the bound listener endpoints reported by /v1/info, e.g.
	// "unix:///run/messq/messq.sock". The serve command fills it from its socket setup.
	Listeners []string

	// InfoCacheTTL is --info-cache (default 5s): how long the syscall-backed fields of
	// /v1/info (file sizes, free disk, counts) may be reused between requests.
	InfoCacheTTL time.Duration

	// LevelSetter backs POST /v1/admin/log-level (#19 supplies the real implementation);
	// nil means the knob is not wired and the endpoint answers not_implemented.
	LevelSetter LevelSetter

	// Metrics is the injected scrape handler (#21 drops promhttp in this slot); nil
	// makes GET /metrics answer 503 not_implemented — never 404.
	Metrics http.Handler

	// PendingMaxLimit is --pending-max-limit (issue #15 §10, default 1000): the cap
	// the pending listing clamps ?limit to while echoing the effective value.
	PendingMaxLimit int

	// Dev is serve --dev (issue #26 §2): streams are auto-created on publish and
	// consumers on fetch with actor=dev-autocreate, and /v1/info carries "dev":true.
	// It changes NO admission or auth behaviour — the #16 public-bind refusal is
	// evaluated before this flag exists to the daemon.
	Dev bool
}

// The §9 defaults for zero Config fields.
const (
	defaultMaxWaiters            = 4096
	defaultMaxWaitersPerConsumer = 256
	defaultMaxFetchWait          = 5 * time.Minute
	defaultFetchEmptyDamper      = 5 * time.Millisecond
	defaultMaxRequestBytes       = int64(1) << 20
	defaultReadHeaderTimeout     = 10 * time.Second
	defaultIdleTimeout           = 120 * time.Second
	defaultMaxRequestHeaderBytes = 16 << 10
	defaultWriterSubmitTimeout   = 5 * time.Second
	defaultMaxConns              = 1024

	// defaultMaxBatchBytes is the NDJSON batch-body ceiling when the serve command does
	// not pass one (tests and embedders). It matches the --max-batch-bytes default (8 MiB).
	defaultMaxBatchBytes = int64(8 << 20)

	defaultInfoCacheTTL = 5 * time.Second

	defaultPendingMaxLimit = 1000
)

// New builds a Server around a live, already-recovered store. Zero Config fields take
// their documented defaults; see Config.
func New(cfg Config) *Server {
	if cfg.Clock == nil {
		cfg.Clock = clock.System{}
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	if cfg.Limits == (queue.Limits{}) {
		cfg.Limits = queue.DefaultLimits()
	}
	if cfg.MaxBatchBytes <= 0 {
		cfg.MaxBatchBytes = defaultMaxBatchBytes
	}
	if cfg.SweepEvery <= 0 {
		cfg.SweepEvery = time.Minute
	}
	if cfg.MaxWaiters <= 0 {
		cfg.MaxWaiters = defaultMaxWaiters
	}
	if cfg.MaxWaitersPerConsumer <= 0 {
		cfg.MaxWaitersPerConsumer = defaultMaxWaitersPerConsumer
	}
	if cfg.MaxFetchWait <= 0 {
		cfg.MaxFetchWait = defaultMaxFetchWait
	}
	if cfg.FetchEmptyDamper <= 0 {
		cfg.FetchEmptyDamper = defaultFetchEmptyDamper
	}
	if cfg.MaxRequestBytes <= 0 {
		cfg.MaxRequestBytes = defaultMaxRequestBytes
	}
	if cfg.ReadHeaderTimeout <= 0 {
		cfg.ReadHeaderTimeout = defaultReadHeaderTimeout
	}
	if cfg.IdleTimeout <= 0 {
		cfg.IdleTimeout = defaultIdleTimeout
	}
	if cfg.MaxRequestHeaderBytes <= 0 {
		cfg.MaxRequestHeaderBytes = defaultMaxRequestHeaderBytes
	}
	if cfg.WriterSubmitTimeout <= 0 {
		cfg.WriterSubmitTimeout = defaultWriterSubmitTimeout
	}
	if cfg.MaxConns <= 0 {
		cfg.MaxConns = defaultMaxConns
	}
	if cfg.HealthState == nil {
		tr := newHealthTracker(cfg.Clock)
		if cfg.Store != nil {
			tr.setLatchedProbe(cfg.Store.LatchedReadOnly)
		}
		cfg.HealthState = tr
	}
	cfg.fillInfoDefaults()
	return &Server{
		store:     cfg.Store,
		clk:       cfg.Clock,
		logger:    cfg.Logger,
		startedAt: cfg.Clock.Now(),
		reqGen:    id.NewGen(cfg.Clock, id.WithEntropy(rand.Reader)),
		conns:     newConnLimiter(cfg.MaxConns),
		waiters:   NewRegistry(cfg.MaxWaiters, cfg.MaxWaitersPerConsumer),
		closing:   make(chan struct{}),
		cfg:       cfg,
		limits:    cfg.Limits,
		health:    cfg.HealthState,
	}
}

// fillInfoDefaults applies the zero-value defaults that depend on cfg being complete.
func (cfg *Config) fillInfoDefaults() {
	if cfg.InfoCacheTTL <= 0 {
		cfg.InfoCacheTTL = defaultInfoCacheTTL
	}
	if cfg.PendingMaxLimit <= 0 {
		cfg.PendingMaxLimit = defaultPendingMaxLimit
	}
}

// RecordDegraded starts one degradation kind's window on the built-in tracker (#15:
// purge_in_progress comes from the chunked-purge chaser). It is a no-op when an
// injected HealthState replaced the tracker — the owner of the injected state records
// its own degradations.
func (s *Server) RecordDegraded(kind string) {
	if tr, ok := s.health.(*healthTracker); ok {
		tr.recordDegraded(kind)
		return
	}
	s.logger.Warn("api: RecordDegraded ignored: HealthState was injected", "kind", kind)
}

// ClearDegraded ends one degradation kind's window on the built-in tracker.
func (s *Server) ClearDegraded(kind string) {
	if tr, ok := s.health.(*healthTracker); ok {
		tr.clearDegraded(kind)
	}
}

// WaiterRegistry exposes the long-poll park/wake fabric for daemon wiring: the serve
// command passes it to the store as the committed-event sink (store.WithEventSink) and
// to the sweeper as its store.Waker.
func (s *Server) WaiterRegistry() *Registry { return s.waiters }

// Handler assembles the middleware chain over the mux built from routes(): recover →
// request id → conn limit → body limit → authz (#16 slots in here) → envelope-
// intercepting wrapper → router.
func (s *Server) Handler() http.Handler {
	return s.chained(s.newRouter())
}

// Serve runs the HTTP server on ln and the dedup sweep loop until ctx is done, then
// shuts both down cleanly and returns nil. It returns the HTTP server's error when Serve
// fails for any other reason. The store is not closed here.
func (s *Server) Serve(ctx context.Context, ln net.Listener) error {
	hs := &http.Server{
		Handler: s.Handler(),
		// ReadTimeout and WriteTimeout stay ZERO: a server-wide WriteTimeout kills
		// every long poll and a ReadTimeout expires under a parked handler. Bounds are
		// per-request via ResponseController in the fetch path instead.
		ReadTimeout:       0,
		WriteTimeout:      0,
		ReadHeaderTimeout: s.cfg.ReadHeaderTimeout,
		IdleTimeout:       s.cfg.IdleTimeout,
		MaxHeaderBytes:    s.cfg.MaxRequestHeaderBytes,
		ErrorLog:          slog.NewLogLogger(s.logger.Handler(), slog.LevelWarn),
	}
	s.httpSrv.Store(hs)

	ticker := s.clk.NewTicker(s.cfg.SweepEvery)
	defer ticker.Stop()

	serveErr := make(chan error, 1)
	go func() { serveErr <- hs.Serve(ln) }()

	for {
		select {
		case err := <-serveErr:
			if errors.Is(err, http.ErrServerClosed) {
				return nil
			}
			return err
		case <-ctx.Done():
			// Release parked long polls FIRST so their handlers finish writing
			// (200 empty, hold_reason shutting_down) while the HTTP drain waits.
			s.closeOnce.Do(func() { close(s.closing) })
			s.waiters.ReleaseAll()
			shCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), s.cfg.ReadHeaderTimeout)
			defer cancel()
			if err := hs.Shutdown(shCtx); err != nil {
				return err
			}
			return nil
		case <-ticker.C():
			s.sweepOnce(ctx)
		}
	}
}

// Shutdown stops the listener and drains active requests (issue #14's exposure for
// #17's graceful drain). It is safe to call when Serve never ran.
func (s *Server) Shutdown(ctx context.Context) error {
	hs := s.httpSrv.Load()
	if hs == nil {
		return nil
	}
	return hs.Shutdown(ctx)
}

// handleHealthz answers "is the process alive" — always 200 once the listener is
// bound, from memory only. Its degraded[] carries kind+since ONLY (issue #15 §2): no
// version, count or path, because these two routes are the product's unauthenticated
// surface and must stay useless for fingerprinting.
func (s *Server) handleHealthz(w http.ResponseWriter, _ *http.Request) {
	s.writeJSON(w, http.StatusOK, healthzResponse{
		Status:   "ok",
		Degraded: orEmptyDegraded(s.health.Degraded()),
	})
}

// healthzResponse is the probe body both probes share when healthy.
type healthzResponse struct {
	Status   string        `json:"status"`
	Degraded []Degradation `json:"degraded"`
}

// orEmptyDegraded keeps degraded an array in JSON, never null.
func orEmptyDegraded(d []Degradation) []Degradation {
	if d == nil {
		return []Degradation{}
	}
	return d
}

// handleReadyz answers "should clients send work here": recovery complete, store
// writable, not draining — read from memory, NEVER SQLite (issue §2/G3). Disk pressure
// deliberately does not enter this answer (PLAN §4.5). Failures are 503 envelopes with
// code not_ready naming the distinct reason, always Retry-After: 1.
func (s *Server) handleReadyz(w http.ResponseWriter, _ *http.Request) {
	ok, reason := s.health.Ready()
	if ok {
		s.writeJSON(w, http.StatusOK, healthzResponse{Status: "ready", Degraded: nil})
		return
	}
	s.writeEnvelope(w, http.StatusServiceUnavailable, ErrorBody{
		Code:    CodeNotReady,
		Message: reason,
		Detail:  map[string]any{"status": reason, "degraded": orEmptyDegraded(s.health.Degraded())},
	})
}

// infoResponse is the extended /v1/info wire shape (issue #15 §2 + #30 §5). The original
// top-level fields stay byte-compatible for #22's decoded subset; schema_version,
// started_at_ms, wal_bytes, disk_free_bytes, counts, listeners, state and degraded[]
// extend it additively. uptime_ms and state are computed per request and NEVER cached;
// the syscall/count block obeys --info-cache via infoSnapshot below. Restored is the
// optional provenance object from #30: present exactly when the store carries
// restored_* rows, so "was this data dir restored?" has one greppable answer.
type infoResponse struct {
	Version       string        `json:"version"`
	UptimeMS      int64         `json:"uptime_ms"`
	Durability    string        `json:"durability"`
	Synchronous   int           `json:"synchronous"` // LIVE pragma readback (#15), not a config copy
	DBBytes       int64         `json:"db_bytes"`
	NodeID        string        `json:"node_id"`
	Commit        string        `json:"commit"`
	GoVersion     string        `json:"go_version,omitempty"`
	SchemaVersion int64         `json:"schema_version"`
	StartedAtMS   int64         `json:"started_at_ms"`
	WALBytes      int64         `json:"wal_bytes"`
	DiskFreeBytes int64         `json:"disk_free_bytes"`
	Counts        InfoCounts    `json:"counts"`
	Listeners     []string      `json:"listeners"`
	State         string        `json:"state"`
	Degraded      []Degradation `json:"degraded"`
	Restored      *restoredInfo `json:"restored,omitempty"`
	// Dev is additive under #18's classifier (issue #26 §2): serve --dev reports
	// itself so doctor (#30) and operators can key off the mode.
	Dev bool `json:"dev"`
}

// restoredInfo is the optional provenance object: present exactly when the
// store carries restored_* rows, so "was this data dir restored?" has one
// greppable answer.
type restoredInfo struct {
	RestoredAt   string           `json:"restored_at"` // RFC3339 UTC
	SnapshotAt   string           `json:"snapshot_at"` // RFC3339 UTC of the backup's read tx
	SourceNodeID string           `json:"source_node_id"`
	StreamHeads  map[string]int64 `json:"stream_heads,omitempty"`
	ToolVersion  string           `json:"tool_version,omitempty"`
}

// InfoCounts re-exports the store census type's name in json docs; the handler embeds
// the store value directly (identical field names), so no aliasing wrapper is needed.
type InfoCounts = store.InfoCounts

// infoSnapshot is the --info-cache-cached part of /v1/info: syscalls and counts whose
// freshness the window defines. uptime/state never enter this struct.
type infoSnapshot struct {
	until         time.Time
	dbBytes       int64
	walBytes      int64
	diskFreeBytes int64
	counts        store.InfoCounts
}

// refreshInfoSnapshot measures everything cached, under one mutex so concurrent GETs
// do not stampede the syscalls. Measurement failures degrade to zero rather than
// failing the endpoint; the log names them.
func (s *Server) refreshInfoSnapshot(now time.Time) infoSnapshot {
	s.infoMu.Lock()
	defer s.infoMu.Unlock()
	if s.infoCache != nil && now.Before(s.infoCache.until) {
		return *s.infoCache
	}
	dbBytes, walBytes, sizeErr := s.store.Sizes()
	if sizeErr != nil {
		s.logger.Warn("info: measure sizes", "err", sizeErr)
		dbBytes, walBytes = 0, 0
	}
	diskFree, diskErr := s.store.DiskFreeBytes()
	if diskErr != nil {
		s.logger.Warn("info: statfs data dir", "err", diskErr)
		diskFree = 0
	}
	counts, countErr := s.store.InfoCounts(context.Background())
	if countErr != nil {
		s.logger.Warn("info: read counts", "err", countErr)
		counts = store.InfoCounts{}
	}
	snap := infoSnapshot{
		until:         now.Add(s.cfg.InfoCacheTTL),
		dbBytes:       dbBytes,
		walBytes:      walBytes,
		diskFreeBytes: diskFree,
		counts:        counts,
	}
	s.infoCache = &snap
	return snap
}

// handleInfo serves the extended /v1/info shape over the real store handle. The live
// synchronous readback rides the request path UNCACHED on purpose: it is the one field
// whose job is to catch a durability promise that silently broke.
func (s *Server) handleInfo(w http.ResponseWriter, r *http.Request) {
	now := s.clk.Now()
	snap := s.refreshInfoSnapshot(now)

	synchronous := s.store.Durability().Synchronous() // fallback when the pool is gone
	if live, err := s.store.LiveSynchronous(r.Context()); err == nil {
		synchronous = live
	} else {
		s.logger.Warn("info: live synchronous readback", "err", err)
	}

	state := "ready"
	if ok, reason := s.health.Ready(); !ok {
		state = reason
	}

	resp := infoResponse{
		Version:       buildinfo.Get().Version,
		UptimeMS:      now.Sub(s.startedAt).Milliseconds(),
		Durability:    s.store.Durability().String(),
		Synchronous:   synchronous,
		DBBytes:       snap.dbBytes,
		NodeID:        s.store.NodeID(),
		Commit:        buildinfo.Get().Commit,
		GoVersion:     runtime.Version(),
		SchemaVersion: int64(s.store.SchemaVersion()),
		StartedAtMS:   s.startedAt.UnixMilli(),
		WALBytes:      snap.walBytes,
		DiskFreeBytes: snap.diskFreeBytes,
		Counts:        snap.counts,
		Listeners:     orEmptyStrings(s.cfg.Listeners),
		State:         state,
		Degraded:      orEmptyDegraded(s.health.Degraded()),
		Dev:           s.cfg.Dev,
	}
	if prov := s.store.Provenance(); prov != nil {
		resp.Restored = &restoredInfo{
			RestoredAt:   prov.RestoredAt.UTC().Format(time.RFC3339),
			SnapshotAt:   prov.SnapshotAt.UTC().Format(time.RFC3339),
			SourceNodeID: prov.SourceNodeID,
			StreamHeads:  prov.StreamHeads,
			ToolVersion:  prov.ToolVersion,
		}
	}
	s.writeJSON(w, http.StatusOK, resp)
}

// orEmptyStrings keeps listeners an array in JSON, never null.
func orEmptyStrings(v []string) []string {
	if v == nil {
		return []string{}
	}
	return v
}

// sweepOnce runs one dedup sweep pass: every stream's expired dedup keys are cleared.
// Errors are logged, not fatal — the next tick retries.
func (s *Server) sweepOnce(ctx context.Context) {
	streams, err := s.store.ListStreams(ctx)
	if err != nil {
		s.logger.Warn("dedup sweep: list streams", "err", err)
		return
	}
	for _, st := range streams {
		if _, err := s.store.SweepDedup(ctx, st.Name); err != nil {
			s.logger.Warn("dedup sweep", "stream", st.Name, "err", err)
		}
	}
}
