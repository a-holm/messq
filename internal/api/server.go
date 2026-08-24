// SPDX-License-Identifier: Apache-2.0

package api

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net"
	"net/http"
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
	return &Server{
		store:     cfg.Store,
		clk:       cfg.Clock,
		logger:    cfg.Logger,
		startedAt: cfg.Clock.Now(),
		reqGen:    id.NewGen(cfg.Clock, id.WithEntropy(rand.Reader)),
		conns:     newConnLimiter(cfg.MaxConns),
		cfg:       cfg,
		limits:    cfg.Limits,
	}
}

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

// handleHealthz reports 200 once the store is open. Recovery runs inside store.Open, so
// a server that is answering /healthz has necessarily finished recovery.
func (s *Server) handleHealthz(w http.ResponseWriter, _ *http.Request) {
	w.WriteHeader(http.StatusOK)
	if _, err := io.WriteString(w, "ok\n"); err != nil {
		s.logger.Warn("healthz: write response", "err", err)
	}
}

// infoResponse is the /v1/info wire shape (issue §7): version, uptime, durability, the
// live synchronous value, db bytes and node id.
type infoResponse struct {
	Version     string `json:"version"`
	UptimeMS    int64  `json:"uptime_ms"`
	Durability  string `json:"durability"`
	Synchronous int    `json:"synchronous"`
	DBBytes     int64  `json:"db_bytes"`
	NodeID      string `json:"node_id"`
}

// handleInfo serves the /v1/info shape. Sizes is best-effort: a measurement failure is
// logged and reported as zero rather than failing the info endpoint.
func (s *Server) handleInfo(w http.ResponseWriter, _ *http.Request) {
	dbBytes, _, err := s.store.Sizes()
	if err != nil {
		s.logger.Warn("info: measure sizes", "err", err)
		dbBytes = 0
	}
	resp := infoResponse{
		Version:     buildinfo.Get().Version,
		UptimeMS:    s.clk.Since(s.startedAt).Milliseconds(),
		Durability:  s.store.Durability().String(),
		Synchronous: s.store.Durability().Synchronous(),
		DBBytes:     dbBytes,
		NodeID:      s.store.NodeID(),
	}
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(resp); err != nil {
		s.logger.Warn("info: write response", "err", err)
	}
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
