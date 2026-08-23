// SPDX-License-Identifier: Apache-2.0

package api

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net"
	"net/http"
	"time"

	"github.com/a-holm/messq/internal/buildinfo"
	"github.com/a-holm/messq/internal/clock"
	"github.com/a-holm/messq/internal/store"
)

// readHeaderTimeout bounds how long a client may take to send request headers. A Unix
// socket broker is still reachable by a client that connects and stalls, so the header
// read is the one unconditional bound this provisional server sets; #14 makes the full
// timeout policy.
const readHeaderTimeout = 10 * time.Second

// Server is the HTTP surface of the daemon: the ServeMux route table plus the
// dedup-sweep ticker. It owns the net/http server and the sweep loop; the store's
// open/close lifecycle belongs to the caller (the serve command), so Serve never closes
// the store.
type Server struct {
	store      *store.Store
	clk        clock.Clock
	logger     *slog.Logger
	startedAt  time.Time
	sweepEvery time.Duration
}

// New builds a Server around a live, already-recovered store. A nil clock or logger falls
// back to the production implementations. sweepEvery must be positive: it is the period
// the dedup sweep ticks at (--dedup-sweep-interval).
func New(st *store.Store, clk clock.Clock, logger *slog.Logger, sweepEvery time.Duration) *Server {
	if clk == nil {
		clk = clock.System{}
	}
	if logger == nil {
		logger = slog.Default()
	}
	return &Server{
		store:      st,
		clk:        clk,
		logger:     logger,
		startedAt:  clk.Now(),
		sweepEvery: sweepEvery,
	}
}

// Handler assembles the route table. Slice A owns /healthz and /v1/info; slices B, C
// and D register the streams, publish/batch and peek routes on this same mux.
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", s.handleHealthz)
	mux.HandleFunc("GET /v1/info", s.handleInfo)
	return mux
}

// Serve runs the HTTP server on ln and the dedup sweep loop until ctx is done, then
// shuts both down cleanly and returns nil. It returns the HTTP server's error when Serve
// fails for any other reason. The store is not closed here.
func (s *Server) Serve(ctx context.Context, ln net.Listener) error {
	hs := &http.Server{
		Handler:           s.Handler(),
		ReadHeaderTimeout: readHeaderTimeout,
	}

	ticker := s.clk.NewTicker(s.sweepEvery)
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
			shCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), readHeaderTimeout)
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
