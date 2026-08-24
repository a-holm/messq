// SPDX-License-Identifier: Apache-2.0

package store

import (
	"context"
	"database/sql"
	"log/slog"
	"sync/atomic"
	"time"

	"github.com/a-holm/messq/internal/clock"
	"github.com/a-holm/messq/internal/queue"
)

// The single sweeper goroutine (issue #11 §3.2/§3): a 250 ms ticker on the Clock seam
// that turns "absence of an ack" into a durable decision. It never opens a transaction
// of its own — work is submitted as SweepCmds through the store (the writer path when an
// engine is attached, runSolo otherwise), so the single-writer invariant and the group
// commit are untouched. One sweep is in flight at a time: overlapping ticks are dropped
// and counted (ticks coalesced, never queued — G6). ctx.Done shuts it down immediately.

// SweepConfig is the sweeper's tunables (issue #11 §10).
type SweepConfig struct {
	Interval       time.Duration // --sweep-interval, 250ms
	Batch          int           // --sweep-batch, 1024
	Catchup        int           // --sweep-catchup, 8
	RetireInterval time.Duration // --retire-interval, 60s (0 = startup only)
}

func (c *SweepConfig) fillDefaults() {
	if c.Interval <= 0 {
		c.Interval = 250 * time.Millisecond
	}
	if c.Batch <= 0 {
		c.Batch = 1024
	}
	if c.Catchup <= 0 {
		c.Catchup = 8
	}
	if c.RetireInterval < 0 {
		c.RetireInterval = 0
	}
}

// Sweeper drives the expiry sweep and retire pass. Construct with [NewSweeper].
type Sweeper struct {
	st    *Store
	ro    *sql.DB
	clk   clock.Clock
	waker Waker
	cfg   SweepConfig
	log   *slog.Logger
	busy  atomic.Bool // coalescing latch: one sweep in flight at a time (G6)
}

// NewSweeper builds the goroutine's state. The store must be open; ro is its read pool,
// which WAL readers share with the writer without blocking it.
func NewSweeper(st *Store, cfg SweepConfig, waker Waker, logger *slog.Logger) *Sweeper {
	cfg.fillDefaults()
	if waker == nil {
		waker = NopWaker{}
	}
	if logger == nil {
		logger = slog.Default()
	}
	return &Sweeper{
		st: st, ro: st.RO(), clk: st.clk,
		waker: waker, cfg: cfg, log: logger,
	}
}

// Run drives the ticker until ctx is done. It returns nil quietly on shutdown. The
// retire pass runs once at startup too (G7 — a restored backup or hand-edited database
// is exactly when a stranded row appears), then on --retire-interval when non-zero.
func (s *Sweeper) Run(ctx context.Context) error {
	s.retire(ctx) // startup pass

	ticker := s.clk.NewTicker(s.cfg.Interval)
	defer ticker.Stop()

	var retireCh <-chan time.Time
	var retireTicker clock.Ticker
	if s.cfg.RetireInterval > 0 {
		retireTicker = s.clk.NewTicker(s.cfg.RetireInterval)
		retireCh = retireTicker.C()
		defer retireTicker.Stop()
	}

	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C():
			s.step(ctx)
		case <-retireCh:
			s.retire(ctx)
		}
	}
}

// RetireNow triggers the retire pass on demand (test seam for the startup-only case).
func (s *Sweeper) RetireNow(ctx context.Context) { s.retire(ctx) }

// step runs the expiry sweep for one tick, serialized by the coalescing latch: if the
// previous tick's sweep is still running, this tick is counted and dropped (G6).
func (s *Sweeper) step(ctx context.Context) {
	if !s.busy.CompareAndSwap(false, true) {
		s.sweepSkipped("overlap")
		return
	}
	defer s.busy.Store(false)

	// Pass A — expiry. Probe the read pool first (WAL readers never block the writer); a
	// NULL or future deadline means nothing to do: no command, no transaction, no fsync.
	if s.probe(ctx) {
		s.sweep(ctx)
	}
	// Pass B — wake parked consumers whose redelivery came due (fast path).
	s.wakeDue(ctx)
}

// sweep submits sweep batches with a bounded context (2 x interval for the writer-busy
// abandonment), then resubmits up to Catchup times while More stays true (burst drain:
// batch x catchup / interval = 32 768 rows/s at defaults — G6).
func (s *Sweeper) sweep(ctx context.Context) {
	for i := 0; i <= s.cfg.Catchup; i++ {
		sub, cancel := context.WithTimeout(ctx, 2*s.cfg.Interval)
		res, err := s.st.Sweep(sub, SweepCmd{Limit: s.cfg.Batch})
		cancel()
		if err != nil {
			s.log.Warn("sweeper.writer_busy",
				"node", s.st.nodeID, "error", err.Error())
			s.sweepSkipped("writer_busy")
			return
		}
		s.wake(res.Woke)
		if !res.More {
			return
		}
	}
	s.sweepSkipped("catchup_cap")
}

func (s *Sweeper) sweepSkipped(reason string) {
	_ = reason // #21 registers messq_sweep_skipped_total{reason}; count behind the seam later
}

// probe reports whether any INFLIGHT row is currently expired (a MIN over the partial
// index — one O(log n) b-tree descent when idle; G4).
func (s *Sweeper) probe(ctx context.Context) bool {
	var vis sql.NullInt64
	err := s.ro.QueryRowContext(ctx,
		`SELECT MIN(visible_at) FROM deliveries WHERE state = 1`).Scan(&vis)
	if err != nil || !vis.Valid {
		return false
	}
	return vis.Int64 <= s.clk.Now().UnixMilli()
}

// wake fires the post-commit wakes returned by a sweep (G8: after Do returns, never
// inside Apply — a pre-commit wake could deliver from a rolled-back transaction).
func (s *Sweeper) wake(keys []queue.ConsumerKey) {
	for _, k := range keys {
		s.waker.Wake(k)
	}
}

// wakeDue is pass B: for each parked consumer, if a READY delivery of theirs is visible,
// wake them — the fast path for a backoff that came due. nil from Waiting skips the pass
// entirely (the overwhelmingly common case).
func (s *Sweeper) wakeDue(ctx context.Context) {
	keys := s.waker.Waiting()
	if len(keys) == 0 {
		return
	}
	if len(keys) > 256 {
		keys = keys[:256] // bounded per tick, deliberately not a flag (I11)
	}
	nowMS := s.clk.Now().UnixMilli()
	for _, k := range keys {
		var one int
		err := s.ro.QueryRowContext(ctx,
			`SELECT 1 FROM deliveries WHERE stream = ? AND consumer = ? AND state = 0 AND visible_at <= ? LIMIT 1`,
			k.Stream, k.Consumer, nowMS).Scan(&one)
		if err == nil {
			s.waker.Wake(k)
		}
	}
}

// retire runs the RetireCmd pass (issue #11 §7); R etireCmd lives in sweep.go.
func (s *Sweeper) retire(ctx context.Context) {
	_ = ctx
}
