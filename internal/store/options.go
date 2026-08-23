// SPDX-License-Identifier: Apache-2.0

package store

import (
	"log/slog"
	"runtime"
	"time"

	"github.com/a-holm/messq/internal/clock"
	"github.com/a-holm/messq/internal/id"
	"github.com/a-holm/messq/internal/queue"
)

// Options configures [Open]. Zero values mean "default", except where a field's comment says
// otherwise; [Options.applyDefaults] fills them in before Open touches the data directory.
// The three seams (Clock, Logger, NewID) default to production implementations so that a
// zero-value Options is safe to hand to Open directly.
type Options struct {
	// DataDir is the directory holding messq.db and the LOCK file (--data-dir).
	DataDir string
	// Durability selects synchronous=FULL (full) or synchronous=NORMAL (relaxed); zero is full.
	Durability Durability
	// FullCheck runs integrity_check instead of quick_check during recovery (--fsck).
	FullCheck bool
	// ReadPoolSize caps the read pool; <= 0 means runtime.NumCPU() (--read-pool).
	ReadPoolSize int
	// BusyTimeout is the SQLite busy_timeout in effect on every connection; <= 0 means 5s
	// (--busy-timeout).
	BusyTimeout time.Duration
	// CacheBytes is the per-connection page-cache budget as a positive byte count (stored in
	// the DSN as cache_size=-N kibibytes); <= 0 means 64 MiB (--cache-size).
	CacheBytes int64
	// ReclaimJitter bounds the random delay spread over lease reclaim at startup. Unlike the
	// numeric fields, 0 is meaningful and kept: it makes reclaim deterministic for tests.
	// A negative value falls back to the 1s default (--reclaim-jitter).
	ReclaimJitter time.Duration
	// ReadOnly opens for offline inspection: no rw handle, no recovery, no lock write.
	ReadOnly bool
	// Limits are the process-wide validation ceilings (issue §4.2) every stream
	// configuration is checked against; zero value means DefaultLimits().
	Limits queue.Limits
	// ConsumerLimits are the consumer-side ceilings (issue #9 §10); zero value means
	// DefaultConsumerLimits().
	ConsumerLimits queue.ConsumerLimits
	// PeekMaxLimit caps a listing page's effective limit (issue §6, --peek-max-limit);
	// pages that include bodies cap at one tenth of it. <= 0 means 1000.
	PeekMaxLimit int
	// PeekScanLimit bounds the rows a wildcard-subject listing may scan before it
	// returns an honest partial answer (issue §6, --peek-scan-limit). <= 0 means 10000.
	PeekScanLimit int
	// MaxBatchMessages caps one PublishBatch command (§7, --max-batch-messages);
	// <= 0 means 1000.
	MaxBatchMessages int
	// DedupSweepInterval is how often serve invokes SweepDedup (§4,
	// --dedup-sweep-interval). The invariant checker allows keys to outlive their
	// window by this much before calling them stale. <= 0 means 60s.
	DedupSweepInterval time.Duration
	// Clock is the time seam from #3; never nil after applyDefaults.
	Clock clock.Clock
	// Logger receives the store's slog lines; nil means slog.Default().
	Logger *slog.Logger
	// NewID mints node_id and recovery event ids through #3's generator; nil means a
	// generator over Clock. Injected so node_id is deterministic in tests. The id type is
	// id.MsgID — internal/id's alias for oklog/ulid/v2's ULID.
	NewID func() id.MsgID
}

const (
	defaultBusyTimeout   = 5 * time.Second
	defaultCacheBytes    = int64(64) << 20
	defaultReclaimJitter = time.Second
	defaultPeekMaxLimit  = 1_000
	defaultPeekScanLimit = 10_000
	defaultMaxBatch      = 1_000
	defaultDedupSweep    = 60 * time.Second
)

// applyDefaults fills unset fields with their documented defaults and never replaces a value
// the caller chose. It must stay idempotent: running it twice leaves the same Options,
// including the same live id generator.
func (o *Options) applyDefaults() {
	if o.ReadPoolSize <= 0 {
		o.ReadPoolSize = runtime.NumCPU()
	}
	if o.BusyTimeout <= 0 {
		o.BusyTimeout = defaultBusyTimeout
	}
	if o.CacheBytes <= 0 {
		o.CacheBytes = defaultCacheBytes
	}
	if o.ReclaimJitter < 0 {
		o.ReclaimJitter = defaultReclaimJitter
	}
	if o.Limits == (queue.Limits{}) {
		o.Limits = queue.DefaultLimits()
	}
	if o.ConsumerLimits == (queue.ConsumerLimits{}) {
		o.ConsumerLimits = queue.DefaultConsumerLimits()
	}
	if o.PeekMaxLimit <= 0 {
		o.PeekMaxLimit = defaultPeekMaxLimit
	}
	if o.PeekScanLimit <= 0 {
		o.PeekScanLimit = defaultPeekScanLimit
	}
	if o.MaxBatchMessages <= 0 {
		o.MaxBatchMessages = defaultMaxBatch
	}
	if o.DedupSweepInterval <= 0 {
		o.DedupSweepInterval = defaultDedupSweep
	}
	if o.Clock == nil {
		o.Clock = clock.System{}
	}
	if o.Logger == nil {
		o.Logger = slog.Default()
	}
	if o.NewID == nil {
		gen := id.NewGen(o.Clock)
		o.NewID = gen.New
	}
}
