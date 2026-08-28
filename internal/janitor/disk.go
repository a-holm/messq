// SPDX-License-Identifier: Apache-2.0

package janitor

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/a-holm/messq/internal/clock"
	"github.com/a-holm/messq/internal/queue"
)

// The disk monitor (issue #27 §8): one component sampling free bytes on the data
// directory's filesystem and running queue.NextDiskState, the pure hysteretic state
// machine slice 1 shipped. Deliberately NOT a Job on the tick list — --janitor-
// interval 0 disables bounded housekeeping, but disk safety keeps sampling, because
// the whole point of --min-free-bytes is keeping D4's ENOSPC latch unreachable.
//
// The monitor decides; downstream seams act: OnState feeds /healthz's degraded[]
// (slices 8) and messq_disk_free_bytes stays #21's scrape-time statfs until the
//   state machine may own later slices.)
// Transitions are logged once each; steady-state is silent.

// DiskProbe samples free bytes for one path.
type DiskProbe interface {
	Free(path string) (int64, error)
}

// StatfsProbe is the production probe: statfs(2) on Linux.
type StatfsProbe struct{}

// Free implements DiskProbe with statfs.
func (StatfsProbe) Free(path string) (int64, error) {
	var st syscall.Statfs_t
	if err := syscall.Statfs(path, &st); err != nil {
		return -1, fmt.Errorf("statfs %s: %w", path, err)
	}
	// Bound-check before narrowing so an absurd mount fails loudly instead of
	// reporting a wrapped-around "huge free" or a negative one.
	const maxI64 = uint64(^uint64(0) >> 1)
	freeU := st.Bavail * uint64(st.Bsize) //nolint:gosec // Bsize is a block size, never negative in practice
	if freeU > maxI64 {
		return -1, fmt.Errorf("statfs %s: free space exceeds int64", path)
	}
	return int64(freeU), nil
}

// DiskMonitorConfig configures a [DiskMonitor].
type DiskMonitorConfig struct {
	Path     string           // data directory under watch
	Policy   queue.DiskPolicy // MinFree / Recover / Reserve straight from flags
	Probe    DiskProbe        // required
	Interval time.Duration    // sample cadence; <= 0 defaults to 60s
	OnState  func(queue.DiskState)

	Log *slog.Logger
}

const defaultDiskInterval = time.Minute

// DiskMonitor is the single disk-safety sampler goroutine.
type DiskMonitor struct {
	cfg   DiskMonitorConfig
	log   *slog.Logger
	clk   clock.Clock
	state atomic.Int32 // current queue.DiskState
	tickC clock.Ticker

	loopCtx  atomic.Pointer[context.Context]
	cancelFn context.CancelFunc
	once     sync.Once
}

// NewDiskMonitor validates config and pins the seam. Start has not run yet.
func NewDiskMonitor(cfg DiskMonitorConfig, clk clock.Clock) *DiskMonitor {
	if cfg.Interval <= 0 {
		cfg.Interval = defaultDiskInterval
	}
	if cfg.Log == nil {
		cfg.Log = slog.Default()
	}
	m := &DiskMonitor{cfg: cfg, log: cfg.Log, clk: clk}
	m.state.Store(int32(queue.DiskOK))
	return m
}

// Name implements lifecycle.Component.
func (m *DiskMonitor) Name() string { return "disk-monitor" }

// State reports the current disk state without racing samplers.
func (m *DiskMonitor) State() queue.DiskState {
	v := m.state.Load() // stored values are only ever 0..1 (DiskOK/DiskLow)
	if v < 0 || v > 127 {
		return queue.DiskLow // defensive: an unwritable memory cell reads as unsafe
	}
	return queue.DiskState(v)
}

// Start implements lifecycle.Component: arms the sampler and returns immediately.
func (m *DiskMonitor) Start(ctx context.Context) error {
	loop, cancel := context.WithCancel(ctx)
	m.loopCtx.Store(&loop)
	m.cancelFn = cancel
	m.tickC = m.clk.NewTicker(m.cfg.Interval)
	go m.loop()
	return nil
}

// loop is THE goroutine: sample → plan → fan out transitions exactly once each.
func (m *DiskMonitor) loop() {
	for range m.tickC.C() {
		if ctx := m.ctx(); ctx.Err() != nil {
			return
		}
		cur := m.State()
		free, pErr := m.cfg.Probe.Free(m.cfg.Path)
		if pErr != nil {
			m.log.Warn("janitor.disk_probe_failed", "path", m.cfg.Path, "error", pErr.Error())
			continue // hold the previous state rather than flapping on bad samples
		}
		next, actions := queue.NextDiskState(cur, free, m.cfg.Policy)
		if next == cur {
			continue
		}
		m.state.Store(int32(next))
		m.log.Warn("disk.degraded",
			"from", cur.String(), "to", next.String(),
			"free_bytes", free, "actions", actionNames(actions))
		if m.cfg.OnState != nil {
			m.cfg.OnState(next)
		}
	}
}

// Stop implements lifecycle.Component: cancel, never await (#17 contract).
func (m *DiskMonitor) Stop(context.Context) error {
	m.once.Do(func() {
		if m.cancelFn != nil {
			m.cancelFn()
		}
	})
	return nil
}

func (m *DiskMonitor) ctx() context.Context {
	if p := m.loopCtx.Load(); p != nil {
		return *p
	}
	return context.Background()
}

// actionNames renders the action list without importing fmt into hot loops.
func actionNames(as []queue.DiskAction) []string {
	out := make([]string, 0, len(as))
	for _, a := range as {
		out = append(out, a.String())
	}
	return out
}
