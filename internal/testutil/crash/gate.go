// SPDX-License-Identifier: Apache-2.0

package crash

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"

	"github.com/a-holm/messq/internal/clock"
	"github.com/a-holm/messq/internal/id"
	"github.com/a-holm/messq/internal/testutil/loadgen"
)

// The M2 throughput gate: a measurement mode of the load generator that turns D1's exit
// criterion ("≥ 5 000 durable 1 KiB msg/s on NVMe") into a recorded number with a recorded
// methodology. It measures publish → durable response under full durability — the quantity
// D1 gates — with a raw fsync probe on the same filesystem so the number is interpretable
// (achievable ≈ batch_size / fsync_latency).

// GateResult is one throughput measurement.
type GateResult struct {
	Publishers   int
	PayloadSize  int
	Duration     time.Duration
	Messages     int64
	MsgsPerSec   float64
	P50          time.Duration
	P99          time.Duration
	P999         time.Duration
	FsyncSamples int
	FsyncP50     time.Duration
	FsyncP99     time.Duration
}

// RunGate measures publish→durable-response throughput: publishers goroutines each publish
// size-byte deterministic bodies for dur, recording the wall-clock latency of every durable
// response (the clock seam's monotonic Since). The returned result is the D1 gate number.
func RunGate(ctx context.Context, cfg Config, publishers, size int, dur time.Duration) (*GateResult, error) {
	cfg = cfg.defaults()
	clk := cfg.clk()
	if err := os.MkdirAll(cfg.Root, 0o700); err != nil {
		return nil, fmt.Errorf("mkdir root: %w", err)
	}
	sock := cfg.sock()
	sut, err := Start(ctx, Config{Durability: "full", Clock: clk}, cfg.dataDir(), sock)
	if err != nil {
		return nil, err
	}
	if readyErr := sut.Ready(ctx); readyErr != nil {
		return nil, readyErr
	}
	defer func() {
		if stopErr := sut.Stop(); stopErr != nil {
			err = stopErr
		}
	}()
	client := loadgen.UnixClient(sock)
	if streamErr := ensureStream(ctx, client, cfg.Stream); streamErr != nil {
		return nil, streamErr
	}

	gen := id.NewGen(clk)
	body := loadgen.Payload("gate", size)
	var (
		mu    sync.Mutex
		lat   []time.Duration
		count int64
		wg    sync.WaitGroup
		stop  = make(chan struct{})
	)
	runCtx, cancel := context.WithTimeout(ctx, dur)
	defer cancel()

	for p := 0; p < publishers; p++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-runCtx.Done():
					return
				case <-stop:
					return
				default:
				}
				key := gen.NewString()
				req, reqErr := http.NewRequestWithContext(runCtx, http.MethodPost,
					"http://messq/v1/streams/"+cfg.Stream+"/messages?subject="+cfg.Subject,
					bytes.NewReader(body))
				if reqErr != nil {
					return
				}
				req.Header.Set("Messq-Msg-Id", key)
				start := clk.Now()
				resp, doErr := client.Do(req)
				if doErr != nil {
					return
				}
				_, readErr := io.Copy(io.Discard, resp.Body)
				closeErr := resp.Body.Close()
				if readErr != nil || closeErr != nil {
					return
				}
				if resp.StatusCode/100 != 2 {
					return
				}
				d := clk.Since(start)
				mu.Lock()
				lat = append(lat, d)
				count++
				mu.Unlock()
			}
		}()
	}
	<-runCtx.Done()
	close(stop)
	wg.Wait()

	fsP50, fsP99, fsN, fsErr := fsyncProbe(cfg.Root, 1000)
	if fsErr != nil {
		return nil, fsErr
	}
	sort.Slice(lat, func(i, j int) bool { return lat[i] < lat[j] })
	return &GateResult{
		Publishers:   publishers,
		PayloadSize:  size,
		Duration:     dur,
		Messages:     count,
		MsgsPerSec:   float64(count) / dur.Seconds(),
		P50:          percentile(lat, 0.50),
		P99:          percentile(lat, 0.99),
		P999:         percentile(lat, 0.999),
		FsyncSamples: fsN,
		FsyncP50:     fsP50,
		FsyncP99:     fsP99,
	}, nil
}

// percentile returns the p-th percentile of a sorted, non-empty duration slice.
func percentile(sorted []time.Duration, p float64) time.Duration {
	if len(sorted) == 0 {
		return 0
	}
	idx := int(float64(len(sorted)-1) * p)
	return sorted[idx]
}

// fsyncProbe measures the raw fsync latency of the filesystem the data dir lives on: write
// a 4 KiB buffer and fdatasync it, samples times. It is what makes the gate number
// interpretable — a per-commit cost far below this is a lying write cache.
func fsyncProbe(dir string, samples int) (p50, p99 time.Duration, n int, err error) {
	path := filepath.Join(dir, "fsync-probe.tmp")
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY, 0o600)
	if err != nil {
		return 0, 0, 0, fmt.Errorf("open fsync probe: %w", err)
	}
	defer func() {
		if cerr := f.Close(); cerr != nil {
			err = cerr
		}
		if rmErr := os.Remove(path); rmErr != nil {
			err = rmErr
		}
	}()
	buf := make([]byte, 4096)
	clk := clock.System{}
	lat := make([]time.Duration, 0, samples)
	for i := 0; i < samples; i++ {
		start := clk.Now()
		if _, werr := f.WriteAt(buf, 0); werr != nil {
			return 0, 0, 0, werr
		}
		if serr := f.Sync(); serr != nil {
			return 0, 0, 0, serr
		}
		lat = append(lat, clk.Since(start))
	}
	sort.Slice(lat, func(i, j int) bool { return lat[i] < lat[j] })
	return percentile(lat, 0.50), percentile(lat, 0.99), len(lat), nil
}
