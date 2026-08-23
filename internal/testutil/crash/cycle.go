// SPDX-License-Identifier: Apache-2.0

package crash

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"os"
	"strings"
	"sync"

	"github.com/a-holm/messq/internal/clock"
	"github.com/a-holm/messq/internal/id"
	"github.com/a-holm/messq/internal/testutil/ledger"
	"github.com/a-holm/messq/internal/testutil/loadgen"
)

// CycleResult is one kill/restart cycle's outcome, printed by the report.
type CycleResult struct {
	Cycle    int
	Seed     int64
	Strategy string
	OK       int64
	Unknown  int64
	Failed   int64
}

// Run drives the whole sweep: Cycles kill/restart cycles against the real binary, appending
// every intent and outcome to the external ledger under Root, and returning the per-cycle
// results. The ledger is opened once and closed here, so the reconciler (a later slice) can
// join the full ledger against the full recovered state.
func Run(ctx context.Context, cfg Config) ([]CycleResult, error) {
	cfg = cfg.defaults()
	clk := cfg.clk()
	if cfg.Seed == 0 {
		cfg.Seed = clk.Now().UnixNano()
	}
	if err := os.MkdirAll(cfg.Root, 0o700); err != nil {
		return nil, fmt.Errorf("mkdir root %s: %w", cfg.Root, err)
	}
	lg, err := ledger.Open(cfg.ledgerPath(), ledger.Config{}, clk)
	if err != nil {
		return nil, err
	}
	defer func() {
		if closeErr := lg.Close(); closeErr != nil {
			// Surface a ledger close failure, but never mask the sweep result.
			err = closeErr
		}
	}()

	results := make([]CycleResult, 0, cfg.Cycles)
	for cycle := 0; cycle < cfg.Cycles; cycle++ {
		res, err := runCycle(ctx, cfg, clk, lg, cycle)
		if err != nil {
			return results, fmt.Errorf("cycle %d: %w", cycle, err)
		}
		results = append(results, res)
	}
	return results, nil
}

// runCycle executes one kill/restart cycle and returns its result. It never leaves a leaked
// daemon behind: every start is paired with a Kill/Stop on every path.
func runCycle(ctx context.Context, cfg Config, clk clock.Clock, lg *ledger.Ledger, cycleN int) (CycleResult, error) {
	seed := cfg.Seed + int64(cycleN)
	rng := rand.New(rand.NewSource(seed))
	strategy := cfg.Kill
	if strategy == nil {
		strategy = pickStrategy(rng)
	}

	sock := cfg.sock()
	sutCfg := Config{Durability: cfg.Durability, Clock: clk}

	// 1-2. Start the daemon and wait for /healthz.
	sut, startErr := Start(ctx, sutCfg, cfg.dataDir(), sock)
	if startErr != nil {
		return CycleResult{}, startErr
	}
	if readyErr := sut.Ready(ctx); readyErr != nil {
		return CycleResult{}, errors.Join(readyErr, sut.Kill())
	}

	// 3. Ensure the harness stream exists (idempotent across the restart).
	client := loadgen.UnixClient(sock)
	if streamErr := ensureStream(ctx, client, cfg.Stream); streamErr != nil {
		return CycleResult{}, errors.Join(streamErr, sut.Kill())
	}

	// 4. Concurrent publishers, writing the ledger before every request.
	obs := &loadgen.Observations{}
	gen := id.NewGen(clk)
	pubErr := make(chan error, cfg.Publishers)
	stop := make(chan struct{})
	var pubWG sync.WaitGroup
	for i := 0; i < cfg.Publishers; i++ {
		pubWG.Add(1)
		go func() {
			defer pubWG.Done()
			p := &loadgen.Publisher{
				Stream:  cfg.Stream,
				Subject: cfg.Subject,
				Sizes:   cfg.Sizes,
				Cycle:   cycleN,
				Ledger:  lg,
				Client:  client,
				NewKey:  gen.NewString,
				Clk:     clk,
				Obs:     obs,
			}
			if runErr := p.Run(ctx, stop); runErr != nil {
				pubErr <- runErr
			}
		}()
	}

	// 5. Kill at the strategy's seeded point.
	if waitErr := strategy.Wait(ctx, clk, rng, obs); waitErr != nil {
		close(stop)
		pubWG.Wait()
		return CycleResult{}, errors.Join(waitErr, sut.Kill())
	}
	if killErr := sut.Kill(); killErr != nil {
		close(stop)
		pubWG.Wait()
		return CycleResult{}, killErr
	}

	// 6. Stop issuing, drain the publishers (each in-flight request resolves UNKNOWN), and
	// make the ledger durable before touching the recovered state.
	close(stop)
	pubWG.Wait()
	close(pubErr)
	for pe := range pubErr {
		if pe != nil {
			return CycleResult{}, pe
		}
	}
	if syncErr := lg.Sync(); syncErr != nil {
		return CycleResult{}, syncErr
	}

	// 7. Restart on the same data dir with zero cleanup.
	sut2, restartErr := Start(ctx, sutCfg, cfg.dataDir(), sock)
	if restartErr != nil {
		return CycleResult{}, restartErr
	}
	if readyErr := sut2.Ready(ctx); readyErr != nil {
		return CycleResult{}, errors.Join(readyErr, sut2.Kill())
	}

	// 9. The §4.4 recovery contract, lite: recovery.unclean was emitted on the restart, and
	// a probe publish after recovery receives a sequence number.
	if !strings.Contains(sut2.Stderr(), "recovery.unclean") {
		return CycleResult{}, errors.Join(
			fmt.Errorf("restart did not emit recovery.unclean:\n%s", sut2.Stderr()),
			sut2.Kill())
	}
	if _, probeErr := probePublish(ctx, loadgen.UnixClient(sock), cfg.Stream, cfg.Subject); probeErr != nil {
		return CycleResult{}, errors.Join(probeErr, sut2.Kill())
	}

	// End the cycle with a graceful stop: a killed subprocess flushes no coverage counters,
	// and clean cycles are the ones that do contribute to the floor.
	if stopErr := sut2.Stop(); stopErr != nil {
		return CycleResult{}, stopErr
	}

	return CycleResult{
		Cycle:    cycleN,
		Seed:     seed,
		Strategy: strategy.Name(),
		OK:       obs.OK.Load(),
		Unknown:  obs.Unknown.Load(),
		Failed:   obs.Failed.Load(),
	}, nil
}

// sock is the Unix socket path under Root.
func (c Config) sock() string { return c.Root + "/messq.sock" }

// ensureStream creates the harness stream with a 24 h dedup window and no retention limits,
// so no message the oracle expects can ever be deleted before reconciliation. It is
// idempotent: a 409 (already exists, e.g. after a restart) is success.
func ensureStream(ctx context.Context, client *http.Client, stream string) error {
	body := fmt.Sprintf(`{"name":%q,"subjects":[">"],"dedup_window_ms":86400000,"max_age_ms":0,"max_msgs":0,"max_bytes":0,"max_msg_size":8388608}`, stream)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, "http://messq/v1/streams", strings.NewReader(body))
	if err != nil {
		return fmt.Errorf("build create-stream request: %w", err)
	}
	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("create stream %q: %w", stream, err)
	}
	closeErr := resp.Body.Close()
	if resp.StatusCode == http.StatusCreated || resp.StatusCode == http.StatusConflict {
		return nil
	}
	if closeErr != nil {
		return fmt.Errorf("create stream %q: close: %w", stream, closeErr)
	}
	return fmt.Errorf("create stream %q: status %d", stream, resp.StatusCode)
}

// probePublish publishes a single control message and returns its sequence number — the
// durable, gap-free allocator's next value after recovery.
func probePublish(ctx context.Context, client *http.Client, stream, subject string) (int64, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost,
		"http://messq/v1/streams/"+stream+"/messages?subject="+subject,
		bytes.NewReader([]byte("probe")))
	if err != nil {
		return 0, fmt.Errorf("build probe request: %w", err)
	}
	resp, err := client.Do(req)
	if err != nil {
		return 0, fmt.Errorf("probe publish: %w", err)
	}
	body, readErr := io.ReadAll(resp.Body)
	if cerr := resp.Body.Close(); cerr != nil && readErr == nil {
		readErr = cerr
	}
	if readErr != nil {
		return 0, fmt.Errorf("probe publish: read: %w", readErr)
	}
	if resp.StatusCode/100 != 2 {
		return 0, fmt.Errorf("probe publish: status %d: %s", resp.StatusCode, body)
	}
	var a struct {
		Seq int64 `json:"seq"`
	}
	if err := json.Unmarshal(body, &a); err != nil {
		return 0, fmt.Errorf("probe publish: ack: %w", err)
	}
	return a.Seq, nil
}
