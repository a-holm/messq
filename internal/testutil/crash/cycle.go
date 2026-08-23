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
	"path/filepath"
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
	WALTail  bool // a non-empty -wal was observed at kill time
}

// Run drives the whole sweep: Cycles kill/restart cycles against the real binary, appending
// every intent and outcome to the external ledger under Root, reconciling after every
// restart, and returning the sweep report with the vacuity guards enforced. The ledger is
// opened once and closed here.
func Run(ctx context.Context, cfg Config) (*Report, error) {
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

	startCycle := 0
	if cfg.Resume {
		startCycle, err = resumeStart(ctx, cfg, clk)
		if err != nil {
			return nil, err
		}
	}

	results := make([]CycleResult, 0, cfg.Cycles)
	for cycle := startCycle; cycle < startCycle+cfg.Cycles; cycle++ {
		res, err := runCycle(ctx, cfg, clk, lg, cycle)
		if err != nil {
			return nil, fmt.Errorf("cycle %d: %w", cycle, err)
		}
		results = append(results, res)
	}

	// The final reconciliation pass: replay the whole ledger and load the final state once,
	// so the report carries the survivorship split and the guard values.
	recs, _, replayErr := ledger.Replay(cfg.ledgerPath())
	if replayErr != nil {
		return nil, fmt.Errorf("replay ledger for report: %w", replayErr)
	}
	state, loadErr := LoadState(ctx, cfg.dataDir())
	if loadErr != nil {
		return nil, fmt.Errorf("load state for report: %w", loadErr)
	}
	report := summarize(results, recs, state)
	if !cfg.SkipGuards {
		if g := report.Guards(); len(g) > 0 {
			// SkipSurvivorship keeps the KILL-LANDS-LOW/HIGH and WAL-TAIL guards but drops the
			// SURVIVORSHIP both-outcome requirement, which is runner-scheduling-dependent (see
			// Config.SkipSurvivorship). The real-sweep both-outcomes survivorship is the nightly
			// lane; the PR smoke asserts only the deterministic guarantees.
			if !cfg.SkipSurvivorship {
				return &report, fmt.Errorf("%d vacuity guard(s) failed:\n%s", len(g), renderGuards(g))
			}
			if g = withoutSurvivorship(g); len(g) > 0 {
				return &report, fmt.Errorf("%d vacuity guard(s) failed:\n%s", len(g), renderGuards(g))
			}
		}
	}
	return &report, nil
}

// renderGuards renders the guard violations as a bulleted list for the failure message.
func renderGuards(vs []Violation) string {
	out := ""
	for _, v := range vs {
		out += fmt.Sprintf("  %s: %s\n", v.Rule, v.Detail)
	}
	return out
}

// resumeStart folds the existing ledger into the recovered state before any new cycle: every
// OK record must survive the driver death, every FAILED record must stay absent, and nothing
// unrecorded may exist. It returns the cycle after the last committed one, where the sweep
// resumes.
func resumeStart(ctx context.Context, cfg Config, clk clock.Clock) (int, error) {
	recs, _, replayErr := ledger.Replay(cfg.ledgerPath())
	if replayErr != nil {
		return 0, fmt.Errorf("resume: replay ledger: %w", replayErr)
	}
	state, loadErr := LoadState(ctx, cfg.dataDir())
	if loadErr != nil {
		return 0, fmt.Errorf("resume: load state: %w", loadErr)
	}
	if vs := Reconcile(state, recs, cfg.Stream, 0); len(vs) > 0 {
		return 0, fmt.Errorf("resume: %d reconciliation violation(s) after driver death:\n%s", len(vs), renderViolations(vs))
	}
	maxCycle := -1
	for _, rec := range recs {
		if rec.Cycle > maxCycle {
			maxCycle = rec.Cycle
		}
	}
	return maxCycle + 1, nil
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
	walTail := walNonEmpty(cfg.dataDir())

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

	// 8-9. The §4.4 recovery contract: recovery.unclean was emitted, then reconcile the full
	// ledger against the recovered state, then a probe publish whose seq must exceed every
	// pre-crash seq (SEQ-REGRESSION).
	if !strings.Contains(sut2.Stderr(), "recovery.unclean") {
		return CycleResult{}, errors.Join(
			fmt.Errorf("restart did not emit recovery.unclean:\n%s", sut2.Stderr()),
			sut2.Kill())
	}
	state, loadErr := LoadState(ctx, cfg.dataDir())
	if loadErr != nil {
		return CycleResult{}, errors.Join(loadErr, sut2.Kill())
	}
	recs, _, replayErr := ledger.Replay(cfg.ledgerPath())
	if replayErr != nil {
		return CycleResult{}, errors.Join(replayErr, sut2.Kill())
	}
	probeSeq, probeErr := probePublish(ctx, loadgen.UnixClient(sock), cfg.Stream, cfg.Subject)
	if probeErr != nil {
		return CycleResult{}, errors.Join(probeErr, sut2.Kill())
	}
	if vs := Reconcile(state, recs, cfg.Stream, probeSeq); len(vs) > 0 {
		return CycleResult{}, errors.Join(
			fmt.Errorf("%d reconciliation violation(s):\n%s", len(vs), renderViolations(vs)),
			sut2.Kill())
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
		WALTail:  walTail,
	}, nil
}

// renderViolations renders reconciliation violations as a bulleted list.
func renderViolations(vs []Violation) string {
	out := ""
	for _, v := range vs {
		out += fmt.Sprintf("  %s %s: %s\n", v.Rule, v.Key, v.Detail)
	}
	return out
}

// walNonEmpty reports whether the data dir's -wal file is non-empty at kill time: a
// non-empty WAL proves the kill landed with durable-but-unreplayed frames, so recovery had
// real work to do.
func walNonEmpty(dataDir string) bool {
	st, err := os.Stat(filepath.Join(dataDir, "messq.db-wal"))
	if err != nil {
		return false
	}
	return st.Size() > 0
}

// sock is the Unix socket path under Root.
func (c Config) sock() string { return c.Root + "/messq.sock" }

// ensureStream creates the harness stream with a 24 h dedup window and no retention limits,
// so no message the oracle expects can ever be deleted before reconciliation. It is
// idempotent: a 201 (created) and a 200 (already exists, matching config) are both success;
// a 409 would mean the harness config drifted from the existing stream, which is a bug.
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
	if resp.StatusCode == http.StatusCreated || resp.StatusCode == http.StatusOK {
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
