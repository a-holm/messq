// SPDX-License-Identifier: Apache-2.0

// Package crash is the crash harness: it builds and drives the real `messq serve` binary,
// SIGKILLs it by process group, and restarts it with no cleanup, so that a kill/restart
// oracle can turn M2's exit criterion into evidence. The three-valued ledger lives in
// internal/testutil/ledger; this package owns the process boundary and the cycle loop.
package crash

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"syscall"
	"time"

	"github.com/a-holm/messq/internal/clock"
)

// readinessDeadline bounds how long a start/restart may take to answer /healthz. Recovery
// runs inside store.Open before the listener exists, so a 200 is honest from the first
// request; a daemon that cannot answer within this budget has failed startup.
const readinessDeadline = 10 * time.Second

// Config carries the harness knobs that belong to the SUT lifecycle. The load-generator,
// kill-strategy and cycle fields are added by later slices alongside their tests; only what
// this slice drives is declared here.
type Config struct {
	// Durability is the serve --durability value: "full" or "relaxed".
	Durability string
	// Clock is the timing seam (readiness polling, kill timing). nil means clock.System.
	Clock clock.Clock
}

func (c *Config) clk() clock.Clock {
	if c.Clock == nil {
		return clock.System{}
	}
	return c.Clock
}

var (
	binOnce sync.Once
	binPath string
	binErr  error
)

// Bin returns the path to the SUT binary, building it exactly once per process, or taking
// $MESSQ_BIN when the environment provides a pre-built one (CI passes a cached build so the
// harness never waits for a compile in the lane). The binary is the real `messq` from
// cmd/messq, built static (CGO_ENABLED=0), never a re-exec of a test binary.
func Bin() (string, error) {
	binOnce.Do(func() {
		binPath, binErr = buildBin()
	})
	return binPath, binErr
}

// buildBin produces the static messq binary. $MESSQ_BIN short-circuits the build.
func buildBin() (string, error) {
	if env := os.Getenv("MESSQ_BIN"); env != "" {
		if _, err := os.Stat(env); err != nil { //nolint:gosec // $MESSQ_BIN is an operator/CI-provided path, never request-derived
			return "", fmt.Errorf("$MESSQ_BIN=%s: %w", env, err)
		}
		return env, nil
	}
	dir, err := os.MkdirTemp("", "messq-bin-")
	if err != nil {
		return "", fmt.Errorf("mktemp for binary: %w", err)
	}
	out := filepath.Join(dir, "messq")
	//nolint:gosec // the harness builds the very binary it then SIGKILLs; the import path is a constant and -o names a fresh temp dir
	cmd := exec.CommandContext(context.Background(), "go", "build", "-trimpath", "-o", out, "github.com/a-holm/messq/cmd/messq")
	cmd.Env = append(os.Environ(), "CGO_ENABLED=0")
	if b, err := cmd.CombinedOutput(); err != nil {
		return "", fmt.Errorf("build messq: %w\n%s", err, b)
	}
	return out, nil
}

// lockedBuffer is a goroutine-safe byte sink for a child's stdout/stderr: the daemon writes
// from its own goroutines while the harness reads the buffer for assertions and failure
// dumps.
type lockedBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *lockedBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *lockedBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

// SUT is one running `messq serve` process, started in its own process group so a kill can
// take the whole group down. It is never the test binary: it is the real daemon.
type SUT struct {
	cmd    *exec.Cmd
	bin    string
	dir    string
	sock   string
	clk    clock.Clock
	stderr *lockedBuffer
	stdout *lockedBuffer
	// waitOnce serialises the single cmd.Wait call a process permits.
	waitOnce sync.Once
	waitErr  error
}

// Start launches the real binary against dataDir/sock and returns the running process. It
// does not wait for readiness — call [SUT.Ready]. A caller that drops the SUT before
// Kill/Stop leaks a live daemon holding the data-dir flock; the cycle loop owns that
// cleanup.
func Start(ctx context.Context, cfg Config, dataDir, sock string) (*SUT, error) {
	bin, err := Bin()
	if err != nil {
		return nil, err
	}
	s := &SUT{
		bin:    bin,
		dir:    dataDir,
		sock:   sock,
		clk:    cfg.clk(),
		stderr: &lockedBuffer{},
		stdout: &lockedBuffer{},
	}
	//nolint:gosec // launching the real binary is the harness's whole purpose; every arg is operator-owned
	cmd := exec.CommandContext(ctx, bin,
		"serve",
		"--data-dir", dataDir,
		"--listen", "unix://"+sock,
		"--durability", cfg.Durability,
	)
	// Setpgid puts the daemon in its own process group, so Kill can SIGKILL the whole
	// group — no descendant can survive a cycle and hold the flock (edge case 7).
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	cmd.Stdout = s.stdout
	cmd.Stderr = s.stderr
	if err := cmd.Start(); err != nil {
		return nil, fmt.Errorf("start %s: %w", bin, err)
	}
	s.cmd = cmd
	return s, nil
}

// Ready polls GET /healthz over the Unix socket until the daemon answers 200 or the
// readiness deadline elapses. The poll sleeps through the clock seam, never time.Sleep.
// On failure the daemon's stderr is included so a startup error is not a silent timeout.
func (s *SUT) Ready(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, readinessDeadline)
	defer cancel()
	client := &http.Client{Transport: &http.Transport{
		DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
			return (&net.Dialer{}).DialContext(ctx, "unix", s.sock)
		},
	}}
	for {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://messq/healthz", nil)
		if err != nil {
			return fmt.Errorf("build healthz request: %w", err)
		}
		resp, err := client.Do(req)
		if err == nil {
			closeErr := resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				if closeErr != nil {
					return fmt.Errorf("close healthz response: %w", closeErr)
				}
				return nil
			}
		}
		if sleepErr := s.clk.Sleep(ctx, 10*time.Millisecond); sleepErr != nil {
			return fmt.Errorf("serve did not become ready on %s: %w\n--- serve stderr ---\n%s",
				s.sock, sleepErr, s.stderr.String())
		}
	}
}

// Kill SIGKILLs the daemon's whole process group and reaps it, asserting the death was a
// SIGKILL signal. An exit code here means the process died some other way — itself a bug
// worth failing on.
func (s *SUT) Kill() error {
	if s.cmd == nil || s.cmd.Process == nil {
		return nil
	}
	if err := syscall.Kill(-s.cmd.Process.Pid, syscall.SIGKILL); err != nil {
		return fmt.Errorf("SIGKILL process group %d: %w", -s.cmd.Process.Pid, err)
	}
	if err := s.Wait(); err != nil {
		var exitErr *exec.ExitError
		if errors.As(err, &exitErr) {
			if ws, ok := exitErr.Sys().(syscall.WaitStatus); ok && ws.Signaled() && ws.Signal() == syscall.SIGKILL {
				return nil // the intended death
			}
			return fmt.Errorf("serve died with exit code %d, want SIGKILL", exitErr.ExitCode())
		}
		return err
	}
	return errors.New("serve exited cleanly after SIGKILL, want a signal death")
}

// Stop asks the daemon to shut down gracefully (SIGTERM) and reaps it. A clean serve exits
// 0; any other exit is reported.
func (s *SUT) Stop() error {
	if s.cmd == nil || s.cmd.Process == nil {
		return nil
	}
	if err := s.cmd.Process.Signal(syscall.SIGTERM); err != nil {
		return fmt.Errorf("SIGTERM serve: %w", err)
	}
	return s.Wait()
}

// Wait reaps the process exactly once and caches the result.
func (s *SUT) Wait() error {
	s.waitOnce.Do(func() {
		if s.cmd == nil || s.cmd.Process == nil {
			s.waitErr = nil
			return
		}
		s.waitErr = s.cmd.Wait()
	})
	return s.waitErr
}

// Stderr returns the captured stderr so far, for failure dumps and recovery-contract
// assertions.
func (s *SUT) Stderr() string { return s.stderr.String() }
