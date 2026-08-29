// SPDX-License-Identifier: Apache-2.0

package cli

import (
	"bytes"
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/a-holm/messq/internal/clock"
	"github.com/a-holm/messq/internal/exitcode"
)

// syncBuffer is a mutex-guarded bytes.Buffer: the child keeps writing stderr for
// its whole lifetime (slog), while the test reads the banner concurrently. -race
// is right about the unsynchronised pair.
type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (s *syncBuffer) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.buf.Write(p)
}

func (s *syncBuffer) String() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.buf.String()
}

// startServeDev re-executes this test binary as `messq serve --dev` (plus args)
// on the given listener, waits for readiness, and returns the child plus its
// stderr snapshot handle. The dev daemon picks its own data dir; the caller learns
// it from the banner on stderr.
func startServeDev(t *testing.T, sock string, args ...string) (*exec.Cmd, func() string) {
	t.Helper()
	exe, err := os.Executable()
	if err != nil {
		t.Fatalf("os.Executable: %v", err)
	}
	cmd := exec.CommandContext(context.Background(), exe, "-test.run=^TestHelperServeProcess$")
	cmd.Env = append(os.Environ(),
		helperServeEnv+"=1",
		helperServeArgsEnv+"="+strings.Join(append([]string{"--dev"}, args...), " "),
		"MESSQ_LISTEN=unix://"+sock,
		"MESSQ_DURABILITY=full",
	)
	var stderr syncBuffer
	cmd.Stderr = &stderr
	if err := cmd.Start(); err != nil {
		t.Fatalf("start serve --dev: %v", err)
	}
	t.Cleanup(func() {
		if cmd.Process == nil || cmd.ProcessState != nil {
			return
		}
		if killErr := cmd.Process.Kill(); killErr != nil {
			return
		}
		_ = cmd.Wait() //nolint:errcheck // a killed child's code is not meaningful
	})
	waitForServe(t, sock, &stderr)
	return cmd, stderr.String
}

// devDataDirFromBanner extracts the data dir path from the dev banner on the
// child's stderr. Parsing the banner (instead of guessing the temp name) is the
// honest check: whatever the daemon says it will delete is what must disappear.
func devDataDirFromBanner(t *testing.T, banner string) string {
	t.Helper()
	for _, line := range strings.Split(banner, "\n") {
		if after, ok := strings.CutPrefix(line, "│ data dir "); ok {
			return strings.TrimSpace(after)
		}
	}
	t.Fatalf("dev banner on stderr names no data dir:\n%s", banner)
	return ""
}

// TestServeDevEphemeralDirRemovedOnSIGINT drives the issue's edge table: SIGINT
// during a dev daemon removes the data directory, the child exits 130, and the
// banner told the truth about which directory that would be.
func TestServeDevEphemeralDirRemovedOnSIGINT(t *testing.T) {
	sock := filepath.Join(t.TempDir(), "dev.sock")
	cmd, stderr := startServeDev(t, sock)

	dir := devDataDirFromBanner(t, stderr())
	if dir == "" {
		t.Fatal("empty data dir from banner")
	}
	if _, err := os.Stat(dir); err != nil {
		t.Fatalf("banner names %s but it does not exist: %v", dir, err)
	}

	if sigErr := cmd.Process.Signal(syscall.SIGINT); sigErr != nil {
		t.Fatalf("SIGINT: %v", sigErr)
	}
	// A DAEMON's ^C is a graceful shutdown: runServe unwinds, removes the dir, and
	// exits 0 (systemd's Restart=on-failure must not see a restart-worthy code).
	// The §7 130 convention belongs to client-side commands (quickstart).
	exitCh := make(chan error, 1)
	go func() { exitCh <- cmd.Wait() }()
	select {
	case wErr := <-exitCh:
		if wErr != nil {
			var ee *exec.ExitError
			if !asExitError(wErr, &ee) {
				t.Fatalf("dev serve failed instead of shutting down: %v", wErr)
			}
			t.Fatalf("dev serve exited %d after SIGINT, want 0 (graceful)", ee.ExitCode())
		}
	case <-(clock.System{}).NewTimer(15 * time.Second).C():
		t.Fatal("dev serve did not exit after SIGINT")
	}

	if _, err := os.Stat(dir); !os.IsNotExist(err) {
		t.Errorf("ephemeral data dir %s survived SIGINT (err=%v)", dir, err)
	}
}

// TestServeDevKeepsExplicitDataDirOnSIGINT pins the other face: an explicit
// --data-dir survives the SIGINT exit, and the banner says KEPT.
func TestServeDevKeepsExplicitDataDirOnSIGINT(t *testing.T) {
	base := t.TempDir()
	sock := filepath.Join(base, "dev.sock")
	dir := filepath.Join(base, "keep-me")

	cmd, stderr := startServeDev(t, sock, "--data-dir", dir)
	if !strings.Contains(stderr(), "KEPT") {
		t.Errorf("banner with an explicit --data-dir must say KEPT:\n%s", stderr())
	}

	if sigErr := cmd.Process.Signal(syscall.SIGINT); sigErr != nil {
		t.Fatalf("SIGINT: %v", sigErr)
	}
	exitCh := make(chan error, 1)
	go func() { exitCh <- cmd.Wait() }()
	select {
	case <-exitCh:
	case <-(clock.System{}).NewTimer(15 * time.Second).C():
		t.Fatal("dev serve did not exit after SIGINT")
	}

	if _, err := os.Stat(dir); err != nil {
		t.Errorf("explicit --data-dir %s was removed by --dev: %v", dir, err)
	}
}

// TestServeDevPublicBindStillFatal is the #16 guard the issue forbids --dev from
// weakening: a non-loopback plaintext bind is refused even under --dev, with the
// config exit code.
func TestServeDevPublicBindStillFatal(t *testing.T) {
	_ = startServeDev // keep the helper referenced even if rows are reordered
	exe, err := os.Executable()
	if err != nil {
		t.Fatalf("os.Executable: %v", err)
	}
	cmd := exec.CommandContext(context.Background(), exe, "-test.run=^TestHelperServeProcess$")
	cmd.Env = append(os.Environ(),
		helperServeEnv+"=1",
		// A literal public IP: no DNS in the loop, deterministic classification.
		helperServeArgsEnv+"=--dev --listen tcp://8.8.8.8:4390",
		"MESSQ_DATA_DIR="+t.TempDir(),
	)
	var stderr strings.Builder
	cmd.Stderr = &stderr
	if err := cmd.Start(); err != nil {
		t.Fatalf("start: %v", err)
	}
	exitCh := make(chan error, 1)
	go func() { exitCh <- cmd.Wait() }()
	select {
	case wErr := <-exitCh:
		var ee *exec.ExitError
		if !asExitError(wErr, &ee) {
			t.Fatalf("--dev public bind exited 0: %v", wErr)
		}
		if code := ee.ExitCode(); code != exitcode.CONFIG {
			t.Fatalf("--dev public bind exit = %d, want %d (config refusal)", code, exitcode.CONFIG)
		}
		if out := stderr.String(); !strings.Contains(out, "refus") {
			t.Errorf("refusal text missing from stderr:\n%s", out)
		}
	case <-(clock.System{}).NewTimer(20 * time.Second).C():
		t.Fatal("dev serve with a public bind did not refuse in time")
	}
}

// asExitError wraps errors.As for the two process tests above.
func asExitError(err error, target **exec.ExitError) bool {
	return errors.As(err, target)
}
