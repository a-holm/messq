// SPDX-License-Identifier: Apache-2.0

package crash

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/a-holm/messq/internal/clock"
)

// TestStartKillRestartNoCleanup is the SUT lifecycle's core assertion (G4, the stale-socket
// and flock regression): start the real daemon, SIGKILL its process group, then restart on
// the SAME data dir and socket with no filesystem intervention — the stale socket file, the
// -wal/-shm files and the flock must all be handled by the daemon itself, not by the
// harness. The mutant that drops the dead-socket probe/unlink fails cycle 2 with "address
// already in use".
func TestStartKillRestartNoCleanup(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	dataDir := filepath.Join(dir, "data")
	sock := filepath.Join(dir, "messq.sock")
	cfg := Config{Durability: "full", Clock: clock.System{}}

	start := func() *SUT {
		t.Helper()
		s, err := Start(ctx, cfg, dataDir, sock)
		if err != nil {
			t.Fatalf("start serve: %v", err)
		}
		t.Cleanup(func() {
			// Leaked-SUT guard: a failed cycle must never leave a live daemon holding the
			// flock. Kill is idempotent (Wait is once), so a reaped SUT is a no-op.
			if err := s.Kill(); err != nil {
				t.Logf("cleanup kill: %v", err)
			}
		})
		if err := s.Ready(ctx); err != nil {
			t.Fatalf("serve did not become ready: %v", err)
		}
		return s
	}

	first := start()
	// SIGKILL the whole group, then prove the death was the intended signal.
	if err := first.Kill(); err != nil {
		t.Fatalf("kill serve: %v", err)
	}

	// Restart with zero cleanup: the socket file still exists on disk, the -wal may be
	// present, and the flock was released by the kernel on death. The daemon must come up.
	second := start()
	if err := second.Stop(); err != nil {
		t.Fatalf("graceful stop of the restarted daemon: %v", err)
	}
}

// TestStopCleanExit proves a graceful SIGTERM shutdown exits 0, the contrast to Kill's
// signal death — the harness uses both, and a clean cycle (for coverage) ends with Stop.
func TestStopCleanExit(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	s, err := Start(ctx, Config{Durability: "full", Clock: clock.System{}},
		filepath.Join(dir, "data"), filepath.Join(dir, "messq.sock"))
	if err != nil {
		t.Fatalf("start serve: %v", err)
	}
	t.Cleanup(func() {
		if err := s.Kill(); err != nil {
			t.Logf("cleanup kill: %v", err)
		}
	})
	if err := s.Ready(ctx); err != nil {
		t.Fatalf("ready: %v", err)
	}
	if err := s.Stop(); err != nil {
		t.Fatalf("graceful stop: %v", err)
	}
}
