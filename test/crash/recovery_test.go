// SPDX-License-Identifier: Apache-2.0

package crash_test

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/a-holm/messq/internal/clock"
	"github.com/a-holm/messq/internal/testutil/crash"
)

// TestRecoveryLatency measures the recovery time after a SIGKILL: the time from relaunch to
// /healthz. It must stay well under a generous bound — recovery is WAL replay, not a
// migration. The bound is deliberately loose so a loaded runner never flakes, while a real
// regression (e.g. a synchronous sweep over the whole messages table) blows straight through
// it.
func TestRecoveryLatency(t *testing.T) {
	ctx := context.Background()
	root := filepath.Join(t.TempDir(), "root")
	dataDir := filepath.Join(root, "data")
	sock := filepath.Join(root, "messq.sock")
	cfg := crash.Config{Durability: "full", Clock: clock.System{}}

	s, err := crash.Start(ctx, cfg, dataDir, sock)
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	if readyErr := s.Ready(ctx); readyErr != nil {
		t.Fatalf("ready: %v", readyErr)
	}
	if killErr := s.Kill(); killErr != nil {
		t.Fatalf("kill: %v", killErr)
	}

	clk := clock.System{}
	s2, err := crash.Start(ctx, cfg, dataDir, sock)
	if err != nil {
		t.Fatalf("restart: %v", err)
	}
	t0 := clk.Now()
	if readyErr := s2.Ready(ctx); readyErr != nil {
		t.Fatalf("recovery ready: %v", readyErr)
	}
	recovery := clk.Since(t0)
	t.Logf("recovery (SIGKILL -> /healthz): %s", recovery)
	if recovery > 10*time.Second {
		t.Fatalf("recovery took %s, want < 10s", recovery)
	}
	if err := s2.Stop(); err != nil {
		t.Fatalf("stop: %v", err)
	}
}

// TestNoLeakedProcesses proves the harness leaves no daemon behind: after a full sweep every
// `messq serve` process the harness launched is dead. It scans /proc for any process whose
// command line carries the harness's unique data-dir marker.
func TestNoLeakedProcesses(t *testing.T) {
	ctx := context.Background()
	root := filepath.Join(t.TempDir(), "root")
	cfg := crash.Config{
		Durability: "full",
		Root:       root,
		Publishers: 4,
		Cycles:     3,
		Seed:       7,
		SkipGuards: true, // the leak test only cares that every process is gone
	}
	if _, err := crash.Run(ctx, cfg); err != nil {
		t.Fatalf("sweep: %v", err)
	}
	if leaked := leakedServeProcs(root); len(leaked) > 0 {
		t.Fatalf("leaked messq serve process(es) after sweep: %v", leaked)
	}
}

// leakedServeProcs scans /proc for running processes whose command line carries marker.
func leakedServeProcs(marker string) []string {
	entries, err := os.ReadDir("/proc")
	if err != nil {
		return nil
	}
	var leaked []string
	for _, e := range entries {
		if !e.IsDir() || e.Name()[0] < '0' || e.Name()[0] > '9' {
			continue
		}
		cmdline, err := os.ReadFile(filepath.Join("/proc", e.Name(), "cmdline"))
		if err != nil {
			continue
		}
		line := strings.ReplaceAll(string(cmdline), "\x00", " ")
		if strings.Contains(line, "messq serve") && strings.Contains(line, marker) {
			leaked = append(leaked, line)
		}
	}
	return leaked
}
