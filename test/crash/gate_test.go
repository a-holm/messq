// SPDX-License-Identifier: Apache-2.0

package crash_test

import (
	"context"
	"flag"
	"path/filepath"
	"testing"
	"time"

	"github.com/a-holm/messq/internal/testutil/crash"
)

var (
	crashGate      = flag.Bool("crash.gate", false, "run the throughput measurement instead of the kill loop")
	gatePublishers = flag.Int("crash.gate.publishers", 32, "publishers for the throughput gate")
	gateSeconds    = flag.Int("crash.gate.seconds", 30, "steady-state seconds for the throughput gate")
	gateDir        = flag.String("crash.gate.dir", "", "bench dir for the gate (default: a temp dir)")
)

// TestDurableThroughput is the M2 throughput gate: publish→durable-response at 1 KiB under
// full durability, plus the raw fsync probe that makes the number interpretable. It runs
// only with -crash.gate — CI asserts only the smoke floor, never this number as evidence.
func TestDurableThroughput(t *testing.T) {
	if !*crashGate {
		t.Skip("-crash.gate not set")
	}
	root := filepath.Join(t.TempDir(), "root")
	if *gateDir != "" {
		root = filepath.Join(*gateDir, "gate")
	}
	cfg := crash.Config{
		Durability: "full",
		Root:       root,
	}
	res, err := crash.RunGate(context.Background(), cfg, *gatePublishers, 1024, time.Duration(*gateSeconds)*time.Second)
	if err != nil {
		t.Fatalf("gate: %v", err)
	}
	t.Logf("gate: %d publishers, %d-byte payloads, %s steady state", res.Publishers, res.PayloadSize, res.Duration)
	t.Logf("gate: %d messages, %.0f msg/s", res.Messages, res.MsgsPerSec)
	t.Logf("gate: publish->durable p50=%s p99=%s p99.9=%s", res.P50, res.P99, res.P999)
	t.Logf("gate: fsync probe p50=%s p99=%s (%d samples)", res.FsyncP50, res.FsyncP99, res.FsyncSamples)
}
