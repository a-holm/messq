// SPDX-License-Identifier: Apache-2.0

package crash

import (
	"context"
	"path/filepath"
	"testing"
)

// TestOneGreenCycle drives kill/restart cycles against the real binary: start, publish,
// SIGKILL, restart with no cleanup, reconcile, probe. The cycle loop itself asserts the
// §4.4 recovery contract (recovery.unclean emitted, probe seq regression) and the seven
// reconciliation rules, so this test's job is to prove a sweep runs green with a
// non-vacuous load — enough OK publishes that the liveness and survivorship guards hold.
func TestOneGreenCycle(t *testing.T) {
	ctx := context.Background()
	cfg := Config{
		Durability: "full",
		Root:       filepath.Join(t.TempDir(), "root"),
		Publishers: 4,
		Cycles:     2,
		Seed:       42,
		Kill:       AfterNOK{N: 60}, // deterministic: kill once 60 OK publishes are durable
	}
	report, err := Run(ctx, cfg)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if report.Cycles != 2 {
		t.Fatalf("report.Cycles = %d, want 2", report.Cycles)
	}
	if report.OK < 120 {
		t.Errorf("sweep observed %d OK publishes, want >= 120 (2 × afterNOK(60))", report.OK)
	}
	t.Logf("report: ok=%d unknown=%d failed=%d present=%d absent=%d wal_tail=%t",
		report.OK, report.Unknown, report.Failed,
		report.UnknownPresent, report.UnknownAbsent, report.WALTailObserved)
}
