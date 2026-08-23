// SPDX-License-Identifier: Apache-2.0

package crash

import (
	"context"
	"path/filepath"
	"testing"
)

// TestOneGreenCycle drives one full kill/restart cycle against the real binary: start,
// publish, SIGKILL, restart with no cleanup, probe. The cycle loop itself asserts the
// §4.4 recovery contract (recovery.unclean emitted, probe publish receives a seq), so this
// test's job is to prove the loop runs green and produces a non-vacuous number of OK
// publishes — a cycle that killed before any publish tests nothing (G7 liveness).
func TestOneGreenCycle(t *testing.T) {
	ctx := context.Background()
	cfg := Config{
		Durability: "full",
		Root:       filepath.Join(t.TempDir(), "root"),
		Publishers: 4,
		Cycles:     1,
		Seed:       42,
		Kill:       afterNOK{n: 20}, // deterministic: kill once 20 OK publishes are durable
	}
	results, err := Run(ctx, cfg)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if len(results) != 1 {
		t.Fatalf("Run returned %d results, want 1", len(results))
	}
	r := results[0]
	if r.OK < 20 {
		t.Errorf("cycle observed %d OK publishes, want >= 20 (the afterNOK kill point)", r.OK)
	}
	t.Logf("cycle result: ok=%d unknown=%d failed=%d strategy=%s",
		r.OK, r.Unknown, r.Failed, r.Strategy)
}
