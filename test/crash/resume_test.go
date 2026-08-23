// SPDX-License-Identifier: Apache-2.0

package crash_test

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/a-holm/messq/internal/testutil/crash"
)

// TestResumeAfterDriverDeath proves the resume mode: a second driver opens the ledger the
// first driver left behind, reconciles every OK record against the recovered state — the
// acknowledged-loss check across the driver's death — and continues the sweep from the next
// cycle. The second run must be green, which means the first run's durable records all
// survived the driver boundary.
func TestResumeAfterDriverDeath(t *testing.T) {
	ctx := context.Background()
	root := filepath.Join(t.TempDir(), "root")

	first := crash.Config{
		Durability: "full",
		Root:       root,
		Publishers: 4,
		Cycles:     2,
		Seed:       42,
		SkipGuards: true, // the resume test proves the fold + continue, not the guard thresholds
	}
	if _, err := crash.Run(ctx, first); err != nil {
		t.Fatalf("first (pre-death) sweep: %v", err)
	}

	second := crash.Config{
		Durability: "full",
		Root:       root,
		Publishers: 4,
		Cycles:     1,
		Seed:       42,
		Resume:     true,
		SkipGuards: true,
	}
	if _, err := crash.Run(ctx, second); err != nil {
		t.Fatalf("resumed sweep: %v", err)
	}
}
