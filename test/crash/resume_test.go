// SPDX-License-Identifier: Apache-2.0

package crash_test

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/a-holm/messq/internal/testutil/crash"
)

// TestResumeAfterDriverDeath exercises the resume mode across a driver boundary that was
// cleanly completed: the first sweep finishes every cycle normally (each cycle's own
// kill/restart + reconcile is contained within that run), so when the second driver opens
// the same ledger with Resume set, resumeStart reconciles the whole existing ledger against
// the recovered state and then continues from the next cycle number. Because the first sweep
// completed cleanly, the resume reconcile has no mid-flight records to adjudicate — this
// test proves the resume path itself (reconciliation of an existing ledger with no new load,
// and cycle-number continuity), not a mid-run driver death. A genuinely mid-run death — a
// driver killed between publishes within a cycle, leaving in-flight records for resume to
// classify — is a follow-up for issue #49.
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
