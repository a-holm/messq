// SPDX-License-Identifier: Apache-2.0

package verify

import (
	"context"
	"testing"
)

// TestI7CatchesSettleFenceViolation drives the registry against a store that contains
// an impossible delivery row (INFLIGHT with attempts=0) — the residue a stale-fenced
// settle write would leave. `messq verify` must report it under I7 (issue #10 §5.2).
func TestI7CatchesSettleFenceViolation(t *testing.T) {
	dir := migratedDir(t)
	// a consumer so the planted row fences against something real.
	if _, err := writable(t, dir).ExecContext(context.Background(),
		`INSERT INTO consumers (stream, name, filters, created_at) VALUES ('orders', 'worker', '["orders.a"]', 1)`); err != nil {
		t.Fatalf("seed consumer: %v", err)
	}
	if _, err := writable(t, dir).ExecContext(context.Background(),
		`INSERT INTO deliveries (stream, consumer, seq, subject, state, attempts, visible_at, generation)
		 VALUES ('orders', 'worker', 999, 'orders.a', 1, 0, 0, 1)`); err != nil {
		t.Fatalf("seed I7 violation: %v", err)
	}

	rep := runVerify(t, dir, Options{})
	if rep.Failed() {
		t.Logf("verify report (expected I7): %+v", rep)
	}
	var sawI7 bool
	for _, c := range rep.Checks {
		if c.ID == I7 && !c.OK {
			sawI7 = true
		}
	}
	if !sawI7 {
		t.Fatalf("verify did not report the seeded I7 violation: %+v", rep.Checks)
	}
}
