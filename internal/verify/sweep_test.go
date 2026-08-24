// SPDX-License-Identifier: Apache-2.0

package verify

import (
	"context"
	"testing"
)

// The sweeper invariant checkers (issue #11 §13): a seeded S2 violation (a READY row
// stranded above max_deliver that the retire pass never collected) and a seeded S3
// violation (a stale-generation delivery row) must both surface through `messq verify`;
// S1 reports an expired-but-unswept INFLIGHT row as advisory.

// TestS2CatchesStrandedRowAboveMaxDeliver seeds the exact residue a lowered max_deliver
// leaves when the retire pass has not run: a READY row at attempts > max_deliver. The
// registry must report it under S2 (issue #11 §13 query verbatim).
func TestS2CatchesStrandedRowAboveMaxDeliver(t *testing.T) {
	dir := migratedDir(t)
	// consumer with max_deliver=1; delivery READY (state=0) at attempts=2 — stranded.
	if _, err := writable(t, dir).ExecContext(context.Background(),
		`INSERT INTO consumers (stream, name, filters, max_deliver, created_at)
		 VALUES ('orders', 'worker', '["orders.a"]', 1, 1)`); err != nil {
		t.Fatalf("seed consumer: %v", err)
	}
	if _, err := writable(t, dir).ExecContext(context.Background(),
		`INSERT INTO deliveries (stream, consumer, seq, subject, state, attempts, visible_at, generation)
		 VALUES ('orders', 'worker', 999, 'orders.a', 0, 2, 1, 1)`); err != nil {
		t.Fatalf("seed S2 violation: %v", err)
	}

	rep := runVerify(t, dir, Options{})
	var sawS2 bool
	for _, c := range rep.Checks {
		if c.ID == S2 && !c.OK {
			sawS2 = true
		}
	}
	if !sawS2 {
		t.Fatalf("verify did not report the seeded S2 violation: %+v", rep.Checks)
	}
}

// TestS3CatchesStaleGenerationDeliveryRow seeds a delivery whose generation no longer
// matches its consumer's — the residue the sweep refuses to mutate (rule 2, the belt).
func TestS3CatchesStaleGenerationDeliveryRow(t *testing.T) {
	dir := migratedDir(t)
	if _, err := writable(t, dir).ExecContext(context.Background(),
		`INSERT INTO consumers (stream, name, filters, generation, created_at)
		 VALUES ('orders', 'worker', '["orders.a"]', 5, 1)`); err != nil {
		t.Fatalf("seed consumer: %v", err)
	}
	if _, err := writable(t, dir).ExecContext(context.Background(),
		`INSERT INTO deliveries (stream, consumer, seq, subject, state, attempts, visible_at, generation)
		 VALUES ('orders', 'worker', 999, 'orders.a', 1, 1, 1, 1)`); err != nil {
		t.Fatalf("seed S3 violation: %v", err)
	}

	rep := runVerify(t, dir, Options{})
	var sawS3 bool
	for _, c := range rep.Checks {
		if c.ID == S3 && !c.OK {
			sawS3 = true
		}
	}
	if !sawS3 {
		t.Fatalf("verify did not report the seeded S3 violation: %+v", rep.Checks)
	}
}

// TestS1ReportsExpiredInflightAsAdvisory seeds an INFLIGHT row whose visible_at is in
// the past relative to the newest event — expired but not swept. S1 must report it
// (advisory liveness, not a hard failure — but the finding must be visible).
func TestS1ReportsExpiredInflightAsAdvisory(t *testing.T) {
	dir := migratedDir(t)
	if _, err := writable(t, dir).ExecContext(context.Background(),
		`INSERT INTO consumers (stream, name, filters, created_at)
		 VALUES ('orders', 'worker', '["orders.a"]', 1)`); err != nil {
		t.Fatalf("seed consumer: %v", err)
	}
	// Insert an event with a far-future ts so the now-proxy (MAX events.ts) is past the
	// row's visible_at.
	if _, err := writable(t, dir).ExecContext(context.Background(),
		`INSERT INTO events (ts, event) VALUES (9999999, 'probe')`); err != nil {
		t.Fatalf("seed future event: %v", err)
	}
	if _, err := writable(t, dir).ExecContext(context.Background(),
		`INSERT INTO deliveries (stream, consumer, seq, subject, state, attempts, visible_at, generation)
		 VALUES ('orders', 'worker', 999, 'orders.a', 1, 1, 1, 1)`); err != nil {
		t.Fatalf("seed S1 finding: %v", err)
	}

	rep := runVerify(t, dir, Options{})
	var sawS1 bool
	for _, c := range rep.Checks {
		if c.ID == S1 && !c.OK {
			sawS1 = true
		}
	}
	if !sawS1 {
		t.Fatalf("verify did not report the expired-but-unswept INFLIGHT row under S1: %+v", rep.Checks)
	}
	_ = context.Background()
}
