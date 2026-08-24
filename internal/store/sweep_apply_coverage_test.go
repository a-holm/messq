// SPDX-License-Identifier: Apache-2.0

package store

import (
	"context"
	"database/sql"
	"path/filepath"
	"testing"
	"time"
)

// The white-box SweepCmd.Apply coverage tests (issue #11): they drive the writer command
// directly against a hand-built migrated transaction so the under-covered DecideSweep
// skip arm (a generation-fence mismatch) and the orphaned-policy skip (a consumer row
// gone under an INFLIGHT delivery) are exercised deterministically — paths the public
// store API never reaches because DeleteConsumer also removes its deliveries and no
// public path bumps a consumer's generation.

// applySweepDirect runs one SweepCmd.Apply against a freshly migrated database, seeding
// the hand-written rows via seed and returning the committed-result type.
func applySweepDirect(t *testing.T, limit int, seed func(tx *sql.Tx)) SweepResult {
	t.Helper()
	const base = int64(1700005000000) // now, in unix ms
	now := time.UnixMilli(base)
	dir := t.TempDir()
	path := filepath.Join(dir, dbFileName)
	migrateFresh(t, path, newStepClock(time.UnixMilli(base)))
	db := openTestDB(t, path)
	tx, err := db.BeginTx(context.Background(), nil)
	if err != nil {
		t.Fatalf("begin apply tx: %v", err)
	}
	t.Cleanup(func() {
		if cerr := tx.Rollback(); cerr != nil {
			t.Errorf("rollback apply tx: %v", cerr)
		}
	})
	seed(tx)

	c := SweepCmd{Limit: limit, DeadSink: DropSink{}, metrics: nopSweepMetrics{}}
	res, _, applyErr := c.Apply(context.Background(), tx, now)
	if applyErr != nil {
		t.Fatalf("SweepCmd.Apply: %v", applyErr)
	}
	out, ok := res.(SweepResult)
	if !ok {
		t.Fatalf("Apply returned %T, want SweepResult", res)
	}
	return out
}

// seedDelivery writes one INFLIGHT delivery for the given (stream, consumer, seq), with
// a lease deadline well in the past (visible_at = 1700003000000 < now = 1700005000000).
func seedDelivery(t *testing.T, tx *sql.Tx, stream, consumer string, seq, generation int64) {
	t.Helper()
	if _, err := tx.ExecContext(context.Background(),
		`INSERT INTO deliveries
			(stream, consumer, seq, subject, state, attempts, visible_at, generation, delivered_at)
			VALUES (?, ?, ?, ?, 1, 1, 1700003000000, ?, 1700004000000)`,
		stream, consumer, seq, stream+".1", generation); err != nil {
		t.Fatalf("insert delivery %q/%q/%d: %v", stream, consumer, seq, err)
	}
}

// seedConsumer writes a consumer row with the given generation.
func seedConsumer(t *testing.T, tx *sql.Tx, stream, name string, generation int64, backoff string) {
	t.Helper()
	if _, err := tx.ExecContext(context.Background(),
		`INSERT INTO consumers
			(stream, name, filters, ack_wait_ms, max_deliver, max_ack_pending, backoff_ms,
			 ordered, dead_policy, cursor_seq, generation, paused, created_at)
			VALUES (?, ?, '[">"]', 30000, 5, 10, ?, 0, 'dlq', 1, ?, 0, 0)`,
		stream, name, backoff, generation); err != nil {
		t.Fatalf("insert consumer %q/%q: %v", stream, name, err)
	}
}

// TestSweepApplySkipsGenerationFence exercises the DecideSweep SweepSkip arm: an
// INFLIGHT row carrying a stale generation (delivery gen 1 vs consumer gen 2) is fenced
// and left untouched, yet still counted as a timeout and reported as Skipped (S3).
func TestSweepApplySkipsGenerationFence(t *testing.T) {
	out := applySweepDirect(t, 10, func(tx *sql.Tx) {
		seedConsumer(t, tx, "orders", "worker", 2, "[1000]")
		seedDelivery(t, tx, "orders", "worker", 1, 1) // stale generation: fence mismatch
	})
	if out.Expired != 1 {
		t.Fatalf("Expired = %d, want 1", out.Expired)
	}
	if out.Skipped != 1 || out.Redelivered != 0 || out.Dead != 0 {
		t.Fatalf("result = %+v, want 1 skipped/0 redelivered/0 dead", out)
	}
	if out.More {
		t.Fatalf("More = true on a fenced single row")
	}
	if out.NextDueMS == 0 {
		t.Fatalf("NextDueMS = 0, want the remaining fenced row's lease reported")
	}
}

// TestSweepApplySkipsWhenConsumerGone exercises the orphaned-policy arm: an INFLIGHT
// delivery whose consumer row was removed is skipped rather than mutated (the policy
// fence cannot be satisfied against a half-config).
func TestSweepApplySkipsWhenConsumerGone(t *testing.T) {
	out := applySweepDirect(t, 10, func(tx *sql.Tx) {
		seedDelivery(t, tx, "orders", "ghost", 1, 1) // no consumers row exists
	})
	if out.Expired != 1 {
		t.Fatalf("Expired = %d, want 1", out.Expired)
	}
	if out.Skipped != 1 || out.Redelivered != 0 || out.Dead != 0 {
		t.Fatalf("result = %+v, want 1 skipped/0 redelivered/0 dead", out)
	}
	if out.NextDueMS == 0 {
		t.Fatalf("NextDueMS = 0, want the orphaned row's lease reported")
	}
}

// TestSweepApplyDetectsCorruptBackoff forces loadSweepPolicy's JSON-decode failure: the
// consumer's backoff_ms holds a non-JSON blob, so the command refuses the whole batch
// rather than applying a half-decoded policy.
func TestSweepApplyDetectsCorruptBackoff(t *testing.T) {
	const base = int64(1700005000000)
	now := time.UnixMilli(base)
	dir := t.TempDir()
	path := filepath.Join(dir, dbFileName)
	migrateFresh(t, path, newStepClock(time.UnixMilli(base)))
	db := openTestDB(t, path)
	tx, err := db.BeginTx(context.Background(), nil)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	t.Cleanup(func() { _ = tx.Rollback() })
	seedConsumer(t, tx, "orders", "worker", 1, "not-json") // corrupt backoff_ms
	seedDelivery(t, tx, "orders", "worker", 1, 1)

	c := SweepCmd{Limit: 10, DeadSink: DropSink{}, metrics: nopSweepMetrics{}}
	if _, _, applyErr := c.Apply(context.Background(), tx, now); applyErr == nil {
		t.Fatal("SweepCmd.Apply succeeded on a corrupt consumer backoff, want error")
	}
}
