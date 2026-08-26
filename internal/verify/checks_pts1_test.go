// SPDX-License-Identifier: Apache-2.0

package verify

import (
	"context"
	"fmt"
	"testing"
)

// Issue #28 slice 1: P-TS1 — within every stream, published_at is
// non-decreasing in seq. Monotonic batch stamping (#6's batchNow) is what makes
// the messages_age index seek equivalent to min(seq) for the shared time→seq
// resolution; this checker proves the assumption instead of assuming it.

// pts1BaseMillis is a fixed stamp base well before any real publish instant, so
// fixtures never depend on the wall clock.
func pts1BaseMillis() int64 { return int64(1_700_000_000_000) }

// violationsByID returns the violations of one check id from a report.
func violationsByID(t *testing.T, rep Report, id string) []Violation {
	t.Helper()
	for _, c := range rep.Checks {
		if c.ID == id {
			return c.Violations
		}
	}
	t.Fatalf("report has no %s check at all", id)
	return nil
}

func TestPTS1CleanOnMigratedDir(t *testing.T) {
	dir := migratedDir(t)
	rep := runVerify(t, dir, Options{Deep: true})
	if vs := violationsByID(t, rep, PTS1); len(vs) > 0 {
		t.Fatalf("clean dir reported P-TS1 violations: %+v", vs)
	}
}

func TestPTS1DetectsInversion(t *testing.T) {
	dir := migratedDir(t)
	db := writable(t, dir)
	ctx := context.Background()

	// A second message whose published_at sits BEFORE the first row's stamps
	// an inversion inside one stream.
	if _, err := db.ExecContext(ctx,
		`INSERT INTO messages (stream, seq, id, subject, hdr, body, size, published_at, trace_id)
		 VALUES ('orders', 2, '01INVERTED0000000000000000X', 'orders.a', NULL, x'78', 1, ?, 'tr')`,
		pts1BaseMillis()-60_000); err != nil {
		t.Fatalf("plant inverted row: %v", err)
	}
	rep := runVerify(t, dir, Options{Deep: true})
	vs := violationsByID(t, rep, PTS1)
	if len(vs) == 0 {
		t.Fatal("inverted published_at was not flagged by P-TS1")
	}
	if got := vs[0].Detail; got == "" {
		t.Fatal("P-TS1 violation detail is empty")
	}
}

func TestPTS1PerStreamScope(t *testing.T) {
	dir := migratedDir(t)
	db := writable(t, dir)
	ctx := context.Background()

	// Stream B's timestamps may sit anywhere relative to stream A's:
	// monotonicity holds per stream, never globally. Planting B entirely below
	// A must stay clean while both are internally monotone.
	base := pts1BaseMillis()
	if _, err := db.ExecContext(ctx,
		`INSERT INTO streams (name, subjects, retention, max_msgs, max_bytes, max_age_ms, max_msg_size, discard, dedup_window_ms, created_at)
		 VALUES ('b', '[">"]', 'limits', 0, 0, 604800000, 1048576, 'old', 120000, ?)`, base); err != nil {
		t.Fatalf("create stream b: %v", err)
	}
	if _, err := db.ExecContext(ctx, `INSERT INTO stream_seq (stream, next) VALUES ('b', 2)`); err != nil {
		t.Fatalf("seed b counter: %v", err)
	}
	if _, err := db.ExecContext(ctx,
		`INSERT INTO messages (stream, seq, id, subject, hdr, body, size, published_at, trace_id)
		 VALUES ('b', 1, '01BSTREAM00000000000000000Y', 'orders.a', NULL, x'78', 1, ?, 'tr')`,
		base-3_600_000); err != nil {
		t.Fatalf("plant b row: %v", err)
	}
	rep := runVerify(t, dir, Options{Deep: true})
	if vs := violationsByID(t, rep, PTS1); len(vs) > 0 {
		t.Fatalf("cross-stream timestamp spread flagged: %+v", vs)
	}
}

func TestPTS1FlagsOnlyTheOffendingRow(t *testing.T) {
	dir := migratedDir(t)
	db := writable(t, dir)
	ctx := context.Background()

	// rows: seq2 stamped well below seq1's real publish time, seq3 monotone
	// again against seq2 — exactly one violation, naming seq 2.
	rows := []struct {
		seq int64
		ts  int64
	}{
		{2, pts1BaseMillis()},
		{3, pts1BaseMillis() + 1_000},
	}
	for i, r := range rows {
		if _, err := db.ExecContext(ctx,
			fmt.Sprintf(`INSERT INTO messages (stream, seq, id, subject, hdr, body, size, published_at, trace_id)
			 VALUES ('orders', %d, '01PTS1FIXTURE%08d00000000000Z', 'orders.a', NULL, x'78', 1, ?, 'tr')`, r.seq, i),
			r.ts); err != nil {
			t.Fatalf("plant row %d: %v", r.seq, err)
		}
	}
	rep := runVerify(t, dir, Options{Deep: true})
	vs := violationsByID(t, rep, PTS1)
	if len(vs) != 1 {
		t.Fatalf("P-TS1 reported %d violations, want exactly 1 (the offending row): %+v", len(vs), vs)
	}
}
