// SPDX-License-Identifier: Apache-2.0

package store

import (
	"context"
	"testing"
)

// Issue #27 slice 9 (events job): the event-table trim writer command enforces the two
// §4.5 bounds on the audit journal — an age retention (--event-retention, 72h default)
// and a hard row ceiling (--event-max-rows) — oldest-first and bounded per call. The
// shared fake clock supplies every timestamp.

// seedTrimEvents wipes the journal and plants n explicit rows spanning [0,n) ts,
// where row i has id=i+1 and ts=baseMS+i*1000. Explicit ids avoid colliding with
// Open's own startup event row.
func seedTrimEvents(t *testing.T, st *Store, n int, baseMS int64) {
	t.Helper()
	ctx := context.Background()
	if _, err := st.rw.ExecContext(ctx, `DELETE FROM events`); err != nil {
		t.Fatalf("clear events: %v", err)
	}
	for i := 0; i < n; i++ {
		if _, err := st.rw.ExecContext(ctx,
			`INSERT INTO events (id, ts, event, stream) VALUES (?, ?, 'stream.create', ?)`,
			i+1, baseMS+int64(i)*1000, "s"); err != nil {
			t.Fatalf("seed event %d: %v", i, err)
		}
	}
}

func countEvents(t *testing.T, st *Store) int {
	return countRows(t, st, `SELECT COUNT(*) FROM events`)
}

func TestTrimEventsDeletesOnlyExpiredOldestFirst(t *testing.T) {
	ctx := context.Background()
	st, _ := openWithStore(t)
	defer func() {
		if cerr := st.Close(ctx); cerr != nil {
			t.Logf("close store: %v", cerr)
		}
	}()

	const base = fakeStartMillis - 10_000 // rows are all in the past relative to the fake clock
	seedTrimEvents(t, st, 6, base)        // ids 1..6, ts base..base+5000; now = base+10_000

	res, err := st.TrimEvents(ctx, TrimEventsCmd{MaxAgeMs: 7500})
	if err != nil {
		t.Fatalf("TrimEvents: %v", err)
	}

	// Cutoff = now-7500 = base+2500; rows with ts < that are ids 1..3.
	if res.Deleted != 3 {
		t.Fatalf("Deleted = %d, want 3 (oldest-first age pass)", res.Deleted)
	}
	if got := countEvents(t, st); got != 3 {
		t.Fatalf("events left = %d, want 3", got)
	}
	var minTS int64
	if err := st.ro.QueryRowContext(ctx, `SELECT min(ts) FROM events`).Scan(&minTS); err != nil {
		t.Fatalf("read horizon: %v", err)
	}
	if minTS != base+3000 {
		t.Fatalf("new horizon ts = %d, want %d", minTS, base+3000)
	}
}

func TestTrimEventsCountCapKeepsNewest(t *testing.T) {
	ctx := context.Background()
	st, _ := openWithStore(t)
	defer func() {
		if cerr := st.Close(ctx); cerr != nil {
			t.Logf("close store: %v", cerr)
		}
	}()

	seedTrimEvents(t, st, 8, fakeStartMillis-60_000)

	res, err := st.TrimEvents(ctx, TrimEventsCmd{MaxRows: 5})
	if err != nil {
		t.Fatalf("TrimEvents: %v", err)
	}
	if res.Deleted != 3 {
		t.Fatalf("Deleted = %d, want 3 (excess over the row ceiling)", res.Deleted)
	}
	if got := countEvents(t, st); got != 5 {
		t.Fatalf("events left = %d, want 5", got)
	}
	// The SURVIVORS must be the NEWEST five (ids 4..8), never a mixed set.
	var minID int64
	if err := st.ro.QueryRowContext(ctx, `SELECT min(id) FROM events`).Scan(&minID); err != nil {
		t.Fatalf("read min id: %v", err)
	}
	if minID != 4 {
		t.Fatalf("min surviving id = %d, want 4", minID)
	}
}

func TestTrimEventsBatchBoundAndBothPassesTogether(t *testing.T) {
	ctx := context.Background()
	st, _ := openWithStore(t)
	defer func() {
		if cerr := st.Close(ctx); cerr != nil {
			t.Logf("close store: %v", cerr)
		}
	}()

	// Six expired rows AND six excess rows: both passes have work, but the batch
	// bound caps this slice at three — More must be true so the janitor resumes.
	seedTrimEvents(t, st, 6, fakeStartMillis-30_000)

	res, err := st.TrimEvents(ctx, TrimEventsCmd{MaxAgeMs: 1000, MaxRows: 2, Batch: 3})
	if err != nil {
		t.Fatalf("TrimEvents: %v", err)
	}
	if res.Deleted != 3 {
		t.Fatalf("Deleted = %d, want 3 (batch-capped slice)", res.Deleted)
	}
	if !res.More {
		t.Fatal("More = false, want true: two passes still hold work")
	}
	if got := countEvents(t, st); got != 3 {
		t.Fatalf("events left = %d, want 3", got)
	}

	// The next bounded slice finishes the remainder without further config churn.
	res2, err := st.TrimEvents(ctx, TrimEventsCmd{MaxAgeMs: 1000, MaxRows: 2, Batch: 50})
	if err != nil {
		t.Fatalf("TrimEvents resume: %v", err)
	}
	if res2.Deleted != 3 || res2.More {
		t.Fatalf("resume deleted %d more=%v, want exactly the leftover three with no more", res2.Deleted, res2.More)
	}
	if got := countEvents(t, st); got != 0 {
		t.Fatalf("events left after resume = %d, want 0", got)
	}
}
