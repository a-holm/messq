// SPDX-License-Identifier: Apache-2.0

package store

import (
	"context"
	"testing"
)

// Issue #27 owns the BACKGROUND COMPLETION of an already-authorised stream deletion
// ("stream purge/rm" stays #24/#28): a crash between the metadata transaction and the
// last message chunk leaves a reap.<name> marker + orphaned message rows. Open finishes
// those once per boot; the janitor reaper job finishes them WITHOUT a restart through
// Store.ReapResume — one bounded writer chunk per call, reporting whether markers
// remain so the job can set Result.More.

// seedInterruptedDelete plants exactly the state a crash leaves behind: the marker is
// on, the streams row is gone, the message rows are not.
func seedInterruptedDelete(t *testing.T, st *Store, name string, msgs int) {
	t.Helper()
	ctx := context.Background()
	if _, _, err := st.CreateStream(ctx, limitsConfig(name, 0, 0), "test"); err != nil {
		t.Fatalf("create %s: %v", name, err)
	}
	publishOrders(t, st, name, msgs)
	if _, err := st.rw.ExecContext(ctx,
		`DELETE FROM streams WHERE name = ?`, name); err != nil {
		t.Fatalf("drop streams row: %v", err)
	}
	if _, err := st.rw.ExecContext(ctx,
		`INSERT INTO meta (k, v) VALUES (?, '42')`, metaReapPrefix+name); err != nil {
		t.Fatalf("plant reap marker: %v", err)
	}
}

func TestReapResumeFinishesAuthorisedDeleteWithoutRestart(t *testing.T) {
	ctx := context.Background()
	st, _ := openWithStore(t)
	defer func() {
		if cerr := st.Close(ctx); cerr != nil {
			t.Logf("close store: %v", cerr)
		}
	}()

	const name = "gone"
	seedInterruptedDelete(t, st, name, 3)

	var total int64
	for i := 0; ; i++ {
		if i > 4 {
			t.Fatal("ReapResume did not converge within five calls")
		}
		res, rErr := st.ReapResume(ctx)
		if rErr != nil {
			t.Fatalf("ReapResume call %d: %v", i, rErr)
		}
		total += res.Removed
		if res.Removed == 0 && !res.Pending {
			break // no marker left and nothing deleted this round
		}
	}
	if total != 3 {
		t.Fatalf("total removed = %d, want all three message rows", total)
	}
	if got := countRows(t, st,
		`SELECT COUNT(*) FROM meta WHERE k = ?`, metaReapPrefix+name); got != 0 {
		t.Fatal("reap marker survived a completed reap")
	}
	if got := countRows(t, st,
		`SELECT COUNT(*) FROM messages WHERE stream = ?`, name); got != 0 {
		t.Fatalf("%d message rows survived a completed reap", got)
	}

	// End-state honesty: once the reap finished, the NAME must be recreatable again
	// (the create-refusal only lives while the marker does).
	if _, _, err := st.CreateStream(ctx, limitsConfig(name, 0, 0), "test"); err != nil {
		t.Fatalf("recreate after reap: %v", err)
	}
}

func TestReapResumeIdlesWhenNoMarkersExist(t *testing.T) {
	ctx := context.Background()
	st, _ := openWithStore(t)
	defer func() {
		if cerr := st.Close(ctx); cerr != nil {
			t.Logf("close store: %v", cerr)
		}
	}()

	res, err := st.ReapResume(ctx)
	if err != nil {
		t.Fatalf("ReapResume: %v", err)
	}
	if res.Pending || res.Removed != 0 {
		t.Fatalf("idle result = %+v, want zero-work not-pending", res)
	}
}
