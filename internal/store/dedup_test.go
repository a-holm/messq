// SPDX-License-Identifier: Apache-2.0

package store

import (
	"context"
	"strconv"
	"testing"
	"time"

	"github.com/a-holm/messq/internal/clock"
	"github.com/a-holm/messq/internal/errs"
	"github.com/a-holm/messq/internal/queue"
)

func queueDefaultWithWindow(name string, windowMS int64) queue.StreamConfig {
	cfg := queue.DefaultConfig(name)
	cfg.DedupWindow = time.Duration(windowMS) * time.Millisecond
	return cfg
}

var keyedSeq int64 // per-process; seeds only, never asserts on

// seedKeyedMessage writes one keyed message directly so expiry can be tested without
// bending the wall clock through Publish.
func seedKeyedMessage(t *testing.T, st *Store, stream, key string, publishedAt int64) {
	t.Helper()
	keyedSeq++
	if _, err := st.rw.ExecContext(context.Background(),
		`INSERT INTO messages (stream, seq, id, subject, body, size, published_at, trace_id, dedup_key)
		 VALUES (?, ?, ?, 'orders.a', x'01', 1, ?, 'trace', ?)`,
		stream, keyedSeq, "id-"+key, publishedAt, key); err != nil {
		t.Fatalf("seed keyed message %q: %v", key, err)
	}
}

func nullKeyCount(t *testing.T, st *Store) int {
	t.Helper()
	return countRows(t, st, `SELECT count(*) FROM messages WHERE dedup_key IS NOT NULL`)
}

func TestSweepDedupExpiresOnlyOldKeys(t *testing.T) {
	ctx := context.Background()
	st, _ := openWithStore(t)
	mustCreate(t, st, queueDefaultWithWindow("orders", 5_000)) // 5 s window

	old := fakeStartMillis - 60_000 // long expired
	young := fakeStartMillis        // just published per the fake clock
	seedKeyedMessage(t, st, "orders", "old-key", old)
	seedKeyedMessage(t, st, "orders", "young-key", young)

	cleared, err := st.SweepDedup(ctx, "orders")
	if err != nil {
		t.Fatalf("sweep: %v", err)
	}
	if cleared != 1 {
		t.Fatalf("sweep cleared %d keys, want 1", cleared)
	}
	if n := nullKeyCount(t, st); n != 1 {
		t.Errorf("%d keys remain, want only the young one", n)
	}
	var kept string
	if err := st.ro.QueryRowContext(ctx,
		`SELECT dedup_key FROM messages WHERE published_at = ?`, young).Scan(&kept); err != nil || kept != "young-key" {
		t.Errorf("young key = %q err=%v, want kept", kept, err)
	}
}

func TestSweepDedupIsBoundedPerCall(t *testing.T) {
	ctx := context.Background()
	st, _ := openWithStore(t)
	mustCreate(t, st, queueDefaultWithWindow("orders", 1_000))

	const total = sweepBound + 50
	tx, tErr := st.rw.BeginTx(ctx, nil)
	if tErr != nil {
		t.Fatalf("begin seed: %v", tErr)
	}
	stmt, sErr := tx.PrepareContext(ctx,
		`INSERT INTO messages (stream, seq, id, subject, body, size, published_at, trace_id, dedup_key)
		 VALUES (?, ?, ?, 'orders.a', x'01', 1, ?, 'trace', ?)`)
	if sErr != nil {
		t.Fatalf("prepare seed: %v", sErr)
	}
	defer func() {
		if cErr := stmt.Close(); cErr != nil {
			t.Logf("close seed stmt: %v", cErr)
		}
	}()
	expired := fakeStartMillis - 60_000 // well outside the 1 s window
	for i := 0; i < total; i++ {
		keyedSeq++
		key := "k" + strconv.Itoa(i)
		if _, xErr := stmt.ExecContext(ctx, "orders", keyedSeq, "id-"+key, expired, key); xErr != nil {
			t.Fatalf("seed keyed message %q: %v", key, xErr)
		}
	}
	if cErr := tx.Commit(); cErr != nil {
		t.Fatalf("commit seed: %v", cErr)
	}

	cleared, err := st.SweepDedup(ctx, "orders")
	if err != nil || cleared != sweepBound {
		t.Fatalf("first sweep = %d err=%v, want exactly %d", cleared, err, sweepBound)
	}
	if n := nullKeyCount(t, st); n != total-sweepBound {
		t.Errorf("%d keys remain after bounded sweep, want %d", n, total-sweepBound)
	}
	cleared2, err := st.SweepDedup(ctx, "orders")
	if err != nil || cleared2 != total-sweepBound {
		t.Fatalf("second sweep = %d err=%v, want %d", cleared2, err, total-sweepBound)
	}
	if n := nullKeyCount(t, st); n != 0 {
		t.Errorf("%d keys survive both sweeps, want 0", n)
	}
}

func TestSweepDedupMissingStreamIsNotFound(t *testing.T) {
	ctx := context.Background()
	st, _ := openWithStore(t)
	_, err := st.SweepDedup(ctx, "nope")
	wantErrIs(t, err, errs.ErrNotFound)
}

func TestDedupSurvivesRestartInsideWindow(t *testing.T) {
	ctx := context.Background()
	dir := testDataDir(t)
	st, _, err := Open(ctx, testOptions(dir, fakeClock(), &logCapture{}))
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	mustCreate(t, st, queueDefaultWithWindow("orders", 120_000))
	first, pErr := st.Publish(ctx, PublishCmd{
		Stream: "orders",
		Req:    queue.PublishReq{Subject: "orders.a", Body: []byte("x"), MsgID: "o-4711"},
	})
	if pErr != nil {
		t.Fatalf("publish: %v", pErr)
	}
	if cerr := st.Close(ctx); cerr != nil {
		t.Fatalf("close: %v", cerr)
	}

	// Reopen one simulated second later: still inside the 120 s window.
	reopened, _, oErr := Open(ctx, testOptions(dir,
		clock.NewFake(time.UnixMilli(fakeStartMillis+1_000)), &logCapture{}))
	if oErr != nil {
		t.Fatalf("reopen: %v", oErr)
	}
	defer func() {
		if cerr := reopened.Close(ctx); cerr != nil {
			t.Logf("close reopened: %v", cerr)
		}
	}()

	dup, pErr := reopened.Publish(ctx, PublishCmd{
		Stream: "orders",
		Req:    queue.PublishReq{Subject: "orders.a", Body: []byte("x"), MsgID: "o-4711"},
	})
	if pErr != nil {
		t.Fatalf("retry after restart: %v", pErr)
	}
	if !dup.Duplicate || dup.Seq != first.Seq || dup.ID != first.ID {
		t.Errorf("retry ack = %+v, want duplicate of %+v", dup, first)
	}
}
