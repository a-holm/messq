// SPDX-License-Identifier: Apache-2.0

package store

import (
	"context"
	"errors"
	"testing"

	"github.com/a-holm/messq/internal/errs"
	"github.com/a-holm/messq/internal/queue"
)

// seedConsumers writes consumer + delivery rows directly (white-box): delete must
// reach every table that references a stream, and the delivery tables have no FK
// cascade (schema v1), so the command deletes them explicitly.
func seedConsumers(t *testing.T, st *Store, stream string, n int) {
	t.Helper()
	ctx := context.Background()
	for i := 1; i <= n; i++ {
		if _, err := st.rw.ExecContext(ctx,
			`INSERT INTO consumers (stream, name, created_at) VALUES (?, ?, ?)`,
			stream, "c"+string(rune('0'+i)), fakeStartMillis); err != nil {
			t.Fatalf("seed consumer %d: %v", i, err)
		}
		if _, err := st.rw.ExecContext(ctx,
			`INSERT INTO deliveries (stream, consumer, seq, subject, state, visible_at, generation)
			 VALUES (?, ?, 1, 'orders.a', 0, 0, 1)`, stream, "c"+string(rune('0'+i))); err != nil {
			t.Fatalf("seed delivery %d: %v", i, err)
		}
	}
}

func countRows(t *testing.T, st *Store, query string, args ...any) int {
	t.Helper()
	var n int
	if err := st.ro.QueryRowContext(context.Background(), query, args...).Scan(&n); err != nil {
		t.Fatalf("count %s: %v", query, err)
	}
	return n
}

func TestDeleteStreamRequiresConfirm(t *testing.T) {
	ctx := context.Background()
	st, _ := openWithStore(t)
	if _, _, err := st.CreateStream(ctx, queue.DefaultConfig("orders"), "test"); err != nil {
		t.Fatalf("create: %v", err)
	}
	if _, err := st.DeleteStream(ctx, "orders", "wrong", "test"); !errors.Is(err, errs.ErrConflict) {
		t.Fatalf("wrong confirm = %v, want ErrConflict", err)
	}
	if _, err := st.DeleteStream(ctx, "orders", "", "test"); !errors.Is(err, errs.ErrConflict) {
		t.Fatalf("empty confirm = %v, want ErrConflict", err)
	}
	if _, err := st.GetStream(ctx, "orders"); err != nil {
		t.Fatalf("refused delete removed the stream: %v", err)
	}
}

func TestDeleteStreamRemovesEverythingReachable(t *testing.T) {
	ctx := context.Background()
	st, _ := openWithStore(t)
	if _, _, err := st.CreateStream(ctx, queue.DefaultConfig("orders"), "test"); err != nil {
		t.Fatalf("create: %v", err)
	}
	seedMessages(t, st, "orders", 7, 100)
	seedConsumers(t, st, "orders", 2)
	if _, err := st.rw.ExecContext(ctx, `UPDATE stream_seq SET next = 8 WHERE stream='orders'`); err != nil {
		t.Fatalf("bump seq: %v", err)
	}

	res, err := st.DeleteStream(ctx, "orders", "orders", "tester")
	if err != nil {
		t.Fatalf("delete: %v", err)
	}
	if res.Messages != 7 || res.Bytes != 700 || res.Consumers != 2 {
		t.Errorf("result = %+v, want 7 msgs / 700 bytes / 2 consumers", res)
	}
	if _, err := st.GetStream(ctx, "orders"); !errors.Is(err, errs.ErrNotFound) {
		t.Fatalf("get after delete = %v, want ErrNotFound", err)
	}
	list, lErr := st.ListStreams(ctx)
	if lErr != nil || len(list) != 0 {
		t.Errorf("list after delete = %v err=%v, want empty", list, lErr)
	}
	for _, table := range []string{"messages", "consumers", "deliveries", "stream_seq", "stream_stats"} {
		if n := countRows(t, st, `SELECT count(*) FROM `+table+` WHERE stream='orders'`); n != 0 {
			t.Errorf("%s kept %d orphan rows", table, n)
		}
	}
	if got := countRows(t, st, `SELECT count(*) FROM events WHERE event='stream.delete' AND stream='orders'`); got != 1 {
		t.Errorf("stream.delete event rows = %d, want 1", got)
	}
}

func TestRecreateResumesAboveHighWaterMark(t *testing.T) {
	ctx := context.Background()
	st, _ := openWithStore(t)
	if _, _, err := st.CreateStream(ctx, queue.DefaultConfig("orders"), "test"); err != nil {
		t.Fatalf("create: %v", err)
	}
	seedMessages(t, st, "orders", 5, 10)
	if _, err := st.rw.ExecContext(ctx, `UPDATE stream_seq SET next = 6 WHERE stream='orders'`); err != nil {
		t.Fatalf("bump seq: %v", err)
	}
	if _, err := st.DeleteStream(ctx, "orders", "orders", "test"); err != nil {
		t.Fatalf("delete: %v", err)
	}
	var hwm string
	if err := st.ro.QueryRowContext(ctx,
		`SELECT v FROM meta WHERE k='seq_hwm.orders'`).Scan(&hwm); err != nil || hwm != "5" {
		t.Fatalf("seq_hwm = %q err=%v, want \"5\"", hwm, err)
	}

	info, existed, err := st.CreateStream(ctx, queue.DefaultConfig("orders"), "test")
	if err != nil || existed {
		t.Fatalf("recreate = %v existed=%v, want fresh", err, existed)
	}
	var next int64
	if err := st.ro.QueryRowContext(ctx,
		`SELECT next FROM stream_seq WHERE stream='orders'`).Scan(&next); err != nil {
		t.Fatalf("recreated seq: %v", err)
	}
	if next != 6 { // hwm 5 + 1
		t.Errorf("recreated stream_seq.next = %d, want 6", next)
	}
	if info.FirstSeq != 0 || info.LastSeq != 5 { // numbering continuity is visible (issue §5)
		t.Errorf("recreated info seq fields = %d/%d, want 0/5", info.FirstSeq, info.LastSeq)
	}
}

func TestDeleteStreamMissingIsNotFound(t *testing.T) {
	ctx := context.Background()
	st, _ := openWithStore(t)
	if _, err := st.DeleteStream(ctx, "nope", "nope", "test"); !errors.Is(err, errs.ErrNotFound) {
		t.Fatalf("missing delete = %v, want ErrNotFound", err)
	}
}
