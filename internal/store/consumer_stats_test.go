// SPDX-License-Identifier: Apache-2.0

package store

import (
	"context"
	"reflect"
	"testing"

	"github.com/a-holm/messq/internal/queue"
)

// plantDelivery inserts one well-formed delivery row directly (white-box fixture):
// READY rows have delivered_at NULL and visible_at 0; INFLIGHT rows have
// delivered_at set and visible_at strictly after it.
func plantDelivery(t *testing.T, st *Store, seq, state, attempts int64) {
	t.Helper()
	var deliveredAt, visibleAt any
	if state == 0 {
		deliveredAt, visibleAt = nil, int64(0)
	} else {
		deliveredAt, visibleAt = seq, seq+1
	}
	if _, err := st.rw.ExecContext(context.Background(), `
		INSERT INTO deliveries (stream, consumer, seq, subject, state, attempts, visible_at, generation, delivered_at)
		VALUES ('orders','worker',?,?,?,?,?,1,?)`, seq, "orders.1", state, attempts, visibleAt, deliveredAt); err != nil {
		t.Fatalf("plant delivery %d: %v", seq, err)
	}
}

func TestShrinkConvergence(t *testing.T) {
	st := newConsumerStream(t)
	ctx := context.Background()
	if _, err := st.CreateConsumer(ctx, "orders", queue.DefaultConsumerConfig("worker"), queue.StartPosition{Kind: queue.StartNew}, "test"); err != nil {
		t.Fatalf("create: %v", err)
	}
	// 3 READY (attempts=0) + 1 INFLIGHT (attempts=1).
	for i := int64(1); i <= 3; i++ {
		plantDelivery(t, st, i, 0, 0)
	}
	plantDelivery(t, st, 4, 1, 1)

	bound := int64(2)
	if _, err := st.UpdateConsumer(ctx, "orders", "worker", ConsumerPatch{MaxAckPending: &bound}, "test"); err != nil {
		t.Fatalf("shrink update: %v", err)
	}

	// Only the two highest READY∧attempts=0 rows (seqs 3,2) were dropped; the INFLIGHT
	// row survived.
	var rows []int64
	r, err := st.RO().QueryContext(ctx, `SELECT seq FROM deliveries WHERE stream='orders' AND consumer='worker' ORDER BY seq`)
	if err != nil {
		t.Fatalf("read deliveries: %v", err)
	}
	defer func() {
		if cerr := r.Close(); cerr != nil {
			t.Errorf("close deliveries rows: %v", cerr)
		}
	}()
	for r.Next() {
		var seq int64
		if scanErr := r.Scan(&seq); scanErr != nil {
			t.Fatalf("scan: %v", scanErr)
		}
		rows = append(rows, seq)
	}
	if rErr := r.Err(); rErr != nil {
		t.Fatalf("iterate deliveries: %v", rErr)
	}
	if !reflect.DeepEqual(rows, []int64{1, 4}) {
		t.Fatalf("surviving deliveries = %v, want [1 4] (dropped READY tail 3,2)", rows)
	}

	// Cursor rewound to the lowest dropped seq (2).
	var cursor int64
	if err := st.RO().QueryRowContext(ctx, `SELECT cursor_seq FROM consumers WHERE stream='orders' AND name='worker'`).Scan(&cursor); err != nil {
		t.Fatalf("read cursor: %v", err)
	}
	if cursor != 2 {
		t.Fatalf("cursor = %d, want 2 (rewound to the lowest dropped seq)", cursor)
	}
}

func TestShrinkOvershootResidue(t *testing.T) {
	st := newConsumerStream(t)
	ctx := context.Background()
	if _, err := st.CreateConsumer(ctx, "orders", queue.DefaultConsumerConfig("worker"), queue.StartPosition{Kind: queue.StartNew}, "test"); err != nil {
		t.Fatalf("create: %v", err)
	}
	// 1 READY + 3 INFLIGHT; lowering to 1 can only drop the one READY row, leaving 3
	// INFLIGHT above the bound (reported as overshoot).
	plantDelivery(t, st, 1, 0, 0)
	for i := int64(2); i <= 4; i++ {
		plantDelivery(t, st, i, 1, 1)
	}
	bound := int64(1)
	if _, err := st.UpdateConsumer(ctx, "orders", "worker", ConsumerPatch{MaxAckPending: &bound}, "test"); err != nil {
		t.Fatalf("shrink update: %v", err)
	}
	evs := consumerEvents(t, st, "orders", "worker")
	// The update event carries the overshoot.
	if len(evs) != 2 || evs[1].Detail != `{"fields":["max_ack_pending"],"overshoot":2}` {
		t.Fatalf("shrink event = %+v, want overshoot 2 in detail", evs)
	}
}

func TestConsumerInfoDerivedStats(t *testing.T) {
	st, _ := newFetchStore(t)
	publishSubjs(t, st, "orders.1", "orders.2", "orders.3")
	cfg := queue.DefaultConsumerConfig("worker")
	cfg.Filters = []string{">"}
	newFetchConsumer(t, st, "worker", cfg)
	ctx := context.Background()

	if _, err := st.Fetch(ctx, FetchReq{Stream: "orders", Consumer: "worker", Batch: 1}); err != nil {
		t.Fatalf("fetch: %v", err)
	}

	info, err := st.GetConsumer(ctx, "orders", "worker")
	if err != nil {
		t.Fatalf("GetConsumer: %v", err)
	}
	// Naive recomputation: pending=3, inflight=1 (seq 1 claimed), ready_now=2, in_backoff=0.
	if info.Pending != 3 || info.Inflight != 1 || info.ReadyNow != 2 || info.InBackoff != 0 {
		t.Fatalf("stats = pending %d inflight %d ready %d backoff %d, want 3/1/2/0",
			info.Pending, info.Inflight, info.ReadyNow, info.InBackoff)
	}
	if info.BlockedBy != "none" {
		t.Fatalf("blocked_by = %q, want none (ready work available)", info.BlockedBy)
	}
}

func TestConsumerBlockedByPrecedence(t *testing.T) {
	cases := []struct {
		name                               string
		paused                             bool
		pending, readyNow, cursorSeq, next int64
		bound                              int64
		want                               string
	}{
		{"paused", true, 0, 0, 1, 1, 1000, "paused"},
		{"flow_control", false, 1000, 5, 1, 1, 1000, "flow_control"},
		{"backoff", false, 3, 0, 1, 1, 1000, "backoff"},
		{"catching_up", false, 0, 0, 5, 100, 1000, "catching_up"},
		{"empty", false, 0, 0, 100, 100, 1000, "empty"},
		{"none", false, 3, 2, 1, 1, 1000, "none"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := consumerBlockedBy(tc.paused, tc.pending, tc.readyNow, tc.cursorSeq, tc.next, tc.bound); got != tc.want {
				t.Fatalf("consumerBlockedBy = %q, want %q", got, tc.want)
			}
		})
	}
}

// dumpTables reads every table's rows into a canonical string map, so a before/after
// comparison proves the read path has no side effects (G8).
func dumpTables(t *testing.T, st *Store) map[string][]string {
	t.Helper()
	ctx := context.Background()
	tables := []string{"streams", "stream_seq", "stream_stats", "messages", "consumers", "deliveries", "events", "meta"}
	out := map[string][]string{}
	for _, tbl := range tables {
		rows, err := st.RO().QueryContext(ctx, `SELECT * FROM `+tbl)
		if err != nil {
			t.Fatalf("dump %s: %v", tbl, err)
		}
		defer func() {
			if cerr := rows.Close(); cerr != nil {
				t.Errorf("close %s: %v", tbl, cerr)
			}
		}()
		cols, err := rows.Columns()
		if err != nil {
			t.Fatalf("columns of %s: %v", tbl, err)
		}
		vals := make([]any, len(cols))
		ptrs := make([]any, len(cols))
		for i := range vals {
			ptrs[i] = &vals[i]
		}
		for rows.Next() {
			if scanErr := rows.Scan(ptrs...); scanErr != nil {
				t.Fatalf("scan %s: %v", tbl, scanErr)
			}
			out[tbl] = append(out[tbl], stringifyRow(vals))
		}
		if rErr := rows.Err(); rErr != nil {
			t.Fatalf("iterate %s: %v", tbl, rErr)
		}
	}
	return out
}

func stringifyRow(vals []any) string {
	s := ""
	for _, v := range vals {
		switch x := v.(type) {
		case nil:
			s += "<nil>|"
		case []byte:
			s += string(x) + "|"
		default:
			s += reflect.ValueOf(x).String() + "|"
		}
	}
	return s
}

func TestConsumerReadSideEffectFree(t *testing.T) {
	st, _ := newFetchStore(t)
	publishSubjs(t, st, "orders.1", "orders.2")
	cfg := queue.DefaultConsumerConfig("worker")
	cfg.Filters = []string{">"}
	newFetchConsumer(t, st, "worker", cfg)
	ctx := context.Background()

	before := dumpTables(t, st)
	for i := 0; i < 3; i++ {
		if _, err := st.GetConsumer(ctx, "orders", "worker"); err != nil {
			t.Fatalf("GetConsumer: %v", err)
		}
		if _, err := st.ListConsumers(ctx, "orders"); err != nil {
			t.Fatalf("ListConsumers: %v", err)
		}
	}
	after := dumpTables(t, st)
	if !reflect.DeepEqual(before, after) {
		t.Fatalf("read workload changed the database:\nbefore=%v\nafter=%v", before, after)
	}
}
