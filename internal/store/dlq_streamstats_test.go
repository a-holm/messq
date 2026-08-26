// SPDX-License-Identifier: Apache-2.0

package store

import (
	"context"
	"testing"
	"time"

	"github.com/a-holm/messq/internal/queue"
)

// TestDLQCopyMaintainsStreamStats pins #27 slice 2's maintenance fix: the dead-letter
// copy updates stream_stats in the SAME transaction as the INSERT, and the auto-created
// DLQ stream gets a stats row at all. Before this slice neither happened — the copy was
// invisible to the counters, which silently disabled discard=new's exact O(1) check for
// every DLQ stream and would fail verify's V7 after the first death (G3).
func TestDLQCopyMaintainsStreamStats(t *testing.T) {
	ctx := context.Background()
	st, fk, _ := openSweepStore(t)
	seedSweep(t, st, func(c *queue.ConsumerConfig) { c.MaxDeliver = 1 }, 1, 1)
	fk.Advance(30 * time.Second)

	res, err := st.Sweep(ctx, SweepCmd{Limit: 10})
	if err != nil {
		t.Fatalf("sweep: %v", err)
	}
	if res.Dead != 1 {
		t.Fatalf("result = %+v, want 1 dead", res)
	}

	rows := readDLQRows(t, st, "orders.dlq")
	if len(rows) != 1 {
		t.Fatalf("dlq has %d copies, want 1", len(rows))
	}

	// The DLQ's stats row exists and counts the copy exactly.
	var msgs, bytes int64
	err = st.RO().QueryRowContext(ctx,
		`SELECT msgs, bytes FROM stream_stats WHERE stream = 'orders.dlq'`,
	).Scan(&msgs, &bytes)
	if err != nil {
		t.Fatalf("read stream_stats of orders.dlq: %v", err)
	}
	if msgs != 1 || bytes != rows[0].size {
		t.Fatalf("dlq stats = (%d msgs, %d bytes), want (1, %d)", msgs, bytes, rows[0].size)
	}

	// The origin's counters are untouched by the copy.
	var oMsgs, oBytes, scanMsgs, scanBytes int64
	if err := st.RO().QueryRowContext(ctx,
		`SELECT msgs, bytes FROM stream_stats WHERE stream = 'orders'`,
	).Scan(&oMsgs, &oBytes); err != nil {
		t.Fatalf("read stream_stats of orders: %v", err)
	}
	if err := st.RO().QueryRowContext(ctx,
		`SELECT count(*), coalesce(sum(size),0) FROM messages WHERE stream = 'orders'`,
	).Scan(&scanMsgs, &scanBytes); err != nil {
		t.Fatalf("scan orders: %v", err)
	}
	if oMsgs != scanMsgs || oBytes != scanBytes {
		t.Fatalf("origin stats (%d,%d) drifted from scan (%d,%d)",
			oMsgs, oBytes, scanMsgs, scanBytes)
	}
}

// TestDLQSecondCopyMaintainsStreamStats pins the incremental path: a SECOND death into
// an existing DLQ adds one more row to the counters instead of resetting or skipping them.
func TestDLQSecondCopyMaintainsStreamStats(t *testing.T) {
	ctx := context.Background()
	st, fk, _ := openSweepStore(t)
	seedSweep(t, st, func(c *queue.ConsumerConfig) { c.MaxDeliver = 1 }, 2, 2)
	fk.Advance(30 * time.Second)

	// One sweep retires what its per-tick budget allows; advance-and-resweep until
	// both leases died, deterministically on the fake clock.
	dead := 0
	for i := 0; i < 8 && dead < 2; i++ {
		res, err := st.Sweep(ctx, SweepCmd{Limit: 10})
		if err != nil {
			t.Fatalf("sweep: %v", err)
		}
		dead += res.Dead
		fk.Advance(5 * time.Second)
	}
	if dead != 2 {
		t.Fatalf("deaths = %d, want 2", dead)
	}

	rows := readDLQRows(t, st, "orders.dlq")
	if len(rows) != 2 {
		t.Fatalf("dlq has %d copies, want 2", len(rows))
	}
	var wantBytes int64
	for _, r := range rows {
		wantBytes += r.size
	}
	var msgs, bytes int64
	if err := st.RO().QueryRowContext(ctx,
		`SELECT msgs, bytes FROM stream_stats WHERE stream = 'orders.dlq'`,
	).Scan(&msgs, &bytes); err != nil {
		t.Fatalf("read stream_stats of orders.dlq: %v", err)
	}
	if msgs != 2 || bytes != wantBytes {
		t.Fatalf("dlq stats = (%d msgs, %d bytes), want (2, %d)", msgs, bytes, wantBytes)
	}
}
