// SPDX-License-Identifier: Apache-2.0

package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/a-holm/messq/internal/clock"
	"github.com/a-holm/messq/internal/queue"
	"pgregory.net/rapid"
)

// Issue #28 slice 1: the time→seq resolution shared by consumer creation, seek
// and replay. The grammar itself is tested in internal/queue; here the store
// resolution semantics are pinned: the same-millisecond tie-break, the
// inclusive boundary, relative offsets against the daemon (writer) clock, and
// clamp reporting through ConsumerCreateResult.Warnings.

// publishSameInstant publishes n messages without advancing the clock, so all
// of them share one published_at millisecond — the group-commit shape the
// tie-break exists for.
func publishSameInstant(t *testing.T, st *Store, n int) int64 {
	t.Helper()
	ctx := context.Background()
	for i := 1; i <= n; i++ {
		if _, err := st.Publish(ctx, PublishCmd{
			Stream: "orders",
			Req:    queue.PublishReq{Subject: fmt.Sprintf("orders.%d", i), Body: []byte("x")},
		}); err != nil {
			t.Fatalf("publish %d: %v", i, err)
		}
	}
	return fakeStartMillis
}

// resolveStart runs resolveStartPosition on a throwaway transaction.
func resolveStart(t *testing.T, st *Store, start queue.StartPosition, now time.Time) (cursor int64, clamped bool) {
	t.Helper()
	ctx := context.Background()
	conn, err := st.rw.Conn(ctx)
	if err != nil {
		t.Fatalf("conn: %v", err)
	}
	defer func() {
		if cerr := conn.Close(); cerr != nil {
			t.Errorf("close conn: %v", cerr)
		}
	}()
	tx, txErr := conn.BeginTx(ctx, nil)
	if txErr != nil {
		t.Fatalf("begin: %v", txErr)
	}
	defer func() {
		if rbErr := tx.Rollback(); rbErr != nil && !errors.Is(rbErr, sql.ErrTxDone) {
			t.Errorf("rollback: %v", rbErr)
		}
	}()
	cursor, clamped, err = resolveStartPosition(ctx, tx, "orders", start, now)
	if err != nil {
		t.Fatalf("resolveStartPosition(%+v): %v", start, err)
	}
	return cursor, clamped
}

// deleteSeqs removes a live prefix so first_seq moves past 1 — the
// retention-trimmed shape the below-floor clamp reports against.
func deleteSeqs(t *testing.T, st *Store, upTo int64) {
	t.Helper()
	ctx := context.Background()
	if _, err := st.rw.ExecContext(ctx, `DELETE FROM messages WHERE stream = ? AND seq <= ?`, "orders", upTo); err != nil {
		t.Fatalf("delete prefix: %v", err)
	}
}

func TestTimeStartSameMillisBatchResolvesMinSeq(t *testing.T) {
	st := newConsumerStream(t)

	stamp := publishSameInstant(t, st, 5) // five rows, one published_at

	// The whole batch sits at or after its own stamp; min(seq) must win over
	// any arbitrary member of the same-millisecond group.
	cursor, _ := resolveStart(t, st, queue.StartPosition{Kind: queue.StartTime, Time: stamp}, fakeClock().Now())
	if cursor != 1 {
		t.Fatalf("same-millis batch resolved to seq %d, want min(seq) = 1", cursor)
	}

	// One millisecond after the stamp no message qualifies: head.
	cursor, _ = resolveStart(t, st, queue.StartPosition{Kind: queue.StartTime, Time: stamp + 1}, fakeClock().Now())
	if cursor != 6 {
		t.Fatalf("probe past the batch resolved to %d, want stream_seq.next = 6", cursor)
	}
}

func TestTimeStartInclusiveBoundary(t *testing.T) {
	st := newConsumerStream(t)
	ctx := context.Background()

	// msg 1 at t0, msg 2 at t0+1s.
	for i := 1; i <= 2; i++ {
		if _, err := st.Publish(ctx, PublishCmd{
			Stream: "orders",
			Req:    queue.PublishReq{Subject: "orders.a", Body: []byte("x")},
		}); err != nil {
			t.Fatalf("publish %d: %v", i, err)
		}
		fk, fOk := st.clk.(*clock.Fake)
		if !fOk {
			t.Fatal("store clock is not *clock.Fake")
		}
		fk.Advance(time.Second)
	}

	cases := []struct {
		name  string
		probe int64
		want  int64
	}{
		{"exactly at t0 is inclusive", fakeStartMillis, 1},
		{"one under", fakeStartMillis - 1, 1},
		{"between stamps", fakeStartMillis + 999, 2},
		{"exactly at t1", fakeStartMillis + 1000, 2},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cursor, _ := resolveStart(t, st, queue.StartPosition{Kind: queue.StartTime, Time: tc.probe}, fakeClock().Now())
			if cursor != tc.want {
				t.Fatalf("resolve(published_at >= %d) = %d, want %d", tc.probe, cursor, tc.want)
			}
		})
	}
}

func TestRelativeStartResolvesAgainstDaemonClock(t *testing.T) {
	st := newConsumerStream(t)
	ctx := context.Background()

	// msg 1 at t0. Two hours later, msg 2. A client asking for time:-1h means
	// one hour before THE DAEMON's now — t0+1h — which lands on msg 2.
	if _, err := st.Publish(ctx, PublishCmd{
		Stream: "orders",
		Req:    queue.PublishReq{Subject: "orders.a", Body: []byte("x")},
	}); err != nil {
		t.Fatalf("publish 1: %v", err)
	}
	fk, ok := st.clk.(*clock.Fake)
	if !ok {
		t.Fatal("store clock is not *clock.Fake")
	}
	fk.Advance(2 * time.Hour)
	if _, err := st.Publish(ctx, PublishCmd{
		Stream: "orders",
		Req:    queue.PublishReq{Subject: "orders.b", Body: []byte("y")},
	}); err != nil {
		t.Fatalf("publish 2: %v", err)
	}

	sp, err := queue.ParseStartPosition("time:-1h")
	if err != nil {
		t.Fatalf("parse time:-1h: %v", err)
	}
	res, err := st.CreateConsumer(ctx, "orders", queue.DefaultConsumerConfig("billing"), sp, "test")
	if err != nil {
		t.Fatalf("CreateConsumer(time:-1h): %v", err)
	}
	if res.Info.CursorSeq != 2 {
		t.Fatalf("time:-1h at now=t0+2h resolved to cursor %d, want 2", res.Info.CursorSeq)
	}

	// The recorded start is the wire spelling, and a re-POST of the SAME
	// relative form stays idempotent (the rendering is byte-compared).
	replay, err := st.CreateConsumer(ctx, "orders", queue.DefaultConsumerConfig("billing"), sp, "test")
	if err != nil {
		t.Fatalf("idempotent re-POST: %v", err)
	}
	if replay.Created || replay.Updated {
		t.Fatalf("re-POST with the same relative form reported created=%v updated=%v, want both false", replay.Created, replay.Updated)
	}
}

func TestStartClampReporting(t *testing.T) {
	t.Run("seq below floor on a live stream warns", func(t *testing.T) {
		st := newConsumerStream(t)
		publishSameInstant(t, st, 3)
		deleteSeqs(t, st, 1) // first_seq is now 2

		sp, err := queue.ParseStartPosition("seq:1")
		if err != nil {
			t.Fatalf("parse: %v", err)
		}
		res, err := st.CreateConsumer(context.Background(), "orders", queue.DefaultConsumerConfig("c"), sp, "test")
		if err != nil {
			t.Fatalf("CreateConsumer: %v", err)
		}
		if res.Info.CursorSeq != 2 {
			t.Fatalf("cursor = %d, want clamped first_seq 2", res.Info.CursorSeq)
		}
		assertClampWarning(t, res.Warnings, `seq:1`)
	})

	t.Run("time before the oldest retained message warns", func(t *testing.T) {
		st := newConsumerStream(t)
		publishSameInstant(t, st, 3)

		sp, err := queue.ParseStartPosition(fmt.Sprintf("time:%d", fakeStartMillis-3600_000))
		if err != nil {
			t.Fatalf("parse: %v", err)
		}
		res, err := st.CreateConsumer(context.Background(), "orders", queue.DefaultConsumerConfig("c"), sp, "test")
		if err != nil {
			t.Fatalf("CreateConsumer: %v", err)
		}
		if res.Info.CursorSeq != 1 {
			t.Fatalf("cursor = %d, want 1", res.Info.CursorSeq)
		}
		assertClampWarning(t, res.Warnings, sp.String())
	})

	t.Run("in-range anchors never warn", func(t *testing.T) {
		st := newConsumerStream(t)
		publishSameInstant(t, st, 3)
		ctx := context.Background()

		for _, form := range []string{"first", "start", "new", "seq:2", fmt.Sprintf("time:%d", fakeStartMillis)} {
			sp, pErr := queue.ParseStartPosition(form)
			if pErr != nil {
				t.Fatalf("parse %q: %v", form, pErr)
			}
			name := "c-" + strings.ReplaceAll(strings.ReplaceAll(form, ":", "-"), "/", "_")
			res, cErr := st.CreateConsumer(ctx, "orders", queue.DefaultConsumerConfig(name), sp, "test")
			if cErr != nil {
				t.Fatalf("CreateConsumer(%q): %v", form, cErr)
			}
			if len(res.Warnings) > 0 {
				t.Fatalf("start %q produced warnings %v, want none", form, res.Warnings)
			}
		}
	})

	t.Run("empty stream resolves to next without warning", func(t *testing.T) {
		st := newConsumerStream(t)

		cursor, clamped := resolveStart(t, st, queue.StartPosition{Kind: queue.StartTime, Time: fakeStartMillis}, fakeClock().Now())
		if cursor != 1 || clamped {
			t.Fatalf("empty-stream time start = (%d, %v), want (1, false)", cursor, clamped)
		}
		cursor, clamped = resolveStart(t, st, queue.StartPosition{Kind: queue.StartSeq, Seq: 99}, fakeClock().Now())
		if cursor != 1 || clamped {
			t.Fatalf("empty-stream seq start = (%d, %v), want (1, false)", cursor, clamped)
		}
	})
}

func assertClampWarning(t *testing.T, ws queue.Warnings, requested string) {
	t.Helper()
	for _, w := range ws {
		if w.Code == queue.WarningStartClamped {
			if w.Message == "" {
				t.Fatalf("clamp warning carries an empty message (requested %q)", requested)
			}
			return
		}
	}
	t.Fatalf("no %q warning in %v (requested %q)", queue.WarningStartClamped, ws, requested)
}

// TestResolveMatchesBruteForceProperty pins resolveTimeStart to its definition:
// resolve(t) = min{seq | published_at >= t}, else stream_seq.next — over random
// layouts that include ties (same-millisecond groups). The layout is planted
// straight into the messages table inside one transaction per draw, so the
// property costs no writer round-trips.
func TestResolveMatchesBruteForceProperty(t *testing.T) {
	st := newConsumerStream(t)
	ctx := context.Background()
	now := fakeClock().Now()

	rapid.Check(t, func(rt *rapid.T) {
		k := rapid.IntRange(0, 24).Draw(rt, "k")
		pub := make([]int64, k)
		at := fakeStartMillis - 1000
		for i := range pub {
			at += rapid.Int64Range(0, 900).Draw(rt, "gap") // ties are the point
			pub[i] = at
		}
		probe := rapid.Int64Range(fakeStartMillis-2000, at+2000).Draw(rt, "probe")

		runPlanted := func(fn func(tx *sql.Tx)) {
			conn, err := st.rw.Conn(ctx)
			if err != nil {
				rt.Fatalf("conn: %v", err)
			}
			defer func() {
				if cerr := conn.Close(); cerr != nil {
					rt.Errorf("close conn: %v", cerr)
				}
			}()
			tx, txErr := conn.BeginTx(ctx, nil)
			if txErr != nil {
				rt.Fatalf("begin: %v", txErr)
			}
			defer func() {
				if rbErr := tx.Rollback(); rbErr != nil && !errors.Is(rbErr, sql.ErrTxDone) {
					rt.Errorf("rollback: %v", rbErr)
				}
			}()
			if _, dErr := tx.ExecContext(ctx, `DELETE FROM messages WHERE stream = ?`, "orders"); dErr != nil {
				rt.Fatalf("clear messages: %v", dErr)
			}
			for i, ts := range pub {
				if _, iErr := tx.ExecContext(ctx,
					`INSERT INTO messages (stream, seq, id, subject, hdr, body, size, published_at, trace_id)
					 VALUES (?, ?, ?, ?, NULL, ?, ?, ?, ?)`,
					"orders", int64(i+1), fmt.Sprintf("prop-seed-%06d-%04d", probe%1000000, i+1),
					"orders.a", []byte("x"), int64(1), ts, "tr"); iErr != nil {
					rt.Fatalf("plant row %d: %v", i+1, iErr)
				}
			}
			if _, sErr := tx.ExecContext(ctx,
				`UPDATE stream_seq SET next = ? WHERE stream = ?`, int64(k+1), "orders"); sErr != nil {
				rt.Fatalf("set next: %v", sErr)
			}
			fn(tx)
		}

		runPlanted(func(tx *sql.Tx) {
			got, clamped, err := resolveStartPosition(ctx, tx, "orders", queue.StartPosition{Kind: queue.StartTime, Time: probe}, now)
			if err != nil {
				rt.Fatalf("resolve: %v", err)
			}
			want := int64(k + 1)
			wantClamped := false
			for i, ts := range pub {
				if ts >= probe {
					want = int64(i + 1)
					break
				}
			}
			if k > 0 && probe < pub[0] {
				wantClamped = true
			}
			if got != want {
				rt.Fatalf("resolve(probe=%d) over pubs=%v = %d, brute force says %d", probe, pub, got, want)
			}
			if clamped != wantClamped {
				rt.Fatalf("clamped = %v, want %v (probe=%d, pubs=%v)", clamped, wantClamped, probe, pub)
			}
		})
	})
}
