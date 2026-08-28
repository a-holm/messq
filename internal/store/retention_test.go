// SPDX-License-Identifier: Apache-2.0

package store

import (
	"context"
	"testing"
	"time"

	"github.com/a-holm/messq/internal/queue"
)

// Issue #27 slice 5+6: the retention sweep writer command. Three guarantees are
// pinned here, all derived from PLAN §4.5 / SEMANTICS S15:
//
//   - G1: a message holding a delivery row is never deleted. The guard lives INSIDE
//     the deleting transaction as a NOT EXISTS over deliveries_seq, so the guarded
//     DELETE is also driven directly against a real transaction, with and without a
//     pin, as the two-way mutation anchor for this cluster.
//   - G2: pinned candidates are SKIPPED, not stalled — the sweep reaches its limits
//     even when the oldest message is stuck, and records blame for retention.blocked.
//   - workqueue mode deletes strictly below min(cursor_seq) and NOTHING when the
//     stream has no consumers (reason=no_consumers).
//
// All clocks come from openWithStore's fake clock — no wall time, no sleeps.

const fakeSize = int64(16) // body bytes published below

// publishOrders publishes n tiny messages to stream through the public path so
// stream_stats maintenance stays exactly the shipped publisher's.
func publishOrders(t *testing.T, st *Store, stream string, n int) {
	t.Helper()
	ctx := context.Background()
	for i := 0; i < n; i++ {
		ack, err := st.Publish(ctx, PublishCmd{
			Stream: stream,
			Req:    queue.PublishReq{Subject: stream + ".a", Body: make([]byte, fakeSize)},
		})
		if err != nil {
			t.Fatalf("publish %d: %v", i, err)
		}
		if ack.Stream != stream {
			t.Fatalf("publish ack stream %q, want %q", ack.Stream, stream)
		}
	}
}

// seedPin plants one READY delivery row + its owning consumer, mirroring
// delete_test.go's white-box seeding (no FK cascade in schema v1). holderCur is the
// pinned consumer's explicit cursor_seq — raw inserts default the column to 1, which
// would silently drag a workqueue floor down and mask the behavior under test.
func seedPin(t *testing.T, st *Store, stream, consumer string, seq int64, holderCur ...int64) {
	t.Helper()
	ctx := context.Background()
	cur := int64(1)
	if len(holderCur) > 0 {
		cur = holderCur[0]
	}
	if _, err := st.rw.ExecContext(ctx,
		`INSERT INTO consumers (stream, name, cursor_seq, created_at) VALUES (?, ?, ?, ?)`,
		stream, consumer, cur, fakeStartMillis); err != nil {
		t.Fatalf("seed consumer %s: %v", consumer, err)
	}
	if _, err := st.rw.ExecContext(ctx,
		`INSERT INTO deliveries (stream, consumer, seq, subject, state, visible_at, generation)
		 VALUES (?, ?, ?, ?, 0, 0, 1)`, stream, consumer, seq, stream+".a"); err != nil {
		t.Fatalf("seed delivery on seq %d: %v", seq, err)
	}
}

// statsOf reads the current maintained counter row.
type statsRow struct{ msgs, bytes, expiredSeq, expiredAt int64 }

func statsOf(t *testing.T, st *Store, stream string) statsRow {
	t.Helper()
	var s statsRow
	err := st.ro.QueryRowContext(context.Background(),
		`SELECT msgs, bytes, expired_seq, expired_at FROM stream_stats WHERE stream = ?`,
		stream).Scan(&s.msgs, &s.bytes, &s.expiredSeq, &s.expiredAt)
	if err != nil {
		t.Fatalf("read stream_stats %s: %v", stream, err)
	}
	return s
}

func limitsConfig(name string, maxMsgs, maxBytes int64) queue.StreamConfig {
	cfg := queue.DefaultConfig(name)
	cfg.Retention = queue.RetentionLimits
	cfg.MaxMsgs = maxMsgs
	cfg.MaxBytes = maxBytes
	return cfg
}

func workqueueConfig(name string) queue.StreamConfig {
	cfg := queue.DefaultConfig(name)
	cfg.Retention = queue.RetentionWorkQueue
	cfg.MaxAge = 0 // interest reaping, not age
	return cfg
}

func TestRetentionSweepSkipsPinnedOldestAndCountsBlame(t *testing.T) {
	ctx := context.Background()
	st, _ := openWithStore(t)
	defer func() {
		if cerr := st.Close(ctx); cerr != nil {
			t.Logf("close store: %v", cerr)
		}
	}()

	if _, _, err := st.CreateStream(ctx, limitsConfig("orders", 3, 0), "test"); err != nil {
		t.Fatalf("create: %v", err)
	}
	publishOrders(t, st, "orders", 5)
	seedPin(t, st, "orders", "c1", 1) // the OLDEST message is stuck mid-flight

	res, err := st.Retention(ctx, RetentionCmd{})
	if err != nil {
		t.Fatalf("Retention: %v", err)
	}

	wantFreed := 2 * fakeSize // seq 2 and seq 3, oldest-first past the pin
	if res.Deleted != 2 {
		t.Fatalf("Deleted = %d, want 2 (skip the pin, keep going)", res.Deleted)
	}
	if res.FreedBytes != wantFreed {
		t.Fatalf("FreedBytes = %d, want %d", res.FreedBytes, wantFreed)
	}
	if res.BlockedCount != 1 {
		t.Fatalf("BlockedCount = %d, want 1", res.BlockedCount)
	}
	if got := countRows(t, st, `SELECT COUNT(*) FROM messages WHERE stream='orders'`); got != 3 {
		t.Fatalf("messages left = %d, want 3", got)
	}
	s := statsOf(t, st, "orders")
	if s.msgs != 3 || s.bytes != 3*fakeSize {
		t.Fatalf("stream_stats after sweep = %+v, want msgs=3 bytes=%d", s, 3*fakeSize)
	}
	if s.expiredSeq != 3 {
		t.Fatalf("expired_seq = %d, want 3", s.expiredSeq)
	}
	if s.expiredAt != fakeStartMillis {
		t.Fatalf("expired_at = %d, want the tick's clock reading %d", s.expiredAt, fakeStartMillis)
	}
}

func TestRetentionSweepEmitsExpireEvent(t *testing.T) {
	ctx := context.Background()
	st, _ := openWithStore(t)
	defer func() {
		if cerr := st.Close(ctx); cerr != nil {
			t.Logf("close store: %v", cerr)
		}
	}()

	if _, _, err := st.CreateStream(ctx, limitsConfig("orders", 1, 0), "test"); err != nil {
		t.Fatalf("create: %v", err)
	}
	publishOrders(t, st, "orders", 3)

	if _, err := st.Retention(ctx, RetentionCmd{}); err != nil {
		t.Fatalf("Retention: %v", err)
	}
	page, err := st.Events(ctx, EventFilter{Stream: "orders"})
	if err != nil {
		t.Fatalf("Events: %v", err)
	}
	nExpire := 0
	for _, ev := range page.Events {
		if ev.Event == "retention.expire" {
			nExpire++
		}
	}
	if nExpire != 1 {
		names := make([]string, 0, len(page.Events))
		for _, ev := range page.Events {
			names = append(names, ev.Event)
		}
		t.Fatalf("retention.expire rows = %d, want 1 (events seen: %v)", nExpire, names)
	}
}

func TestRetentionSweepEmitsBlockedWhenLimitStillViolated(t *testing.T) {
	ctx := context.Background()
	st, _ := openWithStore(t)
	defer func() {
		if cerr := st.Close(ctx); cerr != nil {
			t.Logf("close store: %v", cerr)
		}
	}()

	// Byte pressure that survives the sweep because the ONLY way under the limit is
	// the huge middle message, and that one is pinned.
	total := int64(10 + 900 + 10)
	if _, _, err := st.CreateStream(ctx, limitsConfig("orders", 0, 100), "test"); err != nil {
		t.Fatalf("create: %v", err)
	}
	bodies := []int64{10, 900, 10}
	for i, sz := range bodies {
		if _, err := st.Publish(ctx, PublishCmd{
			Stream: "orders",
			Req:    queue.PublishReq{Subject: "orders.a", Body: make([]byte, sz)},
		}); err != nil {
			t.Fatalf("publish %d: %v", i, err)
		}
	}
	_ = total
	seedPin(t, st, "orders", "c-big", 2)

	res, err := st.Retention(ctx, RetentionCmd{})
	if err != nil {
		t.Fatalf("Retention: %v", err)
	}
	if res.Deleted != 2 || res.FreedBytes != 20 {
		t.Fatalf("side deletes = (%d,%d), want (2,20)", res.Deleted, res.FreedBytes)
	}
	if !stillViolates(t, st, "orders", 0, 100) {
		t.Fatal("precondition gone: stream should still exceed max_bytes after the sweep")
	}

	page, err := st.Events(ctx, EventFilter{Stream: "orders"})
	if err != nil {
		t.Fatalf("Events: %v", err)
	}
	found := false
	for _, ev := range page.Events {
		if ev.Event != "retention.blocked" {
			continue
		}
		found = true
		if ev.Consumer != "c-big" {
			t.Fatalf("blocked blame = %q, want c-big", ev.Consumer)
		}
		if got := ev.Detail["blocking_seq"]; got != float64(2) {
			t.Fatalf("blocking_seq detail = %v, want 2", got)
		}
	}
	if !found {
		t.Fatal("no retention.blocked row despite an unsatisfiable byte limit")
	}
}

// stillViolates reports whether the maintained snapshot still breaks the given
// limits — the sweep's own post-sweep projection made real by reading back stats.
func stillViolates(t *testing.T, st *Store, stream string, maxMsgs, maxBytes int64) bool {
	t.Helper()
	s := statsOf(t, st, stream)
	if maxMsgs > 0 && s.msgs > maxMsgs {
		return true
	}
	return maxBytes > 0 && s.bytes > maxBytes
}

func TestRetentionSweepNoConsumersDeletesNothing(t *testing.T) {
	ctx := context.Background()
	st, _ := openWithStore(t)
	defer func() {
		if cerr := st.Close(ctx); cerr != nil {
			t.Logf("close store: %v", cerr)
		}
	}()

	if _, _, err := st.CreateStream(ctx, workqueueConfig("jobs"), "test"); err != nil {
		t.Fatalf("create: %v", err)
	}
	publishOrders(t, st, "jobs", 4)

	res, err := st.Retention(ctx, RetentionCmd{})
	if err != nil {
		t.Fatalf("Retention: %v", err)
	}
	if res.Deleted != 0 {
		t.Fatalf("Deleted = %d, want 0: no-consumer workqueue deletes by interest, never",
			res.Deleted)
	}
	if got := countRows(t, st, `SELECT COUNT(*) FROM messages WHERE stream='jobs'`); got != 4 {
		t.Fatalf("messages left = %d, want 4", got)
	}
}

func TestRetentionSweepWorkqueueFloorBelowCursor(t *testing.T) {
	ctx := context.Background()
	st, _ := openWithStore(t)
	defer func() {
		if cerr := st.Close(ctx); cerr != nil {
			t.Logf("close store: %v", cerr)
		}
	}()

	if _, _, err := st.CreateStream(ctx, workqueueConfig("jobs"), "test"); err != nil {
		t.Fatalf("create: %v", err)
	}
	publishOrders(t, st, "jobs", 5)
	seedConsumerAtCursor(t, st, "jobs", "w1", 3) // consumed up to seq 3
	seedPin(t, st, "jobs", "slow", 1, 4)         // straggler below the floor, interest above it

	res, err := st.Retention(ctx, RetentionCmd{})
	if err != nil {
		t.Fatalf("Retention: %v", err)
	}
	if res.Deleted != 1 {
		t.Fatalf("Deleted = %d, want 1 (only seq 2: seq 1 is pinned, seq >= 3 is above the floor)",
			res.Deleted)
	}
	s := statsOf(t, st, "jobs")
	if s.expiredSeq != 2 {
		t.Fatalf("expired_seq = %d, want 2", s.expiredSeq)
	}
}

// seedConsumerAtCursor creates a workqueue consumer row whose cursor sits at cur.
func seedConsumerAtCursor(t *testing.T, st *Store, stream, consumer string, cur int64) {
	t.Helper()
	ctx := context.Background()
	ccfg := queue.ConsumerConfig{
		Name:          consumer,
		Filters:       []string{">"},
		AckWait:       time.Second,
		MaxAckPending: 128,
		Backoff:       []time.Duration{time.Second},
		DeadPolicy:    queue.DeadPolicyDrop,
	}
	start := queue.StartPosition{Kind: queue.StartFirst}
	if _, err := st.CreateConsumer(ctx, stream, ccfg, start, "test"); err != nil {
		t.Fatalf("create consumer: %v", err)
	}
	if _, err := st.rw.ExecContext(ctx,
		`UPDATE consumers SET cursor_seq = ? WHERE stream = ? AND name = ?`, cur, stream, consumer); err != nil {
		t.Fatalf("advance cursor: %v", err)
	}
}

// TestRetentionDeleteBatchGuardHoldsUnderPins drives the guarded DELETE directly and
// is THE two-way anchor for G1: a mutant that drops the NOT EXISTS clause flips this
// test red (the pinned row dies), while removing the entire delete-path instead fails
// to compile the sweep. Compiles separately; kills behaviourally.
func TestRetentionDeleteBatchGuardHoldsUnderPins(t *testing.T) {
	ctx := context.Background()
	st, _ := openWithStore(t)
	defer func() {
		if cerr := st.Close(ctx); cerr != nil {
			t.Logf("close store: %v", cerr)
		}
	}()

	if _, _, err := st.CreateStream(ctx, limitsConfig("orders", 0, 0), "test"); err != nil {
		t.Fatalf("create: %v", err)
	}
	publishOrders(t, st, "orders", 3)
	seedPin(t, st, "orders", "c1", 2)

	tx, err := st.rw.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	deleted, freed, delErr := retentionDeleteBatch(ctx, tx, "orders", []int64{1, 2, 3})
	if delErr != nil {
		if rbErr := tx.Rollback(); rbErr != nil {
			t.Fatalf("rollback after error %v: %v", delErr, rbErr)
		}
		t.Fatalf("retentionDeleteBatch: %v", delErr)
	}
	if cErr := tx.Commit(); cErr != nil {
		t.Fatalf("commit: %v", cErr)
	}

	if deleted != 2 || freed != 2*fakeSize {
		t.Fatalf("(deleted,freed) = (%d,%d), want (2,%d): the pinned seq 2 must survive",
			deleted, freed, 2*fakeSize)
	}
	if got := countRows(t, st, `SELECT COUNT(*) FROM messages WHERE stream='orders' AND seq=2`); got != 1 {
		t.Fatal("seq 2 was deleted despite holding a delivery row (G1 breach)")
	}
	if got := countRows(t, st, `SELECT COUNT(*) FROM messages WHERE stream='orders'`); got != 1 {
		t.Fatalf("remaining messages = %d, want 1 (seq 2 alone)", got)
	}
}
