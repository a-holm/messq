// SPDX-License-Identifier: Apache-2.0

package store

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/a-holm/messq/internal/clock"
	"github.com/a-holm/messq/internal/errs"
	"github.com/a-holm/messq/internal/queue"
)

// eventRow is one events-table row as the consumer tests read it back.
type eventRow struct {
	Event    string
	Consumer string
	Detail   string
}

// consumerEvents reads every event row for one consumer in commit order.
func consumerEvents(t *testing.T, st *Store, stream, consumer string) []eventRow {
	t.Helper()
	ctx := context.Background()
	rows, err := st.RO().QueryContext(ctx,
		`SELECT event, coalesce(consumer, ''), coalesce(detail, '') FROM events WHERE stream = ? AND consumer = ? ORDER BY id`, stream, consumer)
	if err != nil {
		t.Fatalf("read events: %v", err)
	}
	defer func() {
		if cerr := rows.Close(); cerr != nil {
			t.Errorf("close events rows: %v", cerr)
		}
	}()
	var out []eventRow
	for rows.Next() {
		var e eventRow
		if scanErr := rows.Scan(&e.Event, &e.Consumer, &e.Detail); scanErr != nil {
			t.Fatalf("scan event: %v", scanErr)
		}
		out = append(out, e)
	}
	if rErr := rows.Err(); rErr != nil {
		t.Fatalf("iterate events: %v", rErr)
	}
	return out
}

// newConsumerStream opens an engine-less store with one stream "orders" created.
func newConsumerStream(t *testing.T) *Store {
	t.Helper()
	st := openCommandPathStore(t, fakeClock())
	if _, _, err := st.CreateStream(context.Background(), queue.DefaultConfig("orders"), "test"); err != nil {
		t.Fatalf("create stream: %v", err)
	}
	return st
}

// publishN publishes n messages with subjects orders.1..orders.n, advancing the clock
// one second between each so published_at is spread.
func publishN(t *testing.T, st *Store, n int) {
	t.Helper()
	ctx := context.Background()
	fk, ok := st.clk.(*clock.Fake)
	if !ok {
		t.Fatal("store clock is not *clock.Fake")
	}
	for i := 1; i <= n; i++ {
		if _, err := st.Publish(ctx, PublishCmd{
			Stream: "orders",
			Req:    queue.PublishReq{Subject: "orders.1", Body: []byte("x")},
		}); err != nil {
			t.Fatalf("publish %d: %v", i, err)
		}
		fk.Advance(time.Second)
	}
}

func TestCreateConsumerIdempotentAndUpdate(t *testing.T) {
	st := newConsumerStream(t)
	ctx := context.Background()

	cfg := queue.DefaultConsumerConfig("worker")
	cfg.Filters = []string{"orders.>"}
	res, err := st.CreateConsumer(ctx, "orders", cfg, queue.StartPosition{Kind: queue.StartNew}, "test")
	if err != nil {
		t.Fatalf("CreateConsumer: %v", err)
	}
	if !res.Created || res.Updated {
		t.Fatalf("CreateConsumer = created %v updated %v, want created", res.Created, res.Updated)
	}
	if res.Info.CursorSeq != 1 || res.Info.Generation != 1 {
		t.Fatalf("fresh consumer cursor/generation = %d/%d, want 1/1", res.Info.CursorSeq, res.Info.Generation)
	}
	if evs := consumerEvents(t, st, "orders", "worker"); len(evs) != 1 || evs[0].Event != "consumer.create" {
		t.Fatalf("events = %+v, want one consumer.create", evs)
	}

	// Identical re-create: no write, no event.
	res2, err := st.CreateConsumer(ctx, "orders", cfg, queue.StartPosition{Kind: queue.StartNew}, "test")
	if err != nil {
		t.Fatalf("idempotent CreateConsumer: %v", err)
	}
	if res2.Created || res2.Updated {
		t.Fatalf("idempotent re-create = created %v updated %v, want neither", res2.Created, res2.Updated)
	}
	if evs := consumerEvents(t, st, "orders", "worker"); len(evs) != 1 {
		t.Fatalf("idempotent re-create wrote %d events, want 1", len(evs))
	}

	// Differing config: applied as an update naming only the changed field.
	cfg2 := cfg
	cfg2.AckWait = 60 * time.Second
	res3, err := st.CreateConsumer(ctx, "orders", cfg2, queue.StartPosition{Kind: queue.StartNew}, "test")
	if err != nil {
		t.Fatalf("update CreateConsumer: %v", err)
	}
	if res3.Created || !res3.Updated {
		t.Fatalf("update = created %v updated %v, want updated", res3.Created, res3.Updated)
	}
	evs := consumerEvents(t, st, "orders", "worker")
	if len(evs) != 2 || evs[1].Event != "consumer.update" || evs[1].Detail != `{"fields":["ack_wait"]}` {
		t.Fatalf("update events = %+v, want a consumer.update with only ack_wait", evs)
	}
	if got := res3.Info.AckWaitMS; got != 60000 {
		t.Fatalf("updated ack_wait_ms = %d, want 60000", got)
	}
}

func TestCreateConsumerStartPositions(t *testing.T) {
	st := newConsumerStream(t)
	publishN(t, st, 5) // seqs 1..5, next=6
	ctx := context.Background()

	mk := func(name string, start queue.StartPosition) (int64, error) {
		cfg := queue.DefaultConsumerConfig(name)
		res, err := st.CreateConsumer(ctx, "orders", cfg, start, "test")
		if err != nil {
			return 0, err
		}
		return res.Info.CursorSeq, nil
	}
	cases := []struct {
		name  string
		start queue.StartPosition
		want  int64
	}{
		{"first", queue.StartPosition{Kind: queue.StartFirst}, 1},
		{"new", queue.StartPosition{Kind: queue.StartNew}, 6},
		{"seq3", queue.StartPosition{Kind: queue.StartSeq, Seq: 3}, 3},
		{"seqbelow", queue.StartPosition{Kind: queue.StartSeq, Seq: 0}, 1},
		{"seqabove", queue.StartPosition{Kind: queue.StartSeq, Seq: 999}, 6},
		{"timebefore", queue.StartPosition{Kind: queue.StartTime, Time: fakeStartMillis - 1000}, 1},
		{"timemid", queue.StartPosition{Kind: queue.StartTime, Time: fakeStartMillis + 2000}, 3},
		{"timeafter", queue.StartPosition{Kind: queue.StartTime, Time: fakeStartMillis + 10000}, 6},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := mk(tc.name, tc.start)
			if err != nil {
				t.Fatalf("create with %s: %v", tc.start.String(), err)
			}
			if got != tc.want {
				t.Fatalf("cursor_seq = %d, want %d", got, tc.want)
			}
		})
	}
}

func TestCreateConsumerStartImmutable(t *testing.T) {
	st := newConsumerStream(t)
	publishN(t, st, 5)
	ctx := context.Background()

	cfg := queue.DefaultConsumerConfig("worker")
	if _, err := st.CreateConsumer(ctx, "orders", cfg, queue.StartPosition{Kind: queue.StartSeq, Seq: 2}, "test"); err != nil {
		t.Fatalf("create: %v", err)
	}
	_, err := st.CreateConsumer(ctx, "orders", cfg, queue.StartPosition{Kind: queue.StartSeq, Seq: 3}, "test")
	var imm *ImmutableFieldError
	if !errors.As(err, &imm) {
		t.Fatalf("differing start = %v, want ImmutableFieldError", err)
	}
	if !errors.Is(err, errs.ErrConflict) {
		t.Fatalf("differing start = %v, want ErrConflict wrap", err)
	}
	// The identical start still succeeds (idempotent deploy re-POST).
	if _, err := st.CreateConsumer(ctx, "orders", cfg, queue.StartPosition{Kind: queue.StartSeq, Seq: 2}, "test"); err != nil {
		t.Fatalf("identical start re-POST: %v", err)
	}
}

func TestDeleteConsumerDropsDeliveries(t *testing.T) {
	st := newConsumerStream(t)
	ctx := context.Background()

	if _, err := st.CreateConsumer(ctx, "orders", queue.DefaultConsumerConfig("worker"), queue.StartPosition{Kind: queue.StartNew}, "test"); err != nil {
		t.Fatalf("create consumer: %v", err)
	}
	// Plant two READY and one INFLIGHT delivery rows directly.
	for _, d := range [][4]any{
		{int64(1), "orders.1", int64(0), int64(0)},
		{int64(2), "orders.2", int64(0), int64(0)},
		{int64(3), "orders.3", int64(1), int64(1)},
	} {
		if _, err := st.rw.ExecContext(ctx, `
			INSERT INTO deliveries (stream, consumer, seq, subject, state, attempts, visible_at, generation, delivered_at)
			VALUES ('orders','worker',?,?,?,?,0,1,?)`, d[0], d[1], d[2], d[3], d[0]); err != nil {
			t.Fatalf("plant delivery: %v", err)
		}
	}
	res, err := st.DeleteConsumer(ctx, "orders", "worker", "test")
	if err != nil {
		t.Fatalf("DeleteConsumer: %v", err)
	}
	if res.Pending != 3 || res.Inflight != 1 {
		t.Fatalf("DeleteConsumer = %+v, want pending 3 inflight 1", res)
	}
	var left int64
	if err := st.RO().QueryRowContext(ctx, `SELECT count(*) FROM deliveries WHERE stream='orders' AND consumer='worker'`).Scan(&left); err != nil {
		t.Fatalf("count leftovers: %v", err)
	}
	if left != 0 {
		t.Fatalf("deliveries left = %d, want 0", left)
	}
	if _, err := st.GetConsumer(ctx, "orders", "worker"); !errors.Is(err, errs.ErrNotFound) {
		t.Fatalf("GetConsumer after delete = %v, want ErrNotFound", err)
	}
	evs := consumerEvents(t, st, "orders", "worker")
	if len(evs) != 2 || evs[1].Event != "consumer.delete" {
		t.Fatalf("events = %+v, want a trailing consumer.delete", evs)
	}
}

func TestSetPaused(t *testing.T) {
	st := newConsumerStream(t)
	ctx := context.Background()
	if _, err := st.CreateConsumer(ctx, "orders", queue.DefaultConsumerConfig("worker"), queue.StartPosition{Kind: queue.StartNew}, "test"); err != nil {
		t.Fatalf("create: %v", err)
	}

	if _, err := st.SetPaused(ctx, "orders", "worker", true, "test"); err != nil {
		t.Fatalf("pause: %v", err)
	}
	info, err := st.GetConsumer(ctx, "orders", "worker")
	if err != nil || !info.Paused {
		t.Fatalf("after pause: paused=%v err=%v, want true", info.Paused, err)
	}
	// Re-pause is a no-op: no extra event.
	if _, err := st.SetPaused(ctx, "orders", "worker", true, "test"); err != nil {
		t.Fatalf("re-pause: %v", err)
	}
	// Resume.
	if _, err := st.SetPaused(ctx, "orders", "worker", false, "test"); err != nil {
		t.Fatalf("resume: %v", err)
	}
	evs := consumerEvents(t, st, "orders", "worker")
	// create + pause(true) + pause(false); the re-pause wrote nothing.
	if len(evs) != 3 {
		t.Fatalf("events = %d (%+v), want 3 (create, pause, resume)", len(evs), evs)
	}
	if evs[1].Event != "consumer.pause" || evs[1].Detail != `{"paused":true}` {
		t.Fatalf("pause event = %+v, want consumer.pause detail paused:true", evs[1])
	}
	if evs[2].Event != "consumer.pause" || evs[2].Detail != `{"paused":false}` {
		t.Fatalf("resume event = %+v, want consumer.pause detail paused:false", evs[2])
	}
}

func TestConsumerColumnDefaultsEqualGoDefaults(t *testing.T) {
	st := newConsumerStream(t)
	ctx := context.Background()
	// Insert a bare row: only stream/name/created_at, everything else defaults.
	if _, err := st.rw.ExecContext(ctx,
		`INSERT INTO consumers (stream, name, created_at) VALUES ('orders','worker', 123)`); err != nil {
		t.Fatalf("insert bare consumer: %v", err)
	}
	info, err := st.GetConsumer(ctx, "orders", "worker")
	if err != nil {
		t.Fatalf("GetConsumer: %v", err)
	}
	want := queue.DefaultConsumerConfig("worker")
	if len(info.Filters) != 1 || info.Filters[0] != want.Filters[0] {
		t.Errorf("filters = %v, want %v", info.Filters, want.Filters)
	}
	if info.AckWaitMS != want.AckWait.Milliseconds() ||
		info.MaxDeliver != want.MaxDeliver ||
		info.MaxAckPending != want.MaxAckPending {
		t.Errorf("ack/max_deliver/max_ack_pending = %d/%d/%d, want %d/%d/%d",
			info.AckWaitMS, info.MaxDeliver, info.MaxAckPending,
			want.AckWait.Milliseconds(), want.MaxDeliver, want.MaxAckPending)
	}
	if len(info.BackoffMS) != len(want.Backoff) || info.DeadPolicy != string(want.DeadPolicy) {
		t.Errorf("backoff/dead_policy = %v/%q, want %v/%q",
			info.BackoffMS, info.DeadPolicy, want.Backoff, want.DeadPolicy)
	}
	if info.CursorSeq != 1 || info.Generation != 1 || info.Paused {
		t.Errorf("cursor/generation/paused = %d/%d/%v, want 1/1/false",
			info.CursorSeq, info.Generation, info.Paused)
	}
}

func TestConsumerDeadPolicyOnDLQStream(t *testing.T) {
	st := openCommandPathStore(t, fakeClock())
	ctx := context.Background()

	// The refusal fires on the pure validation path, before the stream-existence check.
	cfg := queue.DefaultConsumerConfig("worker")
	cfg.DeadPolicy = queue.DeadPolicyDLQ
	if _, err := st.CreateConsumer(ctx, "orders.dlq", cfg, queue.StartPosition{Kind: queue.StartNew}, "test"); !errors.Is(err, errs.ErrBadRequest) {
		t.Fatalf("dlq on .dlq stream = %v, want ErrBadRequest", err)
	}

	// The accept path needs a live .dlq stream; insert it directly (its name is
	// reserved, so CreateStream refuses it).
	if _, err := st.rw.ExecContext(ctx, `INSERT INTO streams (name, subjects, created_at) VALUES ('orders.dlq', '[">"]', 123)`); err != nil {
		t.Fatalf("insert dlq stream: %v", err)
	}
	if _, err := st.rw.ExecContext(ctx, `INSERT INTO stream_seq (stream, next) VALUES ('orders.dlq', 1)`); err != nil {
		t.Fatalf("insert dlq seq: %v", err)
	}
	cfg.DeadPolicy = queue.DeadPolicyDrop
	if _, err := st.CreateConsumer(ctx, "orders.dlq", cfg, queue.StartPosition{Kind: queue.StartNew}, "test"); err != nil {
		t.Fatalf("drop on .dlq stream: %v", err)
	}
}

func TestUpdateConsumerPatch(t *testing.T) {
	st := newConsumerStream(t)
	ctx := context.Background()
	if _, err := st.CreateConsumer(ctx, "orders", queue.DefaultConsumerConfig("worker"), queue.StartPosition{Kind: queue.StartNew}, "test"); err != nil {
		t.Fatalf("create: %v", err)
	}
	maxDeliver := int32(7)
	res, err := st.UpdateConsumer(ctx, "orders", "worker", ConsumerPatch{MaxDeliver: &maxDeliver}, "test")
	if err != nil {
		t.Fatalf("UpdateConsumer: %v", err)
	}
	if res.MaxDeliver != 7 {
		t.Fatalf("max_deliver = %d, want 7", res.MaxDeliver)
	}
	evs := consumerEvents(t, st, "orders", "worker")
	if len(evs) != 2 || evs[1].Event != "consumer.update" || evs[1].Detail != `{"fields":["max_deliver"]}` {
		t.Fatalf("patch events = %+v, want consumer.update with max_deliver", evs)
	}
	// Empty patch: no write, no event.
	if _, err := st.UpdateConsumer(ctx, "orders", "worker", ConsumerPatch{}, "test"); err != nil {
		t.Fatalf("empty patch: %v", err)
	}
	if evs := consumerEvents(t, st, "orders", "worker"); len(evs) != 2 {
		t.Fatalf("empty patch wrote %d events, want 2", len(evs))
	}
}
