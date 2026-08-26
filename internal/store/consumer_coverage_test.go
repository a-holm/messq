// SPDX-License-Identifier: Apache-2.0

package store

import (
	"context"
	"database/sql"
	"errors"
	"testing"
	"time"

	"github.com/a-holm/messq/internal/errs"
	"github.com/a-holm/messq/internal/queue"
)

// TestConsumerCommandKindsAndBytes covers the command-label and byte-budget methods
// that only the writer engine exercises (Kind on the panic path, Bytes on the wired
// budget path) — the engine-less tests never call them.
func TestConsumerCommandKindsAndBytes(t *testing.T) {
	cmds := []Cmd{
		createConsumerCmd{},
		updateConsumerCmd{},
		deleteConsumerCmd{},
		setPausedCmd{},
		fetchCmd{batch: 4},
	}
	wantKinds := []CmdKind{kindCreateConsumer, kindUpdateConsumer, kindDeleteConsumer, kindSetPaused, kindFetch}
	for i, c := range cmds {
		if got := c.Kind(); got != wantKinds[i] {
			t.Fatalf("command %d Kind() = %q, want %q", i, got, wantKinds[i])
		}
		if got := c.Bytes(); got < 0 {
			t.Fatalf("command %d Bytes() = %d, want >= 0", i, got)
		}
	}
	if got := (fetchCmd{batch: 4}).Bytes(); got != 4*256 {
		t.Fatalf("fetchCmd Bytes() = %d, want %d", got, 4*256)
	}
}

// TestConsumerValidationRefusals covers the fast-path validation error returns on every
// public consumer method.
func TestConsumerValidationRefusals(t *testing.T) {
	st := newConsumerStream(t)
	ctx := context.Background()
	cfg := queue.DefaultConsumerConfig("worker")

	cases := []struct {
		name string
		call func() error
	}{
		{"create bad stream name", func() error {
			_, err := st.CreateConsumer(ctx, "bad/name", cfg, queue.StartPosition{Kind: queue.StartNew}, "test")
			return err
		}},
		{"create reserved dead policy", func() error {
			_, err := st.CreateConsumer(ctx, "orders.dlq", cfg, queue.StartPosition{Kind: queue.StartNew}, "test")
			return err
		}},
		{"create invalid config", func() error {
			bad := cfg
			bad.Filters = nil
			_, err := st.CreateConsumer(ctx, "orders", bad, queue.StartPosition{Kind: queue.StartNew}, "test")
			return err
		}},
		{"create missing start", func() error {
			_, err := st.CreateConsumer(ctx, "orders", cfg, queue.StartPosition{}, "test")
			return err
		}},
		{"update bad stream name", func() error {
			_, err := st.UpdateConsumer(ctx, "bad/name", "worker", ConsumerPatch{}, "test")
			return err
		}},
		{"update bad consumer name", func() error {
			_, err := st.UpdateConsumer(ctx, "orders", "bad/name", ConsumerPatch{}, "test")
			return err
		}},
		{"delete bad stream name", func() error {
			_, err := st.DeleteConsumer(ctx, "bad/name", "worker", "test")
			return err
		}},
		{"delete bad consumer name", func() error {
			_, err := st.DeleteConsumer(ctx, "orders", "bad/name", "test")
			return err
		}},
		{"setpaused bad stream name", func() error {
			_, err := st.SetPaused(ctx, "bad/name", "worker", true, "test")
			return err
		}},
		{"setpaused bad consumer name", func() error {
			_, err := st.SetPaused(ctx, "orders", "bad/name", true, "test")
			return err
		}},
		{"fetch bad stream name", func() error {
			_, err := st.Fetch(ctx, FetchReq{Stream: "bad/name", Consumer: "worker", Batch: 1})
			return err
		}},
		{"fetch bad consumer name", func() error {
			_, err := st.Fetch(ctx, FetchReq{Stream: "orders", Consumer: "bad/name", Batch: 1})
			return err
		}},
		{"get bad consumer name", func() error {
			_, err := st.GetConsumer(ctx, "orders", "bad/name")
			return err
		}},
		{"list bad stream name", func() error {
			_, err := st.ListConsumers(ctx, "bad/name")
			return err
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if err := tc.call(); err == nil {
				t.Fatalf("expected a validation refusal, got nil")
			}
		})
	}

	// batch <= 0 is a specific refusal.
	if _, err := st.Fetch(ctx, FetchReq{Stream: "orders", Consumer: "worker", Batch: 0}); !errors.Is(err, errs.ErrBadRequest) {
		t.Fatalf("Fetch batch=0 = %v, want ErrBadRequest", err)
	}
}

// TestResolveStartPositionEdgeCases covers the pure start-resolution helpers' error and
// boundary arms directly (white-box): unknown kind, missing stream, and clamping.
func TestResolveStartPositionEdgeCases(t *testing.T) {
	st := newConsumerStream(t)
	ctx := context.Background()

	if got := clampCursor(0, 5, 100); got != 5 {
		t.Fatalf("clampCursor(0,5,100) = %d, want 5", got)
	}
	if got := clampCursor(999, 5, 100); got != 100 {
		t.Fatalf("clampCursor(999,5,100) = %d, want 100", got)
	}
	if got := clampCursor(50, 5, 100); got != 50 {
		t.Fatalf("clampCursor(50,5,100) = %d, want 50", got)
	}

	run := func(fn func(*sql.Tx) error) {
		t.Helper()
		conn, err := st.rw.Conn(ctx)
		if err != nil {
			t.Fatalf("conn: %v", err)
		}
		defer func() {
			if cerr := conn.Close(); cerr != nil {
				t.Errorf("close conn: %v", cerr)
			}
		}()
		tx, err := conn.BeginTx(ctx, nil)
		if err != nil {
			t.Fatalf("begin: %v", err)
		}
		defer func() {
			if rbErr := tx.Rollback(); rbErr != nil {
				t.Errorf("rollback: %v", rbErr)
			}
		}()
		if err := fn(tx); err != nil {
			t.Fatalf("fn: %v", err)
		}
	}

	// Unknown start kind.
	run(func(tx *sql.Tx) error {
		if _, _, err := resolveStartPosition(ctx, tx, "orders", queue.StartPosition{Kind: queue.Start("bogus")}, time.UnixMilli(fakeStartMillis)); !errors.Is(err, errs.ErrBadRequest) {
			t.Fatalf("resolve unknown kind = %v, want ErrBadRequest", err)
		}
		return nil
	})
	// Missing stream.
	run(func(tx *sql.Tx) error {
		if _, _, err := streamBounds(ctx, tx, "ghost"); !errors.Is(err, errs.ErrNotFound) {
			t.Fatalf("streamBounds missing = %v, want ErrNotFound", err)
		}
		return nil
	})
}

// TestConsumerBacklogLiteralFilters covers the literal-only exact-backlog path.
func TestConsumerBacklogLiteralFilters(t *testing.T) {
	st, _ := newFetchStore(t)
	publishSubjs(t, st, "orders.a", "orders.a", "orders.b")
	cfg := queue.DefaultConsumerConfig("worker")
	cfg.Filters = []string{"orders.a"} // literal-only
	newFetchConsumer(t, st, "worker", cfg)

	ctx := context.Background()
	if _, err := st.Fetch(ctx, FetchReq{Stream: "orders", Consumer: "worker", Batch: 10}); err != nil {
		t.Fatalf("fetch: %v", err)
	}
	info, err := st.GetConsumer(ctx, "orders", "worker")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if !info.BacklogExact {
		t.Fatalf("literal-only filter should report backlog_exact=true, got false")
	}
	// 2 matching messages admitted and pending (batch 10), backlog exact.
	if info.Pending != 2 {
		t.Fatalf("pending = %d, want 2", info.Pending)
	}
}

// TestConsumerWiredCRUD covers the writer enqueue path for every consumer command, so
// the wired engine exercises them exactly once more than the engine-less tests.
func TestConsumerWiredCRUD(t *testing.T) {
	st := openWiredFetchStore(t)
	ctx := context.Background()
	if _, err := st.CreateConsumer(ctx, "orders", queue.DefaultConsumerConfig("worker"), queue.StartPosition{Kind: queue.StartNew}, "test"); err != nil {
		t.Fatalf("create: %v", err)
	}
	maxDeliver := int32(7)
	if _, err := st.UpdateConsumer(ctx, "orders", "worker", ConsumerPatch{MaxDeliver: &maxDeliver}, "test"); err != nil {
		t.Fatalf("update: %v", err)
	}
	if _, err := st.SetPaused(ctx, "orders", "worker", true, "test"); err != nil {
		t.Fatalf("pause: %v", err)
	}
	if _, err := st.DeleteConsumer(ctx, "orders", "worker", "test"); err != nil {
		t.Fatalf("delete: %v", err)
	}
}

// TestConsumerNotFoundRefusals covers the missing-consumer error arms of every command
// and read that looks one up inside its transaction.
func TestConsumerNotFoundRefusals(t *testing.T) {
	st := newConsumerStream(t)
	ctx := context.Background()
	if _, err := st.UpdateConsumer(ctx, "orders", "ghost", ConsumerPatch{}, "test"); !errors.Is(err, errs.ErrNotFound) {
		t.Fatalf("UpdateConsumer missing = %v, want ErrNotFound", err)
	}
	if _, err := st.DeleteConsumer(ctx, "orders", "ghost", "test"); !errors.Is(err, errs.ErrNotFound) {
		t.Fatalf("DeleteConsumer missing = %v, want ErrNotFound", err)
	}
	if _, err := st.SetPaused(ctx, "orders", "ghost", true, "test"); !errors.Is(err, errs.ErrNotFound) {
		t.Fatalf("SetPaused missing = %v, want ErrNotFound", err)
	}
	if _, err := st.GetConsumer(ctx, "orders", "ghost"); !errors.Is(err, errs.ErrNotFound) {
		t.Fatalf("GetConsumer missing = %v, want ErrNotFound", err)
	}
}

// TestCreateConsumerMissingStream covers the in-transaction stream-existence refusal.
func TestCreateConsumerMissingStream(t *testing.T) {
	st := openCommandPathStore(t, fakeClock())
	ctx := context.Background()
	_, err := st.CreateConsumer(ctx, "orders", queue.DefaultConsumerConfig("worker"), queue.StartPosition{Kind: queue.StartNew}, "test")
	if !errors.Is(err, errs.ErrNotFound) {
		t.Fatalf("CreateConsumer missing stream = %v, want ErrNotFound", err)
	}
}

// TestUpdateConsumerInvalidPatch covers the in-transaction re-validation refusal.
func TestUpdateConsumerInvalidPatch(t *testing.T) {
	st := newConsumerStream(t)
	ctx := context.Background()
	if _, err := st.CreateConsumer(ctx, "orders", queue.DefaultConsumerConfig("worker"), queue.StartPosition{Kind: queue.StartNew}, "test"); err != nil {
		t.Fatalf("create: %v", err)
	}
	bad := []string{"orders..bad"}
	if _, err := st.UpdateConsumer(ctx, "orders", "worker", ConsumerPatch{Filters: &bad}, "test"); !errors.Is(err, errs.ErrBadSubject) {
		t.Fatalf("UpdateConsumer invalid filters = %v, want ErrBadSubject", err)
	}
}

// TestReadClaimedPayloadsOrphan covers the orphan-row omission path: a claimed seq whose
// message is gone is dropped from the response, never fatal.
func TestReadClaimedPayloadsOrphan(t *testing.T) {
	st := newConsumerStream(t)
	ctx := context.Background()
	claimed := []claimedDelivery{{seq: 999, attempt: 1, deadlineMS: 1, ackWaitMS: 1}}
	out := st.readClaimedPayloads(ctx, "orders", "worker", claimed, 1, 5, 1)
	if len(out) != 0 {
		t.Fatalf("orphan payload = %+v, want omitted", out)
	}
}

// TestUpdateConsumerAllFields covers every applyConsumerPatch branch (all six fields).
func TestUpdateConsumerAllFields(t *testing.T) {
	st := newConsumerStream(t)
	ctx := context.Background()
	if _, err := st.CreateConsumer(ctx, "orders", queue.DefaultConsumerConfig("worker"), queue.StartPosition{Kind: queue.StartNew}, "test"); err != nil {
		t.Fatalf("create: %v", err)
	}
	filters := []string{"other.>"}
	ackWait := int64(60000)
	maxDeliver := int32(3)
	maxPending := int64(50)
	backoff := []int64{1000}
	dp := queue.DeadPolicyDrop
	res, err := st.UpdateConsumer(ctx, "orders", "worker", ConsumerPatch{
		Filters: &filters, AckWaitMS: &ackWait, MaxDeliver: &maxDeliver,
		MaxAckPending: &maxPending, BackoffMS: &backoff, DeadPolicy: &dp,
	}, "test")
	if err != nil {
		t.Fatalf("update: %v", err)
	}
	if res.AckWaitMS != 60000 || res.MaxDeliver != 3 || res.MaxAckPending != 50 ||
		res.DeadPolicy != "drop" || len(res.Filters) != 1 || res.Filters[0] != "other.>" {
		t.Fatalf("updated = %+v, want all six fields applied", res)
	}
}

// TestCreateConsumerAllFieldsDiff covers every consumerConfigDiff branch via a re-POST
// that changes all six config fields at once.
func TestCreateConsumerAllFieldsDiff(t *testing.T) {
	st := newConsumerStream(t)
	ctx := context.Background()
	if _, err := st.CreateConsumer(ctx, "orders", queue.DefaultConsumerConfig("worker"), queue.StartPosition{Kind: queue.StartNew}, "test"); err != nil {
		t.Fatalf("create: %v", err)
	}
	cfg := queue.DefaultConsumerConfig("worker")
	cfg.Filters = []string{"other.>"}
	cfg.AckWait = 60 * time.Second
	cfg.MaxDeliver = 3
	cfg.MaxAckPending = 50
	cfg.Backoff = []time.Duration{time.Second}
	cfg.DeadPolicy = queue.DeadPolicyDrop
	res, err := st.CreateConsumer(ctx, "orders", cfg, queue.StartPosition{Kind: queue.StartNew}, "test")
	if err != nil {
		t.Fatalf("re-create: %v", err)
	}
	if !res.Updated {
		t.Fatalf("re-create should be an update")
	}
	evs := consumerEvents(t, st, "orders", "worker")
	if len(evs) != 2 || evs[1].Event != "consumer.update" {
		t.Fatalf("events = %+v, want an update", evs)
	}
}

// TestImmutableFieldErrorMessage covers the ImmutableFieldError.Error() rendering.
func TestImmutableFieldErrorMessage(t *testing.T) {
	err := &ImmutableFieldError{Field: "start"}
	if err.Error() == "" {
		t.Fatal("ImmutableFieldError.Error() is empty")
	}
	if !errors.Is(err, errs.ErrConflict) {
		t.Fatalf("ImmutableFieldError must wrap ErrConflict")
	}
}
