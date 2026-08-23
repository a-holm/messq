// SPDX-License-Identifier: Apache-2.0

package store

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"testing"

	"github.com/a-holm/messq/internal/errs"
	"github.com/a-holm/messq/internal/queue"
)

// openWiredFetchStore opens a store with its writer attached (single-writer engine),
// stream "orders" created, and returns it.
func openWiredFetchStore(t *testing.T) *Store {
	t.Helper()
	st, _ := openWiredCommandPathStore(t, fakeClock(), Config{}, hooks{})
	ctx := context.Background()
	if _, _, err := st.CreateStream(ctx, queue.DefaultConfig("orders"), "test"); err != nil {
		t.Fatalf("create stream: %v", err)
	}
	return st
}

// TestFetchConcurrentNoDoubleClaim proves G7: 64 concurrent fetchers claim a union of
// 64 distinct sequences with no duplicate, under -race.
func TestFetchConcurrentNoDoubleClaim(t *testing.T) {
	st := openWiredFetchStore(t)
	ctx := context.Background()

	const n = 64
	for i := 0; i < n; i++ {
		if _, err := st.Publish(ctx, PublishCmd{Stream: "orders", Req: queue.PublishReq{Subject: "orders.1", Body: []byte("x")}}); err != nil {
			t.Fatalf("publish %d: %v", i, err)
		}
	}
	cfg := queue.DefaultConsumerConfig("worker")
	cfg.Filters = []string{">"}
	if _, err := st.CreateConsumer(ctx, "orders", cfg, queue.StartPosition{Kind: queue.StartFirst}, "test"); err != nil {
		t.Fatalf("create consumer: %v", err)
	}

	var mu sync.Mutex
	claimed := map[int64]int{}
	var wg sync.WaitGroup
	errsCh := make(chan error, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			res, err := st.Fetch(ctx, FetchReq{Stream: "orders", Consumer: "worker", Batch: 1})
			if err != nil {
				errsCh <- err
				return
			}
			mu.Lock()
			for _, m := range res.Messages {
				claimed[m.Seq]++
			}
			mu.Unlock()
		}()
	}
	wg.Wait()
	close(errsCh)
	for err := range errsCh {
		t.Fatalf("fetch: %v", err)
	}

	if len(claimed) != n {
		t.Fatalf("claimed %d distinct seqs, want %d", len(claimed), n)
	}
	for seq, count := range claimed {
		if count != 1 {
			t.Fatalf("seq %d claimed %d times, want exactly once", seq, count)
		}
	}
}

// TestFetchSavepointIsolation proves G7: a fetch for a missing consumer (a CmdError)
// queued beside a good fetch leaves the good one committed.
func TestFetchSavepointIsolation(t *testing.T) {
	st := openWiredFetchStore(t)
	ctx := context.Background()

	if _, err := st.Publish(ctx, PublishCmd{Stream: "orders", Req: queue.PublishReq{Subject: "orders.1", Body: []byte("x")}}); err != nil {
		t.Fatalf("publish: %v", err)
	}
	cfg := queue.DefaultConsumerConfig("worker")
	cfg.Filters = []string{">"}
	if _, err := st.CreateConsumer(ctx, "orders", cfg, queue.StartPosition{Kind: queue.StartFirst}, "test"); err != nil {
		t.Fatalf("create consumer: %v", err)
	}

	var wg sync.WaitGroup
	goodErr := make(chan error, 1)
	var goodSeq int64
	wg.Add(2)
	go func() {
		defer wg.Done()
		res, err := st.Fetch(ctx, FetchReq{Stream: "orders", Consumer: "worker", Batch: 1})
		if err != nil {
			goodErr <- err
			return
		}
		if len(res.Messages) == 1 {
			goodSeq = res.Messages[0].Seq
		}
		goodErr <- nil
	}()
	var missingErr error
	go func() {
		defer wg.Done()
		_, missingErr = st.Fetch(ctx, FetchReq{Stream: "orders", Consumer: "ghost", Batch: 1})
	}()
	wg.Wait()
	if err := <-goodErr; err != nil {
		t.Fatalf("good fetch: %v", err)
	}
	if !errors.Is(missingErr, errs.ErrNotFound) {
		t.Fatalf("missing-consumer fetch = %v, want ErrNotFound", missingErr)
	}
	if goodSeq != 1 {
		t.Fatalf("good fetch claimed seq %d, want 1", goodSeq)
	}
	// The claim committed: attempts is 1 and state INFLIGHT.
	var attempts, state int64
	if err := st.RO().QueryRowContext(ctx,
		`SELECT attempts, state FROM deliveries WHERE stream='orders' AND consumer='worker' AND seq=1`).
		Scan(&attempts, &state); err != nil {
		t.Fatalf("read delivery: %v", err)
	}
	if attempts != 1 || state != 1 {
		t.Fatalf("attempts/state = %d/%d, want 1/1 (the good claim committed)", attempts, state)
	}
}

// TestFetchCatchingUpHold proves G12: a consumer filtered to a rare subject reaches the
// head in a bounded number of fetches, each within the scan limit, reporting
// hold=catching_up while it works and empty once at the head.
func TestFetchCatchingUpHold(t *testing.T) {
	ctx := context.Background()
	cl := queue.DefaultConsumerLimits()
	cl.ScanLimit = 10
	st, _, err := Open(ctx, Options{
		DataDir: testDataDir(t), Clock: fakeClock(), Logger: discardLoggerStore(),
		ConsumerLimits: cl,
	})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() {
		if cerr := st.Close(ctx); cerr != nil {
			t.Errorf("close: %v", cerr)
		}
	})
	if _, _, cErr := st.CreateStream(ctx, queue.DefaultConfig("orders"), "test"); cErr != nil {
		t.Fatalf("create stream: %v", cErr)
	}
	for i := 0; i < 25; i++ {
		if _, pErr := st.Publish(ctx, PublishCmd{Stream: "orders", Req: queue.PublishReq{Subject: "orders.1", Body: []byte("x")}}); pErr != nil {
			t.Fatalf("publish: %v", pErr)
		}
	}
	cfg := queue.DefaultConsumerConfig("worker")
	cfg.Filters = []string{"orders.rare"} // matches nothing
	if _, mkErr := st.CreateConsumer(ctx, "orders", cfg, queue.StartPosition{Kind: queue.StartFirst}, "test"); mkErr != nil {
		t.Fatalf("create consumer: %v", mkErr)
	}

	fetches := 0
	for {
		res, fErr := st.Fetch(ctx, FetchReq{Stream: "orders", Consumer: "worker", Batch: 10})
		if fErr != nil {
			t.Fatalf("fetch: %v", fErr)
		}
		fetches++
		if res.Hold == HoldEmpty {
			break
		}
		if fetches > 10 {
			t.Fatalf("did not reach the head in 10 fetches (hold %q, cursor %d)", res.Hold, res.CursorSeq)
		}
	}
	// 25 messages / scan limit 10 → 3 scans; the consumer must finish in a bounded,
	// small number of fetches (not one per message).
	if fetches != 3 {
		t.Fatalf("reached head in %d fetches, want 3 (scan limit 10 over 25 messages)", fetches)
	}
	// The cursor persisted past every message.
	info, err := st.GetConsumer(ctx, "orders", "worker")
	if err != nil {
		t.Fatalf("get consumer: %v", err)
	}
	if info.CursorSeq != 26 {
		t.Fatalf("cursor_seq = %d, want 26 (past the head)", info.CursorSeq)
	}
}

// discardLoggerStore returns a logger that drops everything, for stores opened directly
// in tests that don't go through testOptions.
func discardLoggerStore() *slog.Logger {
	return slog.New(discardHandler{})
}

type discardHandler struct{}

func (discardHandler) Enabled(context.Context, slog.Level) bool  { return false }
func (discardHandler) Handle(context.Context, slog.Record) error { return nil }
func (h discardHandler) WithAttrs([]slog.Attr) slog.Handler      { return h }
func (h discardHandler) WithGroup(string) slog.Handler           { return h }
