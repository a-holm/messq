// SPDX-License-Identifier: Apache-2.0

package client

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"strings"
	"testing"
	"testing/synctest"
	"time"
)

func TestWorkerFetchHandleAckRoundTrip(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		b := newFakeBroker()
		b.msgs = [][]Delivered{{{Stream: "orders", Consumer: "w", Seq: 1, Subject: "orders.west", Body: []byte("hi"), AckToken: "orders/w/1/1/1", AckWaitMS: 30000}}}
		c := newFakeClient(t, b)

		handled := make(chan *Delivered, 1)
		w, err := c.NewWorker(WorkerConfig{Stream: "orders", Consumer: "w"})
		if err != nil {
			t.Fatalf("NewWorker: %v", err)
		}
		done := make(chan error, 1)
		go func() {
			done <- w.Run(context.Background(), func(_ context.Context, m *Delivered) error {
				handled <- m
				return nil
			})
		}()

		m := <-handled
		if m.DedupKey() != "orders/1" || string(m.Body) != "hi" {
			t.Errorf("delivered = %+v", m)
		}
		// The handler returned nil; the ack must land without any wall-clock sleep.
		synctest.Wait()
		if acks := b.ackedTokens(); len(acks) != 1 || len(acks[0]) != 1 || acks[0][0] != "orders/w/1/1/1" {
			t.Errorf("acks = %v, want one ack of the token", acks)
		}
		st := w.Stats()
		if st.Fetched != 1 || st.Acked != 1 {
			t.Errorf("stats = %+v", st)
		}

		w.Drain(context.Background())
		if err := <-done; err != nil {
			t.Errorf("Run = %v", err)
		}
	})
}

func TestWorkerHeartbeatKeepsLongHandlerAlive(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		b := newFakeBroker()
		const token = "orders/w/5/1/1"
		b.msgs = [][]Delivered{func() []Delivered {
			d := delivered(5, token)
			return []Delivered{d}
		}()}
		c := newFakeClient(t, b)

		release := make(chan struct{})
		w, _ := c.NewWorker(WorkerConfig{Stream: "orders", Consumer: "w"})
		done := make(chan error, 1)
		go func() {
			done <- w.Run(context.Background(), func(ctx context.Context, _ *Delivered) error {
				<-release // a 10-minute handler under a 30 s ack_wait
				return nil
			})
		}()

		synctest.Wait() // handler running, first extend window pending
		time.Sleep(16 * time.Second)
		synctest.Wait()
		close(release) // finish the handler just after the 15 s heartbeat
		synctest.Wait()

		w.Drain(context.Background())
		if err := <-done; err != nil {
			t.Fatalf("Run = %v", err)
		}

		extends := b.extendCalls()
		if len(extends) != 1 {
			t.Fatalf("extends = %d calls, want exactly one batched heartbeat", len(extends))
		}
		if len(extends[0]) != 1 || extends[0][0] != token {
			t.Errorf("extend tokens = %v", extends[0])
		}
		// Exactly one ack for the token, after the extends — zero stale-ack surprises.
		if acks := b.ackedTokens(); len(acks) != 1 || acks[0][0] != token {
			t.Errorf("acks = %v", acks)
		}
	})
}

func TestWorkerExtendBatchingIndependentOfConcurrency(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		b := newFakeBroker()
		msgs := make([]Delivered, 64)
		for i := range msgs {
			msgs[i] = Delivered{
				Stream: "orders", Consumer: "w", Seq: int64(i + 1),
				AckToken: fmt.Sprintf("t%d", i+1), AckWaitMS: 60000, Body: []byte{},
			}
		}
		b.msgs = [][]Delivered{msgs}
		c := newFakeClient(t, b)

		started := make(chan struct{}, 64)
		w, _ := c.NewWorker(WorkerConfig{
			Stream: "orders", Consumer: "w",
			Concurrency: 64, // free slots == batch: all 64 claimed in one fetch
			Batch:       64,
			AckWindow:   time.Millisecond,
		})
		done := make(chan error, 1)
		go func() {
			done <- w.Run(context.Background(), func(_ context.Context, _ *Delivered) error {
				started <- struct{}{}
				<-time.After(50 * time.Second) // spans one extend window, finishes inside the drain budget
				return nil
			})
		}()
		for range 64 {
			<-started
		}

		time.Sleep(30 * time.Second)
		synctest.Wait()

		w.Drain(context.Background())
		if err := <-done; err != nil {
			t.Fatalf("Run = %v", err)
		}

		extends := b.extendCalls()
		total := 0
		for _, e := range extends {
			total += len(e)
		}
		if total < 64 {
			t.Fatalf("only %d tokens extended across %d calls; leases were not kept alive", total, len(extends))
		}
		if len(extends) > 4 {
			t.Errorf("%d extend REQUESTS for 64 concurrent handlers; batching must be ~1 per window, not per message", len(extends))
		}
	})
}

func TestWorkerOutcomeMapping(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		mk := func(seq int64) Delivered {
			return Delivered{Stream: "orders", Consumer: "w", Seq: seq, AckToken: fmt.Sprintf("t%d", seq), AckWaitMS: 30000}
		}
		b := newFakeBroker()
		b.msgs = [][]Delivered{{mk(1), mk(2), mk(3), mk(4)}}
		c := newFakeClient(t, b)

		var panics int
		w, _ := c.NewWorker(WorkerConfig{Stream: "orders", Consumer: "w", Concurrency: 4})
		handler := func(_ context.Context, m *Delivered) (err error) {
			switch m.Seq {
			case 1:
				return nil
			case 2:
				return Permanent(errors.New("poison payload"))
			case 3:
				return RetryAfter(7*time.Second, errors.New("downstream busy"))
			case 4:
				panics++
				panic("boom")
			}
			return nil
		}
		done := make(chan error, 1)
		go func() { done <- w.Run(context.Background(), handler) }()

		synctest.Wait()
		synctest.Wait()

		w.Drain(context.Background())
		if err := <-done; err != nil {
			t.Fatalf("Run = %v — a panic must never kill the worker", err)
		}

		if acks := b.ackedTokens(); len(acks) == 0 || len(acks[len(acks)-1]) == 0 || !containsToken(b.ackedTokens(), "t1") {
			t.Errorf("seq 1 (nil outcome) was not acked: %v", b.ackedTokens())
		}
		terms := b.termedItems()
		if len(terms) != 1 || terms[0].Token != "t2" || terms[0].Reason != "poison payload" {
			t.Errorf("term = %+v, want t2 with its reason straight to DLQ", terms)
		}
		naks := b.nakkedItems()
		var retryNak, panicNak bool
		for _, n := range naks {
			if n.Token == "t3" && n.DelayMS != nil && *n.DelayMS == 7000 {
				retryNak = true
			}
			if n.Token == "t4" && strings.HasPrefix(n.Reason, "panic:") {
				panicNak = true
			}
		}
		if !retryNak {
			t.Errorf("RetryAfter nak missing or wrong delay in %v", naks)
		}
		if !panicNak {
			t.Errorf("recovered panic must nak with a 'panic:' reason in %v", naks)
		}
		if panics != 1 {
			t.Errorf("handler ran %d times for seq 4, want 1 (no redelivery inside the test)", panics)
		}
	})
}

func containsToken(groups [][]string, tok string) bool {
	for _, g := range groups {
		for _, t := range g {
			if t == tok {
				return true
			}
		}
	}
	return false
}

func TestWorkerHoldMatrixBackoffs(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		b := newFakeBroker()
		// paused with no server hint: max(RetryAfter, 1s); backoff with hint: exact.
		b.holdQ = []FetchResponse{
			{Hold: HoldPaused},
			{Hold: HoldBackoff, RetryAfter: 2500 * time.Millisecond},
			{Hold: HoldFlowControl},
			{Hold: HoldEmpty},
			{Hold: HoldShuttingDown},
		}
		c := newFakeClient(t, b)
		ec := &eventCollector{}

		w, _ := c.NewWorker(WorkerConfig{Stream: "orders", Consumer: "w", OnEvent: ec.add})
		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan error, 1)
		go func() { done <- w.Run(ctx, func(context.Context, *Delivered) error { return nil }) }()

		// paused: next fetch not before 1 s.
		time.Sleep(500 * time.Millisecond)
		synctest.Wait()
		if n := len(b.fetchCalls()); n != 1 {
			t.Fatalf("paused hold refetched %d times within 500 ms, want exactly the initial fetch", n)
		}
		// past the 1 s floor the second fetch (backoff answer) has landed.
		time.Sleep(600 * time.Millisecond)
		synctest.Wait()
		// backoff hint 2.5 s: third fetch not before it elapses.
		time.Sleep(2 * time.Second)
		synctest.Wait()
		if n := len(b.fetchCalls()); n != 2 {
			t.Fatalf("backoff hint ignored: %d fetches by t≈3.1s, want 2", n)
		}
		time.Sleep(600 * time.Millisecond)
		synctest.Wait()
		// flow_control floor is 100 ms; empty and shutting_down answers follow quickly.
		time.Sleep(200 * time.Millisecond)
		cancel()
		if err := <-done; err != nil && !errors.Is(err, context.Canceled) {
			t.Fatalf("Run = %v", err)
		}
		if ec.count(EventHold) < 2 {
			t.Errorf("OnEvent{Hold} fired %d times, want at least paused+flow_control", ec.count(EventHold))
		}
	})
}

func TestWorkerSlotBoundedBatch(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		b := newFakeBroker()
		c := newFakeClient(t, b)

		block := make(chan struct{})
		w, _ := c.NewWorker(WorkerConfig{Stream: "orders", Consumer: "w", Concurrency: 1, Batch: 64})
		done := make(chan error, 1)
		go func() {
			done <- w.Run(context.Background(), func(context.Context, *Delivered) error {
				<-block
				return nil
			})
		}()
		synctest.Wait()

		for _, fr := range b.fetchCalls() {
			if fr.Batch > 1 {
				t.Errorf("fetch batch = %d with Concurrency 1 and one busy slot; batch must be bounded by FREE slots", fr.Batch)
			}
		}
		close(block)
		w.Drain(context.Background())
		<-done
	})
}

func TestWorkerDrainNaksWhatItHolds(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		b := newFakeBroker()
		b.msgs = [][]Delivered{{delivered(9, "orders/w/9/1/1")}}
		c := newFakeClient(t, b)

		w, _ := c.NewWorker(WorkerConfig{Stream: "orders", Consumer: "w"})
		done := make(chan error, 1)
		go func() {
			done <- w.Run(context.Background(), func(ctx context.Context, _ *Delivered) error {
				<-ctx.Done() // never finishes on its own; Drain must not wait forever
				return ctx.Err()
			})
		}()
		synctest.Wait()

		drained := make(chan error, 1)
		go func() { drained <- w.Drain(context.Background()) }()
		synctest.Wait()

		select {
		case <-drained:
		case <-time.After(45 * time.Second):
			t.Fatal("Drain did not return within DrainTimeout")
		}
		synctest.Wait() // the drain-timeout nak itself has landed

		naks := b.nakkedItems()
		found := false
		for _, n := range naks {
			if n.Token == "orders/w/9/1/1" && n.Reason == "worker draining" && (n.DelayMS == nil || *n.DelayMS == 0) {
				found = true
			}
		}
		if !found {
			t.Errorf("held token not nakked with reason='worker draining' and delay=0: %v", naks)
		}
		<-done
	})
}

func TestWorkerGoroutineCensus(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		b := newFakeBroker()
		c := newFakeClient(t, b)

		before := runtime.NumGoroutine()
		w, _ := c.NewWorker(WorkerConfig{Stream: "orders", Consumer: "w", Concurrency: 4})
		done := make(chan error, 1)
		go func() {
			done <- w.Run(context.Background(), func(context.Context, *Delivered) error { return nil })
		}()
		synctest.Wait()
		during := runtime.NumGoroutine()
		// Run caller + fetcher + keeper + settler + 4 handlers.
		if during-before != 7 {
			t.Errorf("goroutines during Run: +%d, want +7 (3 + Concurrency)", during-before)
		}
		w.Drain(context.Background())
		if err := <-done; err != nil {
			t.Fatalf("Run = %v", err)
		}
		synctest.Wait()
		if after := runtime.NumGoroutine(); after != before {
			t.Errorf("goroutines after Drain: %d, want baseline %d", after, before)
		}
	})
}

func TestWorkerMaxLeaseGiveUp(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		b := newFakeBroker()
		d := delivered(3, "orders/w/3/1/1")
		d.AckWaitMS = 10000 // 10 s lease, extend at 5 s, MaxLease 12 s ⇒ give-up at 12 s
		b.msgs = [][]Delivered{{d}}
		c := newFakeClient(t, b)
		ec := &eventCollector{}

		w, _ := c.NewWorker(WorkerConfig{
			Stream: "orders", Consumer: "w",
			MaxLease: 12 * time.Second,
			OnEvent:  ec.add,
		})
		done := make(chan error, 1)
		handlerReturned := make(chan struct{})
		go func() {
			done <- w.Run(context.Background(), func(ctx context.Context, _ *Delivered) error {
				<-ctx.Done() // the cancellation IS the signal the lease is gone
				close(handlerReturned)
				return ctx.Err()
			})
		}()
		synctest.Wait()
		time.Sleep(13 * time.Second)
		synctest.Wait()
		<-handlerReturned

		w.Drain(context.Background())
		if err := <-done; err != nil {
			t.Fatalf("Run = %v", err)
		}

		if ec.count(EventLeaseCapped)+ec.count(EventHandlerTimeout) == 0 {
			t.Errorf("events = %v, want LeaseCapped/HandlerTimeout on give-up", ec.all())
		}
		naks := b.nakkedItems()
		found := false
		for _, n := range naks {
			if n.Token == "orders/w/3/1/1" && n.Reason == "lease expired" {
				found = true
			}
		}
		if !found {
			t.Errorf("give-up must nak with reason='lease expired': %v", naks)
		}
		if w.Stats().LeakedHandlers != 1 {
			t.Errorf("LeakedHandlers = %d, want 1", w.Stats().LeakedHandlers)
		}
	})
}

func TestWorkerPreflightWarningWhenConcurrencyExceedsMaxAckPending(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		b := newFakeBroker()
		b.consum.MaxAckPending = 2
		c := newFakeClient(t, b)
		ec := &eventCollector{}

		w, _ := c.NewWorker(WorkerConfig{Stream: "orders", Consumer: "w", Concurrency: 8, OnEvent: ec.add})
		done := make(chan error, 1)
		ctx, cancel := context.WithCancel(context.Background())
		go func() { done <- w.Run(ctx, func(context.Context, *Delivered) error { return nil }) }()
		synctest.Wait()
		cancel()
		if err := <-done; err != nil && !errors.Is(err, context.Canceled) {
			t.Fatalf("Run = %v", err)
		}

		warnings := 0
		for _, e := range ec.all() {
			if e.Kind == EventStarted && strings.Contains(e.Err.Error(), "messq consumer edit") {
				warnings++
			}
		}
		if warnings != 1 {
			t.Errorf("got %d Started-warning events naming 'messq consumer edit', want exactly one (%v)", warnings, ec.all())
		}
	})
}

func TestPermanentAndRetryAfterWrappers(t *testing.T) {
	t.Parallel()

	base := errors.New("disk on fire")
	if !errors.Is(Permanent(base), ErrPermanent) || !errors.Is(Permanent(base), base) {
		t.Error("Permanent must wrap so both targets match via errors.Is")
	}
	ra := RetryAfter(3*time.Second, base)
	if !errors.Is(ra, base) {
		t.Error("RetryAfter must keep the cause unwrappable")
	}
	var got *RetryAfterError
	if !errors.As(ra, &got) || got.Delay != 3*time.Second {
		t.Errorf("errors.As(*RetryAfterError) failed: %+v", got)
	}
}
