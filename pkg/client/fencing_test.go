// SPDX-License-Identifier: Apache-2.0

package client

import (
	"context"
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"
)

// shouldFail answers transport errors for the named paths while budget lasts —
// the "daemon died and came back" class (§7.5).
func (b *fakeBroker) armFailures(pathSuffix string, n int) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.failPath, b.failBudget = pathSuffix, n
}

// setExtendOverride installs the extend answer under the broker lock: the test swaps
// it WHILE worker goroutines are live.
func (b *fakeBroker) setExtendOverride(fn func(tokens []string) (any, int)) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.extendOverride = fn
}

func (b *fakeBroker) takeFailure(path string) bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.failBudget > 0 && strings.HasSuffix(path, b.failPath) {
		b.failBudget--
		return true
	}
	return false
}

func TestBrokerRestartFencesHeldTokens(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		b := newFakeBroker()
		b.msgs = [][]Delivered{
			{delivered(1, "orders/w/1/1/1"), delivered(2, "orders/w/2/1/1")},
		}
		c := newFakeClient(t, b)
		ec := &eventCollector{}

		w, _ := c.NewWorker(WorkerConfig{
			Stream: "orders", Consumer: "w", Concurrency: 2,
			AckWindow: time.Millisecond,
			OnEvent:   ec.add,
		})

		causes := make(chan error, 2)
		done := make(chan error, 1)
		go func() {
			done <- w.Run(context.Background(), func(ctx context.Context, m *Delivered) error {
				<-ctx.Done() // hold both leases until the fence fires
				causes <- context.Cause(ctx)
				return context.Cause(ctx)
			})
		}()
		synctest.Wait()

		advance(16 * time.Second) // first heartbeat (t≈15 s) succeeds
		synctest.Wait()
		if got := len(b.extendCalls()); got != 1 {
			t.Fatalf("extends so far = %d, want exactly the first heartbeat", got)
		}

		// THE RESTART (T9): recovery flipped our rows to READY; from now on the
		// broker answers every extend with per-token unknown.
		b.setExtendOverride(func(tokens []string) (any, int) {
			results := make([]SettleItem, len(tokens))
			for i, tok := range tokens {
				results[i] = SettleItem{Token: tok, Status: SettleUnknown, Reason: "no such delivery"}
			}
			return SettleResult{Results: results}, 200
		})
		advance(40 * time.Second) // second heartbeat (t≈45 s) hits the fence
		synctest.Wait()

		for range 2 {
			select {
			case cause := <-causes:
				if !errors.Is(cause, ErrLeaseLost) {
					t.Errorf("handler cancelled with cause %v, want ErrLeaseLost", cause)
				}
			case <-asyncAfter(time.Second):
				t.Fatal("handler context not cancelled within one extend window of the fence")
			}
		}

		w.Drain(context.Background())
		if err := <-done; err != nil && !errors.Is(err, context.Canceled) {
			t.Fatalf("Run = %v", err)
		}

		// Zero stale-ack surprises: NOTHING may be settled for a lost token.
		if acks := b.ackedTokens(); len(acks) != 0 {
			t.Errorf("acks after lease loss = %v, want none", acks)
		}
		if naks := b.nakkedItems(); len(naks) != 0 {
			t.Errorf("naks after lease loss = %v, want none", naks)
		}
		if terms := b.termedItems(); len(terms) != 0 {
			t.Errorf("terms after lease loss = %v, want none", terms)
		}
		if ec.count(EventLeaseLost) != 2 {
			t.Errorf("LeaseLost events = %d, want 2", ec.count(EventLeaseLost))
		}
		if ec.count(EventOutcomeDiscarded) != 2 {
			t.Errorf("OutcomeDiscarded events = %d, want 2 (late handler results dropped)", ec.count(EventOutcomeDiscarded))
		}
		if st := w.Stats(); st.LeaseLost != 2 || st.Acked != 0 {
			t.Errorf("stats = %+v, want 2 lease losses and zero acks", st)
		}
	})
}

func TestExtendTransportFailureRetriesWithinMargin(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		b := newFakeBroker()
		b.msgs = [][]Delivered{{delivered(7, "orders/w/7/1/1")}}
		c := newFakeClient(t, b)

		handlerDone := make(chan struct{})
		var once atomic.Bool
		w, _ := c.NewWorker(WorkerConfig{Stream: "orders", Consumer: "w"})
		b.armFailures("/v1/extend", 2) // the next TWO extend attempts die in transit
		done := make(chan error, 1)
		go func() {
			done <- w.Run(context.Background(), func(context.Context, *Delivered) error {
				if !once.CompareAndSwap(false, true) {
					return nil
				}
				<-asyncAfter(20 * time.Second) // spans the flaky extend attempts
				close(handlerDone)
				return nil
			})
		}()

		advance(21 * time.Second)
		<-handlerDone

		w.Drain(context.Background())
		if err := <-done; err != nil && !errors.Is(err, context.Canceled) {
			t.Fatalf("Run = %v", err)
		}

		if calls := len(b.extendCalls()); calls != 1 {
			t.Fatalf("extend calls = %d, want the single successful batch", calls)
		}
		if st := w.Stats(); st.LeaseLost != 0 {
			t.Errorf("LeaseLost = %d, want 0 — transient failures must be retried inside the margin", st.LeaseLost)
		}
	})
}

func TestAckLostAfterRetryBudgetReportedNotFatal(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		b := newFakeBroker()
		b.msgs = [][]Delivered{{delivered(4, "orders/w/4/1/1")}}
		c := newFakeClient(t, b)
		ec := &eventCollector{}

		w, _ := c.NewWorker(WorkerConfig{Stream: "orders", Consumer: "w", AckWindow: time.Millisecond, OnEvent: ec.add})
		// Every /v1/ack dies in transit for the whole run (armed before Run: there
		// is no ack before a fetch anyway).
		b.armFailures("/v1/ack", 1<<30)
		done := make(chan error, 1)
		go func() {
			done <- w.Run(context.Background(), func(context.Context, *Delivered) error { return nil })
		}()

		synctest.Wait()
		advance(100 * time.Millisecond)
		synctest.Wait()

		w.Drain(context.Background())
		if err := <-done; err != nil && !errors.Is(err, context.Canceled) {
			t.Fatalf("Run = %v — an ack loss must never fail Run", err)
		}

		if ec.count(EventAckLost) == 0 {
			t.Error("OnEvent{AckLost} never fired")
		}
		if st := w.Stats(); st.Acked != 0 {
			t.Errorf("Acked = %d, want 0", st.Acked)
		}
		// The slot machinery still retired the token: no hang, no leak.
		if st := w.Stats(); st.Fetched != 1 {
			t.Errorf("Fetched = %d, want 1", st.Fetched)
		}
	})
}
