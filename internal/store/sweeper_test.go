// SPDX-License-Identifier: Apache-2.0

package store

import (
	"context"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/a-holm/messq/internal/clock"
	"github.com/a-holm/messq/internal/queue"
)

// The sweeper tests (issue #11 G6/G8). Per-tick logic (probe-first expiry, wake fast
// path) is driven synchronously via [Sweeper.step] against an engine-less store, where
// runSolo commits synchronously — deterministic, no writer-timer timing. The Run loop's
// shutdown is covered separately. All timing against clock.Fake.

// recWaker is a recording Waker twin.
type recWaker struct {
	mu    sync.Mutex
	wait  []queue.ConsumerKey
	wakes []queue.ConsumerKey
}

func (r *recWaker) Waiting() []queue.ConsumerKey {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]queue.ConsumerKey(nil), r.wait...)
}

func (r *recWaker) Wake(k queue.ConsumerKey) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.wakes = append(r.wakes, k)
}

func (r *recWaker) setWaiting(keys ...queue.ConsumerKey) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.wait = keys
}

func (r *recWaker) wakeCount() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.wakes)
}

// waitSweep spins until cond holds (bounded Gosched loop, no Sleep). Fails on timeout.
func waitSweep(t *testing.T, cond func() bool, what string) {
	t.Helper()
	for i := 0; i < 500000 && !cond(); i++ {
		runtime.Gosched()
	}
	if !cond() {
		t.Fatalf("timed out waiting for %s", what)
	}
}

// newTestSweeper opens an engine-less store (deterministic jitter, fake clock) and
// returns its Sweeper ready to drive via step/retire.
func newTestSweeper(t *testing.T, cfg SweepConfig) (*Store, *clock.Fake, *recWaker, *Sweeper) {
	t.Helper()
	st, fk, _ := openSweepStore(t)
	cfg.fillDefaults()
	wak := &recWaker{}
	return st, fk, wak, NewSweeper(st, cfg, wak, nil)
}

// TestSweeperStepRedeliversExpiredRows is the canonical worker-dies path through one
// synchronous tick: three expired rows release to READY at now + 1s, attempts untouched.
func TestSweeperStepRedeliversExpiredRows(t *testing.T) {
	st, fk, _, sw := newTestSweeper(t, SweepConfig{
		Interval: 100 * time.Millisecond, Batch: 10, Catchup: 2,
	})
	seedSweep(t, st, nil, 3, 3)

	fk.Advance(31 * time.Second) // all three deadlines passed
	sw.step(context.Background())

	waitSweep(t, func() bool {
		var ready int
		_ = st.RO().QueryRowContext(context.Background(),
			`SELECT count(*) FROM deliveries WHERE state = 0 AND visible_at > 0`).Scan(&ready)
		return ready == 3
	}, "three rows released to READY")
	for seq := int64(1); seq <= 3; seq++ {
		if got := attemptsFor(t, st, seq); got != 1 {
			t.Fatalf("attempts of seq %d after sweep = %d, want 1", seq, got)
		}
	}
}

// TestSweeperWakesDueBackoff drives pass B (G8): a parked consumer is NOT woken while
// its redelivery is invisible, then IS woken the tick its backoff comes due.
func TestSweeperWakesDueBackoff(t *testing.T) {
	st, fk, wak, sw := newTestSweeper(t, SweepConfig{
		Interval: 100 * time.Millisecond, Batch: 10, Catchup: 2,
	})
	seedSweep(t, st, func(c *queue.ConsumerConfig) { c.Backoff = []time.Duration{2 * time.Second} }, 1, 1)
	wak.setWaiting(queue.ConsumerKey{Stream: "orders", Consumer: "worker"})

	// Past the first deadline: the row releases to READY but is invisible until +2s.
	fk.Advance(31 * time.Second)
	sw.step(context.Background())
	if n := wak.wakeCount(); n != 0 {
		t.Fatalf("woken %d time(s) before backoff due; want 0 (row still invisible)", n)
	}
	// A tick later, still before the backoff: still invisible, still no wake.
	fk.Advance(100 * time.Millisecond)
	sw.step(context.Background())
	if n := wak.wakeCount(); n != 0 {
		t.Fatalf("woken %d time(s) before backoff due (t=+31.1s); want 0", n)
	}
	// Cross the backoff: now visible, the next tick wakes the parked consumer.
	fk.Advance(2 * time.Second)
	sw.step(context.Background())
	waitSweep(t, func() bool { return wak.wakeCount() >= 1 }, "parked consumer woken once backoff due")
}

// TestSweeperIdleSubmitsNoCommand asserts the probe-first idle path (G4): nothing
// expired -> a step never reaches the writer, so every row stays INFLIGHT at its
// original deadline.
func TestSweeperIdleSubmitsNoCommand(t *testing.T) {
	st, fk, _, sw := newTestSweeper(t, SweepConfig{
		Interval: 100 * time.Millisecond, Batch: 10, Catchup: 2,
	})
	seedSweep(t, st, nil, 3, 3)

	fk.Advance(500 * time.Millisecond) // well before the +30s deadlines
	sw.step(context.Background())

	var inflight, ready int
	_ = st.RO().QueryRowContext(context.Background(),
		`SELECT count(*) FROM deliveries WHERE state = 1`).Scan(&inflight)
	_ = st.RO().QueryRowContext(context.Background(),
		`SELECT count(*) FROM deliveries WHERE state = 0`).Scan(&ready)
	if inflight != 3 || ready != 0 {
		t.Fatalf("after idle step: %d INFLIGHT / %d READY, want 3 / 0 (no sweep ran)", inflight, ready)
	}
}

// TestSweeperRunShutsDownOnCancel covers G6's ctx.Done path: Run returns promptly when
// the context is cancelled.
func TestSweeperRunShutsDownOnCancel(t *testing.T) {
	st, _, _, _ := newTestSweeper(t, SweepConfig{
		Interval: 100 * time.Millisecond, Batch: 10, Catchup: 2,
	})
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		_ = NewSweeper(st, SweepConfig{
			Interval: 100 * time.Millisecond, Batch: 10, Catchup: 2,
		}, NopWaker{}, nil).Run(ctx)
		close(done)
	}()
	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("sweeper Run did not return on ctx.Done")
	}
}
