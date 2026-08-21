// SPDX-License-Identifier: Apache-2.0

package clock

import (
	"cmp"
	"context"
	"slices"
	"sync"
	"time"
)

// Fake is a deterministic [Clock] that only moves when a test moves it. It is safe for
// concurrent use and it lives in the production build rather than in internal/testutil so
// that every package's tests can take it without an import cycle.
//
// Time never advances on its own: [Fake.Advance] and [Fake.Set] are the only two ways a
// deadline can pass, which is what makes a test that drives thirteen modelled minutes finish
// in microseconds.
type Fake struct {
	mu      sync.Mutex
	cond    *sync.Cond
	now     time.Time
	seq     uint64
	waiters []*fakeWaiter
}

// fakeWaiter is one armed timer or ticker. period is zero for a timer.
type fakeWaiter struct {
	ch       chan time.Time
	deadline time.Time
	period   time.Duration
	seq      uint64
	armed    bool
}

// NewFake returns a fake clock wound to start.
func NewFake(start time.Time) *Fake {
	f := &Fake{now: start}
	f.cond = sync.NewCond(&f.mu)
	return f
}

// Now returns the fake's current time.
func (f *Fake) Now() time.Time {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.now
}

// Since returns the modelled duration between t and now. Unlike [System.Since] it is not
// monotonic: winding the fake backwards makes it negative, which is exactly what a test of
// the clock-regression policy needs.
func (f *Fake) Since(t time.Time) time.Duration {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.now.Sub(t)
}

// NewTimer arms a one-shot timer for d from the fake's current time.
func (f *Fake) NewTimer(d time.Duration) Timer {
	return &fakeTimer{f: f, w: f.arm(d, 0)}
}

// NewTicker arms a ticker with period d. It panics for a non-positive d, as time.NewTicker
// does.
func (f *Fake) NewTicker(d time.Duration) Ticker {
	if d <= 0 {
		panic("clock: non-positive interval for NewTicker")
	}
	return &fakeTicker{f: f, w: f.arm(d, d)}
}

// Sleep waits for d of modelled time or until ctx is done.
func (f *Fake) Sleep(ctx context.Context, d time.Duration) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if d <= 0 {
		return nil
	}
	timer := f.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C():
		return nil
	}
}

// Advance moves the fake forward by d and fires everything that comes due, including timers
// armed for exactly the new time. Advance(0) therefore fires a timer armed with a zero
// duration.
func (f *Fake) Advance(d time.Duration) {
	f.mu.Lock()
	target := f.now.Add(d)
	f.mu.Unlock()
	f.Set(target)
}

// Set moves the fake to t, which may be earlier than the current time: a backwards wall-clock
// step is a fault messq has to survive, and no synctest bubble can model it. Waiters whose
// deadline is at or before t fire.
func (f *Fake) Set(t time.Time) {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.now = t
	for _, w := range f.due(t) {
		// Non-blocking, on a channel with room for one value: an unread tick is dropped
		// rather than blocking the clock, which is what a real ticker does.
		select {
		case w.ch <- w.deadline:
		default:
		}

		if w.period <= 0 {
			w.armed = false
			f.drop(w)
			continue
		}

		// Stay on the period grid: a ticker whose reader fell behind gets one tick and its
		// next deadline is the first grid point after now, not one period after a deadline
		// that is already in the past.
		next := w.deadline.Add(w.period)
		if !next.After(t) {
			next = w.deadline.Add(w.period * (t.Sub(w.deadline)/w.period + 1))
		}
		w.deadline = next
		f.seq++
		w.seq = f.seq
	}
	f.cond.Broadcast()
}

// BlockUntil blocks until at least n timers or tickers are armed. It is what removes the
// "has the goroutine armed its timer yet?" race from a test that drives a background loop.
func (f *Fake) BlockUntil(n int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	for len(f.waiters) < n {
		f.cond.Wait()
	}
}

// Armed reports how many timers and tickers are waiting.
func (f *Fake) Armed() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.waiters)
}

// due returns the waiters that are at or past their deadline at t, in (deadline, arming
// sequence) order. The caller holds f.mu.
func (f *Fake) due(t time.Time) []*fakeWaiter {
	var out []*fakeWaiter
	for _, w := range f.waiters {
		if !w.deadline.After(t) {
			out = append(out, w)
		}
	}
	slices.SortFunc(out, func(a, b *fakeWaiter) int {
		if c := a.deadline.Compare(b.deadline); c != 0 {
			return c
		}
		return cmp.Compare(a.seq, b.seq)
	})
	return out
}

// arm registers a new waiter. period is zero for a one-shot timer.
func (f *Fake) arm(d, period time.Duration) *fakeWaiter {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.seq++
	w := &fakeWaiter{
		ch:       make(chan time.Time, 1),
		deadline: f.now.Add(d),
		period:   period,
		seq:      f.seq,
		armed:    true,
	}
	f.waiters = append(f.waiters, w)
	f.cond.Broadcast()
	return w
}

// stop disarms w and reports whether it was still armed.
func (f *Fake) stop(w *fakeWaiter) bool {
	f.mu.Lock()
	defer f.mu.Unlock()

	if !w.armed {
		return false
	}
	w.armed = false
	f.drop(w)
	f.cond.Broadcast()
	return true
}

// reset re-arms w for d from now with a fresh sequence number and reports whether it was
// still armed. A fresh sequence number is what keeps the fire order of a reset timer
// predictable: it goes behind everything already armed for the same deadline.
func (f *Fake) reset(w *fakeWaiter, d, period time.Duration) bool {
	f.mu.Lock()
	defer f.mu.Unlock()

	was := w.armed
	w.deadline = f.now.Add(d)
	w.period = period
	f.seq++
	w.seq = f.seq
	if !was {
		w.armed = true
		f.waiters = append(f.waiters, w)
	}
	f.cond.Broadcast()
	return was
}

// drop removes w from the waiter list. The caller holds f.mu.
func (f *Fake) drop(w *fakeWaiter) {
	f.waiters = slices.DeleteFunc(f.waiters, func(other *fakeWaiter) bool { return other == w })
}

type fakeTimer struct {
	f *Fake
	w *fakeWaiter
}

func (t *fakeTimer) C() <-chan time.Time        { return t.w.ch }
func (t *fakeTimer) Stop() bool                 { return t.f.stop(t.w) }
func (t *fakeTimer) Reset(d time.Duration) bool { return t.f.reset(t.w, d, 0) }

type fakeTicker struct {
	f *Fake
	w *fakeWaiter
}

func (t *fakeTicker) C() <-chan time.Time { return t.w.ch }
func (t *fakeTicker) Stop()               { t.f.stop(t.w) }

// Reset changes the period and schedules the next tick d from now. It panics for a
// non-positive d, as time.Ticker.Reset does.
func (t *fakeTicker) Reset(d time.Duration) {
	if d <= 0 {
		panic("clock: non-positive interval for Ticker.Reset")
	}
	t.f.reset(t.w, d, d)
}
