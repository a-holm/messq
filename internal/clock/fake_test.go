// SPDX-License-Identifier: Apache-2.0

package clock_test

import (
	"context"
	"errors"
	"math/rand/v2"
	"testing"
	"time"

	"github.com/a-holm/messq/internal/clock"
)

// epoch is the start instant every fake in these tests is wound to. It is a round wall-clock
// time so a failure message reads as an offset from it.
var epoch = time.Date(2026, 8, 21, 12, 0, 0, 0, time.UTC)

func TestFakeNowAdvanceAndSet(t *testing.T) {
	t.Parallel()

	f := clock.NewFake(epoch)
	if got := f.Now(); !got.Equal(epoch) {
		t.Fatalf("Now() = %v, want %v", got, epoch)
	}

	f.Advance(90 * time.Second)
	if got, want := f.Now(), epoch.Add(90*time.Second); !got.Equal(want) {
		t.Fatalf("after Advance: Now() = %v, want %v", got, want)
	}
	if got, want := f.Since(epoch), 90*time.Second; got != want {
		t.Fatalf("Since(epoch) = %v, want %v", got, want)
	}

	// Set may move backwards. That is the whole reason the seam exists: an NTP step is a
	// fault the daemon has to survive, and synctest cannot model it.
	f.Set(epoch.Add(-5 * time.Second))
	if got, want := f.Now(), epoch.Add(-5*time.Second); !got.Equal(want) {
		t.Fatalf("after backwards Set: Now() = %v, want %v", got, want)
	}
	if got, want := f.Since(epoch), -5*time.Second; got != want {
		t.Fatalf("Since(epoch) after backwards Set = %v, want %v", got, want)
	}
}

func TestFakeTimerSemantics(t *testing.T) {
	t.Parallel()

	t.Run("fires at its deadline and not before", func(t *testing.T) {
		t.Parallel()

		f := clock.NewFake(epoch)
		timer := f.NewTimer(time.Second)

		f.Advance(999 * time.Millisecond)
		assertNoFire(t, timer.C())

		f.Advance(time.Millisecond)
		assertFiredAt(t, timer.C(), epoch.Add(time.Second))
	})

	t.Run("Advance(0) fires a timer armed for exactly now", func(t *testing.T) {
		t.Parallel()

		f := clock.NewFake(epoch)
		timer := f.NewTimer(0)
		f.Advance(0)
		assertFiredAt(t, timer.C(), epoch)
	})

	t.Run("Stop before firing reports the timer was active", func(t *testing.T) {
		t.Parallel()

		f := clock.NewFake(epoch)
		timer := f.NewTimer(time.Second)
		if !timer.Stop() {
			t.Fatal("Stop() on an armed timer = false, want true")
		}
		if timer.Stop() {
			t.Fatal("second Stop() = true, want false")
		}
		f.Advance(time.Hour)
		assertNoFire(t, timer.C())
	})

	t.Run("Stop after firing reports the timer was not active", func(t *testing.T) {
		t.Parallel()

		f := clock.NewFake(epoch)
		timer := f.NewTimer(time.Second)
		f.Advance(time.Second)
		assertFiredAt(t, timer.C(), epoch.Add(time.Second))
		if timer.Stop() {
			t.Fatal("Stop() on a fired timer = true, want false")
		}
	})

	t.Run("Reset re-arms and reports the previous state", func(t *testing.T) {
		t.Parallel()

		f := clock.NewFake(epoch)
		timer := f.NewTimer(time.Second)
		if !timer.Reset(2 * time.Second) {
			t.Fatal("Reset() on an armed timer = false, want true")
		}
		f.Advance(time.Second)
		assertNoFire(t, timer.C())
		f.Advance(time.Second)
		assertFiredAt(t, timer.C(), epoch.Add(2*time.Second))

		if timer.Reset(time.Second) {
			t.Fatal("Reset() on a fired timer = true, want false")
		}
		f.Advance(time.Second)
		assertFiredAt(t, timer.C(), epoch.Add(3*time.Second))
	})

	t.Run("Armed counts what is waiting", func(t *testing.T) {
		t.Parallel()

		f := clock.NewFake(epoch)
		if got := f.Armed(); got != 0 {
			t.Fatalf("Armed() on a fresh fake = %d, want 0", got)
		}
		a := f.NewTimer(time.Second)
		f.NewTimer(2 * time.Second)
		if got := f.Armed(); got != 2 {
			t.Fatalf("Armed() with two timers = %d, want 2", got)
		}
		a.Stop()
		if got := f.Armed(); got != 1 {
			t.Fatalf("Armed() after one Stop = %d, want 1", got)
		}
		f.Advance(time.Hour)
		if got := f.Armed(); got != 0 {
			t.Fatalf("Armed() after every timer fired = %d, want 0", got)
		}
	})
}

// TestFakeEveryDueTimerFires is the coarse half of the ordering guarantee: whatever order the
// timers were armed in, each one fires exactly once and is handed its own deadline. The
// (deadline, sequence) order itself is pinned in the internal test, where the fire order is
// observable without a goroutine race deciding it.
func TestFakeEveryDueTimerFires(t *testing.T) {
	t.Parallel()

	deadlines := []time.Duration{
		30 * time.Millisecond,
		10 * time.Millisecond,
		20 * time.Millisecond,
		10 * time.Millisecond, // a tie: the arming sequence breaks it
		20 * time.Millisecond,
	}

	rng := rand.New(rand.NewPCG(1, 2))
	for run := range 100 {
		order := rng.Perm(len(deadlines))

		f := clock.NewFake(epoch)
		timers := make([]clock.Timer, len(deadlines))
		for _, i := range order {
			timers[i] = f.NewTimer(deadlines[i])
		}

		f.Advance(time.Second)
		for i, timer := range timers {
			assertFiredAt(t, timer.C(), epoch.Add(deadlines[i]))
			if got := len(timer.C()); got != 0 {
				t.Fatalf("run %d: timer %d fired %d extra times", run, i, got)
			}
		}
	}
}

func TestFakeTickerSemantics(t *testing.T) {
	t.Parallel()

	t.Run("ticks once per period", func(t *testing.T) {
		t.Parallel()

		f := clock.NewFake(epoch)
		ticker := f.NewTicker(time.Second)
		defer ticker.Stop()

		f.Advance(time.Second)
		assertFiredAt(t, ticker.C(), epoch.Add(time.Second))

		f.Advance(time.Second)
		assertFiredAt(t, ticker.C(), epoch.Add(2*time.Second))
	})

	t.Run("a skipped period coalesces into one tick", func(t *testing.T) {
		t.Parallel()

		f := clock.NewFake(epoch)
		ticker := f.NewTicker(time.Second)
		defer ticker.Stop()

		f.Advance(10 * time.Second)
		assertFiredAt(t, ticker.C(), epoch.Add(time.Second))
		assertNoFire(t, ticker.C())

		// The next deadline is back on the period grid rather than ten seconds late.
		f.Advance(time.Second)
		assertFiredAt(t, ticker.C(), epoch.Add(11*time.Second))
	})

	t.Run("a skipped period keeps the ticker on the grid", func(t *testing.T) {
		t.Parallel()

		// The period deliberately does not divide the advance: re-arming one period after
		// now instead of one period after the missed deadline would put the next tick at
		// 13s rather than at the grid point 12s.
		f := clock.NewFake(epoch)
		ticker := f.NewTicker(3 * time.Second)
		defer ticker.Stop()

		f.Advance(10 * time.Second)
		assertFiredAt(t, ticker.C(), epoch.Add(3*time.Second))

		f.Advance(2 * time.Second)
		assertFiredAt(t, ticker.C(), epoch.Add(12*time.Second))
	})

	t.Run("Reset moves the period", func(t *testing.T) {
		t.Parallel()

		f := clock.NewFake(epoch)
		ticker := f.NewTicker(time.Second)
		defer ticker.Stop()

		ticker.Reset(3 * time.Second)
		f.Advance(2 * time.Second)
		assertNoFire(t, ticker.C())
		f.Advance(time.Second)
		assertFiredAt(t, ticker.C(), epoch.Add(3*time.Second))
	})

	t.Run("Stop ends the ticks", func(t *testing.T) {
		t.Parallel()

		f := clock.NewFake(epoch)
		ticker := f.NewTicker(time.Second)
		ticker.Stop()
		f.Advance(time.Hour)
		assertNoFire(t, ticker.C())
		if got := f.Armed(); got != 0 {
			t.Fatalf("Armed() after Stop = %d, want 0", got)
		}
	})
}

// TestFakeBlockUntil removes the "has the goroutine armed its timer yet?" race that otherwise
// forces a sleep into every test that drives a background loop.
func TestFakeBlockUntil(t *testing.T) {
	t.Parallel()

	f := clock.NewFake(epoch)
	done := make(chan time.Duration, 1)

	go func() {
		timer := f.NewTimer(time.Minute)
		at := <-timer.C()
		done <- at.Sub(epoch)
	}()

	f.BlockUntil(1)
	if got := f.Armed(); got < 1 {
		t.Fatalf("after BlockUntil(1): Armed() = %d, want at least 1", got)
	}
	f.Advance(time.Minute)

	if got := <-done; got != time.Minute {
		t.Fatalf("timer fired at %v, want %v", got, time.Minute)
	}
}

func TestFakeSleep(t *testing.T) {
	t.Parallel()

	t.Run("returns when the deadline passes", func(t *testing.T) {
		t.Parallel()

		f := clock.NewFake(epoch)
		done := make(chan error, 1)
		go func() { done <- f.Sleep(t.Context(), time.Minute) }()

		f.BlockUntil(1)
		f.Advance(time.Minute)
		if err := <-done; err != nil {
			t.Fatalf("Sleep() = %v, want nil", err)
		}
	})

	t.Run("returns the context error when cancelled", func(t *testing.T) {
		t.Parallel()

		f := clock.NewFake(epoch)
		ctx, cancel := context.WithCancel(t.Context())
		done := make(chan error, 1)
		go func() { done <- f.Sleep(ctx, time.Hour) }()

		f.BlockUntil(1)
		cancel()
		if err := <-done; !errors.Is(err, context.Canceled) {
			t.Fatalf("Sleep() = %v, want context.Canceled", err)
		}
		if got := f.Armed(); got != 0 {
			t.Fatalf("Armed() after an abandoned Sleep = %d, want 0", got)
		}
	})

	t.Run("a non-positive duration returns immediately", func(t *testing.T) {
		t.Parallel()

		f := clock.NewFake(epoch)
		if err := f.Sleep(t.Context(), 0); err != nil {
			t.Fatalf("Sleep(0) = %v, want nil", err)
		}
		if err := f.Sleep(t.Context(), -time.Second); err != nil {
			t.Fatalf("Sleep(-1s) = %v, want nil", err)
		}
	})

	t.Run("an already cancelled context is reported without waiting", func(t *testing.T) {
		t.Parallel()

		f := clock.NewFake(epoch)
		ctx, cancel := context.WithCancel(t.Context())
		cancel()
		if err := f.Sleep(ctx, 0); !errors.Is(err, context.Canceled) {
			t.Fatalf("Sleep() on a cancelled context = %v, want context.Canceled", err)
		}
	})
}

// TestFakeIsAClock keeps the fake and the production clock interchangeable at compile time.
func TestFakeIsAClock(t *testing.T) {
	t.Parallel()

	var _ clock.Clock = clock.NewFake(epoch)
	var _ clock.Clock = clock.System{}
}

func assertFiredAt(t *testing.T, ch <-chan time.Time, want time.Time) {
	t.Helper()
	select {
	case got := <-ch:
		if !got.Equal(want) {
			t.Fatalf("fired at %v, want %v", got, want)
		}
	default:
		t.Fatalf("nothing fired, want a fire at %v", want)
	}
}

func assertNoFire(t *testing.T, ch <-chan time.Time) {
	t.Helper()
	select {
	case got := <-ch:
		t.Fatalf("fired at %v, want nothing", got)
	default:
	}
}
