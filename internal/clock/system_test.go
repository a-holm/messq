// SPDX-License-Identifier: Apache-2.0

package clock_test

import (
	"context"
	"errors"
	"testing"
	"testing/synctest"
	"time"

	"github.com/a-holm/messq/internal/clock"
)

// TestSystemClockInSynctestBubble proves the seam works unchanged inside a synctest bubble.
// This is the pattern the delivery engine, the sweeper and the long-poll waiters copy: real
// clock.System, virtual time, no fake to wire up, and every bubble goroutine exits.
func TestSystemClockInSynctestBubble(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		var sys clock.System
		start := sys.Now()

		done := make(chan time.Duration, 1)
		go func() {
			if err := sys.Sleep(context.Background(), time.Hour); err != nil {
				t.Errorf("Sleep() = %v, want nil", err)
			}
			done <- sys.Since(start)
		}()

		// Wait removes the "has it armed its timer yet?" race without a sleep.
		synctest.Wait()

		if got := <-done; got != time.Hour {
			t.Fatalf("Sleep(1h) took %v of bubble time, want 1h", got)
		}
		if got := sys.Since(start); got != time.Hour {
			t.Fatalf("Since(start) = %v, want 1h", got)
		}
	})
}

func TestSystemTimerInSynctestBubble(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		var sys clock.System
		start := sys.Now()

		timer := sys.NewTimer(time.Minute)
		if !timer.Reset(2 * time.Minute) {
			t.Fatal("Reset() on an armed timer = false, want true")
		}
		at := <-timer.C()
		if got := at.Sub(start); got != 2*time.Minute {
			t.Fatalf("timer fired %v after start, want 2m", got)
		}
		if timer.Stop() {
			t.Fatal("Stop() on a fired timer = true, want false")
		}

		spare := sys.NewTimer(time.Hour)
		if !spare.Stop() {
			t.Fatal("Stop() on an armed timer = false, want true")
		}
	})
}

func TestSystemTickerInSynctestBubble(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		var sys clock.System
		start := sys.Now()

		ticker := sys.NewTicker(time.Second)
		defer ticker.Stop()

		first := <-ticker.C()
		if got := first.Sub(start); got != time.Second {
			t.Fatalf("first tick %v after start, want 1s", got)
		}

		ticker.Reset(5 * time.Second)
		second := <-ticker.C()
		if got := second.Sub(start); got != 6*time.Second {
			t.Fatalf("second tick %v after start, want 6s", got)
		}
	})
}

func TestSystemSleepIsCancellable(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		var sys clock.System

		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan error, 1)
		go func() { done <- sys.Sleep(ctx, time.Hour) }()

		synctest.Wait()
		cancel()
		if err := <-done; !errors.Is(err, context.Canceled) {
			t.Fatalf("Sleep() = %v, want context.Canceled", err)
		}
	})
}

func TestSystemSleepEdges(t *testing.T) {
	t.Parallel()

	var sys clock.System

	if err := sys.Sleep(t.Context(), 0); err != nil {
		t.Fatalf("Sleep(0) = %v, want nil", err)
	}
	if err := sys.Sleep(t.Context(), -time.Second); err != nil {
		t.Fatalf("Sleep(-1s) = %v, want nil", err)
	}

	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if err := sys.Sleep(ctx, time.Hour); !errors.Is(err, context.Canceled) {
		t.Fatalf("Sleep() on a cancelled context = %v, want context.Canceled", err)
	}
}

// TestSystemNowAdvances is the one assertion that has to touch the real clock: the production
// Now must not be frozen. It compares two reads through the seam rather than against
// time.Now, so this test is not itself a wall-clock reader.
func TestSystemNowAdvances(t *testing.T) {
	t.Parallel()

	var sys clock.System
	start := sys.Now()
	if err := sys.Sleep(t.Context(), time.Millisecond); err != nil {
		t.Fatalf("Sleep() = %v, want nil", err)
	}
	if got := sys.Since(start); got <= 0 {
		t.Fatalf("Since(start) = %v, want a positive duration", got)
	}
	if !sys.Now().After(start) {
		t.Fatalf("Now() did not advance past %v", start)
	}
}

// TestBackoffScheduleUnderFakeClock drives the whole default backoff schedule, about thirteen
// minutes of modelled time, and asserts it costs no real time worth measuring. This is the
// reason internal/queue takes a Clock instead of calling time.Now.
func TestBackoffScheduleUnderFakeClock(t *testing.T) {
	t.Parallel()

	schedule := []time.Duration{time.Second, 5 * time.Second, 30 * time.Second, 2 * time.Minute, 10 * time.Minute}

	var sys clock.System
	realStart := sys.Now()

	f := clock.NewFake(epoch)
	for attempt, backoff := range schedule {
		timer := f.NewTimer(backoff)
		f.Advance(backoff - time.Nanosecond)
		select {
		case at := <-timer.C():
			t.Fatalf("attempt %d: fired early at %v", attempt, at)
		default:
		}
		f.Advance(time.Nanosecond)
		select {
		case <-timer.C():
		default:
			t.Fatalf("attempt %d: did not fire at its deadline", attempt)
		}
	}

	var modelled time.Duration
	for _, backoff := range schedule {
		modelled += backoff
	}
	if got := f.Since(epoch); got != modelled {
		t.Fatalf("modelled elapsed = %v, want %v", got, modelled)
	}
	if got := sys.Since(realStart); got > 50*time.Millisecond {
		t.Fatalf("thirteen modelled minutes cost %v of wall clock, want under 50ms", got)
	}
}

func TestFakeNewTickerRejectsNonPositivePeriods(t *testing.T) {
	t.Parallel()

	f := clock.NewFake(epoch)
	assertPanics(t, "NewTicker(0)", func() { f.NewTicker(0) })

	ticker := f.NewTicker(time.Second)
	defer ticker.Stop()
	assertPanics(t, "Ticker.Reset(0)", func() { ticker.Reset(0) })
}

func assertPanics(t *testing.T, what string, fn func()) {
	t.Helper()
	defer func() {
		if recover() == nil {
			t.Fatalf("%s did not panic", what)
		}
	}()
	fn()
}
