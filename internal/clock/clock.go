// SPDX-License-Identifier: Apache-2.0

package clock

import (
	"context"
	"time"
)

// Clock is the seam every package takes instead of the time package.
type Clock interface {
	// Now is the wall clock. Use it for values that are persisted or displayed:
	// published_at, visible_at, event ts, ULID timestamps.
	Now() time.Time

	// Since is monotonic for [System]: it measures an in-process duration that survives an
	// NTP step. Use it for long-poll deadlines, the commit window, --exec heartbeats and
	// latency histograms.
	Since(t time.Time) time.Duration

	// NewTimer arms a one-shot timer for d from now.
	NewTimer(d time.Duration) Timer

	// NewTicker arms a repeating ticker with period d. A period of zero or less panics,
	// exactly as time.NewTicker does.
	NewTicker(d time.Duration) Ticker

	// Sleep waits for d or until ctx is done, whichever happens first. It returns
	// ctx.Err() when the context ended the wait and nil when the duration did. A
	// non-positive d returns nil immediately unless ctx is already done.
	Sleep(ctx context.Context, d time.Duration) error
}

// Timer is a one-shot timer. The semantics match time.Timer, including the return values of
// Reset and Stop.
type Timer interface {
	// C is the channel the deadline is delivered on. It is buffered with room for one
	// value, so a fire never blocks the clock.
	C() <-chan time.Time

	// Reset re-arms the timer for d from now and reports whether it was still armed.
	Reset(d time.Duration) bool

	// Stop disarms the timer and reports whether it was still armed.
	Stop() bool
}

// Ticker is a repeating ticker. The semantics match time.Ticker: a tick that nobody has read
// yet is dropped rather than queued.
type Ticker interface {
	// C is the channel ticks are delivered on, buffered with room for one value.
	C() <-chan time.Time

	// Reset changes the period and schedules the next tick d from now.
	Reset(d time.Duration)

	// Stop ends the ticks. It does not close the channel.
	Stop()
}
