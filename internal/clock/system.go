// SPDX-License-Identifier: Apache-2.0

package clock

import (
	"context"
	"time"
)

// System is the production clock. This file is the only one in the repository that calls the
// time package's clock functions; the forbidigo rule in .golangci.yml rejects them everywhere
// else, and test/gates proves that the rule bites.
type System struct{}

// Now returns the current wall-clock time, carrying a monotonic reading.
func (System) Now() time.Time { return time.Now() }

// Since returns the monotonic time elapsed since t.
func (System) Since(t time.Time) time.Duration { return time.Since(t) }

// NewTimer arms a real one-shot timer.
func (System) NewTimer(d time.Duration) Timer { return systemTimer{t: time.NewTimer(d)} }

// NewTicker arms a real ticker.
func (System) NewTicker(d time.Duration) Ticker { return systemTicker{t: time.NewTicker(d)} }

// Sleep blocks for d or until ctx is done. It is the only blocking wait in the tree: every
// other package calls this instead of time.Sleep, so every wait is cancellable.
func (System) Sleep(ctx context.Context, d time.Duration) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if d <= 0 {
		return nil
	}
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

type systemTimer struct{ t *time.Timer }

func (s systemTimer) C() <-chan time.Time        { return s.t.C }
func (s systemTimer) Reset(d time.Duration) bool { return s.t.Reset(d) }
func (s systemTimer) Stop() bool                 { return s.t.Stop() }

type systemTicker struct{ t *time.Ticker }

func (s systemTicker) C() <-chan time.Time   { return s.t.C }
func (s systemTicker) Reset(d time.Duration) { s.t.Reset(d) }
func (s systemTicker) Stop()                 { s.t.Stop() }
