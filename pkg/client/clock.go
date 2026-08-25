// SPDX-License-Identifier: Apache-2.0

package client

import (
	"context"
	"time"
)

// Clock is the package's time seam: every read of the clock and every timer in
// pkg/client goes through it, the same discipline internal/clock enforces server-side.
// Production callers never touch it; tests inject implementations (or run under
// testing/synctest, where the default clock is already virtual).
type Clock interface {
	// Now returns the current time. The monotonic reading matters: the Worker anchors
	// lease deadlines on it, never on the broker's wall-clock deadline_ms.
	Now() time.Time
	// NewTimer fires once after d. Stop and Reset mirror their time.Timer semantics.
	NewTimer(d time.Duration) Timer
}

// Timer is the one timer shape the package needs.
type Timer interface {
	C() <-chan time.Time
	Stop() bool
	Reset(d time.Duration) bool
}

// realClock is the default Clock. It is the single place in this package that touches
// the wall clock directly; everything else takes a Clock.
type realClock struct{}

func (realClock) Now() time.Time {
	return time.Now() //nolint:forbidigo // this file IS pkg/client's internal/clock equivalent; layers.sh forbids importing the internal seam from a public package
}

type stdTimer struct{ t *time.Timer }

func (s *stdTimer) C() <-chan time.Time        { return s.t.C }
func (s *stdTimer) Stop() bool                 { return s.t.Stop() }
func (s *stdTimer) Reset(d time.Duration) bool { return s.t.Reset(d) }

func (realClock) NewTimer(d time.Duration) Timer {
	return &stdTimer{t: time.NewTimer(d)} //nolint:forbidigo // see Now above
}

// ctxClock adapts a Clock to context-friendly sleeps: wait returns after d or when ctx
// is done, reporting whether the full duration elapsed.
func wait(ctx context.Context, c Clock, d time.Duration) bool {
	if d <= 0 {
		err := ctx.Err()
		return err == nil
	}
	t := c.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C():
		return true
	case <-ctx.Done():
		return false
	}
}
