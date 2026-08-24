// SPDX-License-Identifier: Apache-2.0

package store

import "github.com/a-holm/messq/internal/queue"

// The Waker seam (issue #11 §8). #14's waiter registry implements this interface; until
// then NopWaker keeps everything green. Both methods are non-blocking and safe to call
// from the sweeper goroutine. Wakes happen AFTER the commit returns — never inside
// Apply — because waking before commit could deliver from a rolled-back transaction
// (G8). This is the fast path only: #14's fetch handler parks until its own deadline, so
// a sweeper that is late, busy or absent costs latency, never correctness.
type Waker interface {
	// Waiting returns the parked consumers, or nil when nobody is parked (the
	// overwhelmingly common case — pass B is skipped entirely).
	Waiting() []queue.ConsumerKey
	// Wake signals one parked consumer that a delivery of theirs is visible.
	Wake(queue.ConsumerKey)
}

// NopWaker is the default: nobody is ever parked before #14 lands.
type NopWaker struct{}

// Waiting returns nil — the fast-path skip signal.
func (NopWaker) Waiting() []queue.ConsumerKey { return nil }

// Wake does nothing.
func (NopWaker) Wake(queue.ConsumerKey) {}
