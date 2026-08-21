// SPDX-License-Identifier: Apache-2.0

// Package clock is the seam. It is the one package allowed to read the wall clock and the one
// package allowed to block on it, so lint must accept both here and nowhere else.
package clock

import "time"

// Now reads the wall clock.
func Now() time.Time {
	return time.Now()
}

// Pause blocks for d.
func Pause(d time.Duration) {
	time.Sleep(d)
}
