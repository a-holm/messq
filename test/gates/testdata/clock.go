// SPDX-License-Identifier: Apache-2.0

package clock

import "time"

// SabotageNow reads the wall clock. internal/clock is the one package allowed to, so lint has
// to accept this file here and reject the same two calls anywhere else.
func SabotageNow() time.Time {
	return time.Now()
}

// SabotagePause blocks on the wall clock, which is the other half of the same allowance.
func SabotagePause(d time.Duration) {
	time.Sleep(d)
}
