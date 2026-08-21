// SPDX-License-Identifier: Apache-2.0

package queue

import "time"

// SabotageNow reads the wall clock from inside the pure state machine.
func SabotageNow() time.Time {
	return time.Now()
}
