// SPDX-License-Identifier: Apache-2.0

package queue

import "fmt"

// SabotageVet renders a string through an integer verb, which compiles and is wrong.
func SabotageVet() string {
	return fmt.Sprintf("%d", "not a number")
}
