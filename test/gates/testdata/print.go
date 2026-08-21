// SPDX-License-Identifier: Apache-2.0

package queue

import "fmt"

// SabotagePrint writes to the process stdout from a library package.
func SabotagePrint() {
	fmt.Println("pending")
}
