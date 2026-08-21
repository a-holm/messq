// SPDX-License-Identifier: Apache-2.0

package api

import "os"

// SabotageExit ends the process from a library package.
func SabotageExit() {
	os.Exit(1)
}
