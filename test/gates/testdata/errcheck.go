// SPDX-License-Identifier: Apache-2.0

package store

import "os"

// SabotageErrcheck drops the error from a close that can lose data.
func SabotageErrcheck(f *os.File) {
	f.Close()
}
