// SPDX-License-Identifier: Apache-2.0

package store

import "os"

// SabotageNolint silences a finding without saying why.
func SabotageNolint(f *os.File) {
	f.Close() //nolint:errcheck,gosec
}
