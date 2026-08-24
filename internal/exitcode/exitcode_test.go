// SPDX-License-Identifier: Apache-2.0

package exitcode

import "testing"

// TestServeCodesDoNotCollideWithClientRange pins the split from issue #17's body:
// serve's sysexits values (74/75/78) must never land in the client contract's 3–7
// band, and every documented code is distinct.
func TestServeCodesDoNotCollideWithClientRange(t *testing.T) {
	t.Parallel()

	codes := map[string]int{
		"OK":       OK,
		"Error":    Error,
		"Usage":    Usage,
		"IOERR":    IOERR,
		"TEMPFAIL": TEMPFAIL,
		"CONFIG":   CONFIG,
	}
	seen := map[int]string{}
	for name, code := range codes {
		if prev, dup := seen[code]; dup {
			t.Fatalf("%s collides with %s on exit code %d", name, prev, code)
		}
		seen[code] = name
	}
	for _, c := range []int{IOERR, TEMPFAIL, CONFIG} {
		if c >= 3 && c <= 7 {
			t.Fatalf("serve sysexits value %d invades the client 3–7 band", c)
		}
	}
}
