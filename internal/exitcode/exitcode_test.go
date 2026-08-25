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

// TestExitCodeLiteralValues pins every exported constant to its exact integer
// value. The values themselves are the contract (systemd's
// RestartPreventExitStatus="2 78"; sysexits EX_IOERR=74, EX_TEMPFAIL=75), so any
// renumber — e.g. IOERR 74→73 or CONFIG 78→77 — must go RED here first.
func TestExitCodeLiteralValues(t *testing.T) {
	t.Parallel()

	// want is always an integer literal, never derived from the constants under
	// test; got references the constant so a changed definition cannot hide.
	pinned := []struct {
		name string
		got  int
		want int
	}{
		{name: "OK", got: OK, want: 0},
		{name: "Error", got: Error, want: 1},
		{name: "Usage", got: Usage, want: 2},
		{name: "IOERR", got: IOERR, want: 74},
		{name: "TEMPFAIL", got: TEMPFAIL, want: 75},
		{name: "CONFIG", got: CONFIG, want: 78},
	}
	for _, p := range pinned {
		if p.got != p.want {
			t.Errorf("%s = %d, want literal %d (exit-code values are the contract — do not renumber)", p.name, p.got, p.want)
		}
	}
}
