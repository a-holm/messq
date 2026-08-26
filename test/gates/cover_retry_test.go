// SPDX-License-Identifier: Apache-2.0

//go:build gatecheck

package gates

import "testing"

// TestObscuredByEmbeddedSuiteFailure pins the retry classifier's blast radius. The
// retry exists so an unrelated load-flaky test inside a cover row's full suite cannot
// decide the row ("wrong reason"); every case below that must NOT retry is an
// invariant: a real floor breach, a tampered gate, a gate that stops biting, a scratch
// build failure and a hung run all have to stay red on their first transcript.
func TestObscuredByEmbeddedSuiteFailure(t *testing.T) {
	g12 := gate{id: "G12", name: "a floored package below its floor", target: "cover", want: "< 90.0%"}
	b2 := gate{id: "B2", name: "an unsabotaged copy meets its floors", target: "cover", want: "covergate: OK", wantOK: true}
	g17 := gate{id: "G17", name: "a data race in a test", target: "test", want: "DATA RACE"}
	g1 := gate{id: "G1", name: "time.Sleep in a test", target: "lint", want: "time.Sleep is banned"}

	embeddedFailure := "\n--- FAIL: TestNoLostWakeupUnderRapidInterleaving (11.45s)\n" +
		"\tFAIL\tgithub.com/a-holm/messq/internal/api\t60.305s\nFAIL\n"
	realBite := "\ncovergate: FAIL    internal/queue            82.82% < 90.0% (PLAN.md section 11)\n"

	for name, tc := range map[string]struct {
		g      gate
		code   int
		output string
		want   bool
	}{
		"embedded test failure hides the sentinel": {
			g12, 2,
			"CGO_ENABLED=1 go test -race -count=1 ...\n" + embeddedFailure, true,
		},
		"same for the green leg": {
			b2, 2,
			"CGO_ENABLED=1 go test -race -count=1 ...\n" + embeddedFailure, true,
		},
		"a real floor breach decided the row": {
			g12, 2,
			embeddedFailure + realBite + "\nnext: go tool cover -html=cover.out\n", false,
		},
		"gate rendered any verdict":                 {g12, 2, "covergate: OK      internal/queue   97.37% >= 90.0%\n", false},
		"exit 0 never retries (gate does not bite)": {g12, 0, "everything fine\n", false},
		"killed run never retries (hang)":           {g12, -1, "--- FAIL: nothing decisive\n", false},
		"scratch build failure stays red": {
			g12, 2,
			"\tFAIL\tgithub.com/a-holm/messq/internal/store [setup failed]\nFAIL\n", false,
		},
		"G17 wants the embedded failure": {g17, 2, embeddedFailure + "\tWARNING: DATA RACE\n", false},
		"lint rows are deterministic":    {g1, 2, embeddedFailure, false},
	} {
		if got := obscuredByEmbeddedSuiteFailure(tc.g, tc.code, tc.output); got != tc.want {
			t.Errorf("%s: obscuredByEmbeddedSuiteFailure = %v, want %v", name, got, tc.want)
		}
	}
}
