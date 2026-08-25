// SPDX-License-Identifier: Apache-2.0

//go:build gatecheck

package gates

import (
	"strings"
	"testing"
)

// TestStripDownloadNoise pins the exact line grammar of the shared chatter filter: whole lines
// that are nothing but downloader progress vanish (with or without a module name, LF or CRLF),
// while everything else — gate verdicts, sentinels, and downloader ERROR text — survives
// byte-for-byte. A failed download keeps failing its row through its exit code and missing
// sentinel; only benign progress is noise.
func TestStripDownloadNoise(t *testing.T) {
	tests := []struct {
		name string
		in   string
		want string
	}{
		{
			name: "progress line with a module",
			in:   "go: downloading github.com/prometheus/client_golang v1.24.1\n",
			want: "",
		},
		{
			name: "bare progress line without a module",
			in:   "go: downloading\n",
			want: "",
		},
		{
			name: "CRLF progress line",
			in:   "go: downloading golang.org/x/tools v0.24.0\r\ngate verdict\r\n",
			want: "gate verdict\r\n",
		},
		{
			name: "a FAILED download is error text, not chatter",
			in:   "go: github.com/example.invalid/x@v0.1.0: verifying module: checksum mismatch\n",
			want: "go: github.com/example.invalid/x@v0.1.0: verifying module: checksum mismatch\n",
		},
		{
			name: "a line merely mentioning downloads elsewhere is kept",
			in:   "covergate: FAIL 82.82% < 90.0% after go: downloading was observed\n",
			want: "covergate: FAIL 82.82% < 90.0% after go: downloading was observed\n",
		},
		{
			name: "mixed transcript keeps order and content",
			in: "make lint: 47 issues\n" +
				"go: downloading mvdan.cc/gofumpt v0.7.0\n" +
				"sabotage.go:9:2: time.Sleep is banned (banned-time)\n",
			want: "make lint: 47 issues\nsabotage.go:9:2: time.Sleep is banned (banned-time)\n",
		},
		{
			name: "empty input stays empty",
			in:   "",
			want: "",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := stripDownloadNoise(tt.in); got != tt.want {
				t.Fatalf("stripDownloadNoise(%q) = %q, want %q", tt.in, got, tt.want)
			}
		})
	}
}

// TestDecideChatterNeverDecidesARow walks EVERY row of the matrix and pins the #gates-fix-r2
// invariant at both ends:
//
//  1. No row can fail solely because its transcript carries benign `go: downloading ...`
//     chatter — with the row's own exit code and sentinel present, chatter-polluted output
//     passes on every target (tidy, lint, fmt, cover, ratchet, seam-defaults, ...).
//  2. Chatter decides nothing in the other direction either: chatter ALONE never passes a row,
//     an absent sentinel stays red, and a wrong exit code stays red.
//
// This is the guard that keeps the tolerance global: a future row that grew its own
// cache-temperature-dependent comparison would have to bypass decide, and these cases would
// still have to hold for it to ship.
func TestDecideChatterNeverDecidesARow(t *testing.T) {
	const chatter = "go: downloading golang.org/x/tools v0.24.0\n" +
		"go: downloading github.com/prometheus/client_golang v1.24.1\n" +
		"go: downloading klauspost/compress v1.17.9 => github.com/klauspost/compress v1.17.9\n" +
		"go: downloading\n"

	for _, g := range matrix() {
		t.Run(g.id+"_"+g.target, func(t *testing.T) {
			okCode := 2 // a gate that bites exits nonzero...
			if g.wantOK {
				okCode = 0 // ...unless the row asserts success.
			}
			sentinel := "make " + g.target + ": " + g.want + "\n"

			passCases := []struct {
				name       string
				code       int
				transcript string
			}{
				{"clean transcript", okCode, sentinel},
				{"chatter before the sentinel", okCode, chatter + sentinel},
				{"chatter around the sentinel", okCode, chatter + sentinel + chatter},
				{
					"chatter interleaved mid-line-stream", okCode,
					"head of the run\n" + chatter + sentinel + "\ntrailer\n",
				},
			}
			for _, tc := range passCases {
				t.Run("passes/"+tc.name, func(t *testing.T) {
					ok, why := decide(g, tc.code, tc.transcript)
					if !ok {
						t.Fatalf("decide(%s, exit=%d) rejected a transcript whose own sentinel is present: %s", g.id, tc.code, why)
					}
				})
			}

			failCases := []struct {
				name       string
				code       int
				transcript string
			}{
				{"chatter alone decides nothing", okCode, chatter},
				{"absent sentinel stays red", okCode, chatter + "unrelated output, no sentinel here\n"},
			}
			if g.wantOK {
				failCases = append(failCases,
					struct {
						name       string
						code       int
						transcript string
					}{"nonzero exit stays red even with the sentinel", 2, chatter + sentinel})
			} else {
				failCases = append(failCases,
					struct {
						name       string
						code       int
						transcript string
					}{"exit zero when the gate must bite", 0, chatter + sentinel})
			}
			for _, tc := range failCases {
				t.Run("fails/"+tc.name, func(t *testing.T) {
					ok, why := decide(g, tc.code, tc.transcript)
					if ok {
						t.Fatalf("decide(%s, exit=%d) accepted a transcript that must stay red (%s)", g.id, tc.code, tc.name)
					}
					if !strings.Contains(why, g.id) {
						t.Fatalf("failure reason does not name the row: %q", why)
					}
				})
			}
		})
	}
}
