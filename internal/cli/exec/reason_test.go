// SPDX-License-Identifier: Apache-2.0

package exec

import (
	"strings"
	"testing"
	"unicode/utf8"
)

func TestSanitizeStderrRules(t *testing.T) {
	esc := "\x1b"
	tests := []struct {
		name string
		in   string
		max  int
		mode StderrMode
		want string
	}{
		{
			// Red killer S1-c: a bare ESC (terminal escape) must never survive;
			// reasons are echoed to terminals by trace/pending/DLQ rendering.
			name: "CSI sequence stripped",
			in:   esc + "[31mred" + esc + "[0m still text",
			max:  4096,
			mode: HeadStderr,
			want: "red still text",
		},
		{name: "OSC stripped to BEL", in: esc + "]0;title\x07tail", max: 100, mode: HeadStderr, want: "tail"},
		{name: "OSC stripped to ST", in: esc + "]8;;http://x" + esc + "\\link", max: 100, mode: HeadStderr, want: "link"},
		{name: "lone ESC dropped with its operand", in: esc + "Mnext", max: 100, mode: HeadStderr, want: "next"},
		{name: "bare ESC at end dropped", in: "line" + esc, max: 100, mode: HeadStderr, want: "line"},
		{name: "NUL dropped", in: "a\x00b", max: 10, mode: HeadStderr, want: "ab"},
		{name: "CRLF normalised", in: "one\r\ntwo", max: 50, mode: HeadStderr, want: "one\ntwo"},
		{name: "lone CR normalised", in: "spin\rwheel\r\rfast", max: 50, mode: HeadStderr, want: "spin\nwheel\n\nfast"},
		{name: "other controls dropped tab kept", in: "k\tv\x01\x02end", max: 50, mode: HeadStderr, want: "k\tvend"},
		{name: "C1 and DEL dropped", in: "keep\x7fme\x9bwise", max: 50, mode: HeadStderr, want: "keepmewise"},
		{name: "invalid utf8 becomes replacement", in: "good\xffbyte", max: 50, mode: HeadStderr, want: "good\uFFFDbyte"},
		{name: "exactly at cap kept whole", in: "abcde", max: 5, mode: HeadStderr, want: "abcde"},
		{name: "one over cap truncated head", in: "abcdef", max: 4, mode: HeadStderr, want: "abcd"},
		{
			// Multi-byte rune straddling the cap: the WHOLE rune goes, so output is
			// shorter than cap but always valid UTF-8 — never a torn tail byte.
			name: "straddling rune dropped on head", in: "ab" + "øøø", max: 3, mode: HeadStderr, want: "ab",
		},
		{
			// Tail keeps the LAST whole runes: last ø is 2 bytes, next would be 4 > cap.
			name: "tail keeps end runes", in: "xxøøøyy", max: 6, mode: TailStderr, want: "øøyy",
		},
		{name: "tail of short input unchanged", in: "abc", max: 64, mode: TailStderr, want: "abc"},
		{name: "zero cap empty", in: "anything", max: 0, mode: HeadStderr, want: ""},
		{name: "negative cap empty", in: "anything", max: -1, mode: TailStderr, want: ""},
		{name: "empty input", in: "", max: 10, mode: HeadStderr, want: ""},
		{
			// Sanity in one shot: the classic crash log.
			name: "composite crash log",
			in:   esc + "[1;31mpanic: boom\x00at h.sh:12\xff\r\n" + esc + "[34mframe#1 oh no",
			max:  4096,
			mode: HeadStderr,
			want: "panic: boomat h.sh:12\uFFFD\nframe#1 oh no",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := SanitizeStderr([]byte(tt.in), tt.max, tt.mode)
			if got != tt.want {
				t.Fatalf("SanitizeStderr(%q, %d, %d):\n got %q\nwant %q", tt.in, tt.max, tt.mode, got, tt.want)
			}
		})
	}
}

// Red killer S1-d is the same assertion driven through realistic compositions:
// whatever else is true, an ANSI escape may never reach a reason string.
func TestSanitizeStderrNeverLetsEscapeSurvive(t *testing.T) {
	for _, in := range []string{
		"\x1b[38;5;196mharmful\x1b[m",
		"\x1b]52;c;base64\x07",
		"text \x1b between",
	} {
		got := SanitizeStderr([]byte(in), 128, HeadStderr)
		if strings.ContainsRune(got, '\x1b') {
			t.Fatalf("SanitizeStderr(%q) = %q: ESC survived", in, got)
		}
	}
}

func FuzzSanitizeStderr(f *testing.F) {
	f.Add([]byte("\x1b[31mERROR\x1b[0mboom\xff\r\n"), 16)
	f.Add([]byte(""), 5)
	f.Add([]byte("øøø straddle"), 7)
	f.Fuzz(func(t *testing.T, in []byte, max int) {
		if max <= 0 {
			max = 0 // exercise the zero floor deterministically
		}
		got := SanitizeStderr(in, max, HeadStderr)
		if !utf8.ValidString(got) {
			t.Fatalf("output invalid UTF-8: %q", got)
		}
		if len(got) > max {
			t.Fatalf("output %d bytes exceeds cap %d", len(got), max)
		}
		for _, r := range got {
			if r == '\x1b' || r == 0 {
				t.Fatalf("forbidden rune %q survived: %q", r, got)
			}
		}
		// Max bytes ≤ cap also holds when fewer runes remain after cleaning, but the
		// truncation contract only pins bytes: assert the FINITE ceiling again here.
		if max > 0 && len(got) > max {
			t.Fatalf("cap violated (%d > %d)", len(got), max)
		}
	})
}
