// SPDX-License-Identifier: Apache-2.0

package exec

import (
	"errors"
	"strings"
	"testing"
)

func TestSplitWordsQuoting(t *testing.T) {
	tests := []struct {
		name string
		in   string
		want []string
	}{
		{"plain", "run-job --flag v", []string{"run-job", "--flag", "v"}},
		{"collapses runs", "  a   b\tc\n", []string{"a", "b", "c"}},
		{"empty", "", nil},
		{"only space", "   ", nil},
		{"double quoted keeps spaces", `./h.sh "two words"`, []string{"./h.sh", "two words"}},
		{"single quoted is literal", `grep 'foo bar' x`, []string{"grep", "foo bar", "x"}},
		{"single quotes make backslash literal", `echo 'a\b'`, []string{"echo", `a\b`}},
		{"double quote backslash escapes", `"a\"b"`, []string{`a"b`}},
		{"double backslash closes escape, adjacent word joins", `"a\\"b`, []string{`a\b`}},
		{"outside quotes escapes next", `a\ b`, []string{"a b"}},
		{"escaped quote outside", `\--x`, []string{"--x"}},
		{"empty segment is a token", `a "" b`, []string{"a", "", "b"}},
		{"adjacent quotes join words", `"ab"'cd'ef`, []string{"abcdef"}},
		{"dollar not expanded", "$HOME/x", []string{"$HOME/x"}},
		{"glob not expanded", "*.log", []string{"*.log"}},
		{"pipe literal", "a|b>c", []string{"a|b>c"}},
		{"utf8 word", "høst 🎉", []string{"høst", "🎉"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := SplitWords(tt.in)
			if err != nil {
				t.Fatalf("SplitWords(%q) unexpected error: %v", tt.in, err)
			}
			if len(got) != len(tt.want) {
				t.Fatalf("SplitWords(%q) = %#v, want %#v", tt.in, got, tt.want)
			}
			for i := range got {
				if got[i] != tt.want[i] {
					t.Fatalf("SplitWords(%q)[%d] = %q, want %q (full: %#v)", tt.in, i, got[i], tt.want[i], got)
				}
			}
		})
	}
}

// Red killer S1-a: quotes that never close must ERROR, never silently truncate.
// A naive splitter would turn `messq sub s c --exec './worker.sh 'oops` into an
// argv that drops half the operator's command.
func TestSplitWordsUnterminatedQuoteErrors(t *testing.T) {
	for _, in := range []string{`'never closed`, `"also open`, `ok "then nothing`, `"a\\"b"`} {
		got, err := SplitWords(in)
		if err == nil {
			t.Fatalf("SplitWords(%q) = %#v, want ErrUnterminatedQuote", in, got)
		}
		if !errors.Is(err, ErrUnterminatedQuote) {
			t.Fatalf("SplitWords(%q) error = %v, want errors.Is(ErrUnterminatedQuote)", in, err)
		}
	}
}

// Red killer S1-b: a trailing lone backslash has nothing to escape — it is either
// a typo or a cut-and-paste casualty; refuse rather than guess.
func TestSplitWordsTrailingBackslashErrors(t *testing.T) {
	for _, in := range []string{`dir\`, `"quoted end\`} {
		if _, err := SplitWords(in); err == nil {
			t.Fatalf("SplitWords(%q): want error for dangling backslash", in)
		}
	}
}

func FuzzSplitWords(f *testing.F) {
	f.Add("")
	f.Add("./handle.sh --mode fast")
	f.Add(`json '{"k": "v \"nested\""}' tail`)
	f.Add("back\\slash trail\\")
	f.Add("'unterminated")
	f.Add(`"double unterminated`)
	f.Add("$HOME/*.log | grep >> out")
	f.Fuzz(func(t *testing.T, line string) {
		tokens, err := SplitWords(line)
		if err != nil {
			return // rejected inputs are fine; only successful splits carry properties
		}
		if len(tokens) == 0 {
			if strings.TrimSpace(line) == "" && !strings.ContainsAny(line, "\"'\\") {
				return
			}
			return
		}
		// Property 1: re-quoting every token and re-splitting reproduces the argv.
		var b strings.Builder
		for i, tok := range tokens {
			if i > 0 {
				b.WriteByte(' ')
			}
			b.WriteString(quoteForFuzz(tok))
		}
		again, err := SplitWords(b.String())
		if err != nil || len(again) != len(tokens) {
			t.Fatalf("round trip unstable: SplitWords(%q)=%#v err=%v; from %#v via %q",
				b.String(), again, err, tokens, line)
		}
		for i := range tokens {
			if tokens[i] != again[i] {
				t.Fatalf("round trip changed token %d: %q -> %q (line %q)",
					i, tokens[i], again[i], line)
			}
		}
	})
}

// quoteForFuzz renders one token as a double-quoted shell-safe string under the
// grammar this package documents.
func quoteForFuzz(s string) string {
	var b strings.Builder
	b.WriteByte('"')
	for i := 0; i < len(s); i++ {
		switch c := s[i]; c {
		case '\\', '"':
			b.WriteByte('\\')
			b.WriteByte(c)
		default:
			b.WriteByte(c)
		}
	}
	b.WriteByte('"')
	return b.String()
}
