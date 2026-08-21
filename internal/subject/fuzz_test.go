// SPDX-License-Identifier: Apache-2.0

package subject_test

import (
	"errors"
	"strings"
	"testing"
	"unicode"
	"unicode/utf8"

	"github.com/a-holm/messq/internal/errs"
	"github.com/a-holm/messq/internal/subject"
)

// The reference implementation below is deliberately naive: it splits on dots, allocates
// freely and reads like the rules table. The compiled matcher is the one that has to be fast;
// this one only has to be obviously correct, and the fuzz targets fail on any disagreement.

// refValidToken reports whether tok is a legal literal token (rules S2 and S3).
func refValidToken(tok string) bool {
	if tok == "" {
		return false
	}
	for _, r := range tok {
		if r == '.' || r == '*' || r == '>' || r == ' ' || unicode.IsControl(r) {
			return false
		}
	}
	return true
}

// refValidSubject implements rules S1 to S4 and S10 by hand.
func refValidSubject(s string) bool {
	if s == "" || len(s) > 512 || !utf8.ValidString(s) {
		return false
	}
	tokens := strings.Split(s, ".")
	if len(tokens) > 32 {
		return false
	}
	for _, tok := range tokens {
		if !refValidToken(tok) {
			return false
		}
	}
	return true
}

// refValidPattern implements rules S1 to S9 by hand.
func refValidPattern(s string) bool {
	if s == "" || len(s) > 512 || !utf8.ValidString(s) {
		return false
	}
	tokens := strings.Split(s, ".")
	if len(tokens) > 32 {
		return false
	}
	for i, tok := range tokens {
		switch tok {
		case "*":
		case ">":
			if i != len(tokens)-1 {
				return false
			}
		default:
			if !refValidToken(tok) {
				return false
			}
		}
	}
	return true
}

// refMatch is the naive matcher the compiled one is differentially tested against. It assumes
// pat is a valid pattern and makes no assumption at all about s.
func refMatch(pat, s string) bool {
	if s == "" {
		return false
	}
	subjTokens := strings.Split(s, ".")
	for _, tok := range subjTokens {
		// The same structural floor the compiled matcher applies: an empty token is not a
		// token, and a wildcard character standing alone means a pattern was handed in
		// where a subject was expected.
		if tok == "" || tok == "*" || tok == ">" {
			return false
		}
	}

	patTokens := strings.Split(pat, ".")
	for i, tok := range patTokens {
		if tok == ">" {
			return len(subjTokens) > i
		}
		if i >= len(subjTokens) {
			return false
		}
		if tok != "*" && tok != subjTokens[i] {
			return false
		}
	}
	return len(subjTokens) == len(patTokens)
}

// FuzzMatch is the differential target of PLAN.md section 11.6. A matcher bug misroutes
// messages silently and never raises an error, so the only way to trust it is to compare it
// against an implementation written from the rules by a different route.
func FuzzMatch(f *testing.F) {
	for _, row := range truthTable {
		f.Add(row.Pattern, row.Subject)
	}
	for _, row := range rejectTable {
		f.Add(row.In, row.In)
		f.Add(">", row.In)
	}
	for _, seed := range fuzzSeeds {
		f.Add(seed, seed)
		f.Add("a.>", seed)
		f.Add("*.*", seed)
	}

	f.Fuzz(func(t *testing.T, pat, s string) {
		compiled, err := subject.ParsePattern(pat)
		if err != nil {
			// The pattern is not a pattern: nothing to compare, but the matcher must
			// still refuse to match through the zero value.
			if compiled.Match(s) {
				t.Fatalf("a pattern that failed to parse matched %q", s)
			}
			return
		}

		got := compiled.Match(s)
		want := refMatch(pat, s)
		if got != want {
			t.Fatalf("Match(%q, %q) = %t, naive reference says %t", pat, s, got, want)
		}

		// A subject that the grammar accepts and that equals the pattern's literal form
		// must match, or a publish would be routed nowhere.
		if compiled.IsLiteral() && s == pat && refValidSubject(s) && !got {
			t.Fatalf("literal pattern %q does not match its own subject", pat)
		}
		if got && !strings.HasPrefix(s, compiled.Prefix()) {
			t.Fatalf("Match(%q, %q) = true but the subject does not start with the prefix %q", pat, s, compiled.Prefix())
		}
	})
}

// FuzzParsePattern asserts that accept/reject agrees with the naive validator, that an
// accepted pattern round-trips through String, and that nothing panics.
func FuzzParsePattern(f *testing.F) {
	for _, row := range truthTable {
		f.Add(row.Pattern)
		f.Add(row.Subject)
	}
	for _, row := range rejectTable {
		f.Add(row.In)
	}
	for _, seed := range fuzzSeeds {
		f.Add(seed)
	}

	f.Fuzz(func(t *testing.T, s string) {
		pat, err := subject.ParsePattern(s)
		if want := refValidPattern(s); (err == nil) != want {
			t.Fatalf("ParsePattern(%q) err = %v, naive validator says valid = %t", s, err, want)
		}
		if err != nil {
			if !errors.Is(err, errs.ErrBadSubject) {
				t.Fatalf("ParsePattern(%q) = %v, which does not classify as errs.ErrBadSubject", s, err)
			}
			return
		}

		if pat.String() != s {
			t.Fatalf("ParsePattern(%q).String() = %q", s, pat.String())
		}
		again, err := subject.ParsePattern(pat.String())
		if err != nil {
			t.Fatalf("re-parsing %q = %v", pat.String(), err)
		}
		if again.String() != pat.String() {
			t.Fatalf("round trip changed %q into %q", pat.String(), again.String())
		}
		if pat.MinTokens() < 1 {
			t.Fatalf("ParsePattern(%q).MinTokens() = %d", s, pat.MinTokens())
		}

		// A subject-shaped pattern is also a subject, and the two parsers must agree on
		// which inputs are wildcard-free.
		_, subjErr := subject.ParseSubject(s)
		if pat.IsLiteral() != (subjErr == nil) {
			t.Fatalf("ParsePattern(%q).IsLiteral() = %t but ParseSubject err = %v", s, pat.IsLiteral(), subjErr)
		}
	})
}

// fuzzSeeds are the adversarial shapes worth starting from: deep nesting, the byte limits, the
// separator alone, and every reserved character in isolation.
var fuzzSeeds = []string{
	"",
	".",
	"..",
	"...",
	">",
	"*",
	".>",
	">.",
	"*.",
	".*",
	"a",
	"a.",
	".a",
	"a..b",
	"a.>",
	"a.*",
	"*.>",
	">.>",
	"*.*",
	"a.>.b",
	"foo>",
	"foo*",
	"f*o",
	"a\x00b",
	"a\nb",
	"a\tb",
	"a b",
	"a\xffb",
	"å.ø.æ",
	"ÿ",
	"\U0001F600",
	strings.Repeat("a", 512),
	strings.Repeat("a", 513),
	strings.TrimSuffix(strings.Repeat("a.", 32), "."),
	strings.TrimSuffix(strings.Repeat("a.", 33), "."),
	strings.Repeat(".", 64),
	strings.Repeat("*.", 32) + ">",
	strings.TrimSuffix(strings.Repeat(">.", 32), "."),
}
