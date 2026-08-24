// SPDX-License-Identifier: Apache-2.0

package model_test

import (
	"flag"
	"strings"
	"testing"

	"github.com/a-holm/messq/internal/model"
	"github.com/a-holm/messq/internal/subject"
	"pgregory.net/rapid"
)

// TestMain pins the differential's rapid budget to a deterministic seed and a raised check
// count (#49). With rapid's library default (100 checks, a fresh random seed every run) the
// model-vs-subject differential caught the matcher-prefix mutant only 14/15 runs — one run in
// fifteen passed green with the mutant planted. A fixed seed makes the draw sequence
// reproducible, and the raised count makes a divergence such as a prefix-tolerant literal
// essentially certain to be drawn; the two together keep this conformance test from being
// a probabilistic one.
func TestMain(m *testing.M) {
	mustFlagSet("rapid.seed", "0x49A11")
	mustFlagSet("rapid.checks", "4096")
	m.Run()
}

func mustFlagSet(name, value string) {
	if err := flag.Set(name, value); err != nil {
		panic("set " + name + "=" + value + ": " + err.Error())
	}
}

// genToken draws a literal subject token, avoiding the wildcard characters so the generated
// strings are always valid subjects and valid patterns.
func genToken() *rapid.Generator[string] {
	return rapid.Map(
		rapid.SliceOfN(rapid.SampledFrom([]string{"a", "b", "Z", "9", "_", "-", "å", "字"}), 1, 3),
		func(parts []string) string { return strings.Join(parts, "") },
	)
}

// genSubject draws a valid literal subject.
func genSubject() *rapid.Generator[string] {
	return rapid.Map(
		rapid.SliceOfN(genToken(), 1, 5),
		func(tokens []string) string { return strings.Join(tokens, ".") },
	)
}

// genPattern draws a valid pattern: literal tokens and "*" anywhere, ">" only last.
func genPattern() *rapid.Generator[string] {
	return rapid.Custom(func(t *rapid.T) string {
		n := rapid.IntRange(1, 6).Draw(t, "tokens")
		toks := make([]string, 0, n)
		for range n {
			// A "*" is a valid whole token anywhere; literal tokens elsewhere.
			if rapid.Bool().Draw(t, "star") {
				toks = append(toks, "*")
			} else {
				toks = append(toks, genToken().Draw(t, "lit"))
			}
		}
		if rapid.Bool().Draw(t, "trailingGT") {
			toks = append(toks, ">")
		}
		return strings.Join(toks, ".")
	})
}

// TestMatchDifferentialAgainstSubject runs every generated (pattern, subject) pair against
// both the subject matcher and the naive model matcher, and requires them to agree. The two
// are written from the same SEMANTICS prose, so agreement is the conformance signal; a
// divergence means one side transcribed the filter rules wrong.
func TestMatchDifferentialAgainstSubject(t *testing.T) {
	t.Parallel()

	rapid.Check(t, func(rt *rapid.T) {
		patRaw := genPattern().Draw(rt, "pattern")
		subj := genSubject().Draw(rt, "subject")

		pat, err := subject.ParsePattern(patRaw)
		if err != nil {
			rt.Fatalf("subject.ParsePattern(%q) failed: %v", patRaw, err)
		}
		want := pat.Match(subj)
		got := model.Pattern(patRaw).Match(subj)
		if got != want {
			rt.Fatalf("model.Pattern(%q).Match(%q) = %v, subject matcher says %v", patRaw, subj, got, want)
		}
	})
}

// TestMatchUnit pins the transcribed rules from SEMANTICS.md S5–S10 with hand-chosen rows,
// the way the subject matcher's own rules_test.go does.
func TestMatchUnit(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name string
		pat  string
		subj string
		want bool
	}{
		{name: "literal equality", pat: "a.b", subj: "a.b", want: true},
		{name: "literal inequality", pat: "a.b", subj: "a.c", want: false},
		{name: "star matches one token", pat: "a.*.c", subj: "a.b.c", want: true},
		{name: "star rejects empty slot", pat: "a.*", subj: "a", want: false},
		{name: "gt matches one trailing", pat: "a.>", subj: "a.b", want: true},
		{name: "gt matches many trailing", pat: "a.>", subj: "a.b.c.d", want: true},
		{name: "gt requires a token", pat: "a.>", subj: "a", want: false},
		{name: "bare gt matches any", pat: ">", subj: "a.b", want: true},
		{name: "star then gt", pat: "*.>", subj: "a.b.c", want: true},
		{name: "empty subject", pat: "a.b", subj: "", want: false},
		{name: "too few subject tokens", pat: "a.b.c", subj: "a.b", want: false},
		{name: "extra subject tokens (no gt)", pat: "a.b", subj: "a.b.c", want: false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := model.Pattern(tc.pat).Match(tc.subj); got != tc.want {
				t.Errorf("Pattern(%q).Match(%q) = %v, want %v", tc.pat, tc.subj, got, tc.want)
			}
		})
	}
}
