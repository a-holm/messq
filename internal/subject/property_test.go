// SPDX-License-Identifier: Apache-2.0

package subject_test

import (
	"strings"
	"testing"

	"github.com/a-holm/messq/internal/subject"
	"pgregory.net/rapid"
)

// genToken draws a literal token: short, from an alphabet that includes the multi-byte runes
// the grammar has to carry unchanged.
func genToken() *rapid.Generator[string] {
	return rapid.Map(
		rapid.SliceOfN(rapid.SampledFrom([]string{"a", "b", "Z", "9", "_", "-", "å", "字"}), 1, 4),
		func(parts []string) string { return strings.Join(parts, "") },
	)
}

// genSubject draws a valid literal subject.
func genSubject() *rapid.Generator[string] {
	return rapid.Map(
		rapid.SliceOfN(genToken(), 1, 6),
		func(tokens []string) string { return strings.Join(tokens, ".") },
	)
}

// genPattern draws a valid pattern: literal tokens and `*` anywhere, `>` only last.
func genPattern() *rapid.Generator[string] {
	return rapid.Custom(func(t *rapid.T) string {
		tokens := rapid.SliceOfN(rapid.OneOf(genToken(), rapid.Just("*")), 1, 6).Draw(t, "tokens")
		if rapid.Bool().Draw(t, "trailingGT") {
			tokens = append(tokens, ">")
		}
		return strings.Join(tokens, ".")
	})
}

// TestMatchProperties checks the invariants that no finite table can: they must hold for every
// pattern and every subject the generators can produce.
func TestMatchProperties(t *testing.T) {
	t.Parallel()

	t.Run("a literal pattern matches its own subject and nothing else", rapid.MakeCheck(func(t *rapid.T) {
		subj := genSubject().Draw(t, "subject")
		other := genSubject().Draw(t, "other")

		pat, err := subject.ParsePattern(subj)
		if err != nil {
			t.Fatalf("ParsePattern(%q) = %v", subj, err)
		}
		if !pat.IsLiteral() {
			t.Fatalf("ParsePattern(%q).IsLiteral() = false", subj)
		}
		if !pat.Match(subj) {
			t.Fatalf("%q does not match itself", subj)
		}
		if got, want := pat.Match(other), other == subj; got != want {
			t.Fatalf("Match(%q, %q) = %t, want %t", subj, other, got, want)
		}
	}))

	t.Run("the bare pattern matches every valid subject", rapid.MakeCheck(func(t *rapid.T) {
		subj := genSubject().Draw(t, "subject")
		pat, err := subject.ParsePattern(">")
		if err != nil {
			t.Fatal(err)
		}
		if !pat.Match(subj) {
			t.Fatalf(`ParsePattern(">").Match(%q) = false`, subj)
		}
	}))

	t.Run("a pattern without > fixes the token count", rapid.MakeCheck(func(t *rapid.T) {
		raw := genPattern().Draw(t, "pattern")
		if strings.HasSuffix(raw, ">") {
			t.Skip("this property is about patterns with no wildcard tail")
		}
		subj := genSubject().Draw(t, "subject")

		pat, err := subject.ParsePattern(raw)
		if err != nil {
			t.Fatalf("ParsePattern(%q) = %v", raw, err)
		}
		if pat.Match(subj) && strings.Count(subj, ".") != strings.Count(raw, ".") {
			t.Fatalf("%q matched %q despite a different token count", raw, subj)
		}
	}))

	t.Run("Set.Match is the OR of its members", rapid.MakeCheck(func(t *rapid.T) {
		raws := rapid.SliceOfN(genPattern(), 1, 4).Draw(t, "patterns")
		subj := genSubject().Draw(t, "subject")

		set, err := subject.ParseSet(raws)
		if err != nil {
			t.Fatalf("ParseSet(%q) = %v", raws, err)
		}

		var want bool
		for _, raw := range raws {
			pat, err := subject.ParsePattern(raw)
			if err != nil {
				t.Fatalf("ParsePattern(%q) = %v", raw, err)
			}
			want = want || pat.Match(subj)
		}
		if got := set.Match(subj); got != want {
			t.Fatalf("Set%q.Match(%q) = %t, want %t", raws, subj, got, want)
		}
	}))

	t.Run("Match agrees with the naive reference", rapid.MakeCheck(func(t *rapid.T) {
		raw := genPattern().Draw(t, "pattern")
		subj := genSubject().Draw(t, "subject")

		pat, err := subject.ParsePattern(raw)
		if err != nil {
			t.Fatalf("ParsePattern(%q) = %v", raw, err)
		}
		if got, want := pat.Match(subj), refMatch(raw, subj); got != want {
			t.Fatalf("Match(%q, %q) = %t, naive reference says %t", raw, subj, got, want)
		}
	}))

	t.Run("Match is deterministic and String round-trips", rapid.MakeCheck(func(t *rapid.T) {
		raw := genPattern().Draw(t, "pattern")
		subj := genSubject().Draw(t, "subject")

		pat, err := subject.ParsePattern(raw)
		if err != nil {
			t.Fatalf("ParsePattern(%q) = %v", raw, err)
		}
		first := pat.Match(subj)
		for range 3 {
			if pat.Match(subj) != first {
				t.Fatalf("Match(%q, %q) is not deterministic", raw, subj)
			}
		}
		if pat.String() != raw {
			t.Fatalf("String() = %q, want %q", pat.String(), raw)
		}
		again, err := subject.ParsePattern(pat.String())
		if err != nil {
			t.Fatalf("re-parsing %q = %v", pat.String(), err)
		}
		if again.Match(subj) != first {
			t.Fatalf("the round-tripped pattern %q disagrees on %q", raw, subj)
		}
	}))

	t.Run("the prefix is a prefix of every match", rapid.MakeCheck(func(t *rapid.T) {
		raw := genPattern().Draw(t, "pattern")
		subj := genSubject().Draw(t, "subject")

		pat, err := subject.ParsePattern(raw)
		if err != nil {
			t.Fatalf("ParsePattern(%q) = %v", raw, err)
		}
		if pat.Match(subj) && !strings.HasPrefix(subj, pat.Prefix()) {
			t.Fatalf("%q matched %q but the subject does not start with the prefix %q", raw, subj, pat.Prefix())
		}
	}))
}

// TestNamePropertiesHoldForTheDLQDerivation is decision D3 as a property rather than as a
// table: whatever stream name is accepted, the dead-letter stream derived from it is accepted
// too.
func TestNamePropertiesHoldForTheDLQDerivation(t *testing.T) {
	t.Parallel()

	rapid.Check(t, func(t *rapid.T) {
		name := rapid.StringMatching(`[A-Za-z0-9_.-]{1,70}`).Draw(t, "name")
		if subject.ValidateNewStreamName(name) != nil {
			t.Skip("only accepted names carry the obligation")
		}
		if err := subject.ValidateStreamName(name + ".dlq"); err != nil {
			t.Fatalf("%q is accepted but %q.dlq is not: %v", name, name, err)
		}
	})
}
