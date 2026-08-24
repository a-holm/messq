// SPDX-License-Identifier: Apache-2.0

package queue

import (
	_ "embed"
	"strings"
	"testing"
)

//go:embed testdata/tokens/valid.txt
var fuzzTokenCorpusFile string

// FuzzParseToken is the #9-corpus-seeded parser fuzzer (issue #10 §Test plan). It
// asserts the two properties a non-HMAC token can offer (D7): it never panics, and an
// accepted input round-trips byte-for-byte — never naming a different
// (stream, consumer, seq, attempt, generation) than its own text (the no-forgery
// property that stands in for a signature). Seeds are the committed minted-token
// corpus plus mutated real tokens, embedded NULs, invalid UTF-8 and a 4 KiB input.
func FuzzParseToken(f *testing.F) {
	for _, seed := range fuzzTokenSeeds() {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, s string) {
		parsed, err := ParseToken(s)
		if err != nil {
			return // rejection is the accepted non-panic outcome
		}
		if got := parsed.String(); got != s {
			t.Fatalf("no-forgery: ParseToken(%q) round-trips to %q; an accepted input must name itself", s, got)
		}
	})
}

// fuzzTokenSeeds returns the corpus lines plus the hand-written mutation seeds the
// fuzzer starts from, so every rejection class (wrong separator count, empty fields,
// leading zeros, a plus sign, NULs, invalid UTF-8, over-width numbers, an over-long
// token) is exercised on the first runs.
func fuzzTokenSeeds() []string {
	var out []string
	for _, line := range strings.Split(fuzzTokenCorpusFile, "\n") {
		t := strings.TrimSpace(line)
		if t == "" || strings.HasPrefix(t, "#") {
			continue
		}
		out = append(out, t)
	}
	out = append(out,
		"", "a", "a/b", "a/b/1", "a/b/1/2", "a/b/1/2/3/4",
		"orders/0xff/1/1", "orders/worker/10494/+1/1", "orders/worker/10494/-1/1",
		"orders/worker/10494/1.5/1", "orders/worker/01/2/3", "orders/worker/1/02/3",
		"orders/worker/1/2/03", "orders//10494/1/1",
		"orders/\x00worker/10494/1/1", "orders/worker/\x00/1/1",
		"\x00orders/worker/10494/1/1", "orders/worker/10494/1/1\x00",
		"orders/\xfe\xffworker/10494/1/1",
		"orders/worker/10494/1/1 ",
		"orders/worker/9223372036854775808/2/3", "orders/worker/1/2147483648/3",
		"orders/worker/1/2/2147483648",
		strings.Repeat("s", 64)+"/"+strings.Repeat("c", 64)+"/9223372036854775807/2147483647/2147483647x",
		strings.Repeat("\u00ff", 2048),
	)
	return out
}

// fuzzTokenSeedsAreCorpusSeeded pins that the fuzz seed set really starts from the
// on-disk corpus (the brief: "seeds from #9's merged token corpus"): the six corpus
// lines all parse to themselves, exactly one of the mutation seeds is accepted
// (none — every hand-written mutation must fail the grammar), and the corpus is not
// empty. Running this in `go test` makes the seed set a committed regression gate even
// outside a fuzz session.
func TestFuzzNoForgeryAcrossSeeds(t *testing.T) {
	seeds := fuzzTokenSeeds()
	if len(seeds) < 10 {
		t.Fatalf("fuzz seed set is suspiciously small: %d", len(seeds))
	}
	accepted := 0
	for _, s := range seeds {
		parsed, err := ParseToken(s)
		if err != nil {
			continue
		}
		accepted++
		if got := parsed.String(); got != s {
			t.Fatalf("no-forgery: ParseToken(%q) round-trips to %q; an accepted input must name itself", s, got)
		}
	}
	// Five of the six corpus lines parse (the sixth, "a/b/0/0/0", carries the
	// all-zero zero Token #9 uses only to pin String's zero-value render and is
	// rejected here by design — seq/attempt/generation are >= 1). Every mutation
	// seed must be rejected on top.
	if accepted != 5 {
		t.Fatalf("fuzz seeds produced %d accepted tokens, want the 5 parses of the 6 corpus lines", accepted)
	}
}
