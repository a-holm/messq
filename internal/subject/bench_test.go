// SPDX-License-Identifier: Apache-2.0

package subject_test

import (
	"fmt"
	"strings"
	"testing"

	"github.com/a-holm/messq/internal/subject"
)

// benchSubject builds a literal subject of n tokens.
func benchSubject(n int) string {
	tokens := make([]string, n)
	for i := range tokens {
		tokens[i] = fmt.Sprintf("token%d", i)
	}
	return strings.Join(tokens, ".")
}

// benchPattern builds a pattern of n tokens in one of the three shapes: all literal, the last
// token a "*", or the last token a ">".
func benchPattern(shape string, n int) string {
	tokens := strings.Split(benchSubject(n), ".")
	switch shape {
	case "star":
		tokens[n-1] = "*"
	case "gt":
		tokens[n-1] = ">"
	}
	return strings.Join(tokens, ".")
}

// BenchmarkMatch is the M1 baseline for the hot path: cursor top-up calls Match once per
// candidate message, so this is per-message cost.
func BenchmarkMatch(b *testing.B) {
	for _, shape := range []string{"literal", "star", "gt"} {
		for _, tokens := range []int{3, 8, 16} {
			pat, err := subject.ParsePattern(benchPattern(shape, tokens))
			if err != nil {
				b.Fatal(err)
			}
			subj := benchSubject(tokens)

			b.Run(fmt.Sprintf("%s/%dtokens", shape, tokens), func(b *testing.B) {
				b.ReportAllocs()
				for b.Loop() {
					matchSink = pat.Match(subj)
				}
			})
		}
	}
}

func BenchmarkSetMatch(b *testing.B) {
	for _, members := range []int{1, 4, 16} {
		raw := make([]string, members)
		for i := range raw {
			raw[i] = fmt.Sprintf("stream%d.%s", i, benchPattern("star", 4))
		}
		set, err := subject.ParseSet(raw)
		if err != nil {
			b.Fatal(err)
		}
		// The worst case for a disjunction: the member that matches is the last one tried.
		subj := fmt.Sprintf("stream%d.%s", members-1, benchSubject(4))

		b.Run(fmt.Sprintf("%dpatterns", members), func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				matchSink = set.Match(subj)
			}
		})
	}
}
