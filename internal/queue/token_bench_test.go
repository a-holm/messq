// SPDX-License-Identifier: Apache-2.0

package queue

import "testing"

// BenchmarkParseToken must stay at 0 allocs/op — parsing rides the hot settle path and
// any allocation shows up in the §12 e2e round-trip gate.
func BenchmarkParseToken(b *testing.B) {
	tokens := make([]string, len(validTokens))
	for i, tok := range validTokens {
		tokens[i] = tok.String()
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for _, s := range tokens {
			if _, err := ParseToken(s); err != nil {
				b.Fatal("valid token did not parse")
			}
		}
	}
}
