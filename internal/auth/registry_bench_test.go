// SPDX-License-Identifier: Apache-2.0

package auth_test

import (
	"testing"

	"github.com/a-holm/messq/internal/auth"
)

// sinkPrincipal and sinkErr keep the compiler from eliminating Verify in the benchmark loop.
// Package-level sinks are the standard way to benchmark a function whose result is not checked.
var (
	sinkPrincipal auth.Principal
	sinkErr       error
)

// BenchmarkVerify measures the three hot-path cases. The unknown-id and wrong-secret paths do
// identical work — one SHA-256 and one constant-time compare against a decoy digest — so their
// timings are expected to sit within noise of each other; the hit path adds only the map
// lookup win. Auth should be invisible against the group-commit fsync of PLAN §12.
func BenchmarkVerify(b *testing.B) {
	tok, cred := mintToken(idPublisher, testSecret)
	reg := auth.NewRegistry([]auth.Token{tok})

	benches := []struct {
		name string
		cred string
	}{
		{name: "hit", cred: cred},
		{name: "unknown-id", cred: "msq1_nobody_" + testSecret},
		{name: "wrong-secret", cred: "msq1_" + idPublisher + "_" + testWrongSecret},
	}
	for _, bm := range benches {
		b.Run(bm.name, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				sinkPrincipal, sinkErr = reg.Verify(bm.cred)
			}
		})
	}
	_, _ = sinkPrincipal, sinkErr
}
