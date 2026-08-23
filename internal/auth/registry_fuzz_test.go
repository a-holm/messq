// SPDX-License-Identifier: Apache-2.0

package auth_test

import (
	"testing"

	"github.com/a-holm/messq/internal/auth"
)

// FuzzParseCredential asserts the forgery property: against a registry holding exactly one
// minted credential, no other input ever verifies. The stored hash covers the whole presented
// string (prefix and id included), so a non-minted input that verifies would be a SHA-256
// break, not a logic bug.
func FuzzParseCredential(f *testing.F) {
	tok, cred := mintToken(idPublisher, testSecret)
	reg := auth.NewRegistry([]auth.Token{tok})

	f.Add(cred)
	f.Add("msq1_" + idPublisher + "_" + testWrongSecret)
	f.Add("msq1_nobody_" + testSecret)
	f.Add("")
	f.Add("garbage")

	f.Fuzz(func(t *testing.T, input string) {
		p, err := reg.Verify(input)
		if err == nil {
			if input != cred {
				t.Fatalf("forgery: input %q verified as %q", input, p.Actor())
			}
			if p.Actor() != "tok:"+idPublisher {
				t.Fatalf("minted credential verified as %q, want tok:%s", p.Actor(), idPublisher)
			}
		}
	})
}
