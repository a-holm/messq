// SPDX-License-Identifier: Apache-2.0

package auth_test

import (
	"crypto/sha256"
	"errors"
	"strings"
	"testing"

	"github.com/a-holm/messq/internal/auth"
	"github.com/a-holm/messq/internal/errs"
)

const (
	testSecret      = "secretvalue1234567890"
	testWrongSecret = "wrongsecretvalue1234"
)

// mintToken builds a token and the one credential that verifies as it: the SHA-256 covers the
// whole presented string, prefix and id included.
func mintToken(id, secret string) (auth.Token, string) {
	cred := "msq1_" + id + "_" + secret
	hash := sha256.Sum256([]byte(cred))
	tok := auth.Token{
		ID:       id,
		Hash:     hash,
		Roles:    mustRoleSet("publish"),
		Patterns: []auth.Pattern{mustPattern("orders")},
	}
	return tok, cred
}

func TestVerifySuccess(t *testing.T) {
	t.Parallel()

	tok, cred := mintToken(idPublisher, testSecret)
	reg := auth.NewRegistry([]auth.Token{tok})

	p, err := reg.Verify(cred)
	if err != nil {
		t.Fatalf("Verify(minted credential) error = %v, want nil", err)
	}
	if p.ID != idPublisher || p.Actor() != "tok:"+idPublisher {
		t.Errorf("principal id/actor = %q/%q, want %q/tok:%s", p.ID, p.Actor(), idPublisher, idPublisher)
	}
	if !p.Allows(auth.RolePublish, "orders") {
		t.Error("verified principal does not hold publish on orders")
	}
	if p.Allows(auth.RoleAdmin, "orders") {
		t.Error("verified principal unexpectedly holds admin")
	}
}

// TestVerifyRejectsMalformed pins the shape check: a missing prefix, an empty credential, an
// oversized credential and a bad secret shape all fail as malformed, before the hot path hashes
// anything.
func TestVerifyRejectsMalformed(t *testing.T) {
	t.Parallel()

	tok, _ := mintToken(idPublisher, testSecret)
	reg := auth.NewRegistry([]auth.Token{tok})

	over100KiB := "msq1_" + idPublisher + "_" + strings.Repeat("x", 100*1024)

	tests := []struct {
		name string
		cred string
	}{
		{name: "empty", cred: ""},
		{name: "missing prefix", cred: idPublisher + "_" + testSecret},
		{name: "wrong prefix", cred: "mq1_" + idPublisher + "_" + testSecret},
		{name: "no separator", cred: "msq1_" + idPublisher},
		{name: "secret too short", cred: "msq1_" + idPublisher + "_short"},
		{name: "secret bad charset", cred: "msq1_" + idPublisher + "_has spaces here 12"},
		{name: "100 KiB credential", cred: over100KiB},
	}
	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			_, err := reg.Verify(tc.cred)
			if err == nil {
				t.Fatalf("Verify(%q) = nil error, want malformed", tc.cred)
			}
			if !errors.Is(err, errs.ErrUnauthorized) {
				t.Fatalf("Verify(%q) error = %v, want errors.Is ErrUnauthorized", tc.cred, err)
			}
			if !strings.Contains(err.Error(), "malformed credential") {
				t.Errorf("Verify(%q) error = %q, want malformed credential", tc.cred, err)
			}
		})
	}
}

// TestVerifyUnknownAndWrongSecretAreIndistinguishable pins the timing-parity and byte-parity
// contract: a wrong secret for a known id and a valid secret under an unknown id produce the
// same error message, because the unknown-id path hashes a decoy digest instead of returning
// early.
func TestVerifyUnknownAndWrongSecretAreIndistinguishable(t *testing.T) {
	t.Parallel()

	tok, cred := mintToken(idPublisher, testSecret)
	reg := auth.NewRegistry([]auth.Token{tok})

	wrongSecret := "msq1_" + idPublisher + "_" + testWrongSecret
	unknownID := "msq1_nobody_" + testSecret

	_, errWrong := reg.Verify(wrongSecret)
	_, errUnknown := reg.Verify(unknownID)

	for name, err := range map[string]error{"wrong secret": errWrong, "unknown id": errUnknown} {
		if err == nil {
			t.Fatalf("Verify(%s) = nil error, want unknown token or bad secret", name)
		}
		if !errors.Is(err, errs.ErrUnauthorized) {
			t.Fatalf("Verify(%s) error = %v, want errors.Is ErrUnauthorized", name, err)
		}
	}
	if errWrong.Error() != errUnknown.Error() {
		t.Errorf("wrong-secret and unknown-id errors differ: %q vs %q; they must be byte-identical",
			errWrong, errUnknown)
	}
	if errWrong.Error() != "auth.verify: unknown token or bad secret" {
		t.Errorf("error = %q, want the merged teaching message", errWrong)
	}

	// A successful verify of the real credential still works, so the rejections above were not
	// a symptom of a dead registry.
	if _, err := reg.Verify(cred); err != nil {
		t.Fatalf("Verify(minted) after rejections = %v, want nil", err)
	}
}

// TestVerifyEmptyRegistry pins that a registry built with no tokens verifies nothing.
func TestVerifyEmptyRegistry(t *testing.T) {
	t.Parallel()

	reg := auth.NewRegistry(nil)
	if _, err := reg.Verify("msq1_any_" + testSecret); err == nil {
		t.Fatal("empty registry verified a credential")
	}
}

// TestVerifyUnknownIDNeverReturnsTheDecoyPrincipal pins that an unknown id whose digest
// coincidentally matched the decoy would still be refused (the !found guard).
func TestVerifyUnknownIDNeverReturnsTheDecoyPrincipal(t *testing.T) {
	t.Parallel()

	reg := auth.NewRegistry(nil)
	p, err := reg.Verify("msq1_unknown_" + testSecret)
	if err == nil {
		t.Fatalf("Verify returned principal %+v with nil error, want a refusal", p)
	}
}
