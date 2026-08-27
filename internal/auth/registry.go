// SPDX-License-Identifier: Apache-2.0

package auth

import (
	"crypto/sha256"
	"crypto/subtle"
	"sort"
	"strings"
	"sync/atomic"

	"github.com/a-holm/messq/internal/errs"
)

// Credential shape (§4): a fixed greppable prefix, a token id, then the secret. The prefix
// "msq1_" is what the gitleaks rule and GitHub secret scanning grep for; the id survives into
// logs and events; the secret is compared only as a SHA-256 digest.
const (
	credentialPrefix = "msq1_"
	minSecretBytes   = 16
	maxSecretBytes   = 512
)

// decoyDigest is the fixed digest an unknown id is compared against. Hashing a decoy rather
// than early-returning keeps the unknown-id path's work identical to the wrong-secret path's,
// so the two are indistinguishable by timing (issue #16, §4).
var decoyDigest = sha256.Sum256([]byte("messq auth decoy digest"))

// tokenSet is one immutable snapshot of the token registry. A reload builds a fresh tokenSet
// and swaps the pointer, so in-flight requests see a consistent view (issue #16, §9).
type tokenSet struct {
	byID  map[string]entry
	decoy entry
}

// entry is one token's digest and the principal it carries.
type entry struct {
	hash      [32]byte
	principal Principal
}

// Registry verifies bearer credentials against a token set. The zero value has no tokens and
// verifies nothing; build one with [NewRegistry].
type Registry struct {
	set atomic.Pointer[tokenSet]
}

// buildTokenSet constructs one immutable snapshot for NewRegistry and SwapTokens.
func buildTokenSet(tokens []Token) *tokenSet {
	set := &tokenSet{
		byID:  make(map[string]entry, len(tokens)),
		decoy: entry{hash: decoyDigest},
	}
	for _, t := range tokens {
		set.byID[t.ID] = entry{hash: t.Hash, principal: t.Principal()}
	}
	return set
}

// NewRegistry builds a registry over the given tokens. It stores the parsed set behind an
// atomic pointer so a later reload (issue #17/#12) swaps it without touching in-flight
// requests.
func NewRegistry(tokens []Token) *Registry {
	r := &Registry{}
	r.set.Store(buildTokenSet(tokens))
	return r
}

// SwapTokens atomically replaces the live token set with tokens. The new set is
// built FIRST and swapped behind the same atomic pointer Verify reads, so an
// in-flight request sees either the whole old set or the whole new one — never a
// torn mixture — and every reader that started earlier finishes on its snapshot
// without contention (issue #16 §9; the reloader's Apply calls this).
func (r *Registry) SwapTokens(tokens []Token) {
	set := buildTokenSet(tokens)
	r.set.Store(set)
}

// LiveIDs returns the sorted ids of the live snapshot, taken atomically. This is
// what reload diffs are computed FROM (ids only; digests never leave the engine).
func (r *Registry) LiveIDs() []string {
	set := r.set.Load()
	ids := make([]string, 0, len(set.byID))
	for id := range set.byID {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	return ids
}

// Verify checks credential and returns the principal it names. The credential is the whole
// presented string, prefix and id included; the stored hash covers all of it, so there is no
// second parse in the security path and renaming an id invalidates its credential (issue #16,
// decision 2). A malformed credential and a wrong secret are both [errs.ErrUnauthorized], and
// the wrong-secret body is byte-identical to the unknown-id body: the reason lives in the log,
// not in the response.
func (r *Registry) Verify(credential string) (Principal, error) {
	set := r.set.Load()

	id, ok := splitCredential(credential)
	if !ok {
		return Principal{}, errs.E(errs.ErrUnauthorized, "auth.verify", "malformed credential")
	}

	sum := sha256.Sum256([]byte(credential))
	ent, found := set.byID[id]
	if !found {
		ent = set.decoy
	}
	if subtle.ConstantTimeCompare(sum[:], ent.hash[:]) != 1 || !found {
		return Principal{}, errs.E(errs.ErrUnauthorized, "auth.verify", "unknown token or bad secret")
	}
	return ent.principal, nil
}

// splitCredential extracts the id from a credential and shape-checks it: the "msq1_" prefix, a
// valid id, and a secret of 16–512 characters from [A-Za-z0-9._~-]. It returns false for a
// malformed credential, which bounds the work per request and rejects garbage without hashing
// it.
func splitCredential(cred string) (id string, ok bool) {
	if !strings.HasPrefix(cred, credentialPrefix) {
		return "", false
	}
	rest := cred[len(credentialPrefix):]
	sep := strings.IndexByte(rest, '_')
	if sep < 0 {
		return "", false
	}
	id, secret := rest[:sep], rest[sep+1:]
	if !validTokenID(id) || !validSecret(secret) {
		return "", false
	}
	return id, true
}

// validSecret reports whether s is a legal secret: 16–512 characters from [A-Za-z0-9._~-]. The
// server never validates the secret's alphabet for its meaning — any opaque string in this
// shape works — but the shape bound rejects garbage before the hot path hashes it.
func validSecret(s string) bool {
	if len(s) < minSecretBytes || len(s) > maxSecretBytes {
		return false
	}
	for i := 0; i < len(s); i++ {
		switch c := s[i]; {
		case c >= 'A' && c <= 'Z', c >= 'a' && c <= 'z', c >= '0' && c <= '9', c == '.', c == '_', c == '~', c == '-':
		default:
			return false
		}
	}
	return true
}
