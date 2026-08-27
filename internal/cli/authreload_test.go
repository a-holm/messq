// SPDX-License-Identifier: Apache-2.0

package cli

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/a-holm/messq/internal/auth"
	"github.com/a-holm/messq/internal/errs"
	"github.com/a-holm/messq/internal/lifecycle"
)

const (
	reloadCredA = "msq1_old-token_old-ci-reload-secret-shape-ok"
	reloadCredB = "msq1_new-token_new-ci-reload-secret-shape-ok"
)

// tokenLine renders one stored line for a fixture credential.
func tokenLine(id, cred string) string {
	sum := sha256.Sum256([]byte(cred))
	return id + " " + hex.EncodeToString(sum[:]) + " consume orders*\n"
}

// reloadFixtures seeds a 0600 token file holding credential A and a registry
// already serving it — the "serving daemon" state every reload scenario starts
// from.
func reloadFixtures(t *testing.T) (path string, reg *auth.Registry) {
	t.Helper()
	path = filepath.Join(t.TempDir(), "tokens")
	reg = auth.NewRegistry(nil)
	if werr := os.WriteFile(path, []byte(tokenLine("old-token", reloadCredA)), 0o600); werr != nil {
		t.Fatalf("seed tokens: %v", werr)
	}
	f, err := readAuthTokens(path)
	if err != nil {
		t.Fatalf("parse seed: %v", err)
	}
	reg.SwapTokens(f.Tokens)
	if _, verr := reg.Verify(reloadCredA); verr != nil {
		t.Fatalf("precondition: credential A must verify: %v", verr)
	}
	return path, reg
}

func TestAuthfileReloadValidatesAndSwaps(t *testing.T) {
	t.Parallel()
	path, reg := reloadFixtures(t)
	r := newAuthFileReloader(path, reg)
	if r.Name() != "authfile" {
		t.Fatalf("reloader name %q", r.Name())
	}

	// Rotate: replace A's line with B's on disk.
	if werr := os.WriteFile(path, []byte(tokenLine("new-token", reloadCredB)), 0o600); werr != nil {
		t.Fatalf("rotate tokens: %v", werr)
	}

	ctx := context.Background()
	changes, verr := r.Validate(ctx)
	if verr != nil {
		t.Fatalf("validate: %v", verr)
	}
	if len(changes) != 1 || changes[0].Subject != "tokens" {
		t.Fatalf("proposed changes %+v", changes)
	}
	c := changes[0]
	if !c.Secret {
		t.Error("the tokens change must flag Secret so RenderDiff redacts both sides")
	}
	if c.To != "new-token" || !strings.Contains(c.From, "old-token") {
		t.Errorf("diff must carry IDS only: from=%q to=%q", c.From, c.To)
	}
	for _, side := range []string{c.From, c.To} {
		for _, tok := range strings.Split(side, ",") {
			if len(tok) == 64 && isAllHex(tok) {
				t.Errorf("digest-like material leaked into the id diff (%d chars): %q", len(tok), tok)
			}
		}
	}
	if got := lifecycle.RenderDiff(changes); strings.Contains(got, "new-token") || strings.Contains(got, "old-token") {
		t.Errorf("RenderDiff leaked the id set despite Secret=true: %s", got)
	}

	if aerr := r.Apply(ctx, c); aerr != nil {
		t.Fatalf("apply: %v", aerr)
	}
	if _, verr := reg.Verify(reloadCredA); verr == nil {
		t.Error("revoked credential A still verifies after reload")
	}
	p, berr := reg.Verify(reloadCredB)
	if berr != nil {
		t.Fatalf("credential B rejected after reload: %v", berr)
	}
	if p.Actor() != "tok:new-token" {
		t.Errorf("actor attribution = %q, want tok:new-token", p.Actor())
	}
}

func isAllHex(s string) bool {
	for i := 0; i < len(s); i++ {
		switch {
		case s[i] >= '0' && s[i] <= '9', s[i] >= 'a' && s[i] <= 'f':
		default:
			return false
		}
	}
	return true
}

func TestAuthfileReloadMalformedKeepsPreviousSetServing(t *testing.T) {
	t.Parallel()
	path, reg := reloadFixtures(t)
	r := newAuthFileReloader(path, reg)

	corrupt := tokenLine("old-token", reloadCredA) + "fifth field extra\n"
	if werr := os.WriteFile(path, []byte(corrupt), 0o600); werr != nil {
		t.Fatal(werr)
	}

	set := lifecycle.NewRegistry(nil, r)
	err := set.Reload(context.Background())
	if err == nil {
		t.Fatal("a malformed file during reload must fail validation")
	}
	if _, verr := reg.Verify(reloadCredA); verr != nil {
		t.Fatalf("previous set must stay live after a refused reload: %v", verr)
	}
}

// A MISSING file mid-flight survives exactly like a corrupt one: the two-phase
// contract refuses before applying anything.
func TestAuthfileReloadMissingFileKeepsPreviousSetServing(t *testing.T) {
	t.Parallel()
	path, reg := reloadFixtures(t)
	r := newAuthFileReloader(path, reg)

	if rmErr := os.Remove(path); rmErr != nil {
		t.Fatalf("remove tokens: %v", rmErr)
	}
	if _, verr := r.Validate(context.Background()); verr == nil {
		t.Fatal("missing file validated clean; want refusal")
	}
	if _, keepErr := reg.Verify(reloadCredA); keepErr != nil {
		t.Errorf("previous set lost after refusing a missing-file reload: %v", keepErr)
	}
}

// Parser failures keep their teaching class (line-numbered ErrBadRequest): no
// new sentinel entered errs for reload paths (S13 intact).
func TestAuthfileReloadErrorClassifiesBadRequest(t *testing.T) {
	t.Parallel()
	path, _ := reloadFixtures(t)
	r := newAuthFileReloader(path, auth.NewRegistry(nil))

	if werr := os.WriteFile(path, []byte(tokenLine("old-token", reloadCredA)+"two fields only\n"), 0o600); werr != nil {
		t.Fatal(werr)
	}
	_, verr := r.Validate(context.Background())
	if verr == nil || !errors.Is(verr, errs.ErrBadRequest) {
		t.Errorf("parser error should wrap ErrBadRequest for exit-classification, got %v", verr)
	}
}
