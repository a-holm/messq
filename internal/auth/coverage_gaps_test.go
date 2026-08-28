// SPDX-License-Identifier: Apache-2.0

package auth

import (
	"strings"
	"testing"
)

// TestRegistryLiveIDsIsSortedAndAtomic covers the reload-diff source: ids come
// back sorted from a live snapshot and track exactly one swap.
func TestRegistryLiveIDsIsSortedAndAtomic(t *testing.T) {
	credA := "msq1_id-b_lived-id-first-fixture-ok"
	credB := "msq1_id-a_lived-id-second-fixture-ok"

	reg := NewRegistry([]Token{
		{ID: "id-b", Hash: hashOfCredential(credA), Roles: allRoles, Patterns: mustPatternsForTest("*")},
		{ID: "id-a", Hash: hashOfCredential(credB), Roles: allRoles, Patterns: mustPatternsForTest("*")},
	})

	got := reg.LiveIDs()
	if len(got) != 2 || got[0] != "id-a" || got[1] != "id-b" {
		t.Errorf("LiveIDs = %v, want [id-a id-b] sorted", got)
	}

	reg.SwapTokens(nil)
	if got := reg.LiveIDs(); len(got) != 0 {
		t.Errorf("LiveIDs after empty swap = %v, want none", got)
	}
}

// TestFileRenderRoundTripsThroughParse pins File.Render against Parse: every
// rendered line must parse back to an identical token set — the writer form the
// reloader consumes can never drift from the reader grammar.
func TestFileRenderRoundTripsThroughParse(t *testing.T) {
	credA := "msq1_render-one_render-roundtrip-secret-01"
	credB := "msq1_render-two_render-roundtrip-secret-02"
	f := &File{Tokens: []Token{
		{ID: "render-one", Hash: hashOfCredential(credA), Roles: allRoles, Patterns: mustPatternsForTest("orders*")},
		{ID: "render-two", Hash: hashOfCredential(credB), Roles: 1 << RoleConsume, Patterns: []Pattern{
			mustSinglePattern("billing"),
		}},
	}}

	rendered := f.Render()
	if strings.Contains(rendered, "\n\n") {
		t.Errorf("render emits a blank line:\n%s", rendered)
	}
	parsed, err := Parse("roundtrip", strings.NewReader(rendered))
	if err != nil {
		t.Fatalf("render does not parse back: %v\n%s", err, rendered)
	}
	if len(parsed.Tokens) != 2 {
		t.Fatalf("%d tokens round-tripped, want 2", len(parsed.Tokens))
	}
	for i, tok := range parsed.Tokens {
		want := f.Tokens[i]
		if tok.ID != want.ID || tok.Roles != want.Roles || len(tok.Patterns) != len(want.Patterns) ||
			tok.Patterns[0].String() != want.Patterns[0].String() {
			t.Errorf("token %d drifted through render→parse: %+v vs %+v", i, tok, want)
		}
	}
}

func mustSinglePattern(raw string) Pattern {
	p, err := ParsePattern(raw)
	if err != nil {
		panic(err)
	}
	return p
}

// TestRoleSetEmptyIsZeroNotAll closes the semantics hole around RoleSet(0): the
// empty set grants nothing anywhere, it is not a wildcard.
func TestRoleSetEmptyIsZeroNotAll(t *testing.T) {
	var empty RoleSet
	if !empty.Empty() {
		t.Fatal("zero RoleSet must report Empty")
	}
	for _, r := range []Role{RolePublish, RoleConsume, RoleAdmin} {
		if empty.Has(r) {
			t.Errorf("empty set holds %s", r)
		}
	}
	p := NewPrincipal("nobody", KindToken, nil)
	if p.AllowsAny(empty, "anything") {
		t.Error("AllowsAny(empty) must be false for every stream and principal")
	}
	if p.Allows(RoleAdmin, "anything") {
		t.Error("principal with no grants holds admin")
	}
}
