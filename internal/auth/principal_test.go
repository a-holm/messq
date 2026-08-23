// SPDX-License-Identifier: Apache-2.0

package auth_test

import (
	"testing"

	"github.com/a-holm/messq/internal/auth"
	"github.com/google/go-cmp/cmp"
)

// principal builds a token principal with the given grants.
func principal(id string, grants ...auth.Grant) auth.Principal {
	return auth.NewPrincipal(id, auth.KindToken, grants)
}

// grant builds a grant from role and pattern strings, panicking on a bad test literal.
func grant(roles, pattern string) auth.Grant {
	return auth.Grant{Roles: mustRoleSet(roles), Pattern: mustPattern(pattern)}
}

// mustRoleSet parses roles or panics (helpers used inside table literals carry no *testing.T).
func mustRoleSet(roles string) auth.RoleSet {
	rs, err := auth.ParseRoleSet(roles)
	if err != nil {
		panic(err)
	}
	return rs
}

func mustPattern(s string) auth.Pattern {
	p, err := auth.ParsePattern(s)
	if err != nil {
		panic(err)
	}
	return p
}

// TestAllowsIsASetNotAHierarchy pins issue #16 decision 1: roles form a set with no
// implication. A principal holding admin alone is denied on fetch and peek (which need
// consume); only a principal that also holds consume is allowed. There is no "admin covers
// everything" short-cut.
func TestAllowsIsASetNotAHierarchy(t *testing.T) {
	t.Parallel()

	adminOnly := principal("ops-admin", grant("admin", "*"))
	if adminOnly.Allows(auth.RoleConsume, "orders") {
		t.Fatal("admin-only principal was allowed to consume on orders; admin must not imply consume")
	}
	if adminOnly.Allows(auth.RolePublish, "orders") {
		t.Fatal("admin-only principal was allowed to publish on orders; admin must not imply publish")
	}
	if !adminOnly.Allows(auth.RoleAdmin, "orders") {
		t.Fatal("admin-only principal was denied admin on orders")
	}

	adminConsume := principal("ops-admin", grant("admin,consume", "*"))
	if !adminConsume.Allows(auth.RoleConsume, "orders") {
		t.Fatal("admin,consume principal was denied consume on orders")
	}
}

// TestAllowsScopesPerStream walks the role × pattern × stream truth table.
func TestAllowsScopesPerStream(t *testing.T) {
	t.Parallel()

	pub := principal("publisher", grant("publish", "orders*"))
	con := principal("worker", grant("consume", "orders"))

	tests := []struct {
		name   string
		p      auth.Principal
		role   auth.Role
		stream string
		want   bool
	}{
		{name: "publish within prefix", p: pub, role: auth.RolePublish, stream: "orders", want: true},
		{name: "publish dlq via prefix", p: pub, role: auth.RolePublish, stream: "orders.dlq", want: true},
		{name: "publish other stream denied", p: pub, role: auth.RolePublish, stream: "payments", want: false},
		{name: "exact does not cover dlq", p: con, role: auth.RoleConsume, stream: "orders.dlq", want: false},
		{name: "exact covers itself", p: con, role: auth.RoleConsume, stream: "orders", want: true},
		{name: "wrong role denied", p: con, role: auth.RolePublish, stream: "orders", want: false},
	}
	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := tc.p.Allows(tc.role, tc.stream); got != tc.want {
				t.Errorf("Allows(%s, %q) = %v, want %v", tc.role, tc.stream, got, tc.want)
			}
		})
	}
}

// TestAllowsAny pins the "any role in the set" semantics and the empty RoleSet: an empty set
// is satisfied by nothing, so AllowsAny(RoleSet(0), …) is always false.
func TestAllowsAny(t *testing.T) {
	t.Parallel()

	p := principal("both", grant("publish", "orders"), grant("consume", "orders*"))

	if !p.AllowsAny(mustRoleSet("publish,consume"), "orders") {
		t.Fatal("AllowsAny(publish|consume, orders) = false, want true via publish")
	}
	if !p.AllowsAny(mustRoleSet("publish,consume"), "orders.dlq") {
		t.Fatal("AllowsAny(publish|consume, orders.dlq) = false, want true via consume prefix")
	}
	if p.AllowsAny(mustRoleSet("admin"), "orders") {
		t.Fatal("AllowsAny(admin, orders) = true, want false")
	}
	var empty auth.RoleSet
	if p.AllowsAny(empty, "orders") {
		t.Fatal("AllowsAny(empty, orders) = true, want false: an empty set is never satisfied")
	}
}

// TestAllowsGlobal pins that a global route needs the role on the "*" pattern: a principal
// scoped to "orders*" does not hold the role globally, and a "*" grant does.
func TestAllowsGlobal(t *testing.T) {
	t.Parallel()

	scoped := principal("scoped", grant("admin", "orders*"))
	if scoped.AllowsGlobal(auth.RoleAdmin) {
		t.Fatal("admin@orders* was reported global; only a '*' grant is global")
	}

	global := principal("global", grant("admin", "*"))
	if !global.AllowsGlobal(auth.RoleAdmin) {
		t.Fatal("admin@* was not reported global")
	}
	if global.AllowsGlobal(auth.RoleConsume) {
		t.Fatal("consume reported global on an admin-only principal")
	}
}

// TestFilterStreams pins the listing rule: a listing narrows to the in-scope subset rather
// than denying, so a principal with no grants on a stream simply never sees it.
func TestFilterStreams(t *testing.T) {
	t.Parallel()

	p := principal("worker", grant("consume", "orders*"))
	names := []string{"orders", "orders.dlq", "payments", "orders.eu.1"}

	got := p.FilterStreams(mustRoleSet("consume"), names)
	want := []string{"orders", "orders.dlq", "orders.eu.1"}
	if diff := cmp.Diff(want, got); diff != "" {
		t.Fatalf("FilterStreams (-want +got):\n%s", diff)
	}

	var empty auth.RoleSet
	if got := p.FilterStreams(empty, names); len(got) != 0 {
		t.Fatalf("FilterStreams(empty) = %v, want none", got)
	}
}

// TestActor pins the events.actor values: token ids become "tok:<id>", local peers "uid:<n>",
// and an anonymous principal is "anonymous".
func TestActor(t *testing.T) {
	t.Parallel()

	if got := principal("ci-publisher", grant("publish", "orders")).Actor(); got != "tok:ci-publisher" {
		t.Errorf("token Actor() = %q, want %q", got, "tok:ci-publisher")
	}

	local := auth.NewLocalPrincipal("", &auth.Peer{UID: 1000, GID: 1000, PID: 42})
	if got := local.Actor(); got != "uid:1000" {
		t.Errorf("local Actor() = %q, want %q", got, "uid:1000")
	}

	noPeer := auth.NewLocalPrincipal("", nil)
	if got := noPeer.Actor(); got != "uid:unknown" {
		t.Errorf("peer-less local Actor() = %q, want %q", got, "uid:unknown")
	}

	var anon auth.Principal
	if got := anon.Actor(); got != "anonymous" {
		t.Errorf("anonymous Actor() = %q, want %q", got, "anonymous")
	}
}

// TestNewPrincipalIsImmutable pins that a principal copies its grants at construction, so a
// caller that later mutates the input slice cannot change an already-built principal.
func TestNewPrincipalIsImmutable(t *testing.T) {
	t.Parallel()

	grants := []auth.Grant{grant("consume", "orders")}
	p := auth.NewPrincipal("worker", auth.KindToken, grants)

	grants[0] = grant("publish", "payments") // mutate the caller's slice

	if p.Allows(auth.RolePublish, "payments") {
		t.Fatal("mutating the input slice changed the principal; grants must be copied")
	}
	if !p.Allows(auth.RoleConsume, "orders") {
		t.Fatal("the principal lost its original grant")
	}
}
