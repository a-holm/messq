// SPDX-License-Identifier: Apache-2.0

package auth_test

import (
	"errors"
	"testing"

	"github.com/a-holm/messq/internal/auth"
	"github.com/a-holm/messq/internal/errs"
)

func TestRoleString(t *testing.T) {
	t.Parallel()

	tests := []struct {
		role auth.Role
		want string
	}{
		{role: auth.RolePublish, want: "publish"},
		{role: auth.RoleConsume, want: "consume"},
		{role: auth.RoleAdmin, want: "admin"},
	}
	for _, tc := range tests {
		if got := tc.role.String(); got != tc.want {
			t.Errorf("Role(%d).String() = %q, want %q", tc.role, got, tc.want)
		}
	}
}

func TestParseRole(t *testing.T) {
	t.Parallel()

	for _, name := range []string{"publish", "consume", "admin"} {
		r, err := auth.ParseRole(name)
		if err != nil {
			t.Fatalf("ParseRole(%q) error = %v, want nil", name, err)
		}
		if got := r.String(); got != name {
			t.Errorf("ParseRole(%q).String() = %q, want %q", name, got, name)
		}
	}

	if _, err := auth.ParseRole("peek"); err == nil {
		t.Fatal(`ParseRole("peek") = nil error, want unknown role`)
	} else if !errors.Is(err, errs.ErrBadRequest) {
		t.Fatalf(`ParseRole("peek") error = %v, want errors.Is ErrBadRequest`, err)
	}
}

func TestParseRoleSet(t *testing.T) {
	t.Parallel()

	// A role set parses from any comma-separated subset of the three roles, and the empty
	// RoleSet is exactly that — empty, never "all roles".
	tests := []struct {
		in      string
		want    auth.RoleSet
		wantStr string
	}{
		{in: "publish", wantStr: "publish"},
		{in: "admin,consume", wantStr: "consume,admin"},
		{in: "admin,consume,publish", wantStr: "publish,consume,admin"},
	}
	for _, tc := range tests {
		got, err := auth.ParseRoleSet(tc.in)
		if err != nil {
			t.Fatalf("ParseRoleSet(%q) error = %v, want nil", tc.in, err)
		}
		if got.String() != tc.wantStr {
			t.Errorf("ParseRoleSet(%q).String() = %q, want %q", tc.in, got, tc.wantStr)
		}
	}

	for _, bad := range []struct {
		in   string
		frag string
	}{
		{in: "", frag: "empty"},
		{in: "peek", frag: "unknown role"},
		{in: "publish,peek", frag: "unknown role"},
		{in: "admin,admin", frag: "duplicate role"},
	} {
		if _, err := auth.ParseRoleSet(bad.in); err == nil {
			t.Errorf("ParseRoleSet(%q) = nil error, want %q", bad.in, bad.frag)
		} else if !errors.Is(err, errs.ErrBadRequest) {
			t.Errorf("ParseRoleSet(%q) error = %v, want errors.Is ErrBadRequest", bad.in, err)
		}
	}
}

// TestRoleSetEmptyNeverAll pins the bitmask semantics: RoleSet(0) is the empty set, and no
// role is held in it. A missing role list must never be treated as "all roles".
func TestRoleSetEmptyNeverAll(t *testing.T) {
	t.Parallel()

	var empty auth.RoleSet
	if empty.Has(auth.RoleAdmin) || empty.Has(auth.RolePublish) || empty.Has(auth.RoleConsume) {
		t.Fatal("RoleSet(0) holds a role; the empty set must hold none")
	}
	if got := empty.String(); got != "" {
		t.Errorf("RoleSet(0).String() = %q, want empty", got)
	}
}
