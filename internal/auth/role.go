// SPDX-License-Identifier: Apache-2.0

package auth

import (
	"fmt"
	"strings"

	"github.com/a-holm/messq/internal/errs"
)

// Role is one of the three roles issue #16 defines: publish, consume and admin. The values are
// the bit indices into [RoleSet]; they are never used as an ordered set themselves.
type Role uint8

const (
	RolePublish Role = iota
	RoleConsume
	RoleAdmin
)

// numRoles is the number of roles: an untyped count, not a Role value, so the exhaustive
// linter sees exactly three roles to cover in every switch.
const numRoles = 3

// RoleSet is a bitmask of roles. RoleSet(0) is the empty set, never "all roles" — a missing
// role list grants nothing (issue #16, decision 1: roles are a set, not a hierarchy).
type RoleSet uint8

// allRoles is the set the trusted local principal holds on "*".
const allRoles RoleSet = 1<<RolePublish | 1<<RoleConsume | 1<<RoleAdmin

// roleBit returns the bit r occupies in a RoleSet.
func roleBit(r Role) RoleSet { return RoleSet(1) << uint(r) }

// Has reports whether rs holds r.
func (rs RoleSet) Has(r Role) bool { return rs&roleBit(r) != 0 }

// Empty reports whether rs holds no role.
func (rs RoleSet) Empty() bool { return rs == 0 }

// String renders rs in declaration order (publish, consume, admin) as a comma-separated list.
// The empty set renders as the empty string.
func (rs RoleSet) String() string {
	if rs == 0 {
		return ""
	}
	parts := make([]string, 0, numRoles)
	for r := Role(0); r < numRoles; r++ {
		if rs.Has(r) {
			parts = append(parts, r.String())
		}
	}
	return strings.Join(parts, ",")
}

// String returns the role's wire name.
func (r Role) String() string {
	switch r {
	case RolePublish:
		return "publish"
	case RoleConsume:
		return "consume"
	case RoleAdmin:
		return "admin"
	default:
		return fmt.Sprintf("role(%d)", uint8(r))
	}
}

// ParseRole maps a wire name to a Role. The valid names are "publish", "consume" and "admin".
func ParseRole(s string) (Role, error) {
	switch s {
	case "publish":
		return RolePublish, nil
	case "consume":
		return RoleConsume, nil
	case "admin":
		return RoleAdmin, nil
	default:
		return 0, errs.E(errs.ErrBadRequest, "", "unknown role %q (valid: publish, consume, admin)", s)
	}
}

// ParseRoleSet maps a comma-separated role list to a RoleSet. The list must be non-empty, a
// subset of the three roles, and free of duplicates.
func ParseRoleSet(s string) (RoleSet, error) {
	if s == "" {
		return 0, errs.E(errs.ErrBadRequest, "", "the role list is empty (valid: publish, consume, admin)")
	}
	var out RoleSet
	for _, part := range strings.Split(s, ",") {
		r, err := ParseRole(part)
		if err != nil {
			return 0, err
		}
		if out.Has(r) {
			return 0, errs.E(errs.ErrBadRequest, "", "duplicate role %q in %q", part, s)
		}
		out |= roleBit(r)
	}
	return out, nil
}
