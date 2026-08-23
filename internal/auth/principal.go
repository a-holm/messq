// SPDX-License-Identifier: Apache-2.0

package auth

import "fmt"

// Kind tells how a principal authenticated.
type Kind uint8

const (
	// KindAnonymous is a request with no credential and no local peer identity.
	KindAnonymous Kind = iota
	// KindToken is a request carrying a valid bearer token.
	KindToken
	// KindLocal is a Unix-socket peer identified by SO_PEERCRED. Filesystem permissions are
	// the ACL; the peer credential is read for the events.actor attribution only.
	KindLocal
	// KindCert is reserved for #40 (native TLS client identities). Keep this value open.
	// KindCert
)

// Peer is a Unix-socket peer credential read once at connect(2) time via SO_PEERCRED. It is
// set only when Kind == KindLocal, and nil when the platform could not supply it.
type Peer struct {
	UID, GID, PID int32
}

// Grant is one role set scoped to one stream pattern.
type Grant struct {
	Roles   RoleSet
	Pattern Pattern
}

// Principal is an immutable identity and its grants. The zero value is anonymous and holds no
// grants. Build one with [NewPrincipal]; the grants are copied, so a caller may reuse its
// slice. Authorization is a pure function over a principal, a role and a stream name.
type Principal struct {
	ID     string
	Kind   Kind
	grants []Grant
	Peer   *Peer
}

// NewPrincipal builds an immutable principal. grants are copied; the caller keeps ownership of
// its slice.
func NewPrincipal(id string, kind Kind, grants []Grant) Principal {
	cp := make([]Grant, len(grants))
	copy(cp, grants)
	return Principal{ID: id, Kind: kind, grants: cp}
}

// NewLocalPrincipal builds the trusted local principal for a Unix-socket peer: every role on
// every stream, because filesystem permissions on the socket are the ACL (issue #16, §5). peer
// is nil when SO_PEERCRED was unavailable, which changes only the actor attribution.
func NewLocalPrincipal(id string, peer *Peer) Principal {
	return Principal{
		ID:     id,
		Kind:   KindLocal,
		Peer:   peer,
		grants: []Grant{{Roles: allRoles, Pattern: Pattern{raw: "*", star: true}}},
	}
}

// Allows reports whether p holds r on stream. It is the whole authorization engine: no
// hierarchy, no implication table, no deny rules.
func (p Principal) Allows(r Role, stream string) bool {
	for _, g := range p.grants {
		if g.Roles.Has(r) && g.Pattern.Match(stream) {
			return true
		}
	}
	return false
}

// AllowsAny reports whether p holds any role in rs on stream. rs may be the empty set, which
// is satisfied by nothing.
func (p Principal) AllowsAny(rs RoleSet, stream string) bool {
	for _, g := range p.grants {
		if g.Roles&rs != 0 && g.Pattern.Match(stream) {
			return true
		}
	}
	return false
}

// AllowsGlobal reports whether p holds r on every stream, which only a "*" grant does. Global
// routes (process log level, cross-stream events) require it.
func (p Principal) AllowsGlobal(r Role) bool {
	for _, g := range p.grants {
		if g.Roles.Has(r) && g.Pattern.star {
			return true
		}
	}
	return false
}

// FilterStreams narrows a listing to the names p can exercise any role in rs on. A listing
// filters rather than denies, so a principal with no relevant grants gets the empty subset, not
// a refusal (issue #16, §6 rule 2).
func (p Principal) FilterStreams(rs RoleSet, names []string) []string {
	out := make([]string, 0, len(names))
	for _, n := range names {
		if p.AllowsAny(rs, n) {
			out = append(out, n)
		}
	}
	return out
}

// Actor returns the events.actor value: "tok:<id>" for a token principal, "uid:<n>" for a
// local peer, and "anonymous" when there is no identity. This is what §4.2's actor column
// records, so messq trace can answer "who did this?".
func (p Principal) Actor() string {
	switch p.Kind {
	case KindToken:
		return "tok:" + p.ID
	case KindLocal:
		if p.Peer != nil {
			return fmt.Sprintf("uid:%d", p.Peer.UID)
		}
		return "uid:unknown"
	case KindAnonymous:
		return "anonymous"
	default:
		return "anonymous"
	}
}
