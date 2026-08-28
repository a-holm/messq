// SPDX-License-Identifier: Apache-2.0

package auth

import "net"

// UnixPeer reads the SO_PEERCRED credential of an AF_UNIX socket's far end. It
// reports ok=false for anything a kernel cannot answer — non-Linux platforms,
// TCP connections, or platforms without the option — which maps onto
// KindLocal with Peer==nil and Actor "uid:unknown": access is unchanged because
// filesystem permissions were always the ACL, only actor attribution degrades.
//
// The credential is read ONCE per connection, at connect(2) time semantics (the
// kernel freezes ucred at connect(2) for stream sockets), via SyscallConn so the
// descriptor is never duplicated and never left blocking.
//
// Implementation lives in peer_linux.go and its build-tagged stub peer_other.go.
// The connState plumbing into request attribution is wired by internal/api when
// the authz middleware lane (#15) consumes actors; this file stays middleware-free
// by design (internal/auth never imports net/http).
func UnixPeer(conn net.Conn) (*Peer, bool) {
	return unixPeer(conn)
}
