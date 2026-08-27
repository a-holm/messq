// SPDX-License-Identifier: Apache-2.0

//go:build !linux

package auth

import "net"

// unixPeer is the stub for platforms without SO_PEERCRED: the peer credential is
// always unavailable there. KindLocal principals still work — filesystem
// permissions on the socket remain the ACL (issue #16 §5) — and the only loss is
// uid attribution in events.actor ("uid:unknown").
func unixPeer(conn net.Conn) (*Peer, bool) {
	_ = conn // symmetry with the Linux build; nothing to read off a non-Linux socket here
	return nil, false
}
