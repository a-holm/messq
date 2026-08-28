// SPDX-License-Identifier: Apache-2.0

//go:build linux

package auth

import (
	"math"
	"net"
	"syscall"

	"golang.org/x/sys/unix"
)

// maxUcred is the largest value that fits both uid_t and [Peer]'s int32 fields.
// Real Linux uids/gids/pids live far below this line (user namespaces included),
// so overflowing means the answer is nonsense anyway and reporting it unavailable
// is the honest reading.
const maxUcred = math.MaxInt32

// unixPeer is the Linux implementation of [UnixPeer]: getsockopt(SO_PEERCRED)
// through the connection's RawConn control hook, which runs while holding the
// network poller's lock so the fd can be used without Dup().
//
// SO_PEERCRED is only MEANINGFUL for AF_UNIX sockets. Generic SOCK_STREAM options
// also answer it for TCP — with the credentials of whoever CREATED the listening
// socket, i.e. the daemon attributing itself. We therefore refuse anything whose
// local address is not a Unix-domain name before asking the kernel.
func unixPeer(conn net.Conn) (*Peer, bool) {
	if la := conn.LocalAddr(); la == nil || la.Network() != "unix" {
		return nil, false
	}
	syscallConn, ok := conn.(syscall.Conn)
	if !ok {
		return nil, false
	}
	raw, err := syscallConn.SyscallConn()
	if err != nil {
		return nil, false
	}
	var cred *unix.Ucred
	ctrlErr := raw.Control(func(fd uintptr) {
		if c, sockErr := unix.GetsockoptUcred(int(fd), unix.SOL_SOCKET, unix.SO_PEERCRED); sockErr == nil && c != nil {
			cred = c
		}
	})
	if ctrlErr != nil || cred == nil {
		return nil, false
	}
	if cred.Uid > maxUcred || cred.Gid > maxUcred || cred.Pid > maxUcred {
		// A value that cannot round-trip through int32 without lying about the
		// actor's identity is treated like an unreadable credential.
		return nil, false
	}
	return &Peer{UID: int32(cred.Uid), GID: int32(cred.Gid), PID: int32(cred.Pid)}, true
}
