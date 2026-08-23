// SPDX-License-Identifier: Apache-2.0

package auth

import (
	"context"
	"fmt"
	"net"
	"strings"
)

// Class is the classification of a --listen address: local Unix socket, loopback TCP, or a
// public (non-loopback) address. The listener policy of issue #16 §7 is a function of this
// class and whether --auth-file is set.
type Class uint8

const (
	// ClassUnix is a unix:// listener: filesystem permissions are the ACL.
	ClassUnix Class = iota
	// ClassLoopback is a tcp:// listener bound to a loopback-only address.
	ClassLoopback
	// ClassPublic is a tcp:// listener reachable from other hosts: all-interfaces, a non-
	// loopback literal, or a hostname resolving to any non-loopback address.
	ClassPublic
)

// String returns the class's wire name, used in the startup banner and doctor output.
func (c Class) String() string {
	switch c {
	case ClassUnix:
		return "unix"
	case ClassLoopback:
		return "loopback"
	case ClassPublic:
		return "public"
	default:
		return fmt.Sprintf("class(%d)", uint8(c))
	}
}

// lookupIP resolves a hostname to its addresses. Classify takes it so a test can stub the
// resolver and the gated serve wiring can pass net.DefaultResolver.LookupIPAddr.
type lookupIP func(ctx context.Context, host string) ([]net.IPAddr, error)

// Classify resolves a --listen address into a Class. A hostname resolving to any non-loopback
// address classifies as public — the safe direction, and the one place #40 (native TLS) edits
// the table later. lookup is called only for hostnames, never for Unix sockets or literal IPs.
func Classify(ctx context.Context, addr string, lookup lookupIP) (Class, error) {
	switch {
	case strings.HasPrefix(addr, "unix://"):
		return ClassUnix, nil
	case strings.HasPrefix(addr, "tcp://"):
		return classifyTCP(ctx, strings.TrimPrefix(addr, "tcp://"), lookup)
	default:
		return 0, fmt.Errorf("unsupported --listen %q: want unix://PATH or tcp://HOST:PORT", addr)
	}
}

func classifyTCP(ctx context.Context, hostport string, lookup lookupIP) (Class, error) {
	host, _, err := net.SplitHostPort(hostport)
	if err != nil {
		return 0, fmt.Errorf("invalid --listen tcp address %q: %w", hostport, err)
	}
	// An empty host means all interfaces — the public case.
	if host == "" {
		return ClassPublic, nil
	}
	if ip := net.ParseIP(host); ip != nil {
		if ip.IsLoopback() {
			return ClassLoopback, nil
		}
		return ClassPublic, nil
	}
	addrs, err := lookup(ctx, host)
	if err != nil {
		return 0, fmt.Errorf("resolve %q: %w", host, err)
	}
	if len(addrs) == 0 {
		return 0, fmt.Errorf("refusing to bind %q: resolves to no addresses", hostport)
	}
	for _, a := range addrs {
		if !a.IP.IsLoopback() {
			return ClassPublic, nil
		}
	}
	return ClassLoopback, nil
}
