// SPDX-License-Identifier: Apache-2.0

package auth_test

import (
	"context"
	"errors"
	"net"
	"testing"

	"github.com/a-holm/messq/internal/auth"
)

// TestClassify walks the address table from issue #16 §7: a Unix socket is local, a loopback
// literal or a hostname resolving to loopback-only is loopback, and everything else — all-
// interfaces, a private IPv4, an empty host, and a hostname resolving to a mix of loopback and
// public addresses — is public. The mixed case classifies as public, the safe direction.
func TestClassify(t *testing.T) {
	t.Parallel()

	resolve := func(addrs ...string) func(context.Context, string) ([]net.IPAddr, error) {
		return func(_ context.Context, _ string) ([]net.IPAddr, error) {
			out := make([]net.IPAddr, len(addrs))
			for i, a := range addrs {
				out[i] = net.IPAddr{IP: net.ParseIP(a)}
			}
			return out, nil
		}
	}

	// A literal address or a Unix socket must never hit the resolver; a stub that fails does
	// double duty as the proof the resolver was not consulted.
	unusedResolver := func(context.Context, string) ([]net.IPAddr, error) {
		return nil, errors.New("resolver must not be called for a literal address")
	}

	tests := []struct {
		name   string
		addr   string
		lookup func(context.Context, string) ([]net.IPAddr, error)
		want   auth.Class
	}{
		{name: "unix socket", addr: "unix:///run/messq/messq.sock", lookup: unusedResolver, want: auth.ClassUnix},
		{name: "loopback ipv4", addr: "tcp://127.0.0.1:4390", lookup: unusedResolver, want: auth.ClassLoopback},
		{name: "loopback ipv6", addr: "tcp://[::1]:4390", lookup: unusedResolver, want: auth.ClassLoopback},
		{name: "all interfaces v4", addr: "tcp://0.0.0.0:4390", lookup: unusedResolver, want: auth.ClassPublic},
		{name: "all interfaces v6", addr: "tcp://[::]:4390", lookup: unusedResolver, want: auth.ClassPublic},
		{name: "empty host", addr: "tcp://:4390", lookup: unusedResolver, want: auth.ClassPublic},
		{name: "private ipv4", addr: "tcp://10.1.2.3:4390", lookup: unusedResolver, want: auth.ClassPublic},
		{name: "loopback hostname", addr: "tcp://localhost:4390", lookup: resolve("127.0.0.1", "::1"), want: auth.ClassLoopback},
		{name: "mixed hostname", addr: "tcp://dual.example:4390", lookup: resolve("127.0.0.1", "10.0.0.5"), want: auth.ClassPublic},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got, err := auth.Classify(context.Background(), tc.addr, tc.lookup)
			if err != nil {
				t.Fatalf("Classify(%q) error = %v, want nil", tc.addr, err)
			}
			if got != tc.want {
				t.Errorf("Classify(%q) = %s, want %s", tc.addr, got, tc.want)
			}
		})
	}
}

// TestClassifyRejectsMalformed pins the error path for a bad scheme and a bad tcp address.
func TestClassifyRejectsMalformed(t *testing.T) {
	t.Parallel()

	unusedResolver := func(context.Context, string) ([]net.IPAddr, error) { return nil, nil }

	for _, addr := range []string{"http://127.0.0.1:4390", "tcp://127.0.0.1", "tcp://noport"} {
		if _, err := auth.Classify(context.Background(), addr, unusedResolver); err == nil {
			t.Errorf("Classify(%q) = nil error, want a malformed-address error", addr)
		}
	}
}

func TestClassString(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		c    auth.Class
		want string
	}{
		{auth.ClassUnix, "unix"},
		{auth.ClassLoopback, "loopback"},
		{auth.ClassPublic, "public"},
	} {
		if got := tc.c.String(); got != tc.want {
			t.Errorf("Class(%d).String() = %q, want %q", tc.c, got, tc.want)
		}
	}
}
