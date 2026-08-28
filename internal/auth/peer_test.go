// SPDX-License-Identifier: Apache-2.0

package auth

import (
	"context"
	"net"
	"os"
	"runtime"
	"testing"
)

// TestUnixPeerSelfPair asks for the credential of THIS process over a genuine
// AF_UNIX connection: kernel-guaranteed identity, fully deterministic on every
// Linux box regardless of containerisation.
func TestUnixPeerSelfPair(t *testing.T) {
	path := t.TempDir() + "/peer.sock"

	ctx := context.Background()
	ln, err := (&net.ListenConfig{}).Listen(ctx, "unix", path)
	if err != nil {
		t.Fatalf("listen unix: %v", err)
	}
	defer func() {
		if cerr := ln.Close(); cerr != nil {
			t.Logf("close listener: %v", cerr)
		}
	}()

	clientDone := make(chan error, 1)
	go func() {
		conn, derr := (&net.Dialer{}).DialContext(ctx, "unix", path)
		if derr != nil {
			clientDone <- derr
			return
		}
		buf := make([]byte, 1)
		_, _ = conn.Read(buf) //nolint:errcheck // the read's result is irrelevant; close ends the accept
		_ = conn.Close()      //nolint:errcheck // test cleanup
		clientDone <- nil
	}()

	serverConn, aerr := ln.Accept()
	if aerr != nil {
		t.Fatalf("accept: %v", aerr)
	}
	defer func() {
		if cerr := serverConn.Close(); cerr != nil {
			t.Logf("close accepted conn: %v", cerr)
		}
	}()

	switch runtime.GOOS {
	case "linux":
		peer, ok := UnixPeer(serverConn)
		if !ok {
			t.Fatal("SO_PEERCRED unavailable on an AF_UNIX stream socket")
		}
		if peer.UID != int32(os.Getuid()) {
			t.Errorf("peer.UID = %d, want %d", peer.UID, os.Getuid())
		}
		if peer.GID != int32(os.Getgid()) {
			t.Errorf("peer.GID = %d, want %d", peer.GID, os.Getgid())
		}
		if peer.PID != int32(os.Getpid()) {
			t.Errorf("peer.PID = %d, want %d", peer.PID, os.Getpid())
		}
	default:
		if _, ok := UnixPeer(serverConn); ok {
			t.Fatal("non-Linux must report SO_PEERCRED as unavailable")
		}
	}

	if _, werr := serverConn.Write([]byte{0}); werr != nil {
		t.Fatalf("write probe byte: %v", werr)
	}
	if derr := <-clientDone; derr != nil {
		t.Fatalf("client side: %v", derr)
	}
}

// The hook refuses everything that is not an AF_UNIX socket. This is not pedantry:
// generic stream sockets answer SO_PEERCRED for TCP too — with the credentials of
// whoever created the LISTENING socket, i.e. the daemon attributing itself. A
// positive answer there would mis-attribute an actor.
func TestUnixPeerRejectsTCP(t *testing.T) {
	ctx := context.Background()
	ln, err := (&net.ListenConfig{}).Listen(ctx, "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen tcp: %v", err)
	}
	defer func() {
		if cerr := ln.Close(); cerr != nil {
			t.Logf("close tcp listener: %v", cerr)
		}
	}()

	type accepted struct {
		conn net.Conn
		err  error
	}
	in := make(chan accepted, 1)
	go func() {
		c, aerr := ln.Accept()
		in <- accepted{conn: c, err: aerr}
	}()

	client, derr := (&net.Dialer{}).DialContext(ctx, ln.Addr().Network(), ln.Addr().String())
	if derr != nil {
		t.Fatalf("dial: %v", derr)
	}
	defer func() {
		if cerr := client.Close(); cerr != nil {
			t.Logf("close client: %v", cerr)
		}
	}()
	r := <-in
	if r.err != nil {
		t.Fatalf("accept: %v", r.err)
	}
	defer func() {
		if cerr := r.conn.Close(); cerr != nil {
			t.Logf("close server conn: %v", cerr)
		}
	}()

	if _, ok := UnixPeer(r.conn); ok {
		t.Error("UnixPeer(TCP conn) reported success; must report unavailable")
	}
}
