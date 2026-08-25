// SPDX-License-Identifier: Apache-2.0

package lifecycle

import (
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/a-holm/messq/internal/clock"
)

// recvOne reads a single datagram from the test socket with a deadline so a broken
// notifier becomes a failure rather than a hang.
func recvOne(t *testing.T, ln *net.UnixConn) string {
	t.Helper()
	buf := make([]byte, 4096)
	if err := ln.SetReadDeadline(clock.System{}.Now().Add(5 * time.Second)); err != nil {
		t.Fatal(err)
	}
	n, _, err := ln.ReadFrom(buf)
	if err != nil {
		t.Fatalf("test socket never received the datagram: %v", err)
	}
	return string(buf[:n])
}

// newTestSocket binds a unixgram listener at a temp path and returns it plus the
// NOTIFY_SOCKET value pointing at it.
func newTestSocket(t *testing.T) (*net.UnixConn, string) {
	t.Helper()
	sock := filepath.Join(t.TempDir(), "notify.sock")
	addr := &net.UnixAddr{Name: sock, Net: "unixgram"}
	ln, err := net.ListenUnixgram("unixgram", addr)
	if err != nil {
		t.Fatalf("bind test notify socket: %v", err)
	}
	t.Cleanup(func() { _ = ln.Close() }) //nolint:errcheck // best-effort test-socket teardown
	return ln, sock
}

// TestReadyDatagramGolden pins the exact bytes: newline-separated fields, no
// trailing separator, nothing else on the wire.
func TestReadyDatagramGolden(t *testing.T) {
	t.Parallel()

	ln, sock := newTestSocket(t)
	n := dialSdNotify(sock, nil)
	defer func() { _ = n.Close() }() //nolint:errcheck // best-effort notifier teardown

	if err := n.Set("READY=1", "STATUS=serving"); err != nil {
		t.Fatalf("Set over a live socket failed: %v", err)
	}
	if got, want := recvOne(t, ln), "READY=1\nSTATUS=serving"; got != want {
		t.Fatalf("datagram = %q, want %q", got, want)
	}
}

// TestReloadingCarriesMonotonicUsec is the slice's named red: a RELOADING datagram
// without MONOTONIC_USEC is dropped by systemd ≥253, leaving the manager waiting
// forever for the READY that follows.
func TestReloadingCarriesMonotonicUsec(t *testing.T) {
	t.Parallel()

	ln, sock := newTestSocket(t)
	n := dialSdNotify(sock, nil)
	defer func() { _ = n.Close() }() //nolint:errcheck // best-effort notifier teardown

	if err := n.Set("RELOADING=1"); err != nil {
		t.Fatalf("first Set failed: %v", err)
	}
	first := recvOne(t, ln)
	if !strings.HasPrefix(first, "RELOADING=1\nMONOTONIC_USEC=") {
		t.Fatalf("RELOADING datagram %q lacks MONOTONIC_USEC", first)
	}

	if err := n.Set("RELOADING=1"); err != nil {
		t.Fatalf("second Set failed: %v", err)
	}
	second := recvOne(t, ln)

	u1, ok1 := parseMonotonic(first)
	u2, ok2 := parseMonotonic(second)
	if !ok1 || !ok2 {
		t.Fatalf("unparseable MONOTONIC_USEC: %q / %q", first, second)
	}
	if u2 < u1 {
		t.Fatalf("MONOTONIC_USEC went backwards across reloads: %d then %d", u1, u2)
	}
}

func TestPlainFieldsDoNotGrowMonotonic(t *testing.T) {
	t.Parallel()

	ln, sock := newTestSocket(t)
	n := dialSdNotify(sock, nil)
	defer func() { _ = n.Close() }() //nolint:errcheck // best-effort notifier teardown

	if err := n.Set("STOPPING=1"); err != nil {
		t.Fatal(err)
	}
	if got, want := recvOne(t, ln), "STOPPING=1"; got != want {
		t.Fatalf("datagram = %q, want bare %q (MONOTONIC_USEC is RELOADING-only)", got, want)
	}
}

func TestAbstractNamespaceTranslation(t *testing.T) {
	t.Parallel()

	name := "messq-sdnotify-test-" + strconv.Itoa(os.Getpid())
	ln, err := net.ListenUnixgram("unixgram", &net.UnixAddr{Name: "@" + name, Net: "unixgram"})
	if err != nil {
		t.Skipf("abstract unixgram unsupported here: %v", err)
	}
	t.Cleanup(func() { _ = ln.Close() }) //nolint:errcheck // best-effort test-socket teardown

	n := dialSdNotify("@"+name, nil)
	defer func() { _ = n.Close() }() //nolint:errcheck // best-effort notifier teardown
	if err := n.Set("READY=1"); err != nil {
		t.Fatalf("abstract-namespace send failed: %v", err)
	}
	if got := recvOne(t, ln); got != "READY=1" {
		t.Fatalf("abstract socket datagram = %q", got)
	}
}

func TestUnsetOrDeadSocketIsNopNeverError(t *testing.T) {
	t.Parallel()

	for name, socket := range map[string]string{"unset": "", "dead": "/nonexistent-dir-9x/n.sock"} {
		n := dialSdNotify(socket, discardLogger().asLogger())
		if err := n.Set("READY=1"); err != nil {
			t.Fatalf("%s socket: notify returned %v; telling systemd is never an error", name, err)
		}
	}
}

func TestWatchdogIntervalParsing(t *testing.T) {
	t.Parallel()

	env := func(kv map[string]string) func(string) (string, bool) {
		return func(k string) (string, bool) { v, ok := kv[k]; return v, ok }
	}

	tests := []struct {
		name string
		get  func(string) (string, bool)
		pid  int
		want time.Duration
	}{
		{"absent", env(nil), 100, 0},
		{"ours", env(map[string]string{"WATCHDOG_USEC": "30000000"}), 100, 15 * time.Second},
		{"other-pid-disabled", env(map[string]string{
			"WATCHDOG_USEC": "30000000",
			"WATCHDOG_PID":  "7",
		}), 100, 0},
		{"matching-pid-pings", env(map[string]string{
			"WATCHDOG_USEC": "30000000",
			"WATCHDOG_PID":  "100",
		}), 100, 15 * time.Second},
		{"garbage-disabled", env(map[string]string{"WATCHDOG_USEC": "soon"}), 100, 0},
	}
	for _, tt := range tests {
		if got := WatchdogInterval(tt.get, tt.pid); got != tt.want {
			t.Errorf("%s: WatchdogInterval = %v, want %v", tt.name, got, tt.want)
		}
	}
}
