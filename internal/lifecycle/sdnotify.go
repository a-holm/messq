// SPDX-License-Identifier: Apache-2.0

package lifecycle

import (
	"fmt"
	"io"
	"log/slog"
	"net"
	"os"
	"strconv"
	"strings"
	"time"

	"golang.org/x/sys/unix"
)

// notifySocketEnv is the socket path systemd hands a Type=notify service.
const notifySocketEnv = "NOTIFY_SOCKET"

// sdNotify is the production Notifier: one newline-separated datagram per Set,
// sent over SOCK_DGRAM on AF_UNIX (§13: ~60 lines beats coreos/go-systemd and an
// ADR + supply-chain entry).
type sdNotify struct {
	conn *net.UnixConn
}

// NewSdNotifier builds the notifier from $NOTIFY_SOCKET. An unset or dead socket
// downgrades to NopNotifier with at most one WARN — telling systemd is not a
// durability property, so neither condition is ever an error.
func NewSdNotifier(logger *slog.Logger) Notifier {
	return dialSdNotify(os.Getenv(notifySocketEnv), logger)
}

// dialSdNotify connects to an explicit NOTIFY_SOCKET value. A leading '@' names the
// abstract namespace; Go's UnixAddr wants that spelled with a leading NUL byte, so
// it is translated here rather than hoping every caller remembers.
func dialSdNotify(socket string, logger *slog.Logger) Notifier {
	if logger == nil {
		logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	}
	if socket == "" {
		return nopNotifier{}
	}
	name := socket
	if strings.HasPrefix(name, "@") {
		name = "\x00" + name[1:]
	}
	conn, err := net.DialUnix("unixgram", nil, &net.UnixAddr{Name: name, Net: "unixgram"})
	if err != nil {
		logger.Warn(notifyDowngradeEvent, "socket", socket, "err", err)
		return nopNotifier{}
	}
	return &sdNotify{conn: conn}
}

// Set writes fields as one datagram. A RELOADING=1 field automatically carries
// MONOTONIC_USEC from CLOCK_MONOTONIC — systemd ≥253 drops a RELOADING without it,
// which would strand the manager waiting for the READY that ends the reload.
// unix.ClockGettime is a raw syscall on purpose: MONOTONIC_USEC must come from the
// kernel's monotonic clock, not a wall-clock reading.
func (s *sdNotify) Set(fields ...string) error {
	payload := strings.Join(fields, "\n")
	for _, f := range fields {
		if f == "RELOADING=1" {
			payload += fmt.Sprintf("\nMONOTONIC_USEC=%d", monotonicUsec())
			break
		}
	}
	if _, err := s.conn.Write([]byte(payload)); err != nil {
		return fmt.Errorf("sd_notify write: %w", err)
	}
	return nil
}

// Close releases the datagram socket. Datagrams are connectionless in spirit; Close
// exists so tests and long-lived daemons do not pin fds.
func (s *sdNotify) Close() error { return s.conn.Close() }

// monotonicUsec reads CLOCK_MONOTONIC in microseconds. Failure yields 0 — the
// timestamp is advisory for systemd's reload accounting, never worth failing a
// notify over.
func monotonicUsec() uint64 {
	var ts unix.Timespec
	if err := unix.ClockGettime(unix.CLOCK_MONOTONIC, &ts); err != nil || ts.Sec < 0 || ts.Nsec < 0 {
		return 0
	}
	return uint64(ts.Sec)*1_000_000 + uint64(ts.Nsec)/1_000
}

// parseMonotonic extracts the MONOTONIC_USEC value from a rendered datagram;
// test-side helper for the monotonicity assertion.
func parseMonotonic(datagram string) (uint64, bool) {
	const prefix = "MONOTONIC_USEC="
	for _, line := range strings.Split(datagram, "\n") {
		if v, ok := strings.CutPrefix(line, prefix); ok {
			u, err := strconv.ParseUint(v, 10, 64)
			return u, err == nil
		}
	}
	return 0, false
}

// watchdog env names, as systemd documents them for notify services.
const (
	watchdogUsecEnv = "WATCHDOG_USEC"
	watchdogPidEnv  = "WATCHDOG_PID"
)

// notifyDowngradeEvent warns once when a configured NOTIFY_SOCKET cannot be used.
const notifyDowngradeEvent = "lifecycle.notify_downgrade"

// WatchdogInterval returns half of WATCHDOG_USEC — the cadence the daemon must ping
// at to stay inside the service manager's budget — or 0 when pinging is off: env
// unset, unparseable, or WATCHDOG_PID naming some other process (G9: a ping that
// lies is worse than no ping). getenv and pid are injected so tests drive the table
// without mutating process state; production passes os.LookupEnv and os.Getpid.
func WatchdogInterval(getenv func(string) (string, bool), pid int) time.Duration {
	usecStr, ok := getenv(watchdogUsecEnv)
	if !ok {
		return 0
	}
	usec, err := strconv.ParseInt(usecStr, 10, 64)
	if err != nil || usec <= 0 {
		return 0
	}
	if pidStr, ok := getenv(watchdogPidEnv); ok {
		owner, err := strconv.Atoi(pidStr)
		if err != nil || owner != pid {
			return 0
		}
	}
	return time.Duration(usec/2) * time.Microsecond
}
