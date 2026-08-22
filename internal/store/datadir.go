// SPDX-License-Identifier: Apache-2.0

package store

import (
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/a-holm/messq/internal/clock"
	"golang.org/x/sys/unix"
)

// The data-directory lifecycle is the first thing Open touches and the security floor of the
// whole product: messq.db holds message payloads in the clear, so §10 demands a 0700
// directory, a 0600 database, and one process per directory. This file owns those rules as
// composable pieces — verify-or-refuse the directory, create the database file exclusively,
// fsync the directory entry, and take the process-lifetime flock — which Open chains
// together in order. Two rules are absolute:
//
//   - Refuse, never repair. If the operator's directory carries group/other bits or foreign
//     ownership, startup fails with the exact chmod to run; silently chmod'ing would paper
//     over whatever else is wrong in a directory messq was told to trust.
//   - The lock fd is never closed except by unlock. flock is granted per open file
//     description, so a stray Close would silently drop the single-instance guarantee while
//     the process keeps running.

const (
	dbFileName   = "messq.db"
	lockFileName = "LOCK"
	bootIDPath   = "/proc/sys/kernel/random/boot_id"
)

// dirLockMode selects the flavour of flock lockDataDir takes on <data-dir>/LOCK.
type dirLockMode uint8

const (
	// lockExclusive is the read-write daemon's mode: LOCK_EX, one holder per directory,
	// holder line written so a refused second instance can say who is holding it.
	lockExclusive dirLockMode = iota
	// lockShared is the ReadOnly offline-inspection mode: LOCK_SH, so inspectors can
	// overlap each other but never a running daemon. Per the Options.ReadOnly contract
	// ("no lock write") it writes no holder line: concurrent shared holders would
	// otherwise corrupt each other's lines mid-write.
	lockShared
)

// ensureDataDir makes sure dir exists as a private, self-owned directory: an absent path is
// created 0700, and any existing directory carrying group/other bits or a foreign owner is
// refused with [ErrDataDirPerms] whose message names the exact chmod to run. It never
// chmods or chowns an existing directory.
func ensureDataDir(dir string) error {
	_, statErr := os.Stat(dir)
	switch {
	case errors.Is(statErr, fs.ErrNotExist):
		if mkErr := os.MkdirAll(dir, 0o700); mkErr != nil {
			return fmt.Errorf("create data directory %s: %w", dir, mkErr)
		}
	case statErr != nil:
		return fmt.Errorf("stat data directory %s: %w", dir, statErr)
	}
	return verifyDirPrivate(dir)
}

// verifyDirPrivate enforces §10 on an existing directory: mode 0700 (no group/other bits)
// and owned by the running uid. The mode check runs first so the operator sees the cheap,
// self-service fix before the ownership question.
func verifyDirPrivate(dir string) error {
	st, err := os.Stat(dir)
	if err != nil {
		return fmt.Errorf("stat data directory %s: %w", dir, err)
	}
	if !st.IsDir() {
		return fmt.Errorf("%w: %s is not a directory", ErrDataDirPerms, dir)
	}
	if perm := st.Mode().Perm(); perm&0o077 != 0 {
		return fmt.Errorf("%w: %s has mode %04o, want 0700; fix with: chmod 700 %q",
			ErrDataDirPerms, dir, perm, dir)
	}
	stat, ok := st.Sys().(*syscall.Stat_t)
	if !ok {
		// Fail closed: on a platform where ownership is unknowable, the §10 guarantee
		// cannot be verified and the directory must not be trusted.
		return fmt.Errorf("%w: %s: cannot determine the directory owner on this platform",
			ErrDataDirPerms, dir)
	}
	if int(stat.Uid) != os.Geteuid() {
		return fmt.Errorf("%w: %s is owned by uid %d, running uid is %d; messq refuses directories it does not own",
			ErrDataDirPerms, dir, stat.Uid, os.Geteuid())
	}
	return nil
}

// initDBFile creates <dir>/messq.db when it does not exist and reports whether this call
// brought it into being. The O_CREAT|O_EXCL open is the atomic fresh-vs-existing decision
// the recovery logic hangs off: there is no window in which two startups both believe they
// created the database. The 0600 mode is passed explicitly — a conventional umask can only
// strip group/other bits, which 0600 never requests, and the test suite verifies the
// resulting mode under syscall.Umask(0). An existing messq.db with any other permission is
// refused — never silently adopted — with the exact chmod in the message; the owning uid is
// deliberately not re-checked on the file, because anyone able to plant a foreign-owned
// file inside dir has already beaten the 0700 directory check.
//
// On the fresh path the directory fd is fsync'd before returning, so the database file's
// directory entry survives a crash that lands immediately after startup reported success.
func initDBFile(dir string) (bool, error) {
	path := filepath.Join(dir, dbFileName)
	fd, err := unix.Open(path, unix.O_CREAT|unix.O_EXCL|unix.O_WRONLY|unix.O_CLOEXEC, 0o600)
	switch {
	case err == nil:
		if closeErr := unix.Close(fd); closeErr != nil {
			return false, fmt.Errorf("close freshly created %s: %w", path, closeErr)
		}
		if verr := verifyFilePrivate(path); verr != nil {
			return false, verr
		}
		if syncErr := syncDir(dir); syncErr != nil {
			return false, syncErr
		}
		return true, nil
	case errors.Is(err, os.ErrExist):
		if verr := verifyFilePrivate(path); verr != nil {
			return false, verr
		}
		return false, nil
	default:
		return false, fmt.Errorf("create database file %s: %w", path, err)
	}
}

// verifyFilePrivate enforces §10 on an existing messq.db: exactly 0600, nothing more.
func verifyFilePrivate(path string) error {
	st, err := os.Stat(path)
	if err != nil {
		return fmt.Errorf("stat database file %s: %w", path, err)
	}
	if st.IsDir() {
		return fmt.Errorf("%w: %s is a directory, not a database file", ErrDataDirPerms, path)
	}
	if perm := st.Mode().Perm(); perm != 0o600 {
		return fmt.Errorf("%w: %s has mode %04o, want 0600; fix with: chmod 600 %q",
			ErrDataDirPerms, path, perm, path)
	}
	return nil
}

// syncDir fsyncs a directory fd so entries created inside it (messq.db, later the WAL
// siblings) are durable across a crash. Directories cannot be opened for writing; O_RDONLY
// is the blessed way and works on ext4 and tmpfs alike.
func syncDir(dir string) error {
	fd, err := unix.Open(dir, unix.O_RDONLY|unix.O_CLOEXEC|unix.O_DIRECTORY, 0)
	if err != nil {
		return fmt.Errorf("open data directory %s for fsync: %w", dir, err)
	}
	if err := unix.Fsync(fd); err != nil {
		fsyncErr := fmt.Errorf("fsync data directory %s: %w", dir, err)
		if cerr := unix.Close(fd); cerr != nil {
			return errors.Join(fsyncErr, fmt.Errorf("close %s: %w", dir, cerr))
		}
		return fsyncErr
	}
	if err := unix.Close(fd); err != nil {
		return fmt.Errorf("close data directory %s after fsync: %w", dir, err)
	}
	return nil
}

// dirLock is a held flock on <data-dir>/LOCK. It deliberately has no finalizer: the fd must
// stay open for the life of the lock — flock is granted per open file description, so
// closing the file would silently release the single-instance guarantee — and only unlock
// may close it.
type dirLock struct {
	f *os.File
}

// lockDataDir takes the process-lifetime lock on <dir>/LOCK in the given mode. The
// non-blocking flock turns "another instance is running" into an immediate refusal instead
// of a hang: on EWOULDBLOCK the existing holder line is read into the error so the operator
// sees the pid, boot id and start time of the process that owns the directory. In
// lockExclusive mode the winner truncates and rewrites the holder line
// ("pid=… boot=… started=…", RFC3339 in UTC); in lockShared mode nothing is written.
//
// The returned lock must be released exactly once via unlock; the fd inside is never closed
// anywhere else.
func lockDataDir(dir string, mode dirLockMode) (*dirLock, error) {
	path := filepath.Join(dir, lockFileName)
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0o600) // os adds O_CLOEXEC itself on unix
	if err != nil {
		return nil, fmt.Errorf("open lock file %s: %w", path, err)
	}
	var how int
	switch mode {
	case lockExclusive:
		how = unix.LOCK_EX
	case lockShared:
		how = unix.LOCK_SH
	}
	if err := unix.Flock(int(f.Fd()), how|unix.LOCK_NB); err != nil {
		holder := readHolderLine(f)
		cerr := f.Close()
		if errors.Is(err, unix.EWOULDBLOCK) {
			if holder != "" {
				return nil, fmt.Errorf("%w: held by %s", ErrDataDirLocked, holder)
			}
			return nil, fmt.Errorf("%w: held by another process that has not written its holder line yet",
				ErrDataDirLocked)
		}
		flockErr := fmt.Errorf("flock %s: %w", path, err)
		if cerr != nil {
			return nil, errors.Join(flockErr, fmt.Errorf("close %s: %w", path, cerr))
		}
		return nil, flockErr
	}
	if mode == lockExclusive {
		if err := writeHolderLine(f); err != nil {
			// We hold a lock we could not stamp; give it back instead of leaving the
			// directory locked by a startup that failed.
			return nil, errors.Join(err, (&dirLock{f: f}).unlock())
		}
	}
	return &dirLock{f: f}, nil
}

// writeHolderLine stamps the exclusive winner's identity into the LOCK file: pid so a
// refused instance (or a human) can see who holds the directory, boot id so a stale line
// from a previous boot is distinguishable from a live process, and started in RFC3339 UTC.
// Truncate-before-write keeps a shorter line from leaving a stale tail behind.
func writeHolderLine(f *os.File) error {
	line := fmt.Sprintf("pid=%d boot=%s started=%s\n",
		os.Getpid(), bootID(), clock.System{}.Now().UTC().Format(time.RFC3339))
	if err := f.Truncate(0); err != nil {
		return fmt.Errorf("truncate lock file: %w", err)
	}
	if _, err := f.WriteAt([]byte(line), 0); err != nil {
		return fmt.Errorf("write lock holder line: %w", err)
	}
	return nil
}

// bootID returns the kernel's boot id: the same value for every process on this machine
// since the last boot, different across reboots.
func bootID() string {
	return bootIDFrom(bootIDPath)
}

// bootIDFrom reads a boot id from path. An unreadable or empty source degrades to "unknown"
// rather than failing startup — the holder line is diagnostics, not a gate.
func bootIDFrom(path string) string {
	b, err := os.ReadFile(path)
	if id := strings.TrimSpace(string(b)); err == nil && id != "" {
		return id
	}
	return "unknown"
}

// readHolderLine best-effort reads the holder line from an already-open LOCK file for
// inclusion in an ErrDataDirLocked message. Any read failure degrades to an empty string:
// the refusal itself must never depend on the diagnostics.
func readHolderLine(f *os.File) string {
	if _, err := f.Seek(0, io.SeekStart); err != nil {
		return ""
	}
	b, err := io.ReadAll(f)
	if err != nil {
		return ""
	}
	return strings.TrimSpace(string(b))
}

// unlock releases the flock and closes the lock file — the only place that happens. An
// unlock failure means the single-instance guarantee is already in an unknown state, so
// both the flock and the close error are surfaced rather than swallowed. Unlocking twice is
// a programming error and reports one.
func (l *dirLock) unlock() error {
	if l == nil || l.f == nil {
		return errors.New("unlock of a data directory lock that is not held")
	}
	flockErr := unix.Flock(int(l.f.Fd()), unix.LOCK_UN)
	closeErr := l.f.Close()
	l.f = nil
	if flockErr != nil {
		flockErr = fmt.Errorf("flock LOCK_UN: %w", flockErr)
	}
	if closeErr != nil {
		closeErr = fmt.Errorf("close lock file: %w", closeErr)
	}
	return errors.Join(flockErr, closeErr)
}
