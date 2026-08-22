// SPDX-License-Identifier: Apache-2.0

package store

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/a-holm/messq/internal/errs"
	"golang.org/x/sys/unix"
)

// setUmask pins the process umask for the duration of one test and restores it afterwards.
// The code under test passes creation modes explicitly, so with a known umask whatever mode
// os.Stat reports afterwards is exactly what the code asked for — the proof that 0700/0600
// do not depend on ambient umask.
func setUmask(t *testing.T, mask int) {
	t.Helper()
	old := syscall.Umask(mask)
	t.Cleanup(func() { syscall.Umask(old) })
}

// holderFields parses a LOCK holder line ("pid=N boot=B started=S") into key/value pairs,
// failing the test if the line is not shaped like one.
func holderFields(t *testing.T, content string) map[string]string {
	t.Helper()
	fields := map[string]string{}
	for _, pair := range strings.Fields(content) {
		k, v, ok := strings.Cut(pair, "=")
		if !ok {
			t.Fatalf("holder line %q contains malformed field %q", content, pair)
		}
		fields[k] = v
	}
	if _, ok := fields["pid"]; !ok {
		t.Fatalf("holder line %q lacks a pid=… field", content)
	}
	return fields
}

// TestDataDirEnsureCreatesMissingDirAsPrivate covers first-run creation: an absent directory
// is made 0700 regardless of umask, and the call is idempotent on the second run.
func TestDataDirEnsureCreatesMissingDirAsPrivate(t *testing.T) {
	setUmask(t, 0) // any group/other bit in the result would come from the code, not the umask
	dir := filepath.Join(t.TempDir(), "data")
	if err := ensureDataDir(dir); err != nil {
		t.Fatalf("ensureDataDir(%q) = %v, want nil", dir, err)
	}
	st, err := os.Stat(dir)
	if err != nil {
		t.Fatalf("stat %s: %v", dir, err)
	}
	if !st.IsDir() {
		t.Fatalf("%s is not a directory", dir)
	}
	if got := st.Mode().Perm(); got != 0o700 {
		t.Fatalf("%s has mode %04o, want 0700", dir, got)
	}
	if err := ensureDataDir(dir); err != nil {
		t.Fatalf("second ensureDataDir(%q) = %v, want nil (idempotent)", dir, err)
	}
}

// TestDataDirEnsureRefusesBroadModes covers §10: an existing directory with any group/other
// bit is refused with ErrDataDirPerms, the message names the exact chmod to run, and the
// directory is left untouched — the code refuses, it never fixes permissions behind the
// operator's back.
func TestDataDirEnsureRefusesBroadModes(t *testing.T) {
	for _, mode := range []os.FileMode{0o755, 0o777, 0o750, 0o707} {
		t.Run(fmt.Sprintf("%#o", mode), func(t *testing.T) {
			dir := filepath.Join(t.TempDir(), "data")
			if err := os.MkdirAll(dir, 0o700); err != nil {
				t.Fatalf("mkdir %s: %v", dir, err)
			}
			if err := os.Chmod(dir, mode); err != nil {
				t.Fatalf("chmod %s: %v", dir, err)
			}

			err := ensureDataDir(dir)
			if !errors.Is(err, ErrDataDirPerms) {
				t.Fatalf("ensureDataDir(%q) = %v, want ErrDataDirPerms", dir, err)
			}
			wantCmd := fmt.Sprintf("chmod 700 %q", dir)
			if !strings.Contains(err.Error(), wantCmd) {
				t.Errorf("error %q does not tell the operator the exact fix %q", err, wantCmd)
			}
			st, statErr := os.Stat(dir)
			if statErr != nil {
				t.Fatalf("stat %s: %v", dir, statErr)
			}
			if got := st.Mode().Perm(); got != mode {
				t.Errorf("refused directory came back %04o, want untouched %04o: refuse, never chmod", got, mode)
			}
		})
	}
}

// TestDataDirEnsureRefusesForeignOwner covers §10's ownership rule: a directory owned by
// another uid is refused even when its mode bits look private, because that mode protects
// someone else's eyes, not ours. Fabricating the condition needs chown, i.e. root; anywhere
// else the test says so instead of pretending to have run.
func TestDataDirEnsureRefusesForeignOwner(t *testing.T) {
	if os.Geteuid() != 0 {
		t.Skip("fabricating a foreign-owned directory requires root to chown; skipping")
	}
	dir := filepath.Join(t.TempDir(), "data")
	if err := os.MkdirAll(dir, 0o700); err != nil {
		t.Fatalf("mkdir %s: %v", dir, err)
	}
	if err := os.Chown(dir, 65534, 65534); err != nil {
		t.Fatalf("chown %s to nobody: %v", dir, err)
	}

	err := ensureDataDir(dir)
	if !errors.Is(err, ErrDataDirPerms) {
		t.Fatalf("ensureDataDir(%q) = %v, want ErrDataDirPerms for a foreign-owned directory", dir, err)
	}
	if !strings.Contains(err.Error(), strconv.Itoa(65534)) {
		t.Errorf("error %q does not name the owning uid 65534", err)
	}
}

// TestDataDirInitDBFileDetectsFreshThenExisting covers the atomic fresh-vs-existing decision
// the recovery logic hangs off: the first call creates messq.db exclusively (O_EXCL) with
// mode 0600 and reports fresh=true; the second call finds it there and reports fresh=false
// without disturbing it.
func TestDataDirInitDBFileDetectsFreshThenExisting(t *testing.T) {
	setUmask(t, 0)
	dir := filepath.Join(t.TempDir(), "data")
	if err := ensureDataDir(dir); err != nil {
		t.Fatalf("ensureDataDir(%q) = %v, want nil", dir, err)
	}

	fresh, err := initDBFile(dir)
	if err != nil {
		t.Fatalf("first initDBFile(%q) = %v, want nil", dir, err)
	}
	if !fresh {
		t.Fatal("first initDBFile reported fresh=false for a missing database")
	}

	db := filepath.Join(dir, "messq.db")
	st, err := os.Stat(db)
	if err != nil {
		t.Fatalf("stat %s: %v", db, err)
	}
	if got := st.Mode().Perm(); got != 0o600 {
		t.Fatalf("%s has mode %04o, want 0600 (umask-proof creation)", db, got)
	}

	fresh, err = initDBFile(dir)
	if err != nil {
		t.Fatalf("second initDBFile(%q) = %v, want nil", dir, err)
	}
	if fresh {
		t.Fatal("second initDBFile reported fresh=true for an existing database")
	}
}

// TestDataDirInitDBFileRefusesBroadDBMode covers the existing-database guard: a messq.db
// carrying group/other bits is refused with the exact chmod in the message, never silently
// adopted — secrets at rest demand 0600.
func TestDataDirInitDBFileRefusesBroadDBMode(t *testing.T) {
	setUmask(t, 0)
	dir := filepath.Join(t.TempDir(), "data")
	if err := ensureDataDir(dir); err != nil {
		t.Fatalf("ensureDataDir(%q) = %v, want nil", dir, err)
	}
	db := filepath.Join(dir, "messq.db")
	if err := os.WriteFile(db, nil, 0o600); err != nil {
		t.Fatalf("seed %s: %v", db, err)
	}
	if err := os.Chmod(db, 0o644); err != nil {
		t.Fatalf("chmod %s: %v", db, err)
	}

	_, err := initDBFile(dir)
	if !errors.Is(err, ErrDataDirPerms) {
		t.Fatalf("initDBFile(%q) = %v, want ErrDataDirPerms", dir, err)
	}
	wantCmd := fmt.Sprintf("chmod 600 %q", db)
	if !strings.Contains(err.Error(), wantCmd) {
		t.Errorf("error %q does not tell the operator the exact fix %q", err, wantCmd)
	}
}

// TestDataDirSyncDirNoError pins that fsyncing the directory fd works on real Linux
// filesystems (ext4 under home, tmpfs under /tmp): the crash-safety step that makes the
// freshly created messq.db survive a power cut must never be an error path here.
func TestDataDirSyncDirNoError(t *testing.T) {
	dir := t.TempDir()
	if err := syncDir(dir); err != nil {
		t.Fatalf("syncDir(%q) = %v, want nil", dir, err)
	}
}

// TestDataDirExclusiveLockBlocksSecondHolder covers the single-instance rule end to end:
// the first exclusive lock writes a parseable pid/boot/started holder line; a second
// exclusive lock taken through a separate open file description in this same process is
// refused with ErrDataDirLocked (wrapping errs.ErrLocked) whose message carries that pid;
// and after unlock the directory can be locked again.
func TestDataDirExclusiveLockBlocksSecondHolder(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "data")
	if err := ensureDataDir(dir); err != nil {
		t.Fatalf("ensureDataDir(%q) = %v, want nil", dir, err)
	}

	first, err := lockDataDir(dir, lockExclusive)
	if err != nil {
		t.Fatalf("first lockDataDir(%q, exclusive) = %v, want nil", dir, err)
	}

	raw, err := os.ReadFile(filepath.Join(dir, "LOCK"))
	if err != nil {
		t.Fatalf("read LOCK: %v", err)
	}
	fields := holderFields(t, string(raw))
	if got, want := fields["pid"], strconv.Itoa(os.Getpid()); got != want {
		t.Errorf("holder line pid=%s, want %s (this process)", got, want)
	}
	if fields["boot"] == "" || fields["boot"] == "unknown" {
		t.Errorf("holder line boot=%q, want the kernel boot id", fields["boot"])
	}
	started, err := time.Parse(time.RFC3339, fields["started"])
	if err != nil {
		t.Fatalf("holder line started=%q is not RFC3339: %v", fields["started"], err)
	}
	if started.Location() != time.UTC {
		t.Errorf("holder line started=%q is not stamped in UTC", fields["started"])
	}

	_, err = lockDataDir(dir, lockExclusive)
	if !errors.Is(err, ErrDataDirLocked) {
		t.Fatalf("second exclusive lock = %v, want ErrDataDirLocked", err)
	}
	if !errors.Is(err, errs.ErrLocked) {
		t.Errorf("second lock error does not wrap errs.ErrLocked: %v", err)
	}
	if want := fmt.Sprintf("pid=%d", os.Getpid()); !strings.Contains(err.Error(), want) {
		t.Errorf("second lock error %q does not carry the holder pid %q", err, want)
	}

	if uerr := first.unlock(); uerr != nil {
		t.Fatalf("first unlock = %v, want nil", uerr)
	}
	again, err := lockDataDir(dir, lockExclusive)
	if err != nil {
		t.Fatalf("re-lock after unlock = %v, want nil", err)
	}
	if err := again.unlock(); err != nil {
		t.Fatalf("re-lock unlock = %v, want nil", err)
	}
}

// TestDataDirSharedLockAllowsConcurrentInspectors pins the read-only variant: LOCK_SH lets a
// second inspector hold the lock simultaneously and writes nothing — Options.ReadOnly
// promises "no lock write" and concurrent shared holders would otherwise corrupt each
// other's lines — while the exclusive daemon lock stays refused until the inspectors leave.
func TestDataDirSharedLockAllowsConcurrentInspectors(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "data")
	if err := ensureDataDir(dir); err != nil {
		t.Fatalf("ensureDataDir(%q) = %v, want nil", dir, err)
	}
	lockPath := filepath.Join(dir, "LOCK")
	const seed = "pid=1 boot=seed started=2000-01-01T00:00:00Z\n"
	if err := os.WriteFile(lockPath, []byte(seed), 0o600); err != nil {
		t.Fatalf("seed %s: %v", lockPath, err)
	}

	first, err := lockDataDir(dir, lockShared)
	if err != nil {
		t.Fatalf("first shared lock = %v, want nil", err)
	}
	second, err := lockDataDir(dir, lockShared)
	if err != nil {
		t.Fatalf("second shared lock = %v, want nil (LOCK_SH must nest)", err)
	}

	got, err := os.ReadFile(lockPath)
	if err != nil {
		t.Fatalf("re-read LOCK: %v", err)
	}
	if string(got) != seed {
		t.Errorf("shared lock rewrote the holder line: got %q, want untouched %q", got, seed)
	}

	if _, lerr := lockDataDir(dir, lockExclusive); !errors.Is(lerr, ErrDataDirLocked) {
		t.Fatalf("exclusive lock while shared held = %v, want ErrDataDirLocked", lerr)
	}

	for _, l := range []*dirLock{second, first} {
		if uerr := l.unlock(); uerr != nil {
			t.Fatalf("unlock = %v, want nil", uerr)
		}
	}
	exclusive, err := lockDataDir(dir, lockExclusive)
	if err != nil {
		t.Fatalf("exclusive lock after inspectors left = %v, want nil", err)
	}
	if err := exclusive.unlock(); err != nil {
		t.Fatalf("exclusive unlock = %v, want nil", err)
	}
}

// requireNonRoot skips the tests that fabricate filesystem refusals through permission bits:
// as root those bits do not stop anything, so the fixtures would not fail the way they must
// for a real operator.
func requireNonRoot(t *testing.T) {
	t.Helper()
	if os.Geteuid() == 0 {
		t.Skip("permission-refusal fixtures need an unprivileged uid to fail as designed")
	}
}

// TestDataDirEnsureRefusesPlainFile covers the not-a-directory guard: a file sitting where
// the data dir belongs is refused rather than descended into or replaced.
func TestDataDirEnsureRefusesPlainFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "data")
	if err := os.WriteFile(path, nil, 0o600); err != nil {
		t.Fatalf("plant file: %v", err)
	}
	err := ensureDataDir(path)
	if !errors.Is(err, ErrDataDirPerms) {
		t.Fatalf("ensureDataDir(%q) = %v, want ErrDataDirPerms for a plain file", path, err)
	}
	if !strings.Contains(err.Error(), "is not a directory") {
		t.Errorf("error %q does not say the path is not a directory", err)
	}
}

// TestDataDirEnsureCreateFailsWhenParentReadOnly covers the creation error path: an absent
// directory under an unwritable parent fails with the underlying cause instead of being
// silently swallowed.
func TestDataDirEnsureCreateFailsWhenParentReadOnly(t *testing.T) {
	requireNonRoot(t)
	parent := t.TempDir()
	if err := os.Chmod(parent, 0o500); err != nil {
		t.Fatalf("chmod parent: %v", err)
	}
	t.Cleanup(func() {
		if rerr := os.Chmod(parent, 0o700); rerr != nil {
			t.Errorf("restore parent mode: %v", rerr)
		}
	})

	dir := filepath.Join(parent, "data")
	err := ensureDataDir(dir)
	if err == nil {
		t.Fatalf("ensureDataDir(%q) = nil, want an error under a read-only parent", dir)
	}
	if errors.Is(err, ErrDataDirPerms) {
		t.Errorf("creation failure surfaced as ErrDataDirPerms (%v); want the underlying cause", err)
	}
	if !strings.Contains(err.Error(), "create data directory") {
		t.Errorf("error %q does not name the failed creation", err)
	}
}

// TestDataDirInitDBFileRefusesDirectoryNamedMessqDB covers the shape guard: a directory
// occupying the messq.db slot is refused, not adopted as the database.
func TestDataDirInitDBFileRefusesDirectoryNamedMessqDB(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "data")
	if err := ensureDataDir(dir); err != nil {
		t.Fatalf("ensureDataDir: %v", err)
	}
	if err := os.MkdirAll(filepath.Join(dir, "messq.db"), 0o700); err != nil {
		t.Fatalf("plant directory: %v", err)
	}
	fresh, err := initDBFile(dir)
	if fresh {
		t.Error("initDBFile reported fresh=true for a directory in the database slot")
	}
	if !errors.Is(err, ErrDataDirPerms) {
		t.Fatalf("initDBFile(%q) = %v, want ErrDataDirPerms", dir, err)
	}
	if !strings.Contains(err.Error(), "not a database file") {
		t.Errorf("error %q does not say the path is not a database file", err)
	}
}

// TestDataDirInitDBFileCreateFailsWhenDirReadOnly covers the exclusive-create error path:
// a 0700 directory that has been made unwritable out from under us fails the O_EXCL open
// with the underlying cause instead of proceeding without a database file.
func TestDataDirInitDBFileCreateFailsWhenDirReadOnly(t *testing.T) {
	requireNonRoot(t)
	dir := t.TempDir()
	if err := os.Chmod(dir, 0o500); err != nil {
		t.Fatalf("chmod dir: %v", err)
	}
	t.Cleanup(func() {
		if rerr := os.Chmod(dir, 0o700); rerr != nil {
			t.Errorf("restore dir mode: %v", rerr)
		}
	})

	fresh, err := initDBFile(dir)
	if fresh {
		t.Error("initDBFile reported fresh=true although nothing could be created")
	}
	if err == nil {
		t.Fatal("initDBFile = nil, want the O_EXCL open failure under a read-only directory")
	}
	if !strings.Contains(err.Error(), "create database file") {
		t.Errorf("error %q does not name the failed creation", err)
	}
}

// TestDataDirSyncDirErrorsOnMissingDir pins that a directory fsync of a path that does not
// exist is reported, not swallowed: a silent fsync failure would quietly void the
// crash-safety promise the call exists for.
func TestDataDirSyncDirErrorsOnMissingDir(t *testing.T) {
	missing := filepath.Join(t.TempDir(), "nope")
	if err := syncDir(missing); err == nil {
		t.Fatalf("syncDir(%q) = nil, want an error for a missing directory", missing)
	}
}

// TestDataDirLockDataDirErrorsWhenDirMissing covers the open failure path: locking a data
// directory that does not exist fails with the cause instead of conjuring one.
func TestDataDirLockDataDirErrorsWhenDirMissing(t *testing.T) {
	missing := filepath.Join(t.TempDir(), "data")
	if _, err := lockDataDir(missing, lockExclusive); err == nil {
		t.Fatalf("lockDataDir(%q) = nil error, want the open failure", missing)
	}
}

// TestDataDirSecondLockWithoutHolderLineStillRefuses covers the degraded diagnostics path:
// a holder that took an exclusive flock without writing its line (a crashed startup, a
// foreign tool) is still refused — the refusal must not depend on the diagnostics being
// there.
func TestDataDirSecondLockWithoutHolderLineStillRefuses(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "data")
	if err := ensureDataDir(dir); err != nil {
		t.Fatalf("ensureDataDir: %v", err)
	}
	lockPath := filepath.Join(dir, "LOCK")
	raw, err := os.OpenFile(lockPath, os.O_RDWR|os.O_CREATE, 0o600)
	if err != nil {
		t.Fatalf("open LOCK: %v", err)
	}
	if ferr := unix.Flock(int(raw.Fd()), unix.LOCK_EX|unix.LOCK_NB); ferr != nil {
		t.Fatalf("raw flock: %v", ferr)
	}
	defer func() {
		if uerr := unix.Flock(int(raw.Fd()), unix.LOCK_UN); uerr != nil {
			t.Errorf("release raw flock: %v", uerr)
		}
		if cerr := raw.Close(); cerr != nil {
			t.Errorf("close raw LOCK fd: %v", cerr)
		}
	}()

	_, err = lockDataDir(dir, lockExclusive)
	if !errors.Is(err, ErrDataDirLocked) {
		t.Fatalf("lockDataDir = %v, want ErrDataDirLocked against a line-less holder", err)
	}
	if !strings.Contains(err.Error(), "has not written its holder line") {
		t.Errorf("error %q does not explain the missing holder line", err)
	}
}

// TestDataDirUnlockTwiceReports pins the single-use contract: the fd is closed by the first
// unlock, so a second one is a programming error and must say so instead of silently
// operating on a stale handle.
func TestDataDirUnlockTwiceReports(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "data")
	if err := ensureDataDir(dir); err != nil {
		t.Fatalf("ensureDataDir: %v", err)
	}
	lock, err := lockDataDir(dir, lockExclusive)
	if err != nil {
		t.Fatalf("lockDataDir = %v, want nil", err)
	}
	if err := lock.unlock(); err != nil {
		t.Fatalf("first unlock = %v, want nil", err)
	}
	if err := lock.unlock(); err == nil {
		t.Fatal("second unlock = nil, want an error for an unheld lock")
	}
}

// TestDataDirEnsureErrorsWhenPathThroughPlainFile covers the stat error path: a data-dir
// path that runs through an existing regular file fails with the underlying ENOTDIR cause
// instead of being treated as absent-and-creatable.
func TestDataDirEnsureErrorsWhenPathThroughPlainFile(t *testing.T) {
	file := filepath.Join(t.TempDir(), "file")
	if err := os.WriteFile(file, nil, 0o600); err != nil {
		t.Fatalf("plant file: %v", err)
	}
	dir := filepath.Join(file, "data")
	err := ensureDataDir(dir)
	if err == nil {
		t.Fatalf("ensureDataDir(%q) = nil, want the ENOTDIR failure", dir)
	}
	if errors.Is(err, ErrDataDirPerms) {
		t.Errorf("path failure surfaced as ErrDataDirPerms (%v); want the underlying cause", err)
	}
	if !strings.Contains(err.Error(), "stat data directory") {
		t.Errorf("error %q does not name the failed stat", err)
	}
}

// TestDataDirInitDBFileFreshCreateStillVerifiedUnderHostileUmask proves the post-create
// verification is load-bearing: under a umask of 0777 the kernel strips even the owner bits
// from the requested 0600, the freshly created file lands at 0000, and initDBFile refuses
// with the exact chmod rather than adopting a file it cannot vouch for. The refusal — not a
// silent chmod — is the §10 contract.
func TestDataDirInitDBFileFreshCreateStillVerifiedUnderHostileUmask(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "data")
	if err := ensureDataDir(dir); err != nil {
		t.Fatalf("ensureDataDir: %v", err)
	}
	// The directory is in place; only the database file now feels the hostile umask.
	setUmask(t, 0o777)
	fresh, err := initDBFile(dir)
	if fresh {
		t.Error("initDBFile reported fresh=true for a file it refused")
	}
	db := filepath.Join(dir, "messq.db")
	if !errors.Is(err, ErrDataDirPerms) {
		t.Fatalf("initDBFile(%q) = %v, want ErrDataDirPerms for a umask-mangled file", dir, err)
	}
	wantCmd := fmt.Sprintf("chmod 600 %q", db)
	if !strings.Contains(err.Error(), wantCmd) {
		t.Errorf("error %q does not tell the operator the exact fix %q", err, wantCmd)
	}
}

// TestDataDirBootIDDegradesGracefully pins the boot-id contract: a real source is returned
// trimmed, and an unreadable or empty one degrades to "unknown" instead of failing startup
// or writing an empty field into the holder line.
func TestDataDirBootIDDegradesGracefully(t *testing.T) {
	if got := bootID(); got == "" || got == "unknown" {
		t.Errorf("bootID() = %q on a Linux box with /proc; want the kernel boot id", got)
	}
	trimmed := filepath.Join(t.TempDir(), "boot_id")
	if err := os.WriteFile(trimmed, []byte("  uuid-1234 \n"), 0o600); err != nil {
		t.Fatalf("seed: %v", err)
	}
	if got := bootIDFrom(trimmed); got != "uuid-1234" {
		t.Errorf("bootIDFrom(trimmed source) = %q, want uuid-1234", got)
	}
	if got := bootIDFrom(filepath.Join(t.TempDir(), "missing")); got != "unknown" {
		t.Errorf("bootIDFrom(missing) = %q, want unknown", got)
	}
}

// TestDataDirVerifiersReportVanishedPaths pins the verifier contracts at the unit level: a
// path that disappears between the caller's existence decision and the verification is
// reported as the stat failure it is — never silently passed, never mislabelled as a
// permissions problem.
func TestDataDirVerifiersReportVanishedPaths(t *testing.T) {
	missing := filepath.Join(t.TempDir(), "gone")
	if err := verifyDirPrivate(missing); err == nil {
		t.Error("verifyDirPrivate(missing) = nil, want the stat failure")
	} else if errors.Is(err, ErrDataDirPerms) {
		t.Errorf("verifyDirPrivate(missing) = %v; absence is not a permissions violation", err)
	}
	if err := verifyFilePrivate(missing); err == nil {
		t.Error("verifyFilePrivate(missing) = nil, want the stat failure")
	} else if errors.Is(err, ErrDataDirPerms) {
		t.Errorf("verifyFilePrivate(missing) = %v; absence is not a permissions violation", err)
	}
}

// TestDataDirInitDBFileSyncFailureSurfaces pins the crash-safety contract end to end: when
// the directory fd cannot be fsync'd after the exclusive create, initDBFile reports the
// failure and refuses to claim freshness — a database whose directory entry may not have
// survived must never be announced to recovery as freshly created.
func TestDataDirInitDBFileSyncFailureSurfaces(t *testing.T) {
	requireNonRoot(t)
	dir := t.TempDir()
	// write+execute without read: the O_EXCL create succeeds, while the read-only
	// directory open for fsync cannot; that split is exactly the behaviour under test.
	if err := os.Chmod(dir, 0o300); err != nil {
		t.Fatalf("chmod dir: %v", err)
	}
	t.Cleanup(func() {
		if rerr := os.Chmod(dir, 0o700); rerr != nil {
			t.Errorf("restore dir mode: %v", rerr)
		}
	})

	fresh, err := initDBFile(dir)
	if fresh {
		t.Error("initDBFile reported fresh=true although the directory fsync failed")
	}
	if err == nil {
		t.Fatal("initDBFile = nil, want the directory fsync/open failure")
	}
	if !strings.Contains(err.Error(), "data directory") {
		t.Errorf("error %q does not name the failing directory operation", err)
	}
}
