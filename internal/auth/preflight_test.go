// SPDX-License-Identifier: Apache-2.0

package auth_test

import (
	"os"
	"path/filepath"
	"strconv"
	"testing"

	"github.com/a-holm/messq/internal/auth"
)

// validTokenLine is one four-field token-file line the parser accepts.
const validTokenLine = idPublisher + " " + hashPublish + " publish orders\n"

func writeFile(t *testing.T, path string, mode os.FileMode, content string) {
	t.Helper()
	if err := os.WriteFile(path, []byte(content), mode); err != nil {
		t.Fatalf("write %s: %v", path, err)
	}
	if err := os.Chmod(path, mode); err != nil {
		t.Fatalf("chmod %s: %v", path, err)
	}
}

func mkdir(t *testing.T, path string, mode os.FileMode) {
	t.Helper()
	if err := os.MkdirAll(path, mode); err != nil {
		t.Fatalf("mkdir %s: %v", path, err)
	}
	if err := os.Chmod(path, mode); err != nil {
		t.Fatalf("chmod %s: %v", path, err)
	}
}

func findFinding(fs []auth.Finding, what string) *auth.Finding {
	for i := range fs {
		if fs[i].What == what {
			return &fs[i]
		}
	}
	return nil
}

func assertFatal(t *testing.T, fs []auth.Finding, what, wantFix string) {
	t.Helper()
	f := findFinding(fs, what)
	if f == nil {
		t.Fatalf("no finding for %q in %+v", what, fs)
	}
	if f.Level != auth.LevelFatal {
		t.Fatalf("%q finding level = %v, want fatal", what, f.Level)
	}
	if f.Fix != wantFix {
		t.Errorf("%q fix = %q, want %q", what, f.Fix, wantFix)
	}
}

func TestPreflightDataDir(t *testing.T) {
	t.Parallel()

	t.Run("0700 is clean", func(t *testing.T) {
		dir := t.TempDir()
		mkdir(t, dir, 0o700)
		fs := auth.Preflight(auth.Options{DataDir: dir, UID: os.Geteuid()})
		if f := findFinding(fs, "data dir"); f != nil {
			t.Fatalf("0700 data dir flagged: %+v", f)
		}
	})

	t.Run("0755 is fatal with the exact fix", func(t *testing.T) {
		dir := t.TempDir()
		mkdir(t, dir, 0o755)
		fs := auth.Preflight(auth.Options{DataDir: dir, UID: os.Geteuid()})
		assertFatal(t, fs, "data dir", `chmod 700 "`+dir+`"`)
	})
}

func TestPreflightDBFiles(t *testing.T) {
	t.Parallel()

	t.Run("0600 db and missing siblings are clean", func(t *testing.T) {
		dir := t.TempDir()
		mkdir(t, dir, 0o700)
		writeFile(t, filepath.Join(dir, "messq.db"), 0o600, "")
		fs := auth.Preflight(auth.Options{DataDir: dir, UID: os.Geteuid()})
		if f := findFinding(fs, "database file"); f != nil {
			t.Fatalf("0600 db flagged: %+v", f)
		}
	})

	t.Run("0644 db is fatal", func(t *testing.T) {
		dir := t.TempDir()
		mkdir(t, dir, 0o700)
		db := filepath.Join(dir, "messq.db")
		writeFile(t, db, 0o644, "")
		fs := auth.Preflight(auth.Options{DataDir: dir, UID: os.Geteuid()})
		assertFatal(t, fs, "database file", `chmod 600 "`+db+`"`)
	})

	t.Run("0644 wal sibling is fatal", func(t *testing.T) {
		dir := t.TempDir()
		mkdir(t, dir, 0o700)
		wal := filepath.Join(dir, "messq.db-wal")
		writeFile(t, wal, 0o644, "")
		fs := auth.Preflight(auth.Options{DataDir: dir, UID: os.Geteuid()})
		assertFatal(t, fs, "database file", `chmod 600 "`+wal+`"`)
	})
}

func TestPreflightAuthFileMode(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	mkdir(t, dir, 0o700)
	authFile := filepath.Join(dir, "tokens")
	writeFile(t, authFile, 0o644, validTokenLine)

	fs := auth.Preflight(auth.Options{DataDir: dir, AuthFile: authFile, UID: os.Geteuid()})
	assertFatal(t, fs, "auth file", `chmod 600 "`+authFile+`"`)
}

func TestPreflightAuthFileForeignOwner(t *testing.T) {
	t.Parallel()

	euid := os.Geteuid()
	if euid == 0 {
		t.Skip("cannot create a foreign-owned file as root: the file is always root-owned and allowed")
	}

	dir := t.TempDir()
	mkdir(t, dir, 0o700)
	authFile := filepath.Join(dir, "tokens")
	writeFile(t, authFile, 0o600, validTokenLine)

	// The file is owned by the real euid; reporting a different uid makes it foreign.
	foreign := euid + 12345
	fs := auth.Preflight(auth.Options{DataDir: dir, AuthFile: authFile, UID: foreign})
	assertFatal(t, fs, "auth file", "chown "+strconv.Itoa(foreign)+` "`+authFile+`"`)

	// Owned by the reported uid is clean.
	fs = auth.Preflight(auth.Options{DataDir: dir, AuthFile: authFile, UID: euid})
	if f := findFinding(fs, "auth file"); f != nil {
		t.Fatalf("own auth file flagged: %+v", f)
	}
}

func TestPreflightAuthFileUnparseable(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	mkdir(t, dir, 0o700)
	authFile := filepath.Join(dir, "tokens")
	writeFile(t, authFile, 0o600, "not a valid token line\n")

	fs := auth.Preflight(auth.Options{DataDir: dir, AuthFile: authFile, UID: os.Geteuid()})
	f := findFinding(fs, "auth file")
	if f == nil || f.Level != auth.LevelFatal {
		t.Fatalf("unparseable auth file not fatal: %+v", fs)
	}
}

func TestPreflightZeroTokensWhileRequired(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	mkdir(t, dir, 0o700)
	authFile := filepath.Join(dir, "tokens")
	writeFile(t, authFile, 0o600, "# no tokens\n")

	// Required: fatal.
	fs := auth.Preflight(auth.Options{DataDir: dir, AuthFile: authFile, RequireAuth: true, UID: os.Geteuid()})
	assertFatal(t, fs, "auth file", `messq auth add <id> --auth-file "`+authFile+`"`)

	// Not required: clean.
	fs = auth.Preflight(auth.Options{DataDir: dir, AuthFile: authFile, UID: os.Geteuid()})
	if f := findFinding(fs, "auth file"); f != nil {
		t.Fatalf("zero tokens without a required listener flagged: %+v", f)
	}
}

func TestPreflightSocketMode(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	mkdir(t, dir, 0o700)

	for _, mode := range []os.FileMode{0o666, 0o777} {
		fs := auth.Preflight(auth.Options{DataDir: dir, SocketMode: mode, UID: os.Geteuid()})
		assertFatal(t, fs, "socket mode", "--socket-mode must not grant other read or write")
	}

	for _, mode := range []os.FileMode{0, 0o660} {
		fs := auth.Preflight(auth.Options{DataDir: dir, SocketMode: mode, UID: os.Geteuid()})
		if f := findFinding(fs, "socket mode"); f != nil {
			t.Fatalf("socket mode %04o flagged: %+v", mode, f)
		}
	}
}

func TestPreflightRootWarns(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	mkdir(t, dir, 0o700)

	fs := auth.Preflight(auth.Options{DataDir: dir, UID: 0})
	f := findFinding(fs, "uid")
	if f == nil || f.Level != auth.LevelWarn {
		t.Fatalf("uid 0 not warned: %+v", fs)
	}

	fs = auth.Preflight(auth.Options{DataDir: dir, UID: 1})
	if f := findFinding(fs, "uid"); f != nil {
		t.Fatalf("non-root flagged: %+v", f)
	}
}
