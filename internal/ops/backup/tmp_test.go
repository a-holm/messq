// SPDX-License-Identifier: Apache-2.0

package backup

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestNewTempDir(t *testing.T) {
	dest := t.TempDir()
	tmp, err := NewTempDir(dest)
	if err != nil {
		t.Fatalf("NewTempDir(%q) = %v, want nil", dest, err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(tmp) }) //nolint:errcheck // best-effort cleanup of scratch a failed subtest left behind

	if filepath.Dir(tmp) != dest {
		t.Fatalf("temp dir %q is not inside the destination directory %q", tmp, dest)
	}
	base := filepath.Base(tmp)
	if len(base) <= len(tempPrefix) || base[:len(tempPrefix)] != tempPrefix {
		t.Fatalf("temp dir name %q does not carry the %q prefix", base, tempPrefix)
	}
	info, statErr := os.Stat(tmp)
	if statErr != nil {
		t.Fatalf("stat temp dir: %v", statErr)
	}
	if got := info.Mode().Perm(); got != 0o700 {
		t.Fatalf("temp dir mode = %o, want 700 (private: symlink and partial-write defence)", got)
	}

	// A second call must mint a distinct directory — two concurrent backups in one
	// destination must never share scratch space.
	second, secondErr := NewTempDir(dest)
	if secondErr != nil {
		t.Fatalf("second NewTempDir(%q) = %v, want nil", dest, secondErr)
	}
	t.Cleanup(func() { _ = os.RemoveAll(second) }) //nolint:errcheck // best-effort cleanup of scratch a failed subtest left behind
	if second == tmp {
		t.Fatal("two NewTempDir calls returned the same path")
	}
}

func TestRenameIntoPlace(t *testing.T) {
	destDir := t.TempDir()
	dest := filepath.Join(destDir, "snap.db")

	t.Run("places the new file at the destination", func(t *testing.T) {
		tmp, mkErr := NewTempDir(destDir)
		if mkErr != nil {
			t.Fatalf("NewTempDir: %v", mkErr)
		}
		t.Cleanup(func() { _ = os.RemoveAll(tmp) }) //nolint:errcheck // best-effort cleanup of scratch a failed subtest left behind
		snap := filepath.Join(tmp, "snap.db")
		if writeErr := os.WriteFile(snap, []byte("new-snapshot"), 0o600); writeErr != nil {
			t.Fatalf("seed snapshot: %v", writeErr)
		}
		if renameErr := RenameIntoPlace(snap, dest); renameErr != nil {
			t.Fatalf("RenameIntoPlace: %v", renameErr)
		}
		got, readErr := os.ReadFile(dest)
		if readErr != nil {
			t.Fatalf("read renamed destination: %v", readErr)
		}
		if string(got) != "new-snapshot" {
			t.Fatalf("destination holds %q, want the new snapshot bytes", got)
		}
		if _, statErr := os.Stat(snap); !errors.Is(statErr, os.ErrNotExist) {
			t.Fatalf("source still exists after rename (stat err = %v), want moved", statErr)
		}
	})

	t.Run("replaces an existing destination whole (never truncated)", func(t *testing.T) {
		old := filepath.Join(destDir, "previous.db")
		if writeErr := os.WriteFile(old, []byte("old-complete-backup"), 0o600); writeErr != nil {
			t.Fatalf("seed old destination: %v", writeErr)
		}
		tmp, mkErr := NewTempDir(destDir)
		if mkErr != nil {
			t.Fatalf("NewTempDir: %v", mkErr)
		}
		t.Cleanup(func() { _ = os.RemoveAll(tmp) }) //nolint:errcheck // best-effort cleanup of scratch a failed subtest left behind
		snap := filepath.Join(tmp, "snap.db")
		if writeErr := os.WriteFile(snap, []byte("new"), 0o600); writeErr != nil {
			t.Fatalf("seed snapshot: %v", writeErr)
		}
		if renameErr := RenameIntoPlace(snap, old); renameErr != nil {
			t.Fatalf("RenameIntoPlace over an existing file: %v", renameErr)
		}
		got, readErr := os.ReadFile(old)
		if readErr != nil || string(got) != "new" {
			t.Fatalf("destination after replace = %q (%v), want the complete new file", got, readErr)
		}
	})

	t.Run("missing source fails without touching the destination", func(t *testing.T) {
		guard := filepath.Join(destDir, "guard.db")
		if writeErr := os.WriteFile(guard, []byte("keep"), 0o600); writeErr != nil {
			t.Fatalf("seed guard: %v", writeErr)
		}
		err := RenameIntoPlace(filepath.Join(destDir, "nope.db"), guard)
		if err == nil {
			t.Fatal("rename from a missing source = nil error, want failure")
		}
		got, readErr := os.ReadFile(guard)
		if readErr != nil || string(got) != "keep" {
			t.Fatalf("failed rename disturbed the destination (%q, %v)", got, readErr)
		}
	})
}

func TestFsyncFileAndDir(t *testing.T) {
	dir := t.TempDir()
	file := filepath.Join(dir, "f.db")
	if writeErr := os.WriteFile(file, []byte("x"), 0o600); writeErr != nil {
		t.Fatalf("seed file: %v", writeErr)
	}
	if err := FsyncFile(file); err != nil {
		t.Fatalf("FsyncFile(%q) = %v, want nil", file, err)
	}
	if err := FsyncDir(dir); err != nil {
		t.Fatalf("FsyncDir(%q) = %v, want nil", dir, err)
	}
	if err := FsyncFile(filepath.Join(dir, "missing")); err == nil {
		t.Fatal("FsyncFile on a missing file = nil error, want failure")
	}
	if err := FsyncFile(dir); err == nil {
		t.Fatal("FsyncFile on a directory = nil error, want failure")
	}
}

func TestSweepStale(t *testing.T) {
	const maxAge = time.Hour
	now := time.Date(2026, 11, 4, 2, 0, 0, 0, time.UTC)

	seed := func(t *testing.T, root, name string, mod time.Time) string {
		t.Helper()
		dir := filepath.Join(root, name)
		if mkErr := os.MkdirAll(dir, 0o700); mkErr != nil {
			t.Fatalf("seed %s: %v", name, mkErr)
		}
		if chtErr := os.Chtimes(dir, mod, mod); chtErr != nil {
			t.Fatalf("set mtime on %s: %v", name, chtErr)
		}
		return dir
	}

	t.Run("removes only directories older than the age", func(t *testing.T) {
		root := t.TempDir()
		oldDir := seed(t, root, tempPrefix+"oldulid", now.Add(-2*maxAge))
		freshDir := seed(t, root, tempPrefix+"freshulid", now.Add(-time.Minute))

		removed, err := SweepStale(root, now, maxAge)
		if err != nil {
			t.Fatalf("SweepStale: %v", err)
		}
		if removed != 1 {
			t.Fatalf("SweepStale removed %d entries, want 1", removed)
		}
		if _, statErr := os.Stat(oldDir); !errors.Is(statErr, os.ErrNotExist) {
			t.Fatalf("stale temp dir still exists after sweep (stat err = %v)", statErr)
		}
		if _, statErr := os.Stat(freshDir); statErr != nil {
			t.Fatalf("fresh temp dir was swept: %v", statErr)
		}
	})

	t.Run("boundary: exactly maxAge old is kept", func(t *testing.T) {
		root := t.TempDir()
		exactDir := seed(t, root, tempPrefix+"exactulid", now.Add(-maxAge))
		removed, err := SweepStale(root, now, maxAge)
		if err != nil {
			t.Fatalf("SweepStale: %v", err)
		}
		if removed != 0 {
			t.Fatalf("sweep removed a directory exactly maxAge old (%d removals), want kept", removed)
		}
		if _, statErr := os.Stat(exactDir); statErr != nil {
			t.Fatalf("boundary directory was swept: %v", statErr)
		}
	})

	t.Run("leaves everything that is not a backup temp dir", func(t *testing.T) {
		root := t.TempDir()
		stranger := seed(t, root, "someone-elses-dir", now.Add(-24*time.Hour))
		strangerFile := filepath.Join(root, tempPrefix+"notadir")
		if writeErr := os.WriteFile(strangerFile, []byte("x"), 0o600); writeErr != nil {
			t.Fatalf("seed file: %v", writeErr)
		}
		removed, err := SweepStale(root, now, maxAge)
		if err != nil {
			t.Fatalf("SweepStale: %v", err)
		}
		if removed != 0 {
			t.Fatalf("sweep removed %d non-temp entries, want 0", removed)
		}
		if _, statErr := os.Stat(stranger); statErr != nil {
			t.Fatalf("unrelated directory was swept: %v", statErr)
		}
		if _, statErr := os.Stat(strangerFile); statErr != nil {
			t.Fatalf("a stale-named FILE was swept; only directories are ours to remove: %v", statErr)
		}
	})

	t.Run("empty root sweeps nothing", func(t *testing.T) {
		removed, err := SweepStale(t.TempDir(), now, maxAge)
		if err != nil || removed != 0 {
			t.Fatalf("SweepStale on an empty dir = (%d, %v), want (0, nil)", removed, err)
		}
	})
}

func TestRemoveTemp(t *testing.T) {
	dir := t.TempDir()
	tmp, mkErr := NewTempDir(dir)
	if mkErr != nil {
		t.Fatalf("NewTempDir: %v", mkErr)
	}
	nested := filepath.Join(tmp, "snap.db")
	if writeErr := os.WriteFile(nested, []byte("x"), 0o600); writeErr != nil {
		t.Fatalf("seed nested file: %v", writeErr)
	}
	if rmErr := RemoveTemp(tmp); rmErr != nil {
		t.Fatalf("RemoveTemp: %v", rmErr)
	}
	if _, statErr := os.Stat(tmp); !errors.Is(statErr, os.ErrNotExist) {
		t.Fatalf("temp dir still exists after RemoveTemp (stat err = %v)", statErr)
	}
	if rmErr := RemoveTemp(tmp); rmErr != nil {
		t.Fatalf("RemoveTemp on an already-removed dir = %v, want idempotent nil", rmErr)
	}
}
