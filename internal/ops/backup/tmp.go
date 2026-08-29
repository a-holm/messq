// SPDX-License-Identifier: Apache-2.0

package backup

import (
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/a-holm/messq/internal/clock"
	"github.com/a-holm/messq/internal/id"
)

// tempPrefix names the private scratch directory a backup runs inside:
// <destDir>/.messq-backup-<ulid>/. SQLite refuses a VACUUM INTO target that
// already exists, so the O_EXCL pre-create trick is off the table; the private
// 0700 directory is the defence against symlink games and partial writes at
// the final path instead. Do not "simplify" this away (issue #30 §6).
const tempPrefix = ".messq-backup-"

// NewTempDir creates and returns a fresh private scratch directory under
// destDir. The mode is forced to 0700 after creation so the guarantee holds
// under any process umask.
func NewTempDir(destDir string) (string, error) {
	name := tempPrefix + id.NewGen(clock.System{}).New().String()
	dir := filepath.Join(destDir, name)
	if err := os.Mkdir(dir, 0o700); err != nil {
		return "", fmt.Errorf("create backup temp dir: %w", err)
	}
	if chErr := os.Chmod(dir, 0o700); chErr != nil { //nolint:gosec // G302: 0700 IS the point — a directory needs its owner-exec bit; D12 secure-by-default
		return "", fmt.Errorf("force mode 0700 on backup temp dir: %w", chErr)
	}
	return dir, nil
}

// RenameIntoPlace atomically moves the finished snapshot onto dest with
// rename(2). Source and destination share one filesystem by construction (the
// snapshot lives in destDir), which is what makes the rename atomic: the
// destination is always the old file or the new one, never a truncated file,
// because the rename is the last step of every successful run.
func RenameIntoPlace(snapPath, dest string) error {
	if err := os.Rename(snapPath, dest); err != nil {
		return fmt.Errorf("rename %s into place at %s: %w", snapPath, dest, err)
	}
	return nil
}

// FsyncFile flushes one regular file's contents to stable storage. A directory
// is refused: fsync-on-a-directory-fd is FsyncDir's job, and silently accepting
// the wrong kind of path would let a caller believe it durably flushed a file
// when it only flushed a directory entry.
func FsyncFile(path string) error {
	info, err := os.Stat(path)
	if err != nil {
		return fmt.Errorf("open %s for fsync: %w", path, err)
	}
	if !info.Mode().IsRegular() {
		return fmt.Errorf("fsync %s: not a regular file", path)
	}
	f, openErr := os.Open(path)
	if openErr != nil {
		return fmt.Errorf("open %s for fsync: %w", path, openErr)
	}
	defer func() {
		if closeErr := f.Close(); closeErr != nil {
			err = fmt.Errorf("close %s after fsync: %w", path, closeErr)
		}
	}()
	if syncErr := f.Sync(); syncErr != nil {
		return fmt.Errorf("fsync %s: %w", path, syncErr)
	}
	return err
}

// FsyncDir fsyncs a directory entry so a rename or creation inside it survives
// a power loss (the parent-dir half of atomic placement).
func FsyncDir(path string) error {
	d, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("open directory %s for fsync: %w", path, err)
	}
	defer func() {
		if closeErr := d.Close(); closeErr != nil {
			err = fmt.Errorf("close directory %s after fsync: %w", path, closeErr)
		}
	}()
	if syncErr := d.Sync(); syncErr != nil {
		return fmt.Errorf("fsync directory %s: %w", path, syncErr)
	}
	return err
}

// SweepStale removes leftover .messq-backup-* directories under root whose
// modification time is strictly older than maxAge before now — a killed backup
// leaves one behind, and the next run sweeps it (issue #30 §2 step 8).
// Directories exactly maxAge old are kept; anything under root that is not a
// backup temp directory (other files, other tools' directories) is never
// touched. It returns how many entries it removed.
func SweepStale(root string, now time.Time, maxAge time.Duration) (removed int, err error) {
	entries, readErr := os.ReadDir(root)
	if readErr != nil {
		return 0, fmt.Errorf("sweep stale backup temps in %s: %w", root, readErr)
	}
	for _, entry := range entries {
		if !entry.IsDir() || len(entry.Name()) <= len(tempPrefix) || entry.Name()[:len(tempPrefix)] != tempPrefix {
			continue
		}
		info, statErr := entry.Info()
		if statErr != nil {
			return removed, fmt.Errorf("stat %s: %w", filepath.Join(root, entry.Name()), statErr)
		}
		if age := now.Sub(info.ModTime()); age <= maxAge {
			continue
		}
		if rmErr := os.RemoveAll(filepath.Join(root, entry.Name())); rmErr != nil {
			return removed, fmt.Errorf("remove stale backup temp %s: %w",
				filepath.Join(root, entry.Name()), rmErr)
		}
		removed++
	}
	return removed, nil
}

// RemoveTemp deletes a finished or failed run's scratch tree. A missing
// directory is already the wanted end state, so absence is not an error.
func RemoveTemp(dir string) error {
	if err := os.RemoveAll(dir); err != nil {
		return fmt.Errorf("remove backup temp dir %s: %w", dir, err)
	}
	return nil
}
