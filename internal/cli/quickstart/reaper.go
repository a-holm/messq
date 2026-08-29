// SPDX-License-Identifier: Apache-2.0

package quickstart

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/a-holm/messq/internal/clock"
)

// The stale-directory reaper's four guards (issue §8): a crashed tour leaves
// messq-quickstart-* dirs behind, and the NEXT run reaps only directories that
// carry ALL of a name prefix, the tour marker file, current-uid ownership, and
// an age past the reap horizon. Three guards on a delete loop; the fourth keeps
// a slow disk's live tour safe.
const (
	dirPrefix    = "messq-quickstart-"
	markerName   = ".messq-quickstart"
	reapHorizon  = time.Hour
	reapPageSize = 64
)

// reapStale removes leftover tour directories that carry every guard, and
// returns how many it removed. It never follows symlinks (RemoveAll on a
// symlink removes the link, not the target) and never touches a directory that
// fails any guard.
func reapStale(tmpDir string, now clock.Clock) int {
	entries, err := os.ReadDir(tmpDir)
	if err != nil {
		return 0
	}
	uid := os.Getuid()
	removed := 0
	for i, e := range entries {
		if i >= reapPageSize || !e.IsDir() || !strings.HasPrefix(e.Name(), dirPrefix) {
			continue
		}
		dir := filepath.Join(tmpDir, e.Name())
		if !ownedByUs(dir, uid) {
			continue
		}
		if !markerPresent(dir) {
			continue
		}
		if !olderThan(dir, now, reapHorizon) {
			continue
		}
		if rmErr := os.RemoveAll(dir); rmErr == nil {
			removed++
		}
	}
	return removed
}

// ownedByUs checks the directory's owner is the current uid — a directory
// another user happened to leave with the same prefix is theirs, not ours.
// Linux-only by §1.2: the syscall.Stat_t shape is the platform contract.
func ownedByUs(dir string, uid int) bool {
	info, err := os.Stat(dir)
	if err != nil {
		return false
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		// Non-Unix build: ownership is unprovable, so the guard fails closed.
		return false
	}
	return int(stat.Uid) == uid
}

// markerPresent requires the tour marker file: a prefix collision with some
// other tool's temp dir is not a tour leftover.
func markerPresent(dir string) bool {
	info, err := os.Stat(filepath.Join(dir, markerName))
	return err == nil && info.Mode().IsRegular()
}

// olderThan reads the marker's mtime: the tour writes it at start, so the age
// of the marker IS the age of the abandoned tour.
func olderThan(dir string, now clock.Clock, horizon time.Duration) bool {
	info, err := os.Stat(filepath.Join(dir, markerName))
	if err != nil {
		return false
	}
	return now.Since(info.ModTime()) > horizon
}

// socketDir picks the directory the tour's unix socket lives in, falling back
// down the ladder the issue pins when the path would exceed the kernel's
// sun_path limit (104 bytes on Linux): the long TMPDIR loses to /tmp, which
// loses to a loopback TCP port.
func socketDir(preferred string) (string, string, error) {
	if fits(filepath.Join(preferred, "messq.sock")) {
		return preferred, "", nil
	}
	if fallback, err := os.MkdirTemp("/tmp", dirPrefix); err == nil {
		if fits(filepath.Join(fallback, "messq.sock")) {
			return fallback, "", nil
		}
		sinkErr(os.Remove(fallback))
	}
	// Last rung: a TCP port is the honest fallback, and the banner says so.
	return "", "tcp://127.0.0.1:0", nil
}

// sunPathLimit is the unix socket path limit on Linux; keep one byte of headroom.
const sunPathLimit = 104

func fits(path string) bool {
	return len(path) < sunPathLimit
}

var (
	_ = fmt.Sprintf
	_ = strconv.Itoa
)

// sinkErr consumes a cleanup error whose only handler could be a log line.
func sinkErr(err error) { _ = err }
