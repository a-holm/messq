// SPDX-License-Identifier: Apache-2.0

// Package backup implements the messq backup pipeline (issue #30, PLAN §4.5):
// a consistent online snapshot of one data directory via SQLite's VACUUM INTO,
// taken on a dedicated single connection, self-verified, stamped with
// provenance, and atomically renamed into place. Restore is a procedure —
// stop + copy + start — never a verb.
package backup

import (
	"fmt"

	"golang.org/x/sys/unix"
)

// EstimateBytes returns the expected on-disk size of a VACUUM INTO snapshot:
// (page_count − freelist_count) × page_size. The freelist pages are not copied
// by VACUUM INTO (the output is compacted), so charging for them would make
// every free-space precheck refuse honest backups of fragmented databases.
// A negative page count or a freelist larger than the page count clamps to 0 —
// the estimate must never go backwards.
func EstimateBytes(pageCount, freelistCount, pageSize int64) int64 {
	if pageCount <= 0 || pageSize <= 0 {
		return 0
	}
	live := pageCount - freelistCount
	if live < 0 {
		live = 0
	}
	return live * pageSize
}

// RequiredBytes is the estimate carrying the 10 % index-rebuild headroom that
// issue #30 §2 step 2 demands of the free-space precheck: VACUUM INTO rebuilds
// every index and its sort scratch shares the destination filesystem's budget
// (--temp-dir defaults to the destination dir). Rounded up so a fractional
// requirement never slips under a disk sitting exactly at the boundary.
func RequiredBytes(estimate int64) int64 {
	if estimate <= 0 {
		return 0
	}
	return (estimate*11 + 9) / 10
}

// InsufficientSpaceError is the free-space precheck refusal: the destination
// filesystem reports fewer free bytes than the snapshot needs. The CLI renders
// both numbers; the operator can act on either side of the inequality.
type InsufficientSpaceError struct {
	Free     int64 // bytes free on the destination filesystem at precheck time
	Required int64 // bytes the snapshot needs including headroom
}

func (e *InsufficientSpaceError) Error() string {
	return fmt.Sprintf("destination filesystem has %d free bytes, the backup needs %d",
		e.Free, e.Required)
}

// CheckSpace refuses when free bytes are below the required budget. Exactly the
// required budget passes: the precheck is a lower bound, not a margin on top
// of the margin.
func CheckSpace(free, required int64) error {
	if free >= required {
		return nil
	}
	return &InsufficientSpaceError{Free: free, Required: required}
}

// FreeBytes reports the bytes available to an unprivileged process on the
// filesystem holding dir (statfs f_bavail × f_bsize — f_bfree would count the
// root reserve a backup can never use).
func FreeBytes(dir string) (int64, error) {
	var st unix.Statfs_t
	if err := unix.Statfs(dir, &st); err != nil {
		return 0, fmt.Errorf("statfs %s: %w", dir, err)
	}
	return int64(st.Bavail) * int64(st.Bsize), nil //nolint:gosec // G115: kernel counts fit in int64 by construction
}
