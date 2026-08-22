// SPDX-License-Identifier: Apache-2.0

package obs

import (
	"errors"
	"syscall"
	"testing"
)

// TestClassifyStorageError pins the shared fault-class vocabulary used by the storage.fatal
// log line (via the writer), the Prometheus adapter's messq_commit_errors_total{class} label
// (via internal/obs/prommetrics), and nothing else.
func TestClassifyStorageError(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name string
		err  error
		want string
	}{
		{"nil", nil, "unknown"},
		{"eio direct", syscall.EIO, "eio"},
		{"eio wrapped", errors.Join(syscall.EIO), "eio"},
		{"enospc wrapped", errors.Join(syscall.ENOSPC), "enospc"},
		{"corrupt text", errors.New("database disk image is malformed"), "corrupt"},
		{"plain", errors.New("mystery"), "unknown"},
	}
	for _, tc := range cases {
		if got := ClassifyStorageError(tc.err); got != tc.want {
			t.Errorf("%s: ClassifyStorageError(%v) = %q, want %q", tc.name, tc.err, got, tc.want)
		}
	}
}
