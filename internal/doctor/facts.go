// SPDX-License-Identifier: Apache-2.0

package doctor

import (
	"fmt"
	"time"
)

// skippedCheck is the uniform "could not judge" verdict every data-gap takes:
// same layout, greppable id, and a reason that names WHAT was missing.
func skippedCheck(id, reason string) Finding {
	return Finding{
		ID:       id,
		Severity: SevSkipped,
		Title:    reason,
		NoFix:    "this is informational; rerun with more sources collected",
		Docs:     docsAnchor(id),
	}
}

// humanBytes is doctor's local byte formatter; internal/cli/render.Bytes has
// the CLI-tuned twin for table faces.
func humanBytes(n int64) string {
	units := []struct {
		name string
		size int64
	}{
		{"TiB", 1 << 40}, {"GiB", 1 << 30}, {"MiB", 1 << 20}, {"KiB", 1 << 10},
	}
	for _, u := range units {
		if n >= u.size {
			return fmt.Sprintf("%.1f %s", float64(n)/float64(u.size), u.name)
		}
	}
	return fmt.Sprintf("%d B", n)
}

var _ = time.Second

// Knobs returns the daemon runtime knobs doctor judges; zero means the source
// did not publish them and consuming checks answer skip.
func (s *Snapshot) Knobs() ListenerConfigFacts { return s.ServerKnobs }

// DataDir returns the data directory path as known to storage facts, falling
// back to "" so findings never invent a path they never saw.
func (s *Snapshot) DataDir() string {
	if s.Storage != nil {
		return s.Storage.DataDir
	}
	return ""
}
