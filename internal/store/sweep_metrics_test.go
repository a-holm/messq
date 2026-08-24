// SPDX-License-Identifier: Apache-2.0

package store

import "testing"

// TestSweepMetricNameConstants pins the §9.4 metric-name table this issue hands to #21.
// A rename here is the cross-repository breakage #21's cardinality golden depends on.
func TestSweepMetricNameConstants(t *testing.T) {
	names := []struct {
		constant, name string
	}{
		{metricTimeoutsTotal, "messq_timeouts_total"},
		{metricRedeliveredTotal, "messq_redelivered_total"},
		{metricDeadTotal, "messq_dead_total"},
		{metricSweepLatenessSecs, "messq_sweep_lateness_seconds"},
		{metricSweepDurationSecs, "messq_sweep_duration_seconds"},
		{metricSweepRows, "messq_sweep_rows"},
		{metricSweepBacklog, "messq_sweep_backlog"},
		{metricSweepSkippedTotal, "messq_sweep_skipped_total"},
	}
	if len(names) != 8 {
		t.Fatalf("metric-name table has %d entries, want 8", len(names))
	}
	for _, e := range names {
		if e.constant != e.name {
			t.Fatalf("metric constant %q != wire name %q", e.constant, e.name)
		}
	}
}
