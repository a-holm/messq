// SPDX-License-Identifier: Apache-2.0

package store

import "testing"

// TestSettleMetricNameConstants pins the §9.4 metric-name table this issue hands to
// #21. A rename here is the cross-repository breakage #21's cardinality golden depends
// on (PLAN §9.4 annotates messq_stale_acks_total as alert-on-any-nonzero-rate).
func TestSettleMetricNameConstants(t *testing.T) {
	names := []struct {
		constant, name string
	}{
		{metricAckedTotal, "messq_acked_total"},
		{metricNakedTotal, "messq_naked_total"},
		{metricTermedTotal, "messq_termed_total"},
		{metricExtendsTotal, "messq_extends_total"},
		{metricStaleAcksTotal, "messq_stale_acks_total"},
		{metricLateAcksTotal, "messq_late_acks_total"},
		{metricAckLatencySeconds, "messq_ack_latency_seconds"},
	}
	if len(names) != 7 {
		t.Fatalf("metric-name table has %d entries, want 7", len(names))
	}
	for _, e := range names {
		if e.constant != e.name {
			t.Fatalf("metric constant %q != wire name %q", e.constant, e.name)
		}
	}
}
