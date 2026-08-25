// SPDX-License-Identifier: Apache-2.0

package metrics

import (
	"slices"
	"testing"

	"github.com/a-holm/messq/internal/wirecode"
)

// TestCatalogueTableAComplete pins PLAN §9.4 Table A — the 1.0 compatibility contract.
// Operators build alerts on these names; a missing row, a type change or a label
// reorder here is a breaking change and must fail before it ships.
func TestCatalogueTableAComplete(t *testing.T) {
	want := []struct {
		name   string
		kind   metricKind
		labels []string
	}{
		{"messq_published_total", kindCounter, []string{"stream"}},
		{"messq_duplicates_total", kindCounter, []string{"stream"}},
		{"messq_delivered_total", kindCounter, []string{"stream", "consumer"}},
		{"messq_redelivered_total", kindCounter, []string{"stream", "consumer", "cause"}},
		{"messq_acked_total", kindCounter, []string{"stream", "consumer"}},
		{"messq_naked_total", kindCounter, []string{"stream", "consumer"}},
		{"messq_termed_total", kindCounter, []string{"stream", "consumer"}},
		{"messq_timeouts_total", kindCounter, []string{"stream", "consumer"}},
		{"messq_dead_total", kindCounter, []string{"stream", "consumer"}},
		{"messq_stale_acks_total", kindCounter, []string{"stream", "consumer"}},
		{"messq_pending", kindGauge, []string{"stream", "consumer"}},
		{"messq_inflight", kindGauge, []string{"stream", "consumer"}},
		{"messq_backlog", kindGauge, []string{"stream", "consumer"}},
		{"messq_oldest_pending_age_seconds", kindGauge, []string{"stream", "consumer"}},
		{"messq_dlq_depth", kindGauge, []string{"stream"}},
		{"messq_ack_latency_seconds", kindHistogram, []string{"stream", "consumer"}},
		{"messq_commit_duration_seconds", kindHistogram, nil},
		{"messq_commit_batch_size", kindHistogram, nil},
		{"messq_db_bytes", kindGauge, nil},
		{"messq_wal_bytes", kindGauge, nil},
		{"messq_events_rows", kindGauge, nil},
		{"messq_disk_free_bytes", kindGauge, nil},
		{"messq_build_info", kindGauge, []string{"version", "commit", "durability"}},
	}
	for _, w := range want {
		spec, ok := Spec(w.name)
		if !ok {
			t.Errorf("Table A metric %q missing from the catalogue", w.name)
			continue
		}
		if spec.Kind != w.kind {
			t.Errorf("%s: kind = %s, want %s", w.name, spec.Kind, w.kind)
		}
		if !slices.Equal(spec.Labels, w.labels) {
			t.Errorf("%s: labels = %v, want %v (order is the label-value order every caller uses)", w.name, spec.Labels, w.labels)
		}
		if spec.Help == "" {
			t.Errorf("%s: empty help text (promlint rejects it)", w.name)
		}
	}
	if t.Failed() {
		t.Fatal("Table A diverged from PLAN §9.4")
	}
}

// TestCatalogueHandoverNamesHonoured pins the wire names the merged sibling issues
// handed over for #21 to register. Each entry cites its source issue's own pinning
// test; a rename on either side is the cross-repository breakage the cardinality
// golden depends on. Names the #21 tables explicitly REJECTED are asserted absent so
// they cannot sneak back in as zombie series.
func TestCatalogueHandoverNamesHonoured(t *testing.T) {
	handover := map[string]string{
		// #10 settle.go (TestSettleMetricNameConstants).
		metricAckedTotal:        "messq_acked_total",
		metricNakedTotal:        "messq_naked_total",
		metricTermedTotal:       "messq_termed_total",
		metricExtendsTotal:      "messq_extends_total",
		metricStaleAcksTotal:    "messq_stale_acks_total",
		metricLateAcksTotal:     "messq_late_acks_total",
		metricAckLatencySeconds: "messq_ack_latency_seconds",
		// #11 sweep.go (TestSweepMetricNameConstants) minus Table C rejections
		// (sweep_rows, sweep_duration_seconds are subsumed; not registered).
		metricTimeoutsTotal:     "messq_timeouts_total",
		metricRedeliveredTotal:  "messq_redelivered_total",
		metricDeadTotal:         "messq_dead_total",
		metricSweepLatenessSecs: "messq_sweep_lateness_seconds",
		metricSweepBacklog:      "messq_sweep_backlog",
		metricSweepSkippedTotal: "messq_sweep_skipped_total",
		// #12 dead.go handover constants minus messq_dlq_deferred_total (Table C:
		// kept internal to #12). messq_dlq_bytes_total is in no #21 table either.
		MessqDLQWrittenTotal: "messq_dlq_written_total",
		MessqDeadOrphanTotal: "messq_dead_orphan_total",
		MessqDLQDepth:        "messq_dlq_depth",
	}
	for constant, want := range handover {
		if constant != want {
			t.Errorf("handover constant %q does not equal the pinned wire name %q", constant, want)
			continue
		}
		if _, ok := Spec(want); !ok {
			t.Errorf("pinned metric %q (%s) missing from the catalogue", want, want)
		}
	}

	for _, rejected := range []string{
		"messq_sweep_rows", "messq_sweep_duration_seconds", // #11 proposals, Table C
		"messq_dlq_deferred_total", // #12 proposal, Table C: kept internal to #12
		"messq_dlq_bytes_total",    // #12 handover constant accepted by no #21 table
	} {
		if _, ok := Spec(rejected); ok {
			t.Errorf("%s was rejected by the #21 tables but is catalogued", rejected)
		}
	}
}

// TestCatalogueLabelsCardinalitySafe encodes the reconciled cardinality rule: identity
// labels are stream and consumer and nothing else; every other label must be a closed
// enum with a compile-time value list in this package. The forbidden set is the one
// the allow-list test enforces over reg.Gather().
func TestCatalogueLabelsCardinalitySafe(t *testing.T) {
	forbidden := map[string]bool{
		"subject": true, "msg_id": true, "id": true, "seq": true, "trace_id": true,
		"token": true, "peer": true, "client": true, "path": true, "url": true,
		"error": true, "user": true, "instance": true,
	}
	enumLabels := map[string]bool{
		"cause": true, "class": true, "reason": true, "tier": true,
		"route": true, "method": true, "code": true,
	}
	identity := map[string]bool{
		"stream": true, "consumer": true,
		"version": true, "commit": true, "durability": true, // build_info: three fixed values set at startup
	}

	for _, name := range Names() {
		spec, ok := Spec(name)
		if !ok {
			t.Fatalf("Names()/Spec() disagree about %q", name)
		}
		for _, l := range spec.Labels {
			switch {
			case identity[l]:
			case enumLabels[l]:
				vals, ok := EnumValues(l)
				if !ok || len(vals) == 0 {
					t.Errorf("%s label %q: enum label without a declared constant list", name, l)
				}
			case forbidden[l]:
				t.Errorf("%s: forbidden identity label %q", name, l)
			default:
				t.Errorf("%s: label %q is neither identity, nor a declared enum — add it to the catalogue's enum table or remove it", name, l)
			}
		}
	}
}

// TestCatalogueCodeEnumMatchesWirecode keeps the code label's constant list equal to
// the closed §7 enum's HTTP-capable members: internal/wirecode is the single source,
// so the metrics layer derives instead of copying.
func TestCatalogueCodeEnumMatchesWirecode(t *testing.T) {
	want := make([]string, 0)
	never := map[string]bool{}
	for _, c := range wirecode.NeverOverHTTPSet() {
		never[string(c)] = true
	}
	for _, c := range wirecode.All() {
		if !never[string(c)] {
			want = append(want, string(c))
		}
	}
	got, ok := EnumValues("code")
	if !ok {
		t.Fatal("no code enum declared")
	}
	if !slices.Equal(got, want) {
		t.Errorf("code enum = %v, want the wirecode HTTP-capable set %v", got, want)
	}
}

// TestCatalogueRowsWellFormed checks the structural invariants docs generation and the
// registry builder rely on: unique names, counters carry _total, histograms declare
// buckets unless their buckets are owned by the adopted #6 instruments.
func TestCatalogueRowsWellFormed(t *testing.T) {
	names := Names()
	seen := make(map[string]bool, len(names))
	for _, name := range names {
		if seen[name] {
			t.Errorf("duplicate catalogue row for %q", name)
		}
		seen[name] = true
		spec, _ := Spec(name)
		switch spec.Kind {
		case kindCounter:
			if len(name) < len("_total") || name[len(name)-len("_total"):] != "_total" {
				t.Errorf("%s: counter without _total suffix (promlint)", name)
			}
		case kindHistogram:
			if len(spec.Buckets) == 0 && spec.Source != srcAdopted {
				t.Errorf("%s: histogram without buckets", name)
			}
		case kindGauge:
			// gauges carry no structural requirements beyond the cardinality test
		}
		if spec.Source == srcAdopted && (name == "messq_commit_batch_size") {
			continue // buckets owned by prommetrics, verified by its own tests
		}
	}
}
