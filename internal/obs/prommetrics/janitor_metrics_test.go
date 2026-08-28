// SPDX-License-Identifier: Apache-2.0

package prommetrics

import (
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
)

func TestJanitorMetricsObservesPerJobSeries(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := NewJanitorMetrics(reg)

	m.ObserveJob("retention", 1)
	m.ObserveJob("retention", 2)
	m.ObserveJob("checkpoint", 3)

	families, err := reg.Gather()
	if err != nil {
		t.Fatalf("gather: %v", err)
	}
	var counts map[string]uint64
	for _, mf := range families {
		if mf.GetName() != "messq_janitor_duration_seconds" {
			continue
		}
		counts = make(map[string]uint64)
		for _, metric := range mf.Metric {
			job := ""
			for _, lbl := range metric.GetLabel() {
				if lbl.GetName() == "job" {
					job = lbl.GetValue()
				}
			}
			counts[job] = metric.GetHistogram().GetSampleCount()
		}
	}
	if counts == nil {
		t.Fatal("messq_janitor_duration_seconds missing from the registry")
	}
	if counts["retention"] != 2 || counts["checkpoint"] != 1 {
		t.Fatalf("sample counts = %v, want retention=2 checkpoint=1", counts)
	}
}

func TestJanitorMetricsDuplicateRegistrationPanics(t *testing.T) {
	reg := prometheus.NewRegistry()
	_ = NewJanitorMetrics(reg)
	defer func() {
		if recover() == nil {
			t.Fatal("double registration must fail loud (wiring bug, not runtime state)")
		}
	}()
	_ = NewJanitorMetrics(reg)
}

var _ = strings.Contains // keeps strings available if the assertion text evolves
