package prommetrics

import (
	"errors"
	"syscall"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
)

// TestCommitMetricsShapeAndValues drives the observer with a scripted history and checks the
// registered instruments: right names, right label dimensions, sane values.
func TestCommitMetricsShapeAndValues(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := NewCommitMetrics(reg)

	// Three healthy single-command commits, then a failed one.
	m.ObserveCommit(1, 2*time.Millisecond, nil)
	m.ObserveCommit(1, 3*time.Millisecond, nil)
	m.ObserveCommit(1, 4*time.Millisecond, nil)
	m.ObserveCommit(2, 9*time.Millisecond, errors.Join(syscall.EIO))
	m.SetReadOnly(true)
	m.ObserveQueueDepth(7)

	fams, err := reg.Gather()
	if err != nil {
		t.Fatalf("gather: %v", err)
	}
	byName := map[string]*dto.MetricFamily{}
	for _, f := range fams {
		byName[f.GetName()] = f
	}

	wantPresent := []string{
		"messq_commit_batch_size",
		"messq_commit_duration_seconds",
		"messq_commit_errors_total",
		"messq_writer_queue_depth",
		"messq_readonly",
	}
	for _, name := range wantPresent {
		if byName[name] == nil {
			t.Errorf("metric family %s missing; have %v", name, familyNames(fams))
		}
	}
	if len(fams) != len(wantPresent) {
		t.Errorf("registry exposes %d families, want exactly %d", len(fams), len(wantPresent))
	}

	// batch_size: three observations (successful batches only), sum 3.
	if bs := byName["messq_commit_batch_size"]; bs != nil {
		h := bs.GetMetric()[0].GetHistogram()
		if h.GetSampleCount() != 3 || h.GetSampleSum() != 3 {
			t.Errorf("batch_size count=%d sum=%v, want count=3 sum=3 (failures excluded)",
				h.GetSampleCount(), h.GetSampleSum())
		}
	}
	// duration: every commit observed, success or failure.
	if dur := byName["messq_commit_duration_seconds"]; dur != nil {
		if h := dur.GetMetric()[0].GetHistogram(); h.GetSampleCount() != 4 {
			t.Errorf("duration count=%d, want 4 (failed commits are timed too)", h.GetSampleCount())
		}
	}
	// errors_total{class="eio"} = 1.
	eio := -1.0
	if errTotal := byName["messq_commit_errors_total"]; errTotal != nil {
		for _, metric := range errTotal.GetMetric() {
			for _, lp := range metric.GetLabel() {
				if lp.GetName() == "class" && lp.GetValue() == "eio" {
					eio = metric.GetCounter().GetValue()
				}
			}
		}
	}
	if eio != 1 {
		t.Errorf("errors_total{class=\"eio\"} = %v, want 1", eio)
	}
	// readonly flipped to 1.
	if ro := byName["messq_readonly"]; ro != nil {
		if v := ro.GetMetric()[0].GetGauge().GetValue(); v != 1 {
			t.Errorf("readonly = %v, want 1", v)
		}
	}
	// queue depth carried the last sample.
	if qd := byName["messq_writer_queue_depth"]; qd != nil {
		if v := qd.GetMetric()[0].GetGauge().GetValue(); v != 7 {
			t.Errorf("queue_depth = %v, want 7", v)
		}
	}
}

// TestCommitMetricsHaveNoHighCardinalityLabels pins D11's absolute rule at this layer: no
// instrument may carry stream, consumer, subject or identifier labels. The only permitted
// label in the whole set is class on the error counter.
func TestCommitMetricsHaveNoHighCardinalityLabels(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := NewCommitMetrics(reg)
	m.ObserveCommit(1, time.Millisecond, errors.Join(syscall.EIO))
	m.SetReadOnly(true)
	m.ObserveQueueDepth(0)

	fams, err := reg.Gather()
	if err != nil {
		t.Fatalf("gather: %v", err)
	}
	forbidden := map[string]bool{
		"stream": true, "consumer": true, "subject": true,
		"msg_id": true, "msgid": true, "trace_id": true, "id": true,
	}
	for _, fam := range fams {
		for _, metric := range fam.GetMetric() {
			for _, lp := range metric.GetLabel() {
				if forbidden[lp.GetName()] {
					t.Errorf("%s carries forbidden label %q", fam.GetName(), lp.GetName())
				}
				if lp.GetName() != "class" {
					t.Errorf("%s carries label %q: only class is permitted", fam.GetName(), lp.GetName())
				}
			}
		}
	}
}

// familyNames lists gathered family names for failure messages.
func familyNames(fams []*dto.MetricFamily) []string {
	out := make([]string, 0, len(fams))
	for _, f := range fams {
		out = append(out, f.GetName())
	}
	return out
}
