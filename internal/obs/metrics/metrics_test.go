// SPDX-License-Identifier: Apache-2.0

package metrics

import (
	"bytes"
	"log/slog"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/a-holm/messq/internal/clock"
	dto "github.com/prometheus/client_model/go"
)

// newTestMetrics builds a Metrics over a fake clock with runtime collectors off, so
// gathers hold exactly what this package registered.
func newTestMetrics(t *testing.T, mutate func(*Options)) *Metrics {
	t.Helper()
	o := Options{
		Version:    "test-version",
		Commit:     "deadbeef",
		Durability: "full",
		Runtime:    false,
		Clock:      clock.NewFake(time.Unix(1000, 0)),
		Log:        slog.New(slog.NewTextHandler(&bytes.Buffer{}, nil)),
	}
	if mutate != nil {
		mutate(&o)
	}
	m, err := New(o)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return m
}

// gather collects m's registry into a name→family map.
func gather(t *testing.T, m *Metrics) map[string]*dto.MetricFamily {
	t.Helper()
	fams, err := m.reg.Gather()
	if err != nil {
		t.Fatalf("gather: %v", err)
	}
	out := make(map[string]*dto.MetricFamily, len(fams))
	for _, f := range fams {
		out[f.GetName()] = f
	}
	return out
}

// TestNewInstancesDoNotCollide proves New registers on its OWN registry: two live
// instances export identical metric names simultaneously, which a shared (default)
// registry makes impossible — the second construction would panic with
// duplicate-registration, and forbidigo bans the default registerer outright.
func TestNewInstancesDoNotCollide(t *testing.T) {
	a := newTestMetrics(t, nil)
	b := newTestMetrics(t, nil)

	for name, m := range map[string]*Metrics{"a": a, "b": b} {
		fams := gather(t, m)
		if fams[nameBuildInfo] == nil {
			t.Fatalf("instance %s: messq_build_info missing from its own registry", name)
		}
	}
}

// TestNewFillsOptionDefaults pins the §10 flag defaults at the seam the CLI wiring
// will consume: an operator who passes nothing gets the PLAN §9.4 scrape behaviour.
func TestNewFillsOptionDefaults(t *testing.T) {
	m := newTestMetrics(t, func(o *Options) { o.Clock = nil }) // zero-value fields apart from what the helper forces
	if m.o.Cache != 5*time.Second {
		t.Errorf("Cache = %v, want the 5s cheap-tier TTL", m.o.Cache)
	}
	if m.o.CacheHeavy != time.Minute {
		t.Errorf("CacheHeavy = %v, want 60s", m.o.CacheHeavy)
	}
	if m.o.QueryTimeout != 2*time.Second {
		t.Errorf("QueryTimeout = %v, want 2s", m.o.QueryTimeout)
	}
	if m.o.MaxInFlight != 4 {
		t.Errorf("MaxInFlight = %d, want 4", m.o.MaxInFlight)
	}
	if m.o.MaxSeries != 10000 {
		t.Errorf("MaxSeries = %d, want 10000", m.o.MaxSeries)
	}
	if m.o.CountLimit != 5_000_000 {
		t.Errorf("CountLimit = %d, want 5000000", m.o.CountLimit)
	}
	if m.o.FilterScan != 1000 {
		t.Errorf("FilterScan = %d, want 1000", m.o.FilterScan)
	}
}

// TestBuildInfoExported checks the startup identity row: value 1 under the three
// fixed startup labels, in the catalogue's label order.
func TestBuildInfoExported(t *testing.T) {
	m := newTestMetrics(t, nil)
	fams := gather(t, m)
	fam := fams[nameBuildInfo]
	if fam == nil {
		t.Fatal("messq_build_info missing")
	}
	if got := fam.GetType().String(); got != "GAUGE" {
		t.Errorf("build_info type = %s, want GAUGE", got)
	}
	ms := fam.GetMetric()
	if len(ms) != 1 {
		t.Fatalf("build_info has %d series, want 1", len(ms))
	}
	// Gathered DTOs sort label pairs by name; the catalogue's order governs the
	// WithLabelValues call, not the exposition.
	want := map[string]string{"version": "test-version", "commit": "deadbeef", "durability": "full"}
	got := map[string]string{}
	for _, l := range ms[0].GetLabel() {
		got[l.GetName()] = l.GetValue()
	}
	if len(got) != len(want) {
		t.Fatalf("build_info labels = %v, want %v", got, want)
	}
	for k, v := range want {
		if got[k] != v {
			t.Errorf("build_info label %s = %q, want %q", k, got[k], v)
		}
	}
	if ms[0].GetGauge().GetValue() != 1 {
		t.Errorf("build_info value = %v, want 1", ms[0].GetGauge().GetValue())
	}
}

// TestHandlerServesTextExposition drives the promhttp handler end to end: a plain
// text scrape answers 200 and carries the exposition header lines, so a scraper
// without OpenMetrics negotiation still sees every family.
func TestHandlerServesTextExposition(t *testing.T) {
	m := newTestMetrics(t, nil)
	srv := httptest.NewServer(m.Handler())
	t.Cleanup(srv.Close)

	resp, err := srv.Client().Get(srv.URL) //nolint:noctx // local httptest server in a test
	if err != nil {
		t.Fatalf("scrape: %v", err)
	}
	defer resp.Body.Close() //nolint:errcheck // test cleanup
	if resp.StatusCode != 200 {
		t.Fatalf("scrape status = %d, want 200", resp.StatusCode)
	}
	body := new(bytes.Buffer)
	if _, err := body.ReadFrom(resp.Body); err != nil {
		t.Fatalf("read body: %v", err)
	}
	for _, want := range []string{"# TYPE messq_build_info gauge", "messq_build_info{"} {
		if !bytes.Contains(body.Bytes(), []byte(want)) {
			t.Errorf("exposition missing %q:\n%s", want, body.String())
		}
	}
}

// TestAdoptedCommitInstrumentsWired proves #6's five instruments are wired to the
// registry #21 owns (adopted, not reimplemented — G10): the always-live families are
// present straight after New, observations move them through the exposed
// obs.CommitObserver, and a second New cannot collide with the first.
func TestAdoptedCommitInstrumentsWired(t *testing.T) {
	m := newTestMetrics(t, nil)

	fams := gather(t, m)
	for _, name := range []string{
		"messq_readonly", "messq_writer_queue_depth",
		"messq_commit_duration_seconds", "messq_commit_batch_size",
	} {
		if fams[name] == nil {
			t.Errorf("adopted family %s missing right after New", name)
		}
	}

	obs := m.CommitObserver()
	if obs == nil {
		t.Fatal("CommitObserver() returned nil")
	}
	obs.ObserveCommit(2, time.Millisecond, nil)
	obs.SetReadOnly(true)

	fams = gather(t, m)
	bs := fams["messq_commit_batch_size"]
	if bs == nil || bs.GetMetric()[0].GetHistogram().GetSampleCount() != 1 ||
		bs.GetMetric()[0].GetHistogram().GetSampleSum() != 2 {
		t.Errorf("batch_size did not observe the committed batch: %v", bs)
	}
	ro := fams["messq_readonly"]
	if ro == nil || ro.GetMetric()[0].GetGauge().GetValue() != 1 {
		t.Errorf("readonly latch did not move: %v", ro)
	}
}

// TestSlogAdapterImplementsPromLogger pins the adapter shape client_golang's
// HandlerOpts.ErrorLog actually wants (a Println interface, NOT *log.Logger).
func TestSlogAdapterImplementsPromLogger(t *testing.T) {
	buf := &bytes.Buffer{}
	m := newTestMetrics(t, func(o *Options) { o.Log = slog.New(slog.NewTextHandler(buf, nil)) })

	var pl promLogger = slogAdapter{m.o.Log}
	pl.Println("boom", 7)

	if !bytes.Contains(buf.Bytes(), []byte("promhttp: boom 7")) {
		t.Errorf("adapter lost the message: %q", buf.String())
	}
}
