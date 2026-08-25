// SPDX-License-Identifier: Apache-2.0

// Package metrics is #21's Prometheus wiring (PLAN §9.4): one custom registry, one
// catalogue (catalogue.go), the event→counter projection (project.go) and the
// scrape-time collector (collector.go). Every instrument in the binary is registered
// here and nowhere else — the forbidigo rules ban every default-registerer form
// outside this subtree, and New never touches prometheus's globals.
//
// Counters move only in response to committed events: the fan-out delivers obs.Event
// values after their batch committed, and Publish projects them (Decision 1). Gauges
// are computed from the store at scrape time and never mirrored (Decision 2), and a
// scrape never fails because a query did (Decision 3).
package metrics

import (
	"fmt"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/a-holm/messq/internal/clock"
	"github.com/a-holm/messq/internal/obs"
	"github.com/a-holm/messq/internal/obs/prommetrics"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// Options configures [New]. Zero-value duration/int fields take their §10 flag
// defaults; the CLI wiring maps the --metrics-* flags onto them one-to-one.
type Options struct {
	Version, Commit, Durability string

	Cache        time.Duration // --metrics-cache         (default 5s)
	CacheHeavy   time.Duration // --metrics-cache-heavy   (default 60s)
	QueryTimeout time.Duration // --metrics-query-timeout (default 2s)
	MaxInFlight  int           // --metrics-max-inflight  (default 4)
	MaxSeries    int           // --metrics-max-series    (default 10000)
	CountLimit   int64         // --metrics-count-limit   (default 5_000_000)
	FilterScan   int           // --metrics-filter-scan   (default 1000)

	DataDir string // statfs source of messq_disk_free_bytes (#27 replaces)
	Runtime bool   // --metrics-runtime-collectors (default true)

	Clock   clock.Clock
	Waiters func() int64 // api waiter registry; nil leaves messq_waiters unregistered
	Log     *slog.Logger
}

// withDefaults fills the zero-value fields with the §10 defaults.
func (o Options) withDefaults() Options {
	if o.Cache <= 0 {
		o.Cache = 5 * time.Second
	}
	if o.CacheHeavy <= 0 {
		o.CacheHeavy = time.Minute
	}
	if o.QueryTimeout <= 0 {
		o.QueryTimeout = 2 * time.Second
	}
	if o.MaxInFlight <= 0 {
		o.MaxInFlight = 4
	}
	if o.MaxSeries <= 0 {
		o.MaxSeries = 10000
	}
	if o.CountLimit <= 0 {
		o.CountLimit = 5_000_000
	}
	if o.FilterScan <= 0 {
		o.FilterScan = 1000
	}
	if o.Clock == nil {
		o.Clock = clock.System{}
	}
	if o.Log == nil {
		o.Log = slog.Default()
	}
	return o
}

// Metrics owns the custom registry and every instrument registered on it. It is the
// binary's single registration point: construction goes through [New], event-driven
// counters move through [Metrics.Publish] (it satisfies obs.Sink), gauges are served
// by the scrape-time collector.
type Metrics struct {
	o   Options
	reg *prometheus.Registry

	instrs map[string]prometheus.Collector // catalogue name → vec/histogram/counter instrument

	buildInfo *prometheus.GaugeVec
	dropped   prometheus.Counter // messq_metrics_dropped_series_total
	truncated prometheus.Counter // messq_metrics_truncated_total
	waiters   prometheus.Gauge   // nil unless Options.Waiters is set

	commit *prommetrics.CommitMetrics
}

// New builds the registry, constructs one instrument per catalogue row it owns,
// adopts #6's CommitMetrics onto the same registry and wires the scrape-time
// collector. A duplicate registration anywhere is a wiring bug and panics loudly at
// the point of the mistake — exactly like prommetrics does.
func New(o Options) (*Metrics, error) {
	o = o.withDefaults()
	m := &Metrics{
		o:      o,
		reg:    prometheus.NewRegistry(), // never the default registry (G8)
		instrs: make(map[string]prometheus.Collector, len(catalogue)),
	}

	for _, spec := range catalogue {
		if descFed(spec.Name) || spec.Source == srcAdopted {
			continue // collector-owned const metrics / #6's own constructions
		}
		switch spec.Kind {
		case kindCounter:
			if len(spec.Labels) == 0 { // unlabelled counters register as themselves
				c := prometheus.NewCounter(prometheus.CounterOpts{
					Name: spec.Name, Help: spec.Help,
				})
				m.instrs[spec.Name] = c
				m.reg.MustRegister(c)
				continue
			}
			vec := prometheus.NewCounterVec(prometheus.CounterOpts{
				Name: spec.Name, Help: spec.Help,
			}, spec.Labels)
			m.instrs[spec.Name] = vec
			m.reg.MustRegister(vec)
		case kindHistogram:
			opts := prometheus.HistogramOpts{
				Name: spec.Name, Help: spec.Help, Buckets: spec.Buckets,
			}
			if spec.Native {
				nativeHistogram(&opts)
			}
			hist := prometheus.NewHistogramVec(opts, spec.Labels)
			m.instrs[spec.Name] = hist
			m.reg.MustRegister(hist)
		case kindGauge:
			switch {
			case spec.Name == nameBuildInfo:
				g := prometheus.NewGaugeVec(prometheus.GaugeOpts{
					Name: spec.Name, Help: spec.Help,
				}, spec.Labels)
				m.buildInfo = g
				m.reg.MustRegister(g)
			case spec.Name == metricWaitersGauge && o.Waiters == nil:
				continue // unregistered, not zero: an unwired gauge would read as "no waiters", a lie
			case spec.Name == metricWaitersGauge:
				g := prometheus.NewGauge(prometheus.GaugeOpts{
					Name: spec.Name, Help: spec.Help,
				})
				m.waiters = g
				m.reg.MustRegister(g)
			case len(spec.Labels) > 0:
				// Labelled bookkeeping gauges the collector feeds (the self-metrics).
				g := prometheus.NewGaugeVec(prometheus.GaugeOpts{
					Name: spec.Name, Help: spec.Help,
				}, spec.Labels)
				m.instrs[spec.Name] = g
				m.reg.MustRegister(g)
			default:
				return nil, fmt.Errorf("metrics: catalogue row %s is gauge-shaped but has no builder", spec.Name)
			}
		}
	}

	// The two unlabelled self-counters the projection and collector feed directly
	// (already registered by the catalogue loop above).
	m.dropped = m.counter(nameDroppedSeriesTotal)
	m.truncated = m.counter(nameMetricsTruncatedTotl)

	if m.buildInfo != nil {
		m.buildInfo.WithLabelValues(o.Version, o.Commit, o.Durability).Set(1)
	}

	// Adopt #6's five commit instruments onto OUR registry (G10). Registering them
	// twice panics loudly, which is the correct response to a wiring bug.
	m.commit = prommetrics.NewCommitMetrics(m.reg)

	if o.Runtime {
		m.reg.MustRegister(collectors.NewGoCollector(), collectors.NewProcessCollector(
			collectors.ProcessCollectorOpts{}))
	}

	return m, nil
}

// nativeHistogram turns on native histograms alongside the classic buckets (§7): a
// protobuf scraper gets high-resolution latencies for free, a text scraper sees the
// classic buckets, and the golden test compares only text.
func nativeHistogram(opts *prometheus.HistogramOpts) {
	opts.NativeHistogramBucketFactor = 1.1
	opts.NativeHistogramMaxBucketNumber = 100
	opts.NativeHistogramMinResetDuration = time.Hour
}

// descFed reports catalogue rows whose series are produced by the scrape-time
// collector as const metrics rather than by instruments built here: every value is a
// function of the StatsReader/statfs/waiters snapshot, so an instrument would be a
// mirrored gauge (Decision 2 forbids). build_info is deliberately absent — it is an
// instrument set once at startup. collector.go consumes the same list for its descs.
func descFed(name string) bool {
	switch name {
	case namePending, nameInflight, nameBacklog, nameOldestPendingAge,
		nameConsumerPaused, nameDLQDepth, metricSweepBacklog,
		nameDBBytes, nameWALBytes, nameEventsRows, nameDiskFreeBytes:
		return true
	}
	return false
}

// counter type-asserts an unlabelled counter instrument out of the map.
func (m *Metrics) counter(name string) prometheus.Counter {
	c, ok := m.instrs[name].(prometheus.Counter)
	if !ok {
		panic("metrics: catalogue row " + name + " is not an unlabelled counter")
	}
	return c
}

// Handler returns the /metrics handler (#15 injects it into its Deps.Metrics slot).
// ContinueOnError keeps partial scrapes alive; MaxRequestsInFlight bounds concurrent
// scrapes (503 beyond it); Timeout bounds a hung gather. --metrics=false leaves the
// slot nil upstream so #15's honest 503 not_implemented answers instead.
func (m *Metrics) Handler() http.Handler {
	return promhttp.HandlerFor(m.reg, promhttp.HandlerOpts{
		Registry:            m.reg, // exposes promhttp_metric_handler_errors_total{cause}
		MaxRequestsInFlight: m.o.MaxInFlight,
		Timeout:             m.o.QueryTimeout + time.Second,
		ErrorHandling:       promhttp.ContinueOnError, // a partial scrape beats a blind operator
		ErrorLog:            slogAdapter{m.o.Log},
		EnableOpenMetrics:   true,
	})
}

// Registry exposes the custom registry to tests and to the golden exposition work.
func (m *Metrics) Registry() *prometheus.Registry { return m.reg }

// CommitObserver returns the adopted #6 observer wired to this registry; serve
// wiring hands it to the store writer.
func (m *Metrics) CommitObserver() obs.CommitObserver { return m.commit }

// vec fetches a labelled instrument by catalogue name.
func (m *Metrics) vec(name string) prometheus.Collector {
	c, ok := m.instrs[name]
	if !ok {
		panic("metrics: no instrument registered for " + name)
	}
	return c
}

// promLogger is the interface client_golang's HandlerOpts.ErrorLog wants (Println,
// not *log.Logger).
type promLogger interface{ Println(v ...any) }

// slogAdapter routes promhttp's internal error reports into the process logger at
// WARN, prefixed so an operator can tell scraper problems from broker problems.
type slogAdapter struct{ l *slog.Logger }

// Println implements [promLogger].
func (a slogAdapter) Println(v ...any) {
	a.l.Warn("promhttp: " + strings.TrimSuffix(fmt.Sprintln(v...), "\n"))
}
