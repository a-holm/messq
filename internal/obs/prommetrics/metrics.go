// SPDX-License-Identifier: Apache-2.0

// Package prommetrics is the Prometheus implementation of the [obs.CommitObserver] seam (#6).
//
// It is a subpackage of internal/obs rather than a file in it because Go dependencies are
// package-granular: client_golang reaches net/http, and layers.sh forbids internal/store from
// reaching net/http directly or transitively — while internal/store legitimately depends on
// obs itself for the observer seam, the event vocabulary and ClassifyStorageError. Keeping the
// instruments here lets store import obs without importing the metrics client; nothing below
// the API layer imports prommetrics. Metric registration still lives in exactly one package
// subtree (see the forbidigo rules); #21 owns registry wiring, naming policy sign-off and the
// golden exposition test.
package prommetrics

import (
	"sync"
	"time"

	"github.com/a-holm/messq/internal/obs"
	"github.com/prometheus/client_golang/prometheus"
)

// CommitMetrics implements the writer-facing half of the seam.
var _ obs.CommitObserver = (*CommitMetrics)(nil)

// commitMetricsBuckets are the messq_commit_batch_size buckets: powers of two up to PLAN
// §4.3's default cap of 256 commands per transaction.
var commitMetricsBuckets = []float64{1, 2, 4, 8, 16, 32, 64, 128, 256}

// commitDurationBuckets cover 0.1 ms to 2.5 s on a rough log scale: an fsync-dominated
// commit lands between the third tick (0.5 ms) and a few hundred; anything past 2.5 s is
// the +Inf bucket and means the disk is dying anyway.
var commitDurationBuckets = []float64{
	0.0001, 0.0002, 0.0005, 0.001, 0.002, 0.005,
	0.01, 0.02, 0.05, 0.1, 0.25, 0.5, 1, 2.5,
}

// CommitMetrics is the Prometheus [obs.CommitObserver]. Create it with [NewCommitMetrics] over
// the registry #21 owns; registering it twice panics loudly, which is the correct response
// to a wiring bug.
type CommitMetrics struct {
	batchSize prometheus.Histogram
	duration  prometheus.Histogram
	errTotal  *prometheus.CounterVec
	queue     prometheus.Gauge
	readonly  prometheus.Gauge

	mu sync.Mutex // serialises readonly transitions; the latch is the only writer in practice
}

// NewCommitMetrics registers all five instruments on reg and returns the observer.
func NewCommitMetrics(reg prometheus.Registerer) *CommitMetrics {
	batchSize := prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:    "messq_commit_batch_size",
		Help:    "Commands per committed batch transaction; sum/counts give the mean batching depth.",
		Buckets: commitMetricsBuckets,
	})
	duration := prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:    "messq_commit_duration_seconds",
		Help:    "Wall time to commit one batch transaction; dominated by the WAL fsync under durability=full.",
		Buckets: commitDurationBuckets,
	})
	errorsTotal := prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "messq_commit_errors_total",
		Help: "Batch commits that failed, by storage-fault class.",
	}, []string{"class"})
	queue := prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "messq_writer_queue_depth",
		Help: "Commands waiting in the writer queue, sampled once per batch cycle; sustained growth means saturation.",
	})
	readonly := prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "messq_readonly",
		Help: "1 when the process latched read-only after an unrecoverable storage fault; alert on 1.",
	})

	for _, c := range []prometheus.Collector{batchSize, duration, errorsTotal, queue, readonly} {
		if regErr := reg.Register(c); regErr != nil {
			// A duplicate registration is a wiring bug, not a runtime condition: fail loud
			// at the point of the mistake rather than shipping half a metric set.
			panic("prommetrics: NewCommitMetrics registration failed: " + regErr.Error())
		}
	}
	return &CommitMetrics{
		batchSize: batchSize,
		duration:  duration,
		errTotal:  errorsTotal,
		queue:     queue,
		readonly:  readonly,
	}
}

// ObserveCommit implements [obs.CommitObserver].
func (m *CommitMetrics) ObserveCommit(batch int, d time.Duration, err error) {
	m.duration.Observe(d.Seconds())
	if err != nil {
		m.errTotal.WithLabelValues(obs.ClassifyStorageError(err)).Inc()
		return
	}
	m.batchSize.Observe(float64(batch))
}

// ObserveQueueDepth implements [obs.CommitObserver].
func (m *CommitMetrics) ObserveQueueDepth(n int) {
	m.queue.Set(float64(n))
}

// SetReadOnly implements [obs.CommitObserver].
func (m *CommitMetrics) SetReadOnly(ro bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	v := 0.0
	if ro {
		v = 1
	}
	m.readonly.Set(v)
}
