// SPDX-License-Identifier: Apache-2.0

package obs

import (
	"errors"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

// CommitObserver is the metrics seam of the group-commit engine (#6). The writer calls it;
// an implementation turns the calls into instruments. The Prometheus adapter lives here next
// to the interface because metric registration is this package's job alone (see the forbidigo
// rules); #21 owns registry wiring, naming policy sign-off and the golden exposition test.
//
// Implementations must be safe for concurrent use and must never block: every method runs on
// or adjacent to the writer goroutine's commit path.
//
// Cardinality rule (PLAN §D11): none of these observations may introduce stream, consumer,
// subject or identifier labels. The only label in the set is class on commit errors.
type CommitObserver interface {
	// ObserveCommit records one finished transaction: batch is the number of commands in
	// it, d its wall duration (dominated by fsync under durability=full), err the commit
	// outcome. On success the batch size feeds messq_commit_batch_size and d feeds
	// messq_commit_duration_seconds; on failure only d is observed (no commands were
	// applied, so a batch-size observation would poison sum/count) and the error class
	// increments messq_commit_errors_total{class}.
	ObserveCommit(batch int, d time.Duration, err error)

	// ObserveQueueDepth samples len(command channel) once per batch cycle: the saturation
	// signal behind messq_writer_queue_depth.
	ObserveQueueDepth(n int)

	// SetReadOnly flips messq_readonly between 0 and 1. Exactly one true transition ever
	// happens per writer: the fsyncgate latch.
	SetReadOnly(ro bool)
}

// NopCommitObserver discards everything. It is the default when no observer is injected.
type NopCommitObserver struct{}

// ObserveCommit does nothing.
func (NopCommitObserver) ObserveCommit(int, time.Duration, error) {}

// ObserveQueueDepth does nothing.
func (NopCommitObserver) ObserveQueueDepth(int) {}

// SetReadOnly does nothing.
func (NopCommitObserver) SetReadOnly(bool) {}

// ClassifyStorageError names the storage-fault family of err for storage.fatal logs and the
// messq_commit_errors_total{class} label. Errno-bearing errors are matched through errors.Is
// so arbitrary wrapping does not hide them; SQLite's textual signatures cover the errno-less
// spellings modernc emits; anything else is unknown. The class is a label and a log field,
// never a control-flow decision.
func ClassifyStorageError(err error) string {
	if err == nil {
		return "unknown"
	}
	if errors.Is(err, syscall.EIO) {
		return "eio"
	}
	if errors.Is(err, syscall.ENOSPC) {
		return "enospc"
	}
	msg := strings.ToLower(err.Error())
	switch {
	case strings.Contains(msg, "i/o error"), strings.Contains(msg, "input/output error"):
		return "eio"
	case strings.Contains(msg, "no space left"), strings.Contains(msg, "disk is full"):
		return "enospc"
	case strings.Contains(msg, "corrupt"), strings.Contains(msg, "malformed"),
		strings.Contains(msg, "not a database"), strings.Contains(msg, "encrypted"):
		return "corrupt"
	}
	return "unknown"
}

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

// CommitMetrics is the Prometheus [CommitObserver]. Create it with [NewCommitMetrics] over
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
			panic("obs: NewCommitMetrics registration failed: " + regErr.Error())
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

// ObserveCommit implements [CommitObserver].
func (m *CommitMetrics) ObserveCommit(batch int, d time.Duration, err error) {
	m.duration.Observe(d.Seconds())
	if err != nil {
		m.errTotal.WithLabelValues(ClassifyStorageError(err)).Inc()
		return
	}
	m.batchSize.Observe(float64(batch))
}

// ObserveQueueDepth implements [CommitObserver].
func (m *CommitMetrics) ObserveQueueDepth(n int) {
	m.queue.Set(float64(n))
}

// SetReadOnly implements [CommitObserver].
func (m *CommitMetrics) SetReadOnly(ro bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	v := 0.0
	if ro {
		v = 1
	}
	m.readonly.Set(v)
}
