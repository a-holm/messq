// SPDX-License-Identifier: Apache-2.0

package prommetrics

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

// JanitorMetrics is the Prometheus implementation of janitor.Metrics (#27): one
// duration observation per completed job Run, labelled by the job's closed-set name.
// The label cardinality is therefore bounded by the number of jobs the serve layer
// wires — reaper, retention, dedup, events, checkpoint, vacuum, stats — and can never
// leak a stream or subject value (PLAN §9.4). Create it with [NewJanitorMetrics] over
// #21's injected registry; double registration panics loudly like [NewCommitMetrics].
type JanitorMetrics struct {
	vec *prometheus.HistogramVec
}

// NewJanitorMetrics registers the family on reg and returns the seam.
func NewJanitorMetrics(reg prometheus.Registerer) *JanitorMetrics {
	vec := prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name: "messq_janitor_duration_seconds",
		Help: "Wall time of one completed housekeeping-job Run, by closed-set job name; " +
			"the interference gate (§5.4) wants p99s far under --commit-window.",
		Buckets: commitDurationBuckets,
	}, []string{"job"})
	if rErr := reg.Register(vec); rErr != nil {
		panic("prommetrics: NewJanitorMetrics registration failed: " + rErr.Error())
	}
	return &JanitorMetrics{vec: vec}
}

// ObserveJob implements janitor.Metrics.
func (m *JanitorMetrics) ObserveJob(name string, d time.Duration) {
	m.vec.WithLabelValues(name).Observe(d.Seconds())
}
