// SPDX-License-Identifier: Apache-2.0

package metrics

import (
	"time"

	"github.com/a-holm/messq/internal/obs"
	"github.com/prometheus/client_golang/prometheus"
)

// The event→counter projection (Decision 1): counters move only in response to
// committed events, delivered by #19's fan-out through [Metrics.Publish] (it
// satisfies obs.Sink). One decision table below — a new vocabulary member must
// appear either in projectionRows or in noCounter with a reason, and the
// exhaustiveness test fails until it does.

// seriesKey identifies one (stream, consumer) counter pair; it is the unit of the
// label-value cache and of reaping.
type seriesKey struct{ stream, consumer string }

// Redelivery causes: the closed value list for messq_redelivered_total (I9: every
// duplicate has a named cause). unknown is defensive: reachable only when a
// redelivery outlives its cause event across a restart, where counters reset anyway.
const (
	causeTimeout       = "timeout"
	causeNak           = "nak"
	causeBrokerRestart = "broker_restart"
	causeUnknown       = "unknown"

	detailCauseAckWait = "ack_wait" // emitters' spelling of timeout, normalised
)

// noCounter records every non-projected event kind with the reason it earns no
// counter — the explicit "no" the exhaustiveness test checks for.
var noCounter = map[obs.Kind]string{
	obs.ServerStart:       "process_start_time_seconds already encodes restarts",
	obs.ServerStop:        "the process is exiting; process_start_time_seconds encodes its lifetime",
	obs.ServerReload:      "administrative config event; audit log surface",
	obs.RecoveryUnclean:   "restarts are visible via process_start_time_seconds; detail lives in the log",
	obs.RecoveryReclaimed: "not a counter: arms broker_restart as the next delivery's redelivery cause",
	obs.StorageFatal:      "latches messq_readonly through the CommitObserver seam, not an event counter",
	obs.StreamCreate:      "series appear with first activity; creation exports no zombie zero-series",
	obs.StreamUpdate:      "configuration change; audit log surface",
	obs.StreamDelete:      "reaper: deletes the stream's counter series (I11), not an increment",
	obs.StreamPurge:       "gauges recompute from live rows; counters are event totals and never reset",
	obs.ConsumerCreate:    "series appear with first activity; creation exports no zombie zero-series",
	obs.ConsumerUpdate:    "configuration change; audit log surface",
	obs.ConsumerDelete:    "reaper: deletes the consumer's counter series (I11), not an increment",
	obs.ConsumerSeek:      "cursor movement; gauges recompute at the next scrape (Decision 2)",
	obs.ConsumerPause:     "messq_consumer_paused is computed at scrape time; the event mirrors the column",
	obs.ConsumerLag:       "advisory event; the backlog gauge is authoritative",
	obs.MsgAckDup:         "idempotent replay; visible in trace, not worth a series",
	obs.DLQRedrive:        "#29's accounting; no v1 counter in the frozen tables",
	obs.FlowBlocked:       "backpressure signal; flow_control surfaces via messq_api_errors_total",
	obs.DiskDegraded:      "disk_free_bytes plus #27's state machine carry disk state",
	obs.AuthDenied:        "auth surfaces via messq_api_errors_total{code=unauthorized|forbidden}",
	obs.AdminAction:       "audit log surface; admin actions are rare and human-initiated",
}

// streamDimmedRows are the vecs whose only label is stream; stream.delete reaps them.
var streamDimmedRows = []string{
	namePublishedTotal, nameDuplicatesTotal,
	MessqDLQWrittenTotal, nameExpiredTotal, nameRetentionBlocked,
}

// pairDimmedRows are the vecs labelled (stream, consumer); consumer.delete reaps them.
var pairDimmedRows = []string{
	nameDeliveredTotal, metricRedeliveredTotal,
	metricAckedTotal, metricNakedTotal, metricTermedTotal,
	metricTimeoutsTotal, metricDeadTotal, metricStaleAcksTotal,
	metricLateAcksTotal, metricExtendsTotal, MessqDeadOrphanTotal,
	nameAckLatencyHist, metricFetchWaitSeconds,
}

// projectedKinds returns the set of kinds this file projects to instruments.
func projectedKinds() map[obs.Kind]struct{} {
	out := make(map[obs.Kind]struct{}, len(projectionRows))
	for k := range projectionRows {
		out[k] = struct{}{}
	}
	return out
}

// projectionRow is one entry of the decision table: which instrument a kind moves,
// how label values derive from the event, and what else the same committed event
// observes (extras never trigger a second fan-out walk).
type projectionRow struct {
	vec    string                                  // catalogue name of the primary target
	labels func(m *Metrics, e *obs.Event) []string // ordered values matching the catalogue
	extra  func(m *Metrics, e *obs.Event)
}

func byStream(_ *Metrics, e *obs.Event) []string { return []string{e.Stream} }
func byPair(_ *Metrics, e *obs.Event) []string   { return []string{e.Stream, e.Consumer} }

// projectionRows IS the table.
var projectionRows = map[obs.Kind]projectionRow{
	obs.MsgPublish:  {vec: namePublishedTotal, labels: byStream},
	obs.MsgDup:      {vec: nameDuplicatesTotal, labels: byStream},
	obs.MsgDeliver:  {vec: nameDeliveredTotal, labels: byPair, extra: (*Metrics).projectRedelivery},
	obs.MsgAck:      {vec: metricAckedTotal, labels: byPair, extra: (*Metrics).projectAck},
	obs.MsgAckStale: {vec: metricStaleAcksTotal, labels: byPair},
	obs.MsgNak:      {vec: metricNakedTotal, labels: byPair, extra: (*Metrics).armCauseNak},
	obs.MsgTerm:     {vec: metricTermedTotal, labels: byPair},
	obs.MsgExtend:   {vec: metricExtendsTotal, labels: byPair},
	obs.MsgTimeout:  {vec: metricTimeoutsTotal, labels: byPair, extra: (*Metrics).projectTimeout},
	obs.MsgDead:     {vec: metricDeadTotal, labels: byPair, extra: (*Metrics).projectDeadOutcome},
	obs.RetentionExpire: {
		vec: nameExpiredTotal, labels: byStream,
	},
	obs.RetentionBlocked: {
		vec: nameRetentionBlocked, labels: byStream,
	},
	obs.APIError: {vec: metricAPIErrorsTotal, labels: apiErrorLabels},
}

// apiErrorLabels adapts the code-enum check into the row's label-function shape.
func apiErrorLabels(m *Metrics, e *obs.Event) []string { return m.apiErrorLabelValues(e) }

// Publish implements obs.Sink. It runs on the fan-out pump and MUST NOT block:
// every operation below is a bounded map lookup plus an increment or observation.
func (m *Metrics) Publish(evs []obs.Event) {
	for i := range evs {
		m.project(&evs[i])
	}
}

// project routes one committed event through the decision table.
func (m *Metrics) project(e *obs.Event) {
	k, ok := m.kinds[e.Event]
	if !ok || k == obs.KindInvalid {
		return // names outside the closed set cannot come from #19; ignore anyway
	}

	switch k {
	case obs.StreamDelete:
		m.reapStream(e.Stream)
		return
	case obs.ConsumerDelete:
		m.reapConsumer(e.Stream, e.Consumer)
		return
	case obs.RecoveryReclaimed:
		m.armCause(e, causeBrokerRestart)
		return
	}

	row, ok := projectionRows[k]
	if !ok {
		return // a documented noCounter kind
	}

	lvs := row.labels(m, e)
	if lvs == nil {
		return // dropped loudly (undeclared api code), never silently widened
	}
	switch v := m.vec(row.vec).(type) {
	case *prometheus.CounterVec:
		lvs, ok := m.admit(lvs)
		if !ok {
			return // refused at --metrics-max-series; dropped_series_total says so
		}
		v.WithLabelValues(lvs...).Inc()
	case prometheus.Counter:
		v.Inc()
	}
	if row.extra != nil {
		row.extra(m, e)
	}
}

// admit applies the I11 series bound: known pairs proceed; new pairs beyond
// --metrics-max-series are refused and counted — never bucketed into __other__.
func (m *Metrics) admit(lvs []string) ([]string, bool) {
	if len(lvs) < 2 {
		return lvs, true // stream-only rows are bounded by the closed route/code enums instead
	}
	k := seriesKey{lvs[0], lvs[1]}
	if _, ok := m.pairs[k]; ok {
		return lvs, true
	}
	if len(m.pairs) >= m.o.MaxSeries {
		m.dropped.Inc()
		m.logLimited("metrics: refusing new counter series at --metrics-max-series",
			"stream", lvs[0], "consumer", lvs[1])
		return lvs, false
	}
	m.pairs[k] = struct{}{}
	return lvs, true
}

// ── redelivery causes ───────────────────────────────────────────────────────────

// armCause remembers why this pair's NEXT duplicate will exist (I9 ordering: the
// cause event strictly precedes the redelivery). The fan-out is single-goroutine,
// so the map needs no lock (§8).
func (m *Metrics) armCause(e *obs.Event, cause string) {
	m.causes[seriesKey{e.Stream, e.Consumer}] = cause
}

// armCauseNak is msg.nak's extra.
func (m *Metrics) armCauseNak(e *obs.Event) { m.armCause(e, causeNak) }

// normaliseCause maps emitter spellings onto the closed enum; anything unrecognised
// falls back to unknown rather than widening cardinality.
func normaliseCause(s string) string {
	switch s {
	case causeTimeout, causeNak, causeBrokerRestart:
		return s
	case detailCauseAckWait:
		return causeTimeout
	}
	return causeUnknown
}

// projectRedelivery counts attempt>1 deliveries per named cause (Table A source:
// msg.deliver with attempt > 1). A detail.cause overrides the armed fallback and
// leaves the armed entry for a duplicate without one.
func (m *Metrics) projectRedelivery(e *obs.Event) {
	if e.Attempt <= 1 {
		return
	}
	var cause string
	if c, ok := e.Detail["cause"].(string); ok && c != "" {
		cause = normaliseCause(c)
	} else {
		k := seriesKey{e.Stream, e.Consumer}
		armed, ok := m.causes[k]
		if !ok {
			armed = causeUnknown
		}
		delete(m.causes, k)
		cause = armed
	}
	lvs, admit := m.admit(byPair(m, e))
	if !admit {
		return
	}
	m.counterVec(metricRedeliveredTotal).
		WithLabelValues(lvs[0], lvs[1], cause).Inc()
}

// projectTimeout arms the redelivery cause AND feeds the sweeper's own SLI (#11).
func (m *Metrics) projectTimeout(e *obs.Event) {
	if c, ok := e.Detail["cause"].(string); ok && c != "" {
		m.armCause(e, normaliseCause(c))
	} else {
		m.armCause(e, causeTimeout)
	}
	if ms, ok := detailMS(e, "lateness_ms"); ok {
		m.histogram(metricSweepLatenessSecs).WithLabelValues().Observe(float64(ms) / 1000)
	}
}

// projectAck observes held_ms into the latency histogram and counts the late-ack
// flag (#10: rising alongside stale_acks means ack_wait is marginal).
func (m *Metrics) projectAck(e *obs.Event) {
	if ms, ok := detailMS(e, "held_ms"); ok {
		if lvs, admit := m.admit(byPair(m, e)); admit {
			m.histogram(nameAckLatencyHist).WithLabelValues(lvs...).
				Observe(float64(ms) / 1000)
		}
	}
	if late, _ := e.Detail["late"].(bool); late {
		if lvs, admit := m.admit(byPair(m, e)); admit {
			m.counterVec(metricLateAcksTotal).WithLabelValues(lvs...).Inc()
		}
	}
}

// projectDeadOutcome splits msg.dead into DLQ copies written vs origin-missing
// orphans (#12: the orphan count is a bug signal; alert on any value).
func (m *Metrics) projectDeadOutcome(e *obs.Event) {
	outcome, _ := e.Detail["dlq"].(string)
	switch outcome {
	case "written":
		if lvs, ok := m.admit(byStream(m, e)); ok {
			m.counterVec(MessqDLQWrittenTotal).WithLabelValues(lvs...).Inc()
		}
	case "origin_missing":
		if lvs, ok := m.admit(byPair(m, e)); ok {
			m.counterVec(MessqDeadOrphanTotal).WithLabelValues(lvs...).Inc()
		}
	}
}

// apiErrorLabelValues extracts the closed code enum value; an undeclared code is
// logged once a minute and dropped rather than allowed to widen cardinality.
func (m *Metrics) apiErrorLabelValues(e *obs.Event) []string {
	code, _ := e.Detail["code"].(string)
	if !m.codeEnum[code] {
		m.logLimited("metrics: api.error carries an undeclared code; dropping the projection",
			"code", code)
		return nil
	}
	return []string{code}
}

// ── helpers ─────────────────────────────────────────────────────────────────────

// detailMS reads one millisecond field out of the JSON-decoded detail map;
// encoding/json hands numbers to any-typed maps as float64.
func detailMS(e *obs.Event, key string) (int64, bool) {
	v, ok := e.Detail[key].(float64)
	if !ok {
		return 0, false
	}
	return int64(v), true
}

func (m *Metrics) histogram(name string) *prometheus.HistogramVec {
	return m.instrs[name].(*prometheus.HistogramVec)
}

func (m *Metrics) counterVec(name string) *prometheus.CounterVec {
	return m.instrs[name].(*prometheus.CounterVec)
}

// logLimited warns at most once a minute so a pathological rate cannot turn the log
// into the problem.
func (m *Metrics) logLimited(msg string, args ...any) {
	now := m.o.Clock.Now()
	if !m.lastWarn.IsZero() && now.Sub(m.lastWarn) < time.Minute {
		return
	}
	m.lastWarn = now
	if m.o.Log != nil {
		m.o.Log.Warn(msg, args...)
	}
}

// deleteLabels removes one series from any vec kind (counters and histograms both).
func deleteLabels(c prometheus.Collector, lvs ...string) {
	switch v := c.(type) {
	case *prometheus.CounterVec:
		v.DeleteLabelValues(lvs...)
	case *prometheus.HistogramVec:
		v.DeleteLabelValues(lvs...)
	}
}

// reapStream deletes every stream-dimensioned series and cache entry for a stream.
func (m *Metrics) reapStream(stream string) {
	for _, name := range streamDimmedRows {
		deleteLabels(m.vec(name), stream)
	}
	m.dropPairs(func(k seriesKey) bool { return k.stream == stream })
}

// reapConsumer deletes every pair-dimensioned series and cache entry for a consumer.
func (m *Metrics) reapConsumer(stream, consumer string) {
	for _, name := range pairDimmedRows {
		deleteLabels(m.vec(name), stream, consumer)
	}
	m.dropPairs(func(k seriesKey) bool {
		return k.stream == stream && k.consumer == consumer
	})
}

// dropPairs evicts cache entries matching match=true.
func (m *Metrics) dropPairs(match func(seriesKey) bool) {
	for k := range m.pairs {
		if match(k) {
			delete(m.pairs, k)
		}
	}
	for k := range m.causes {
		if match(k) {
			delete(m.causes, k)
		}
	}
}
