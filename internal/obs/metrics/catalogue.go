// SPDX-License-Identifier: Apache-2.0

package metrics

import (
	"sort"

	"github.com/a-holm/messq/internal/wirecode"
	"github.com/prometheus/client_golang/prometheus"
)

// metricKind is the Prometheus instrument type of one catalogue row.
type metricKind uint8

const (
	kindCounter metricKind = iota
	kindGauge
	kindHistogram
)

func (k metricKind) String() string {
	switch k {
	case kindCounter:
		return "counter"
	case kindGauge:
		return "gauge"
	case kindHistogram:
		return "histogram"
	}
	return "unknown"
}

// source names the component that constructs and feeds a row's instrument. The split
// keeps registration in exactly one place (this package's New) while making ownership
// reviewable: adopted rows are built by internal/obs/prommetrics (#6), seam rows are
// registered here today for a sibling seam that will feed them (their emitters land
// with later wiring), collector rows are computed at scrape time, projection rows move
// only on committed events.
type source uint8

const (
	srcProjection source = iota
	srcSeam
	srcCollector
	srcAdopted
)

func (s source) String() string {
	switch s {
	case srcProjection:
		return "projection"
	case srcSeam:
		return "seam"
	case srcCollector:
		return "collector"
	case srcAdopted:
		return "adopted"
	}
	return "unknown"
}

// metricSpec is one row of THE metric table (PLAN §9.4): everything the binary knows
// about one metric — name, type, labels, help, buckets, owner. docs/metrics.md is
// generated from this table and the cardinality allow-list test judges gathered series
// against it, so a row edit is a three-way reviewed diff, never a silent rename.
type metricSpec struct {
	Name    string
	Kind    metricKind
	Labels  []string
	Help    string
	Buckets []float64
	Native  bool   // classic buckets plus native histograms (factor 1.1)
	Source  source // who constructs and feeds it
}

// catalogue is THE metric table (PLAN §9.4 Tables A and B plus this issue's
// self-metrics), in wire-documentation order. Table C rejections are recorded as
// comments so they are not re-litigated: messq_sweep_rows and
// messq_sweep_duration_seconds (#11) are subsumed by lateness + backlog + the commit
// histogram; messq_dlq_deferred_total (#12) stays internal to #12; per-subject
// anything is unbounded by design; stream message/byte gauges are one GET /v1/streams
// away; goroutine/fd counts belong to the runtime collectors.
var catalogue = []metricSpec{
	// ── Table A — PLAN §9.4 normative, names frozen at 1.0 ──────────────────────
	{
		namePublishedTotal, kindCounter,
		[]string{"stream"},
		"Messages committed to a stream, by stream.", nil, false, srcProjection,
	},
	{
		nameDuplicatesTotal, kindCounter,
		[]string{"stream"},
		"Duplicate publishes rejected by the dedup window, by stream.", nil, false, srcProjection,
	},
	{
		nameDeliveredTotal, kindCounter,
		[]string{"stream", "consumer"},
		"Deliveries claimed, including redeliveries.", nil, false, srcProjection,
	},
	{
		metricRedeliveredTotal, kindCounter,
		[]string{"stream", "consumer", "cause"},
		"Deliveries with attempt > 1, by named cause.", nil, false, srcProjection,
	},
	{
		metricAckedTotal, kindCounter,
		[]string{"stream", "consumer"},
		"Acks that deleted a delivery row.", nil, false, srcProjection,
	},
	{
		metricNakedTotal, kindCounter,
		[]string{"stream", "consumer"},
		"Naks that changed a delivery state.", nil, false, srcProjection,
	},
	{
		metricTermedTotal, kindCounter,
		[]string{"stream", "consumer"},
		"Terms that skipped the remaining attempts.", nil, false, srcProjection,
	},
	{
		metricTimeoutsTotal, kindCounter,
		[]string{"stream", "consumer"},
		"Leases that expired past ack_wait.", nil, false, srcProjection,
	},
	{
		metricDeadTotal, kindCounter,
		[]string{"stream", "consumer"},
		"Dead-letter routings from the ORIGIN stream, by origin consumer.", nil, false, srcProjection,
	},
	{
		metricStaleAcksTotal, kindCounter,
		[]string{"stream", "consumer"},
		"Acks that lost a lease race (stale_ack / wrong_generation); alert on any nonzero rate.", nil, false, srcProjection,
	},

	{
		namePending, kindGauge,
		[]string{"stream", "consumer"},
		"Unfinished materialised work (READY + INFLIGHT delivery rows).", nil, false, srcCollector,
	},
	{
		nameInflight, kindGauge,
		[]string{"stream", "consumer"},
		"Delivery rows currently leased to a worker.", nil, false, srcCollector,
	},
	{
		nameBacklog, kindGauge,
		[]string{"stream", "consumer"},
		"Total work owed: pending + unscanned messages at or above the cursor.", nil, false, srcCollector,
	},
	{
		nameOldestPendingAge, kindGauge,
		[]string{"stream", "consumer"},
		"Age of the oldest unfinished message; THE user-facing SLI; alert on it.", nil, false, srcCollector,
	},
	{
		nameDLQDepth, kindGauge,
		[]string{"stream"},
		"Messages in <stream>.dlq, labelled with the ORIGIN stream.", nil, false, srcCollector,
	},

	{
		nameAckLatencyHist, kindHistogram,
		[]string{"stream", "consumer"},
		"Held duration of acked deliveries, from msg.ack held_ms.",
		ackLatencyBuckets, true, srcProjection,
	},

	{
		"messq_commit_duration_seconds", kindHistogram, nil,
		"Wall time to commit one batch transaction; dominated by the WAL fsync under durability=full.",
		nil, false, srcAdopted,
	}, // buckets owned by internal/obs/prommetrics (#6)
	{
		"messq_commit_batch_size", kindHistogram, nil,
		"Commands per committed batch transaction; sum/counts give the mean batching depth.",
		nil, false, srcAdopted,
	},

	{
		nameDBBytes, kindGauge, nil,
		"Size of the SQLite database file.", nil, false, srcCollector,
	},
	{
		nameWALBytes, kindGauge, nil,
		"Size of the SQLite write-ahead log.", nil, false, srcCollector,
	},
	{
		nameEventsRows, kindGauge, nil,
		"Rows in the events table; MAX(id)-MIN(id)+1 stays exact because trimming removes a prefix.",
		nil, false, srcCollector,
	},
	{
		nameDiskFreeBytes, kindGauge, nil,
		"Free bytes on the data directory's filesystem (statfs; #27 replaces with its disk monitor).",
		nil, false, srcCollector,
	},
	{
		nameBuildInfo, kindGauge,
		[]string{"version", "commit", "durability"},
		"Build identification; always 1.", nil, false, srcCollector,
	},

	// ── Table B — inherited additions, each driving a shipped alert or panel ────
	{
		nameReadOnlyGauge, kindGauge, nil,
		"1 when the process latched read-only after an unrecoverable storage fault; alert on 1.",
		nil, false, srcAdopted,
	},
	{
		nameCommitErrorsTotal, kindCounter,
		[]string{"class"},
		"Batch commits that failed, by storage-fault class.", nil, false, srcAdopted,
	},
	{
		nameWriterQueueDepth, kindGauge, nil,
		"Commands waiting in the writer queue, sampled once per batch cycle.",
		nil, false, srcAdopted,
	},

	{
		metricLateAcksTotal, kindCounter,
		[]string{"stream", "consumer"},
		"Ack races a duplicate narrowly avoided; rising with stale_acks means ack_wait is marginal.",
		nil, false, srcProjection,
	},
	{
		metricExtendsTotal, kindCounter,
		[]string{"stream", "consumer"},
		"Heartbeat extends; a worker extending forever is a wedge.", nil, false, srcProjection,
	},
	{
		metricSweepLatenessSecs, kindHistogram, nil,
		"Sweeper lateness: now - visible_at when an expired row was swept.",
		sweepLatenessBuckets, false, srcProjection,
	},
	{
		metricSweepBacklog, kindGauge, nil,
		"Expired-but-unswept rows: late does not mean lost, and this says which.",
		nil, false, srcCollector,
	},
	{
		metricSweepSkippedTotal, kindCounter,
		[]string{"reason"},
		"Sweeper ticks dropped under saturation, by reason.", nil, false, srcSeam,
	},

	{
		MessqDLQWrittenTotal, kindCounter,
		[]string{"stream"},
		"Dead-letter copies written, labelled with the ORIGIN stream.", nil, false, srcProjection,
	},
	{
		MessqDeadOrphanTotal, kindCounter,
		[]string{"stream", "consumer"},
		"origin_missing deaths; structurally impossible, alert on any value.", nil, false, srcProjection,
	},

	{
		metricWaitersGauge, kindGauge, nil,
		"Parked long-poll fetch waiters; approaching --max-waiters precedes 503s.",
		nil, false, srcSeam,
	},
	{
		metricFetchAbandonedTotal, kindCounter,
		[]string{"stream", "consumer"},
		"Clients disconnecting mid-fetch after their claim committed.", nil, false, srcSeam,
	},
	{
		metricFetchWaitSeconds, kindHistogram,
		[]string{"stream", "consumer"},
		"Long-poll wake latency.", fetchWaitBuckets, true, srcSeam,
	},
	{
		metricHTTPRequestsTotal, kindCounter,
		[]string{"route", "method", "code"},
		"HTTP requests answered, by registered route pattern, method and envelope code.",
		nil, false, srcSeam,
	},
	{
		metricHTTPDurationSeconds, kindHistogram,
		[]string{"route"},
		"Request duration excluding fetch (dominated by wait_ms; it has its own).",
		httpDurationBuckets, false, srcSeam,
	},
	{
		metricAPIErrorsTotal, kindCounter,
		[]string{"code"},
		"API error envelopes emitted, by closed machine code.", nil, false, srcProjection,
	},

	{
		nameExpiredTotal, kindCounter,
		[]string{"stream"},
		"Messages dropped by retention; data loss must be watchable.", nil, false, srcProjection,
	},
	{
		nameRetentionBlocked, kindCounter,
		[]string{"stream"},
		"Retention passes that had to block on a stuck consumer; PLAN §9.4 alert set.",
		nil, false, srcProjection,
	},
	{
		nameConsumerPaused, kindGauge,
		[]string{"stream", "consumer"},
		"1 when the consumer is deliberately paused; alerts unless it instead of paging.",
		nil, false, srcCollector,
	},

	// ── Self-metrics — the collector is honest about its own failures ───────────
	{
		nameScrapeErrorsTotal, kindCounter,
		[]string{"tier"},
		"Snapshot refreshes that failed, by cache tier; the previous snapshot keeps serving.",
		nil, false, srcCollector,
	},
	{
		nameSnapshotAgeSeconds, kindGauge,
		[]string{"tier"},
		"Age of the gauge snapshot serving this scrape.", nil, false, srcCollector,
	},
	{
		nameCollectDurationSecs, kindHistogram,
		[]string{"tier"},
		"Scrape cost per tier refresh, so it can be budgeted.",
		collectDurationBucket, false, srcCollector,
	},
	{
		nameDroppedSeriesTotal, kindCounter, nil,
		"Counter projections refused at --metrics-max-series; refusing beats an __other__ lie.",
		nil, false, srcProjection,
	},
	{
		nameMetricsTruncatedTotl, kindCounter, nil,
		"oldest-pending-age pairs whose head scan hit --metrics-filter-scan without a match; the gauge under-estimates for those pairs only.",
		nil, false, srcCollector,
	},
}

// enumValues is the compile-time constant list behind every non-identity label (the
// reconciled cardinality rule): identity labels are stream and consumer and nothing
// else; any other label's values must come from here, and the allow-list test fails
// on a value outside the list.
var enumValues = map[string][]string{
	// Redelivery causes (I9: every duplicate has a named cause). timeout = expired
	// ack_wait, nak = explicit release, broker_restart = T9 recovery reclaim;
	// unknown is the defensive member for a redelivery observed before its cause
	// event (possible only right after a restart, when counters reset anyway).
	"cause": {"timeout", "nak", "broker_restart", "unknown"},
	// Storage-fault classes, mirroring obs.ClassifyStorageError's outputs.
	"class": {"eio", "enospc", "corrupt", "unknown"},
	// Sweeper tick-skip reasons (#11).
	"reason": {"writer_busy", "overlap"},
	// Snapshot cache tiers.
	"tier": {"cheap", "heavy"},
	// HTTP methods of the route registry.
	"method": {"GET", "POST", "PUT", "PATCH", "DELETE", "HEAD", "OPTIONS"},
	// Registered ServeMux patterns, frozen from internal/api routes() at the time of
	// #21's registration slice; the wiring slice asserts equality against api's
	// golden. The catch-all answers every method, hence no method prefix.
	"route": {
		"GET /healthz", "GET /v1/info",
		"POST /v1/streams", "GET /v1/streams",
		"GET /v1/streams/{stream}", "PATCH /v1/streams/{stream}", "DELETE /v1/streams/{stream}",
		"POST /v1/streams/{stream}/messages", "POST /v1/streams/{stream}/messages:batch",
		"GET /v1/streams/{stream}/messages", "GET /v1/streams/{stream}/messages/{seq}",
		"GET /v1/streams/{stream}/messages/{seq}/data", "GET /v1/messages/{id}",
		"POST /v1/streams/{stream}/consumers", "GET /v1/streams/{stream}/consumers",
		"GET /v1/streams/{stream}/consumers/{consumer}",
		"PATCH /v1/streams/{stream}/consumers/{consumer}",
		"DELETE /v1/streams/{stream}/consumers/{consumer}",
		"POST /v1/streams/{stream}/consumers/{consumer}/fetch",
		"POST /v1/ack", "POST /v1/nak", "POST /v1/term", "POST /v1/extend",
		"/",
	},
}

func init() {
	// The code label derives from internal/wirecode, the single closed-code source:
	// every HTTP-capable member, never the NeverOverHTTP set. Deriving beats copying
	// so the two cannot drift apart.
	var codes []string
	never := map[string]bool{}
	for _, c := range wirecode.NeverOverHTTPSet() {
		never[string(c)] = true
	}
	for _, c := range wirecode.All() {
		if !never[string(c)] {
			codes = append(codes, string(c))
		}
	}
	sort.Strings(codes)
	enumValues["code"] = codes
}

// Spec looks one catalogue row up by wire name.
func Spec(name string) (metricSpec, bool) {
	for _, s := range catalogue {
		if s.Name == name {
			return s, true
		}
	}
	return metricSpec{}, false
}

// Names returns every catalogue name in sorted order.
func Names() []string {
	out := make([]string, 0, len(catalogue))
	for _, s := range catalogue {
		out = append(out, s.Name)
	}
	sort.Strings(out)
	return out
}

// bucket helpers shared by several rows.
var (
	ackLatencyBuckets     = prometheus.ExponentialBuckets(0.005, 2, 14) // 5 ms … ~40 s
	sweepLatenessBuckets  = []float64{0.05, 0.1, 0.25, 0.5, 1, 2, 5, 30}
	fetchWaitBuckets      = []float64{0.001, 0.005, 0.025, 0.1, 0.5, 1, 5, 30, 60}
	httpDurationBuckets   = prometheus.DefBuckets
	collectDurationBucket = prometheus.DefBuckets
)

// Metric wire names. The handover blocks carry the exact names the merged sibling
// issues pinned for #21 (see each source issue's own pinning test); a rename on
// either side is cross-repository breakage.
const (
	namePublishedTotal   = "messq_published_total"
	nameDuplicatesTotal  = "messq_duplicates_total"
	nameDeliveredTotal   = "messq_delivered_total"
	nameAckLatencyHist   = "messq_ack_latency_seconds"
	namePending          = "messq_pending"
	nameInflight         = "messq_inflight"
	nameBacklog          = "messq_backlog"
	nameOldestPendingAge = "messq_oldest_pending_age_seconds"
	nameDLQDepth         = "messq_dlq_depth"
	nameBuildInfo        = "messq_build_info"
	nameDBBytes          = "messq_db_bytes"
	nameWALBytes         = "messq_wal_bytes"
	nameEventsRows       = "messq_events_rows"
	nameDiskFreeBytes    = "messq_disk_free_bytes"

	// Handover: #10 settle.go metric-name constants.
	metricAckedTotal        = "messq_acked_total"
	metricNakedTotal        = "messq_naked_total"
	metricTermedTotal       = "messq_termed_total"
	metricExtendsTotal      = "messq_extends_total"
	metricStaleAcksTotal    = "messq_stale_acks_total"
	metricLateAcksTotal     = "messq_late_acks_total"
	metricAckLatencySeconds = "messq_ack_latency_seconds"

	// Handover: #11 sweep.go metric-name constants (minus Table C rejections).
	metricTimeoutsTotal     = "messq_timeouts_total"
	metricRedeliveredTotal  = "messq_redelivered_total"
	metricDeadTotal         = "messq_dead_total"
	metricSweepLatenessSecs = "messq_sweep_lateness_seconds"
	metricSweepBacklog      = "messq_sweep_backlog"
	metricSweepSkippedTotal = "messq_sweep_skipped_total"

	// Handover: #12 dead.go exported DLQ constants (minus Table C rejections).
	MessqDLQWrittenTotal = "messq_dlq_written_total"
	MessqDeadOrphanTotal = "messq_dead_orphan_total"
	MessqDLQDepth        = "messq_dlq_depth"

	// Handover: #14 proposed fetch/waiter/HTTP/API names (issue #14 §"Prometheus
	// registration → #21").
	metricWaitersGauge        = "messq_waiters"
	metricFetchAbandonedTotal = "messq_fetch_abandoned_total"
	metricFetchWaitSeconds    = "messq_fetch_wait_seconds"
	metricHTTPRequestsTotal   = "messq_http_requests_total"
	metricHTTPDurationSeconds = "messq_http_request_duration_seconds"
	metricAPIErrorsTotal      = "messq_api_errors_total"

	// Self-metrics and lifecycle rows owned by this issue.
	nameReadOnlyGauge        = "messq_readonly"
	nameCommitErrorsTotal    = "messq_commit_errors_total"
	nameWriterQueueDepth     = "messq_writer_queue_depth"
	nameConsumerPaused       = "messq_consumer_paused"
	nameExpiredTotal         = "messq_expired_total"
	nameRetentionBlocked     = "messq_retention_blocked_total"
	nameScrapeErrorsTotal    = "messq_metrics_scrape_errors_total"
	nameSnapshotAgeSeconds   = "messq_metrics_snapshot_age_seconds"
	nameCollectDurationSecs  = "messq_metrics_collect_duration_seconds"
	nameDroppedSeriesTotal   = "messq_metrics_dropped_series_total"
	nameMetricsTruncatedTotl = "messq_metrics_truncated_total"
)

// EnumValues returns the compile-time constant list behind one enum label.
func EnumValues(label string) ([]string, bool) {
	v, ok := enumValues[label]
	return v, ok
}
