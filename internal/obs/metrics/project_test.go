// SPDX-License-Identifier: Apache-2.0

package metrics

import (
	"strings"
	"testing"

	"github.com/a-holm/messq/internal/obs"
	"github.com/prometheus/client_golang/prometheus/testutil"
)

// scenarioEvents drives one of everything the projection can say. It is the seed
// of the shared M5 golden scenario: publish → dup → deliver → timeout → redeliver →
// nak → redeliver → extend → ack(late) → stale ack → term → dead(written) →
// dead(orphan) → retention expire/blocked → api error.
func scenarioEvents() []obs.Event {
	return []obs.Event{
		{Event: "msg.publish", Stream: "orders"},
		{Event: "msg.publish", Stream: "orders"},
		{Event: "msg.dup", Stream: "orders"},
		{Event: "msg.deliver", Stream: "orders", Consumer: "worker", Attempt: 1},
		{
			Event: "msg.timeout", Stream: "orders", Consumer: "worker",
			Detail: map[string]any{"lateness_ms": float64(1200)},
		},
		{Event: "msg.deliver", Stream: "orders", Consumer: "worker", Attempt: 2},
		{Event: "msg.nak", Stream: "orders", Consumer: "worker"},
		{Event: "msg.deliver", Stream: "orders", Consumer: "worker", Attempt: 3},
		{Event: "msg.extend", Stream: "orders", Consumer: "worker"},
		{
			Event: "msg.ack", Stream: "orders", Consumer: "worker",
			Detail: map[string]any{"held_ms": float64(250), "late": true},
		},
		{Event: "msg.ack_stale", Stream: "orders", Consumer: "worker"},
		{Event: "msg.term", Stream: "orders", Consumer: "worker"},
		{
			Event: "msg.dead", Stream: "orders", Consumer: "worker",
			Detail: map[string]any{"dlq": "written"},
		},
		{
			Event: "msg.dead", Stream: "orders", Consumer: "worker2",
			Detail: map[string]any{"dlq": "origin_missing"},
		},
		{Event: "retention.expire", Stream: "orders"},
		{Event: "retention.blocked", Stream: "orders"},
		{Event: "api.error", Detail: map[string]any{"code": "busy"}},
	}
}

// TestProjectionExhaustiveness is Decision 1's enforcement: every member of the
// closed vocabulary either projects to instruments or carries a recorded reason why
// not. Adding a vocabulary member without a decision fails here.
func TestProjectionExhaustiveness(t *testing.T) {
	projected := projectedKinds()
	for _, k := range obs.AllKinds() {
		_, proj := projected[k]
		reason, excused := noCounter[k]
		switch {
		case proj && excused:
			t.Errorf("%q is both projected and excused: %q", k.String(), reason)
		case !proj && !excused:
			t.Errorf("event %q has no metric decision: add it to the projection or to noCounter with a reason", k.String())
		case !proj && reason == "":
			t.Errorf("event %q has an empty noCounter reason — write why it has no counter", k.String())
		}
	}
	for k := range projected {
		if k.String() == "" {
			t.Errorf("projectedKinds contains a non-member kind %d", k)
		}
	}
}

// TestProjectionGolden is the GatherAndCompare proof that the scripted scenario moves
// exactly the right counters by exactly the right labels (G11's metrics leg).
func TestProjectionGolden(t *testing.T) {
	m := newTestMetrics(t, func(o *Options) { o.MaxSeries = 100 })
	m.Publish(scenarioEvents())

	want := `
# HELP messq_api_errors_total API error envelopes emitted, by closed machine code.
# TYPE messq_api_errors_total counter
messq_api_errors_total{code="busy"} 1
# HELP messq_acked_total Acks that deleted a delivery row.
# TYPE messq_acked_total counter
messq_acked_total{consumer="worker",stream="orders"} 1
# HELP messq_dead_orphan_total origin_missing deaths; structurally impossible, alert on any value.
# TYPE messq_dead_orphan_total counter
messq_dead_orphan_total{consumer="worker2",stream="orders"} 1
# HELP messq_dead_total Dead-letter routings from the ORIGIN stream, by origin consumer.
# TYPE messq_dead_total counter
messq_dead_total{consumer="worker",stream="orders"} 1
messq_dead_total{consumer="worker2",stream="orders"} 1
# HELP messq_delivered_total Deliveries claimed, including redeliveries.
# TYPE messq_delivered_total counter
messq_delivered_total{consumer="worker",stream="orders"} 3
# HELP messq_dlq_written_total Dead-letter copies written, labelled with the ORIGIN stream.
# TYPE messq_dlq_written_total counter
messq_dlq_written_total{stream="orders"} 1
# HELP messq_duplicates_total Duplicate publishes rejected by the dedup window, by stream.
# TYPE messq_duplicates_total counter
messq_duplicates_total{stream="orders"} 1
# HELP messq_expired_total Messages dropped by retention; data loss must be watchable.
# TYPE messq_expired_total counter
messq_expired_total{stream="orders"} 1
# HELP messq_extends_total Heartbeat extends; a worker extending forever is a wedge.
# TYPE messq_extends_total counter
messq_extends_total{consumer="worker",stream="orders"} 1
# HELP messq_late_acks_total Ack races a duplicate narrowly avoided; rising with stale_acks means ack_wait is marginal.
# TYPE messq_late_acks_total counter
messq_late_acks_total{consumer="worker",stream="orders"} 1
# HELP messq_naked_total Naks that changed a delivery state.
# TYPE messq_naked_total counter
messq_naked_total{consumer="worker",stream="orders"} 1
# HELP messq_published_total Messages committed to a stream, by stream.
# TYPE messq_published_total counter
messq_published_total{stream="orders"} 2
# HELP messq_redelivered_total Deliveries with attempt > 1, by named cause.
# TYPE messq_redelivered_total counter
messq_redelivered_total{cause="nak",consumer="worker",stream="orders"} 1
messq_redelivered_total{cause="timeout",consumer="worker",stream="orders"} 1
# HELP messq_retention_blocked_total Retention passes that had to block on a stuck consumer; PLAN §9.4 alert set.
# TYPE messq_retention_blocked_total counter
messq_retention_blocked_total{stream="orders"} 1
# HELP messq_stale_acks_total Acks that lost a lease race (stale_ack / wrong_generation); alert on any nonzero rate.
# TYPE messq_stale_acks_total counter
messq_stale_acks_total{consumer="worker",stream="orders"} 1
# HELP messq_termed_total Terms that skipped the remaining attempts.
# TYPE messq_termed_total counter
messq_termed_total{consumer="worker",stream="orders"} 1
# HELP messq_timeouts_total Leases that expired past ack_wait.
# TYPE messq_timeouts_total counter
messq_timeouts_total{consumer="worker",stream="orders"} 1
`
	if err := testutil.GatherAndCompare(m.reg, strings.NewReader(want),
		"messq_api_errors_total", "messq_acked_total", "messq_dead_orphan_total",
		"messq_dead_total", "messq_delivered_total", "messq_dlq_written_total",
		"messq_duplicates_total", "messq_expired_total", "messq_extends_total",
		"messq_late_acks_total", "messq_naked_total", "messq_published_total",
		"messq_redelivered_total", "messq_retention_blocked_total",
		"messq_stale_acks_total", "messq_termed_total", "messq_timeouts_total",
	); err != nil {
		t.Fatal(err)
	}
}

// TestProjectionObservesHistograms checks the two event-driven observations beyond
// plain increments: msg.ack held_ms into the latency histogram (including the late
// flag) and msg.timeout lateness_ms into the sweeper lateness histogram.
func TestProjectionObservesHistograms(t *testing.T) {
	m := newTestMetrics(t, nil)
	m.Publish(scenarioEvents())

	fams := gather(t, m)
	ack := fams[nameAckLatencyHist]
	if ack == nil || len(ack.GetMetric()) != 1 {
		t.Fatalf("ack_latency missing or wrongly labelled: %v", ack)
	}
	h := ack.GetMetric()[0].GetHistogram()
	sum := h.GetSampleSum() - 0.25
	if h.GetSampleCount() != 1 || sum < -1e-9 || sum > 1e-9 {
		t.Errorf("ack_latency count=%d sum=%v, want 1 × 0.25s (held_ms)", h.GetSampleCount(), h.GetSampleSum())
	}
	late := fams[metricLateAcksTotal]
	if late.GetMetric()[0].GetCounter().GetValue() != 1 {
		t.Errorf("late flag did not feed messq_late_acks_total")
	}

	lat := fams[metricSweepLatenessSecs]
	if lat == nil || len(lat.GetMetric()) != 1 {
		t.Fatalf("sweep_lateness missing: %v", lat)
	}
	sh := lat.GetMetric()[0].GetHistogram()
	lsum := sh.GetSampleSum() - 1.2
	if sh.GetSampleCount() != 1 || lsum < -1e-9 || lsum > 1e-9 {
		t.Errorf("sweep_lateness count=%d sum=%v, want 1 × 1.2s (lateness_ms)", sh.GetSampleCount(), sh.GetSampleSum())
	}
}

// TestUnknownRedeliveryCauseIsNamed proves the defensive branch of the cause enum: a
// redelivery whose cause event was never seen (only possible just after a restart,
// when counters reset anyway) still lands inside the closed set.
func TestUnknownRedeliveryCauseIsNamed(t *testing.T) {
	m := newTestMetrics(t, nil)
	m.Publish([]obs.Event{
		{Event: "msg.deliver", Stream: "s", Consumer: "c", Attempt: 4},
	})
	fams := gather(t, m)
	rd := fams[metricRedeliveredTotal]
	if rd == nil || len(rd.GetMetric()) != 1 ||
		rd.GetMetric()[0].GetLabel()[0].GetValue() != "unknown" {
		t.Errorf("redelivery without an armed cause must report cause=\"unknown\": %v", rd)
	}
}

// TestDetailCauseOverridesArming lets emitters that already know their cause say so
// directly; ack_wait normalises onto the closed timeout member.
func TestDetailCauseOverridesArming(t *testing.T) {
	m := newTestMetrics(t, nil)
	m.Publish([]obs.Event{
		{Event: "msg.nak", Stream: "s", Consumer: "c"},
		{
			Event: "msg.deliver", Stream: "s", Consumer: "c", Attempt: 2,
			Detail: map[string]any{"cause": "ack_wait"},
		},
		{Event: "recovery.reclaimed", Stream: "s", Consumer: "c"},
		{Event: "msg.deliver", Stream: "s", Consumer: "c", Attempt: 3},
	})
	fams := gather(t, m)
	rd := fams[metricRedeliveredTotal]
	if rd == nil {
		t.Fatal("redelivered missing")
	}
	causes := map[string]float64{}
	for _, mtrc := range rd.GetMetric() {
		causes[mtrc.GetLabel()[0].GetValue()] = mtrc.GetCounter().GetValue()
	}
	if causes["timeout"] != 1 || causes["broker_restart"] != 1 {
		t.Errorf("causes = %v, want timeout:1 (detail override beats armed nak) and broker_restart:1", causes)
	}
}

// TestSeriesBoundRefusesNewPairs is the I11 bound: at --metrics-max-series distinct
// pairs new pairs stop being projected and the refusal itself is counted — never an
// __other__ bucket, never an unbounded collection.
func TestSeriesBoundRefusesNewPairs(t *testing.T) {
	m := newTestMetrics(t, func(o *Options) { o.MaxSeries = 1 })
	m.Publish([]obs.Event{
		{Event: "msg.deliver", Stream: "a", Consumer: "w"},
		{Event: "msg.deliver", Stream: "a", Consumer: "w"},
		{Event: "msg.deliver", Stream: "b", Consumer: "v"}, // second pair → refused
		{
			Event: "msg.dead", Stream: "b", Consumer: "v",
			Detail: map[string]any{"dlq": "written"},
		}, // also refused
	})
	fams := gather(t, m)
	del := fams[nameDeliveredTotal]
	if del == nil || len(del.GetMetric()) != 1 {
		t.Fatalf("delivered after refusals = %v, want exactly the admitted pair", del)
	}
	for _, l := range del.GetMetric()[0].GetLabel() {
		if l.GetValue() == "b" || l.GetValue() == "v" {
			t.Error("refused pair b/v still exported a delivered series")
		}
	}
	dropped := fams[nameDroppedSeriesTotal]
	if dropped.GetMetric()[0].GetCounter().GetValue() != 2 {
		t.Errorf("dropped_series = %v, want 2 refusals", dropped.GetMetric()[0].GetCounter().GetValue())
	}
}

// TestReapingRemovesDeletedSeries is I11's anti-zombie rule: deleting a consumer or
// stream must remove its counter series from the next scrape.
func TestReapingRemovesDeletedSeries(t *testing.T) {
	m := newTestMetrics(t, nil)
	m.Publish([]obs.Event{
		{Event: "msg.publish", Stream: "orders"},
		{Event: "msg.deliver", Stream: "orders", Consumer: "gone"},
		{Event: "msg.deliver", Stream: "orders", Consumer: "stays"},
		{Event: "consumer.delete", Stream: "orders", Consumer: "gone"},
	})
	fams := gather(t, m)
	del := fams[nameDeliveredTotal]
	if del == nil || len(del.GetMetric()) != 1 {
		t.Fatalf("delivered after consumer.delete: want exactly the surviving series, got %v", del)
	}
	for _, l := range del.GetMetric()[0].GetLabel() {
		if l.GetName() == "consumer" && l.GetValue() != "stays" {
			t.Errorf("surviving series has consumer=%q, want stays", l.GetValue())
		}
	}

	m.Publish([]obs.Event{{Event: "stream.delete", Stream: "orders"}})
	fams = gather(t, m)
	if fams[namePublishedTotal] != nil {
		t.Error("stream.delete left messq_published_total series behind")
	}
}

// TestCardinalityAllowList walks a post-scenario gather and enforces the reconciled
// cardinality rule against reality: label sets equal the catalogue exactly, every
// enum value sits inside its declared constant list, and no forbidden identity label
// ever appears (G7 layer 1).
func TestCardinalityAllowList(t *testing.T) {
	m := newTestMetrics(t, nil)
	m.Publish(scenarioEvents())

	fams, err := m.reg.Gather()
	if err != nil {
		t.Fatal(err)
	}
	if len(fams) < 20 {
		t.Fatalf("scenario produced only %d families; the allow-list would pass vacuously", len(fams))
	}
	forbidden := map[string]bool{
		"subject": true, "msg_id": true, "id": true, "seq": true, "trace_id": true,
		"token": true, "peer": true, "client": true, "path": true, "url": true,
		"error": true, "user": true, "instance": true,
	}
	for _, fam := range fams {
		name := fam.GetName()
		if strings.HasPrefix(name, "go_") || strings.HasPrefix(name, "process_") ||
			strings.HasPrefix(name, "promhttp_") {
			continue // third-party runtime collectors are not catalogue rows
		}
		spec, ok := Spec(name)
		if !ok {
			t.Errorf("gathered family %s is not in the catalogue", name)
			continue
		}
		want := map[string]bool{}
		for _, l := range spec.Labels {
			want[l] = true
		}
		for _, mtrc := range fam.GetMetric() {
			got := map[string]bool{}
			for _, l := range mtrc.GetLabel() {
				got[l.GetName()] = true
				if forbidden[l.GetName()] {
					t.Errorf("%s: forbidden identity label %q reached the wire", name, l.GetName())
				}
				if vals, ok := EnumValues(l.GetName()); ok && !contains(vals, l.GetValue()) {
					t.Errorf("%s{%s=%q}: value outside the declared enum %v", name, l.GetName(), l.GetValue(), vals)
				}
			}
			for l := range want {
				if !got[l] {
					t.Errorf("%s: missing label %q", name, l)
				}
			}
			for l := range got {
				if !want[l] {
					t.Errorf("%s: unexpected label %q (catalogue declares %v)", name, l, spec.Labels)
				}
			}
		}
	}
}

func contains(list []string, v string) bool {
	for _, s := range list {
		if s == v {
			return true
		}
	}
	return false
}
