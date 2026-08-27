// SPDX-License-Identifier: Apache-2.0

package doctor

import (
	"strings"
	"testing"
	"time"
)

var consumerNow = time.Date(2026, 11, 4, 12, 0, 0, 0, time.UTC)

func pendingSnap(lastDelivered time.Time, pendingCount int64) *Snapshot {
	pf := PendingFacts{PendingCount: pendingCount}
	if !lastDelivered.IsZero() {
		pf.LastDeliveredMS = lastDelivered.UnixMilli()
	}
	return &Snapshot{
		Now:     consumerNow,
		Pending: map[string]PendingFacts{"orders\x00invoices": pf},
		Consumers: []ConsumerState{{
			Stream: "orders", Name: "invoices",
		}},
	}
}

func TestConsumerIdleFailsWithBacklog(t *testing.T) {
	silence := consumerNow.Add(-25 * time.Hour) // default idle-after is 24h
	f := mustFire(t, evalCheck(t, "consumer.idle",
		pendingSnap(silence, 3)), "consumer.idle", SevFail)
	if !strings.Contains(f.Title, "3 waiting messages") {
		t.Fatalf("idle(fail) should name the backlog: %q", f.Title)
	}

	// Fresh delivery: nothing fires either id.
	calm := pendingSnap(consumerNow.Add(-1*time.Minute), 1)
	mustNotFire(t, evalCheck(t, "consumer.idle", calm), "consumer.idle")
	mustNotFire(t, evalCheck(t, "consumer.idle_no_backlog", calm), "consumer.idle_no_backlog")
}

func TestConsumerIdleNoBacklogWarnsAndBoundaryAtIdleAfter(t *testing.T) {
	silence := consumerNow.Add(-25 * time.Hour)
	exactly := consumerNow.Add(-24 * time.Hour) // exactly the threshold: fires
	boundary := pendingSnap(exactly, 0)
	mustFire(t, evalCheck(t, "consumer.idle_no_backlog", boundary),
		"consumer.idle_no_backlog", SevWarn)
	justUnder := pendingSnap(consumerNow.Add(-24*time.Hour+time.Second), 0)
	mustNotFire(t, evalCheck(t, "consumer.idle_no_backlog", justUnder),
		"idle boundary leaks below the threshold")

	// Backlog present: only consumer.idle speaks.
	mustNotFire(t, evalCheck(t, "consumer.idle_no_backlog", pendingSnap(silence, 2)),
		"consumer.idle_no_backlog")
}

func TestConsumerPausedHourFloor(t *testing.T) {
	pausedAt := func(age time.Duration) *Snapshot {
		return &Snapshot{
			Now: consumerNow,
			Pending: map[string]PendingFacts{
				"orders\x00invoices": {PausedAtMS: consumerNow.Add(-age).UnixMilli()},
			},
			Consumers: []ConsumerState{{Stream: "orders", Name: "invoices", Paused: true}},
		}
	}
	w := mustFire(t, evalCheck(t, "consumer.paused", pausedAt(2*time.Hour)),
		"consumer.paused", SevWarn)
	if !strings.Contains(w.Fix[0], "messq consumer resume") {
		t.Fatalf("paused fix should be one resume away: %v", w.Fix)
	}
	mustNotFire(t, evalCheck(t, "consumer.paused", pausedAt(time.Minute)),
		"consumer.paused")

	// Age unknown still names it as ongoing rather than silently passing.
	unknown := &Snapshot{
		Now:       consumerNow,
		Pending:   map[string]PendingFacts{},
		Consumers: []ConsumerState{{Stream: "orders", Name: "invoices", Paused: true}},
	}
	mustFire(t, evalCheck(t, "consumer.paused", unknown), "consumer.paused", SevWarn)
}

func TestConsumerOldestPendingBoundaries(t *testing.T) {
	old := func(age time.Duration) *Snapshot {
		return &Snapshot{
			Now: consumerNow,
			Pending: map[string]PendingFacts{"orders\x00invoices": {
				OldestReadyMS: consumerNow.Add(-age).UnixMilli(),
			}},
			Consumers: []ConsumerState{{Stream: "orders", Name: "invoices"}},
		}
	}
	failedAge := old(time.Hour + 5*time.Second)
	mustFire(t, evalCheck(t, "consumer.oldest_pending", failedAge),
		"consumer.oldest_pending", SevFail)

	warnOnly := old(15*time.Minute + 30*time.Second)
	if got := mustFire(t, evalCheck(t, "consumer.oldest_pending", warnOnly),
		"consumer.oldest_pending", SevWarn); got.Severity != SevWarn {
		t.Fatalf("expected warn between 15m and 1h")
	}
	mustNotFire(t, evalCheck(t, "consumer.oldest_pending",
		old(10*time.Minute)), "consumer.oldest_pending")
}

func TestConsumerFlowBlockedPinnedAtCap(t *testing.T) {
	blocked := &Snapshot{
		Pending:   map[string]PendingFacts{"orders\x00invoices": {InflightCount: 100}},
		Consumers: []ConsumerState{{Stream: "orders", Name: "invoices", MaxAckPending: 100}},
	}
	i := mustFire(t, evalCheck(t, "consumer.flow_blocked", blocked),
		"consumer.flow_blocked", SevInfo)
	if i.Evidence["max_ack_pending"] != int64(100) {
		t.Fatalf("evidence should carry the cap: %+v", i.Evidence)
	}
	free := &Snapshot{
		Pending:   map[string]PendingFacts{"orders\x00invoices": {InflightCount: 99}},
		Consumers: blocked.Consumers,
	}
	mustNotFire(t, evalCheck(t, "consumer.flow_blocked", free), "consumer.flow_blocked")
}

func TestConsumerStaleAcksNeedsMetricsThenWarns(t *testing.T) {
	res := evalCheck(t, "consumer.stale_acks", &Snapshot{})
	if len(res) != 1 || res[0].Severity != SevSkipped {
		t.Fatalf("live-only check should skip offline: %+v", res)
	}
	live := func(total int64) *Snapshot {
		return &Snapshot{
			Source:    SourceLive,
			Consumers: []ConsumerState{{Stream: "orders", Name: "invoices"}},
			Metrics: &MetricFacts{
				StaleAcksTotal:       total,
				StaleAckTopConsumers: map[string]int64{"orders/invoices": total},
			},
		}
	}
	mustNotFire(t, evalCheck(t, "consumer.stale_acks", live(0)), "consumer.stale_acks")
	f := mustFire(t, evalCheck(t, "consumer.stale_acks", live(12)),
		"consumer.stale_acks", SevWarn)
	if f.Subject.Stream != "orders" || f.Subject.Consumer != "invoices" {
		t.Fatalf("subject should name the worst offender: %+v", f.Subject)
	}
}

func TestConsumerTTDvsEventsRetentionBoundary(t *testing.T) {
	ladder := []int64{1_000, 5_000, 30_000} // last step × max_deliver drives TTD
	mk := func(maxDeliver int32) *Snapshot {
		return &Snapshot{
			Consumers: []ConsumerState{{
				Stream: "orders", Name: "invoices",
				BackoffMS: ladder, MaxDeliver: maxDeliver,
			}},
		}
	}
	under := mk(3)
	under.Events = EventStats{RetentionMS: approxTimeToDead(under.Consumers[0]).Milliseconds()}
	mustNotFire(t, evalCheck(t, "consumer.time_to_dead_exceeds_events", under),
		"consumer.time_to_dead_exceeds_events")

	over := mk(50)
	retention := approxTimeToDead(over.Consumers[0]).Milliseconds()
	over.Events = EventStats{RetentionMS: retention / ttdRetentionRatio} // way below TTD
	f := mustFire(t, evalCheck(t, "consumer.time_to_dead_exceeds_events", over),
		"consumer.time_to_dead_exceeds_events", SevWarn)
	if len(f.Fix) != 2 {
		t.Fatalf("both retention and ladder levers belong in the fix set: %v", f.Fix)
	}
}

func TestConsumerFilterMatchesNothingUsesSamples(t *testing.T) {
	snap := func(filters ...string) *Snapshot {
		return &Snapshot{
			Streams: []StreamState{{
				Name: "orders", Msgs: 10,
				SampleSubjects: []string{
					"orders.created", "orders.updated",
					"orders.created", "orders.cancelled",
				},
			}},
			Consumers: []ConsumerState{{Stream: "orders", Name: "tap", Filters: filters}},
		}
	}
	f := mustFire(t, evalCheck(t, "consumer.filter_matches_nothing", snap("billing.*")),
		"consumer.filter_matches_nothing", SevWarn)
	if !strings.Contains(f.Detail, "orders.created") {
		t.Fatalf("evidence should show common subjects: %q", f.Detail)
	}
	mustNotFire(t, evalCheck(t, "consumer.filter_matches_nothing",
		snap(">")), "consumer.filter_matches_nothing") // fan-out matches everything
	mustNotFire(t, evalCheck(t, "consumer.filter_matches_nothing",
		snap("orders.*")), "consumer.filter_matches_nothing")
}

func TestSubjectMatchesAnyGrammarCut(t *testing.T) {
	cases := []struct {
		filter, subject string
		want            bool
	}{
		{"orders.>", "orders.created.europe", true},
		{"orders.*", "orders.created", true},
		{"orders.*", "orders.created.x", false}, // single-star matches exactly one token
		{"orders.created", "orders.created", true},
		{"billing.>", "orders.created", false},
	}
	for _, tc := range cases {
		if got := subjectMatchesAny([]string{tc.filter}, tc.subject); got != tc.want {
			t.Fatalf("match(%q,%q)=%v want %v", tc.filter, tc.subject, got, tc.want)
		}
	}
}
