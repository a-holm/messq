// SPDX-License-Identifier: Apache-2.0

package metrics

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/a-holm/messq/internal/clock"
	"github.com/prometheus/client_golang/prometheus/testutil"
)

// fakeReader scripts the StatsReader seam so the collector's arithmetic is tested
// against known numbers without SQLite (the real store-side implementation is the
// store slice's work).
type fakeReader struct {
	cheap      CheapStats
	heavy      HeavyStats
	candidates []Candidate
	headSeen   map[string][2]int64 // stream → (fromSeq, limit)

	heavyCountLimit        int64
	cheapCalls, heavyCalls int
	cheapErr, heavyErr     error
}

func (f *fakeReader) CheapStats(context.Context) (CheapStats, error) {
	f.cheapCalls++
	if f.cheapErr != nil {
		return CheapStats{}, f.cheapErr
	}
	return f.cheap, nil
}

func (f *fakeReader) HeavyStats(_ context.Context, countLimit int64) (HeavyStats, error) {
	f.heavyCalls++
	if f.heavyCountLimit == 0 {
		f.heavyCountLimit = countLimit
	}
	if f.heavyErr != nil {
		return HeavyStats{}, f.heavyErr
	}
	return f.heavy, nil
}

func (f *fakeReader) HeadCandidates(_ context.Context, stream string, fromSeq int64, limit int) ([]Candidate, error) {
	if f.headSeen == nil {
		f.headSeen = map[string][2]int64{}
	}
	f.headSeen[stream] = [2]int64{fromSeq, int64(limit)}
	return f.candidates, nil
}

// newCollectorTest wires Metrics + fakeReader over a fake clock starting at
// 2026-01-01T00:00:00Z and returns both plus the clock for advancing TTLs.
func newCollectorTest(t *testing.T, mutate func(*Options), f *fakeReader) (*Metrics, *clock.Fake) {
	t.Helper()
	clk := clock.NewFake(time.Unix(1767225600, 0))
	m := newTestMetrics(t, func(o *Options) {
		o.Clock = clk
		o.Stats = f
		o.DataDir = "/var/lib/messq"
		if mutate != nil {
			mutate(o)
		}
	})
	return m, clk
}

// TestCollectorS53Definitions pins the four gauge definitions of SEMANTICS S5.3 on
// a materialised consumer: pending/inflight from the delivery rows, backlog =
// pending + max(0, last-cursor+1), oldest age from the oldest delivery row.
func TestCollectorS53Definitions(t *testing.T) {
	now := time.Unix(1767225600, 0)
	f := &fakeReader{
		cheap: CheapStats{
			Consumers: []ConsumerStat{{
				Stream: "orders", Consumer: "worker",
				Pending: 2, Inflight: 1,
				OldestPublishedAtMS: now.Add(-4 * time.Second).UnixMilli(),
				CursorSeq:           10, LastSeq: 15, // unscanned = 15-10+1 = 6
				Filters: []string{">"},
			}},
			SweepBacklog: 3,
			DBBytes:      1024, WALBytes: 256,
		},
		heavy: HeavyStats{
			DLQ:        []DLQDepth{{Origin: "orders", Depth: 3}},
			EventsRows: 42,
		},
	}
	m, _ := newCollectorTest(t, nil, f)

	want := `
# HELP messq_backlog Total work owed: pending + unscanned messages at or above the cursor.
# TYPE messq_backlog gauge
messq_backlog{consumer="worker",stream="orders"} 8
# HELP messq_consumer_paused 1 when the consumer is deliberately paused; alerts unless it instead of paging.
# TYPE messq_consumer_paused gauge
messq_consumer_paused{consumer="worker",stream="orders"} 0
# HELP messq_db_bytes Size of the SQLite database file.
# TYPE messq_db_bytes gauge
messq_db_bytes 1024
# HELP messq_dlq_depth Messages in <stream>.dlq, labelled with the ORIGIN stream.
# TYPE messq_dlq_depth gauge
messq_dlq_depth{stream="orders"} 3
# HELP messq_events_rows Rows in the events table; MAX(id)-MIN(id)+1 stays exact because trimming removes a prefix.
# TYPE messq_events_rows gauge
messq_events_rows 42
# HELP messq_inflight Delivery rows currently leased to a worker.
# TYPE messq_inflight gauge
messq_inflight{consumer="worker",stream="orders"} 1
# HELP messq_oldest_pending_age_seconds Age of the oldest unfinished message; THE user-facing SLI; alert on it.
# TYPE messq_oldest_pending_age_seconds gauge
messq_oldest_pending_age_seconds{consumer="worker",stream="orders"} 4
# HELP messq_pending Unfinished materialised work (READY + INFLIGHT delivery rows).
# TYPE messq_pending gauge
messq_pending{consumer="worker",stream="orders"} 2
# HELP messq_sweep_backlog Expired-but-unswept rows: late does not mean lost, and this says which.
# TYPE messq_sweep_backlog gauge
messq_sweep_backlog 3
# HELP messq_wal_bytes Size of the SQLite write-ahead log.
# TYPE messq_wal_bytes gauge
messq_wal_bytes 256
`
	if err := testutil.GatherAndCompare(m.reg, strings.NewReader(want),
		nameBacklog, nameConsumerPaused, nameDBBytes, nameDLQDepth, nameEventsRows,
		nameInflight, nameOldestPendingAge, namePending, metricSweepBacklog,
		nameWALBytes,
	); err != nil {
		t.Fatal(err)
	}
}

// TestNeverFetchedConsumerReadsNonZero is THE stopped-worker case (G5): zero
// delivery rows must not zero the SLI while a million messages sit unscanned.
func TestNeverFetchedConsumerReadsNonZero(t *testing.T) {
	now := time.Unix(1767225600, 0)
	f := &fakeReader{
		cheap: CheapStats{
			Consumers: []ConsumerStat{{
				Stream: "orders", Consumer: "stalled",
				Pending: 0, Inflight: 0,
				CursorSeq: 5, LastSeq: 1004, // 1000 messages never admitted
				Filters: []string{">"},
			}},
		},
		candidates: []Candidate{
			{
				Seq: 5, PublishedAtMS: now.Add(-time.Minute).UnixMilli(),
				Subject: "orders.created",
			},
		},
		headSeen: map[string][2]int64{},
	}
	m, _ := newCollectorTest(t, func(o *Options) { o.FilterScan = 10 }, f)

	want := `
# HELP messq_backlog Total work owed: pending + unscanned messages at or above the cursor.
# TYPE messq_backlog gauge
messq_backlog{consumer="stalled",stream="orders"} 1000
# HELP messq_oldest_pending_age_seconds Age of the oldest unfinished message; THE user-facing SLI; alert on it.
# TYPE messq_oldest_pending_age_seconds gauge
messq_oldest_pending_age_seconds{consumer="stalled",stream="orders"} 60
# HELP messq_pending Unfinished materialised work (READY + INFLIGHT delivery rows).
# TYPE messq_pending gauge
messq_pending{consumer="stalled",stream="orders"} 0
`
	if err := testutil.GatherAndCompare(m.reg, strings.NewReader(want),
		nameBacklog, nameOldestPendingAge, namePending); err != nil {
		t.Fatal(err)
	}
	if _, seen := f.headSeen["orders"]; !seen {
		t.Error("head-candidate scan never ran")
	} else if got := f.headSeen["orders"]; got[0] != 5 || got[1] != 10 {
		t.Errorf("head scan ran with (from=%d, limit=%d), want (5, 10)", got[0], got[1])
	}
}

// TestHeadScanBudgetFallsBack covers the narrow-filter case: no match inside
// --metrics-filter-scan means the delivery-row age stands in and
// messq_metrics_truncated_total records the under-estimate.
func TestHeadScanBudgetFallsBack(t *testing.T) {
	now := time.Unix(1767225600, 0)
	f := &fakeReader{
		cheap: CheapStats{
			Consumers: []ConsumerStat{{
				Stream: "orders", Consumer: "narrow",
				Pending: 1, OldestPublishedAtMS: now.Add(-2 * time.Second).UnixMilli(),
				CursorSeq: 1, LastSeq: 50,
				Filters: []string{"a.b.c"}, // nothing matches inside the budget
			}},
		},
		candidates: []Candidate{
			{Seq: 1, PublishedAtMS: now.Add(-30 * time.Second).UnixMilli(), Subject: "x.y.z"},
			{Seq: 2, PublishedAtMS: now.Add(-29 * time.Second).UnixMilli(), Subject: "x.y.z"},
		},
		headSeen: map[string][2]int64{},
	}
	m, _ := newCollectorTest(t, func(o *Options) { o.FilterScan = 2 }, f)

	fams := gather(t, m)
	age := fams[nameOldestPendingAge]
	if age.GetMetric()[0].GetGauge().GetValue() != 2 {
		t.Errorf("oldest age = %v, want the 2s delivery-row fallback",
			age.GetMetric()[0].GetGauge().GetValue())
	}
	trunc := fams[nameMetricsTruncatedTotl]
	if trunc == nil || trunc.GetMetric()[0].GetCounter().GetValue() != 1 {
		t.Errorf("truncated_total missing or != 1: %v", trunc)
	}
}

// TestScrapeFailureServesStaleSnapshot is Decision 3: a failed refresh keeps the
// previous numbers, counts the failure per tier, lets snapshot age climb — and the
// handler still answers 200 (never a 500, never a hang).
func TestScrapeFailureServesStaleSnapshot(t *testing.T) {
	now := time.Unix(1767225600, 0)
	f := &fakeReader{
		cheap: CheapStats{Consumers: []ConsumerStat{
			{
				Stream: "s", Consumer: "c", Pending: 7, Filters: []string{">"},
				OldestPublishedAtMS: now.Add(-time.Second).UnixMilli(),
			},
		}},
		headSeen: map[string][2]int64{},
	}
	m, clk := newCollectorTest(t, nil, f)

	gather(t, m) // warm the caches

	f.cheapErr = context.DeadlineExceeded
	clk.Advance(2 * m.o.Cache) // force a refresh that will fail

	fams := gather(t, m)
	pend := fams[namePending]
	if pend == nil || len(pend.GetMetric()) != 1 || pend.GetMetric()[0].GetGauge().GetValue() != 7 {
		t.Fatalf("stale pending series lost after refresh failure: %v", pend)
	}
	errs := fams[nameScrapeErrorsTotal]
	found := false
	for _, mtrc := range errs.GetMetric() {
		labels := map[string]string{}
		for _, l := range mtrc.GetLabel() {
			labels[l.GetName()] = l.GetValue()
		}
		if labels["tier"] == "cheap" && mtrc.GetCounter().GetValue() == 1 {
			found = true
		}
	}
	if !found {
		t.Errorf("scrape_errors_total{tier=cheap} did not count exactly one failure: %v", errs)
	}
	age := fams[nameSnapshotAgeSeconds]
	stale := false
	for _, mtrc := range age.GetMetric() {
		for _, l := range mtrc.GetLabel() {
			if l.GetValue() == "cheap" && mtrc.GetGauge().GetValue() >= 2*m.o.Cache.Seconds()-1 {
				stale = true
			}
		}
	}
	if !stale {
		t.Errorf("snapshot_age_seconds{tier=cheap} did not climb: %v", age)
	}

	// A concurrent heavy-tier failure is counted separately.
	f.heavyErr = errors.New("sqlite busy")
	f.cheapErr = nil
	clk.Advance(m.o.CacheHeavy)
	fams = gather(t, m)
	for _, mtrc := range fams[nameScrapeErrorsTotal].GetMetric() {
		for _, l := range mtrc.GetLabel() {
			if l.GetValue() == "heavy" && mtrc.GetCounter().GetValue() != 1 {
				t.Errorf("scrape_errors_total{tier=heavy} = %v, want 1", mtrc.GetCounter())
			}
		}
	}
}

// TestCacheSingleFlightPerTTL proves the two tiers are read at most once per TTL
// however many scrapes arrive: five gathers inside the TTL cost exactly one
// CheapStats call after the warm-up.
func TestCacheSingleFlightPerTTL(t *testing.T) {
	f := &fakeReader{cheap: CheapStats{}, heavy: HeavyStats{}, headSeen: map[string][2]int64{}}
	m, _ := newCollectorTest(t, nil, f)

	for i := 0; i < 5; i++ {
		gather(t, m)
	}
	if f.cheapCalls != 1 || f.heavyCalls != 1 {
		t.Fatalf("cheap=%d heavy=%d calls for 5 scrapes inside one TTL, want 1/1",
			f.cheapCalls, f.heavyCalls)
	}
}

// TestBacklogClampsNegativeRange keeps a gauge non-negative when a seek leaves the
// cursor ahead of the head (clock/format edges must not invent negative work).
func TestBacklogClampsNegativeRange(t *testing.T) {
	f := &fakeReader{
		cheap: CheapStats{Consumers: []ConsumerStat{
			{Stream: "s", Consumer: "c", Pending: 4, CursorSeq: 100, LastSeq: 90, Filters: []string{">"}},
		}},
		headSeen: map[string][2]int64{},
	}
	m, _ := newCollectorTest(t, nil, f)

	fams := gather(t, m)
	bl := fams[nameBacklog]
	if bl.GetMetric()[0].GetGauge().GetValue() != 4 {
		t.Errorf("backlog = %v, want pending alone (4)", bl.GetMetric()[0].GetGauge().GetValue())
	}
}

// TestWaitersGaugeWiredWhenInjected checks the api seam: injected → exported;
// absent → family entirely absent (not zero).
func TestWaitersGaugeWiredWhenInjected(t *testing.T) {
	f := &fakeReader{headSeen: map[string][2]int64{}}
	m, _ := newCollectorTest(t, func(o *Options) { o.Waiters = func() int64 { return 9 } }, f)

	fams := gather(t, m)
	w := fams[metricWaitersGauge]
	if w == nil || w.GetMetric()[0].GetGauge().GetValue() != 9 {
		t.Fatalf("waiters = %v, want 9", w)
	}

	bare, _ := newTestMetrics(t, func(o *Options) { o.Stats = f }), f
	if fams := gather(t, bare); fams[metricWaitersGauge] != nil {
		t.Error("waiters exported without an injected seam; it would lie as 0")
	}
}
