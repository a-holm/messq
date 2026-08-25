// SPDX-License-Identifier: Apache-2.0

package metrics

import (
	"context"
	"fmt"
	"sync"
	"syscall"
	"time"

	"github.com/a-holm/messq/internal/subject"
	"github.com/prometheus/client_golang/prometheus"
)

// StatsReader is the narrow read seam behind the scrape-time gauges (issue #21 §1).
// internal/store implements it on the read-only pool — the collector NEVER imports
// the store concretely, so its arithmetic is testable against a fake and layers.sh
// keeps net/http out of the storage layer's importers.
type StatsReader interface {
	// CheapStats serves the 5s tier: one pass over deliveries/consumers/stream_seq
	// plus the file sizes (os.Stat, not SQL).
	CheapStats(ctx context.Context) (CheapStats, error)
	// HeavyStats serves the 60s tier: DLQ depths and the events-table row count.
	// countLimit is --metrics-count-limit; above it the reader may estimate and say
	// so via Depth.Estimated.
	HeavyStats(ctx context.Context, countLimit int64) (HeavyStats, error)
	// HeadCandidates scans forward from fromSeq for at most limit message rows
	// ascending — the bounded lookup behind the never-fetched consumer's age.
	HeadCandidates(ctx context.Context, stream string, fromSeq int64, limit int) ([]Candidate, error)
}

// ConsumerStat is one (stream, consumer) pair's cheap-tier state.
type ConsumerStat struct {
	Stream, Consumer    string
	Pending, Inflight   int64
	OldestPublishedAtMS int64 // min(published_at) over unfinished delivery rows; 0 when none
	CursorSeq           int64 // last contiguously consumed seq (§9 §8 semantics)
	LastSeq             int64 // stream_seq.next - 1; 0 when the stream is empty
	Paused              bool
	Filters             []string
}

// CheapStats is one 5s-tier snapshot.
type CheapStats struct {
	Consumers         []ConsumerStat
	SweepBacklog      int64  // expired-but-unswept INFLIGHT rows
	DBBytes, WALBytes uint64 // os.Stat sizes, not SQL
}

// Candidate is one head-scan row.
type Candidate struct {
	Seq           int64
	PublishedAtMS int64
	Subject       string
}

// DLQDepth is one origin stream's dead-letter depth (#12: labelled by ORIGIN).
type DLQDepth struct {
	Origin string
	Depth  int64
}

// HeavyStats is one 60s-tier snapshot.
type HeavyStats struct {
	DLQ        []DLQDepth
	EventsRows int64
}

// The scrape-time Collector (Decision 2): gauges are COMPUTED from the read seam at
// scrape time and never mirrored, so a purge, a seek or a restart cannot leave them
// lying (PLAN §9.4 verbatim). Decision 3 governs failure handling: a scrape never
// fails because a query did — the previous snapshot keeps serving,
// messq_metrics_scrape_errors_total counts per tier and snapshot age climbs.
//
// Two cache tiers bound the cost: 5 s for the cheap pass (--metrics-cache) and 60 s
// for the heavy COUNT queries (--metrics-cache-heavy). A mutex serialises refreshes,
// which IS single-flight: however many scrapes arrive inside one TTL, SQLite is hit
// at most once per tier.

// snapshot is one tier's cached values.
type snapshot struct {
	takenAt time.Time
	valid   bool

	cheap CheapStats
	heavy HeavyStats
	disk  uint64 // free bytes on DataDir; 0 when statfs never succeeded
}

// Cache tiers, also the messq_metrics_*{tier} label values.
const (
	tierCheap = "cheap"
	tierHeavy = "heavy"
)

// histAccum accumulates one tier's collect-duration histogram between scrapes;
// it is exported as a const histogram, not a shared vec, so every number a scrape
// serves belongs to THAT scrape (a vec mutated inside Collect lands one scrape late).
type histAccum struct {
	count   uint64
	sum     float64
	bounds  []float64
	buckets map[float64]uint64 // cumulative, keyed by upper bound
}

func (h *histAccum) observe(sec float64) {
	h.count++
	h.sum += sec
	for _, b := range h.bounds {
		if sec <= b {
			h.buckets[b]++
		}
	}
}

// collector implements prometheus.Collector over the StatsReader seam.
type collector struct {
	m *Metrics

	descs map[string]*prometheus.Desc

	mu        sync.Mutex // guards everything below AND the refreshes (single-flight)
	cheapSnap snapshot
	heavySnap snapshot
	errCount  map[string]uint64
	duration  map[string]*histAccum
}

// newCollector builds the descs for every desc-fed catalogue row and registers the
// collector on m's registry.
func newCollector(m *Metrics) {
	c := &collector{
		m:        m,
		descs:    make(map[string]*prometheus.Desc, len(catalogue)),
		errCount: make(map[string]uint64),
		duration: make(map[string]*histAccum),
	}
	var collectBuckets []float64
	for _, spec := range catalogue {
		if !descFed(spec.Name) || spec.Name == nameBuildInfo {
			continue
		}
		c.descs[spec.Name] = prometheus.NewDesc(spec.Name, spec.Help, spec.Labels, nil)
		if spec.Name == nameCollectDurationSecs {
			collectBuckets = spec.Buckets
		}
	}
	for _, tier := range []string{tierCheap, tierHeavy} {
		c.duration[tier] = &histAccum{bounds: collectBuckets, buckets: map[float64]uint64{}}
	}
	m.reg.MustRegister(c)
}

// Describe sends every desc so /metrics carries HELP+TYPE before the first series.
func (c *collector) Describe(ch chan<- *prometheus.Desc) {
	for _, d := range c.descs {
		ch <- d
	}
}

// Collect serves both tiers from cache-or-refresh and emits const metrics.
func (c *collector) Collect(ch chan<- prometheus.Metric) {
	start := c.m.o.Clock.Now()
	ctx, cancel := context.WithTimeout(context.Background(), c.m.o.QueryTimeout)
	defer cancel()

	cheapSnap := snapshot{}
	heavySnap := snapshot{}
	if c.m.o.Stats == nil {
		// No read seam wired yet (startup transient): there is nothing truthful to
		// compute, so the collector says nothing rather than inventing zeroes.
		return
	}
	cheapSnap = c.tier(ctx, &c.cheapSnap, c.m.o.Cache, refreshCheap, tierCheap)
	heavySnap = c.tier(ctx, &c.heavySnap, c.m.o.CacheHeavy, refreshHeavy, tierHeavy)
	cheap := cheapSnap.cheap
	heavy := heavySnap.heavy

	const gaugeType = prometheus.GaugeValue

	emit := func(name string, value float64, lvs ...string) {
		sendConst(ch, c.descs[name], gaugeType, value, name, lvs...)
	}

	if c.m.o.Waiters != nil {
		emit(metricWaitersGauge, float64(c.m.o.Waiters()))
	}

	// Per-pair gauges with the S5.3 definitions.
	for i := range cheap.Consumers {
		cs := &cheap.Consumers[i]
		unscanned := cs.LastSeq - cs.CursorSeq + 1
		if unscanned < 0 {
			unscanned = 0
		}
		pair := []string{cs.Stream, cs.Consumer}
		emit(namePending, float64(cs.Pending), pair...)
		emit(nameInflight, float64(cs.Inflight), pair...)
		emit(nameBacklog, float64(cs.Pending)+float64(unscanned), pair...)
		emit(nameOldestPendingAge, c.oldestAgeSec(ctx, cs, start), pair...)
		if cs.Paused {
			emit(nameConsumerPaused, 1, pair...)
		} else {
			emit(nameConsumerPaused, 0, pair...)
		}
	}

	emit(metricSweepBacklog, float64(cheap.SweepBacklog))
	emit(nameDBBytes, float64(cheap.DBBytes))
	emit(nameWALBytes, float64(cheap.WALBytes))
	if cheapSnap.disk > 0 || cheapSnap.valid {
		emit(nameDiskFreeBytes, float64(cheapSnap.disk))
	}
	for _, d := range heavy.DLQ {
		emit(nameDLQDepth, float64(d.Depth), d.Origin)
	}
	emit(nameEventsRows, float64(heavy.EventsRows))

	// Self-observability, emitted as part of THIS scrape: how old were the numbers
	// just served, what did refreshes fail, what did the scrape cost?
	now := c.m.o.Clock.Now()
	for _, tc := range []struct {
		tier string
		snap snapshot
	}{
		{tierCheap, cheapSnap},
		{tierHeavy, heavySnap},
	} {
		emit(nameSnapshotAgeSeconds, now.Sub(tc.snap.takenAt).Seconds(), tc.tier)
		if n := c.errCount[tc.tier]; n > 0 {
			sendCounter(ch, c.descs[nameScrapeErrorsTotal], float64(n), nameScrapeErrorsTotal, tc.tier)
		}
	}
	c.duration[tierCheap].observe(now.Sub(start).Seconds())
	d := c.duration[tierCheap]
	sendHistogram(ch, c.descs[nameCollectDurationSecs], d.count, d.sum, d.buckets,
		nameCollectDurationSecs, tierCheap)
}

// tier returns a fresh-enough snapshot, refreshing at most once per TTL.
func (c *collector) tier(ctx context.Context, s *snapshot, ttl time.Duration,
	refresh func(context.Context, *Metrics) (snapshot, error), tier string,
) snapshot {
	c.mu.Lock()
	defer c.mu.Unlock()

	now := c.m.o.Clock.Now()
	if s.valid && now.Sub(s.takenAt) < ttl {
		return *s
	}
	fresh, err := refresh(ctx, &Metrics{o: c.m.o}) // reader calls take o.Stats/o.Clock only
	if err != nil {
		// Decision 3: serve the previous snapshot; count and warn (≤1/min).
		c.errCount[tier]++
		c.m.logLimited("metrics: "+tier+"-tier refresh failed; serving previous snapshot",
			"err", err.Error())
		return *s
	}
	fresh.takenAt = now
	fresh.valid = true
	*s = fresh
	return *s
}

// refreshCheap runs the 5s tier: reader pass + one statfs.
func refreshCheap(ctx context.Context, m *Metrics) (snapshot, error) {
	var snap snapshot
	cheap, err := m.o.Stats.CheapStats(ctx)
	if err != nil {
		return snap, fmt.Errorf("cheap stats: %w", err)
	}
	snap.cheap = cheap
	if m.o.DataDir != "" {
		if free, statErr := defaultStatfs(m.o.DataDir); statErr == nil {
			snap.disk = free
		} // a failed statfs keeps yesterday's number rather than zeroing it
	}
	return snap, nil
}

// refreshHeavy runs the 60s tier.
func refreshHeavy(ctx context.Context, m *Metrics) (snapshot, error) {
	heavy, err := m.o.Stats.HeavyStats(ctx, m.o.CountLimit)
	if err != nil {
		return snapshot{}, fmt.Errorf("heavy stats: %w", err)
	}
	return snapshot{heavy: heavy}, nil
}

// oldestAgeSec computes the S5.3 SLI: now - min(published_at) over {oldest delivery
// row} ∪ {first filter-matching message at seq ≥ cursor}, clamped at ≥ 0. The head
// scan makes a never-fetched consumer read non-zero — exactly during the stopped-
// worker outage the metric exists to detect.
func (c *collector) oldestAgeSec(ctx context.Context, cs *ConsumerStat, now time.Time) float64 {
	bestMS := int64(0)
	if cs.OldestPublishedAtMS > 0 {
		bestMS = cs.OldestPublishedAtMS
	}

	if cs.CursorSeq <= cs.LastSeq && len(cs.Filters) > 0 {
		set, err := subject.ParseSet(cs.Filters)
		if err == nil {
			cands, scanErr := c.m.o.Stats.HeadCandidates(ctx, cs.Stream, cs.CursorSeq, c.m.o.FilterScan)
			switch {
			case scanErr != nil:
				c.m.truncated.Inc() // cannot see unscanned work; say the estimate shrank
			default:
				matched := false
				scannedAny := false
				for _, cand := range cands {
					scannedAny = true
					if set.Match(cand.Subject) {
						if bestMS == 0 || cand.PublishedAtMS < bestMS {
							bestMS = cand.PublishedAtMS
						}
						matched = true
						break // ascending scan: first match is the oldest candidate
					}
				}
				if !matched && scannedAny {
					c.m.truncated.Inc()
				}
			}
		} else {
			c.m.logLimited("metrics: consumer filters do not parse; age falls back to delivery rows",
				"stream", cs.Stream, "consumer", cs.Consumer)
		}
	}

	if bestMS <= 0 {
		return 0
	}
	age := now.Sub(time.UnixMilli(bestMS)).Seconds()
	if age < 0 {
		return 0 // clock jumps backwards: a gauge is never negative
	}
	return age
}

// sendConst emits one const metric, handling NewConstMetric's error honestly: it
// fires on label-count bugs, which are programming errors — logged loudly, never a
// panic inside someone else's scrape goroutine.
func sendConst(ch chan<- prometheus.Metric, desc *prometheus.Desc, vt prometheus.ValueType,
	value float64, name string, lvs ...string,
) {
	if desc == nil {
		panic("metrics: missing desc for " + name)
	}
	metric, err := prometheus.NewConstMetric(desc, vt, value, lvs...)
	if err != nil {
		panic("metrics: " + name + ": " + err.Error()) // label-count bug: fail loud in dev
	}
	ch <- metric
}

// sendCounter emits an unlabelled-per-instance counter const metric.
func sendCounter(ch chan<- prometheus.Metric, desc *prometheus.Desc, value float64,
	name string, lvs ...string,
) {
	metric, err := prometheus.NewConstMetric(desc, prometheus.CounterValue, value, lvs...)
	if err != nil {
		panic("metrics: " + name + ": " + err.Error())
	}
	ch <- metric
}

// sendHistogram emits a const histogram from accumulated state.
func sendHistogram(ch chan<- prometheus.Metric, desc *prometheus.Desc, count uint64, sum float64,
	buckets map[float64]uint64, name string, lvs ...string,
) {
	metric, err := prometheus.NewConstHistogram(desc, count, sum, buckets, lvs...)
	if err != nil {
		panic("metrics: " + name + ": " + err.Error())
	}
	ch <- metric
}

// defaultStatfs reads free bytes for the data directory (replaced by #27's monitor).
var defaultStatfs = func(dir string) (uint64, error) {
	var st syscall.Statfs_t
	if err := syscall.Statfs(dir, &st); err != nil {
		return 0, err
	}
	return st.Bavail * uint64(st.Bsize), nil
}
