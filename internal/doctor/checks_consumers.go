// SPDX-License-Identifier: Apache-2.0

package doctor

import (
	"context"
	"fmt"
	"strings"
	"time"
)

func init() {
	defaultRegistry.Register(Check{
		ID:      "consumer.stale_acks",
		Summary: "acks are arriving after ack_wait (any nonzero rate alerts)",
		Explain: "A stale ack means the worker held a message past its lease and the " +
			"delivery already redelivered to someone else — every occurrence is a " +
			"duplicate somebody absorbs. §9.4 says alert on any nonzero rate; doctor " +
			"agrees.",
		Needs: SourceLive,
		Eval:  evalConsumerStaleAcks,
	})
	defaultRegistry.Register(Check{
		ID:      "consumer.time_to_dead_exceeds_events",
		Summary: "TimeToDead outruns event retention, so failures vanish before they can be diagnosed",
		Explain: "When the full backoff ladder outlives --event-retention, the events " +
			"that would explain a dead message are already trimmed when it dies (#11, " +
			"#19). Raise retention above roughly ten TimeToDead or shorten the ladder.",
		Needs: SourceEither,
		Eval:  evalConsumerTTDvsEvents,
	})
	defaultRegistry.Register(Check{
		ID:      "consumer.idle",
		Summary: "a consumer with backlog stopped being delivered anything",
		Explain: "Backlog with no deliveries is a stuck worker or an exhausted retry " +
			"budget — either way messages are late RIGHT NOW. Is the worker running? " +
			"messq lag and messq pending name the concrete rows.",
		Needs: SourceEither,
		Eval:  evalConsumerIdle(true),
	})
	defaultRegistry.Register(Check{
		ID:      "consumer.idle_no_backlog",
		Summary: "nothing delivered for a long while, but there is no backlog either",
		Explain: "The broker is healthy; the WORKER may be gone. If nobody re-created " +
			"it that is usually the point at which you decide it was intentional and " +
			"remove the consumer so lag views stop mourning it.",
		Needs: SourceEither,
		Eval:  evalConsumerIdle(false),
	})
	defaultRegistry.Register(Check{
		ID:      "consumer.paused",
		Summary: "this consumer has been paused for over an hour",
		Explain: "Pausing is a human decision; a pause older than an hour is more often " +
			"a forgotten experiment than a deliberate state. messq consumer resume " +
			"undoes it in one line.",
		Needs: SourceEither,
		Eval:  evalConsumerPaused,
	})
	defaultRegistry.Register(Check{
		ID:      "consumer.oldest_pending",
		Summary: "the oldest pending delivery is aging past useful",
		Explain: "The §9.4 SLI: oldest_pending_age_seconds. At fifteen minutes a worker " +
			"is visibly behind; past an hour every SLA conversation needs this number " +
			"and an excuse.",
		Needs: SourceEither,
		Eval:  evalConsumerOldestPending,
	})
	defaultRegistry.Register(Check{
		ID:      "consumer.flow_blocked",
		Summary: "inflight pinned at max_ack_pending — workers are the bottleneck",
		Explain: "Flow control is doing exactly what max_ack_pending is for, which here " +
			"means: adding workers (or raising the cap) moves real throughput today.",
		Needs: SourceEither,
		Eval:  evalConsumerFlowBlocked,
	})
	defaultRegistry.Register(Check{
		ID:      "consumer.filter_matches_nothing",
		Summary: "consumer filters match none of the stream's recent subjects",
		Explain: "Publishes land on the stream but never on THIS consumer because its " +
			"filter set selects nothing real. Usually a typo against the stream's actual " +
			"subjects; the three most common ones ride along as evidence.",
		Needs: SourceEither,
		Eval:  evalConsumerFilterMatchesNothing,
	})
}

const (
	staleAckWarnFloor = int64(1) // §9.4: any nonzero rate
	ttdRetentionRatio = 10       // retention should be ≈10× TimeToDead
	idleDefaultAfter  = 24 * time.Hour
	pausedWarnFloor   = time.Hour
)

func evalConsumerStaleAcks(_ context.Context, snap *Snapshot) []Finding {
	if snap.Metrics == nil {
		return []Finding{skippedCheck("consumer.stale_acks",
			"needs a running daemon (try --addr)")}
	}
	if snap.Metrics.StaleAcksTotal < staleAckWarnFloor {
		return nil
	}
	culprits := snap.Metrics.StaleAckTopConsumers
	detail := fmt.Sprintf("%d stale acks recorded since start", snap.Metrics.StaleAcksTotal)
	if len(culprits) > 0 {
		names := make([]string, 0, len(culprits))
		for k := range culprits {
			names = append(names, k)
		}
		sortStrings(names)
		rendered := make([]string, 0, len(names))
		for _, n := range names {
			rendered = append(rendered, fmt.Sprintf("%s=%d", n, culprits[n]))
		}
		detail += "; worst offenders " + strings.Join(rendered[:min(3, len(rendered))], ", ")
	}
	f := Finding{
		ID: "consumer.stale_acks", Severity: SevWarn,
		Title:  "workers are acknowledging after their leases expired",
		Detail: renderSafe(detail),
		Fix: []string{
			"raise ack_wait above observed processing p99",
			"enable heartbeats instead of long fixed waits (#22, #25)",
		},
		Evidence: map[string]any{"stale_acks_total": snap.Metrics.StaleAcksTotal},
		Docs:     docsAnchor("consumer.stale_acks"),
	}
	if len(culprits) > 0 {
		for _, c := range snap.Consumers {
			key := c.Stream + "/" + c.Name
			if culprits[key] > 0 {
				f.Subject = Subject{Stream: c.Stream, Consumer: c.Name}
				break
			}
		}
	}
	return []Finding{f}
}

// approxTimeToDead walks the backoff ladder for max_deliver steps — the honest
// lower bound of how long a message can thrash before dying.
func approxTimeToDead(c ConsumerState) time.Duration {
	if len(c.BackoffMS) == 0 || c.MaxDeliver == 0 {
		return 0 // unlimited retries: death has no schedule at all
	}
	last := c.BackoffMS[len(c.BackoffMS)-1]
	totalMS := last * int64(c.MaxDeliver)
	for _, b := range c.BackoffMS {
		totalMS += b
	}
	return time.Duration(totalMS) * time.Millisecond
}

func evalConsumerTTDvsEvents(_ context.Context, snap *Snapshot) []Finding {
	retention := snap.Events.RetentionMS
	if retention <= 0 {
		return []Finding{skippedCheck("consumer.time_to_dead_exceeds_events",
			"event retention unknown; doctor cannot compare ladders to trims")}
	}
	var out []Finding
	for _, c := range snap.Consumers {
		ttd := approxTimeToDead(c)
		if ttd == 0 || retention <= 0 {
			continue
		}
		if ttd > time.Duration(retention)*time.Millisecond {
			out = append(out, Finding{
				ID: "consumer.time_to_dead_exceeds_events", Severity: SevWarn,
				Title: fmt.Sprintf("TimeToDead ≈ %s exceeds event retention %s (%s/%s)",
					ttd.Truncate(time.Second), time.Duration(retention)*time.Millisecond,
					renderSafe(c.Stream), renderSafe(c.Name)),
				Detail: "By the time a message dies, the events explaining its death " +
					"are gone; the diagnosis dies first.",
				Fix: []string{
					fmt.Sprintf("raise --event-retention above %s",
						(time.Duration(ttd) * ttdRetentionRatio).Truncate(time.Second)),
					fmt.Sprintf("messq consumer edit %s %s --backoff <shorter-ladder>",
						c.Stream, c.Name),
				},
				Subject: Subject{Stream: c.Stream, Consumer: c.Name},
				Evidence: map[string]any{
					"time_to_dead_ms": ttd.Milliseconds(), "retention_ms": retention,
				},
				Docs: docsAnchor("consumer.time_to_dead_exceeds_events"),
			})
		}
	}
	return out
}

// evalConsumerIdle parameterises both idle ids over one walker.
func evalConsumerIdle(requireBacklog bool) func(context.Context, *Snapshot) []Finding {
	id := "consumer.idle"
	if !requireBacklog {
		id = "consumer.idle_no_backlog"
	}
	return func(_ context.Context, snap *Snapshot) []Finding {
		if snap.Pending == nil {
			return []Finding{skippedCheck(id,
				"pending-set facts were not collected")}
		}
		var out []Finding
		for _, c := range snap.Consumers {
			pf, ok := snap.Pending[c.Stream+"\x00"+c.Name]
			if !ok || pf.LastDeliveredMS == 0 {
				continue // never delivered at all: create-time silence is different business
			}
			if c.Paused {
				continue // paused consumers have their own id
			}
			silence := snap.Now.Sub(unixMilliToTime(pf.LastDeliveredMS))
			idleAfter := snap.IdleAfter
			if idleAfter <= 0 {
				idleAfter = idleDefaultAfter
			}
			if silence < idleAfter {
				continue
			}
			hasBacklog := pf.PendingCount > 0
			if requireBacklog != hasBacklog {
				continue
			}
			if requireBacklog {
				out = append(out, Finding{
					ID: id, Severity: SevFail,
					Title: fmt.Sprintf("has %d waiting messages but nothing delivered for %s",
						pf.PendingCount, silence.Truncate(time.Second)),
					Detail: "Messages are late right now; deliveries stopped while work " +
						"remains. Stuck worker, exhausted ladder, or paused upstream.",
					Fix: []string{
						fmt.Sprintf("messq lag %s        # see where the backlog sits", c.Stream),
						fmt.Sprintf("messq pending %s %s --older-than 15m", c.Stream, c.Name),
					},
					Subject: Subject{Stream: c.Stream, Consumer: c.Name},
					Evidence: map[string]any{
						"silence_ms": silence.Milliseconds(), "pending": pf.PendingCount,
					},
					Docs: docsAnchor(id),
				})
				continue
			}
			out = append(out, Finding{
				ID: id, Severity: SevWarn,
				Title:  fmt.Sprintf("no delivery for %s and zero backlog", silence.Truncate(time.Second)),
				Detail: "Nothing is wrong with the broker; the worker may simply be gone.",
				Fix: []string{
					fmt.Sprintf("messq consumer rm %s %s --confirm %s   # if the worker is retired",
						c.Stream, c.Name, c.Name),
				},
				Subject:  Subject{Stream: c.Stream, Consumer: c.Name},
				Evidence: map[string]any{"silence_ms": silence.Milliseconds()},
				Docs:     docsAnchor(id),
			})
		}
		return out
	}
}

func evalConsumerPaused(_ context.Context, snap *Snapshot) []Finding {
	if snap.Pending == nil {
		return []Finding{skippedCheck("consumer.paused",
			"pause ages were not collected")}
	}
	var out []Finding
	for _, c := range snap.Consumers {
		if !c.Paused {
			continue
		}
		pf := snap.Pending[c.Stream+"\x00"+c.Name]
		if pf.PausedAtMS == 0 {
			// Pause age unknown: still worth naming loudly as ongoing.
			out = append(out, findingPaused(c, time.Duration(-1)))
			continue
		}
		age := snap.Now.Sub(unixMilliToTime(pf.PausedAtMS))
		if age >= pausedWarnFloor || age < 0 {
			out = append(out, findingPaused(c, age))
		}
	}
	return out
}

func findingPaused(c ConsumerState, age time.Duration) Finding {
	when := ""
	if age >= 0 {
		when = " for " + age.Truncate(time.Second).String()
	}
	return Finding{
		ID: "consumer.paused", Severity: SevWarn,
		Title: fmt.Sprintf("paused%s — nothing will deliver until resumed", when),
		Detail: "An hour-old pause is more often forgotten than deliberate; if it WAS " +
			"deliberate, note why somewhere future-you trusts.",
		Fix:      []string{fmt.Sprintf("messq consumer resume %s %s", c.Stream, c.Name)},
		Subject:  Subject{Stream: c.Stream, Consumer: c.Name},
		Evidence: map[string]any{"paused_age_ms": age.Milliseconds()},
		Docs:     docsAnchor("consumer.paused"),
	}
}

const (
	oldestPendingWarn = 15 * time.Minute
	oldestPendingFail = time.Hour
)

func evalConsumerOldestPending(_ context.Context, snap *Snapshot) []Finding {
	if snap.Pending == nil {
		return []Finding{skippedCheck("consumer.oldest_pending",
			"pending-set facts were not collected")}
	}
	var out []Finding
	for _, c := range snap.Consumers {
		pf := snap.Pending[c.Stream+"\x00"+c.Name]
		if pf.OldestReadyMS == 0 {
			continue
		}
		age := snap.Now.Sub(unixMilliToTime(pf.OldestReadyMS))
		sev := SevWarn
		if age >= oldestPendingFail {
			sev = SevFail
		} else if age < oldestPendingWarn {
			continue
		}
		out = append(out, Finding{
			ID: "consumer.oldest_pending", Severity: sev,
			Title: fmt.Sprintf("oldest pending delivery is %s old (warn ≥15m, fail ≥1h)",
				age.Truncate(time.Second)),
			Detail: "This is the number §9.4 graphs; when it spikes, capacity is behind " +
				"something specific and messq pending names the rows.",
			Fix:      []string{fmt.Sprintf("messq pending %s %s --older-than 15m", c.Stream, c.Name)},
			Subject:  Subject{Stream: c.Stream, Consumer: c.Name},
			Evidence: map[string]any{"oldest_ready_ms": age.Milliseconds()},
			Docs:     docsAnchor("consumer.oldest_pending"),
		})
	}
	return out
}

func evalConsumerFlowBlocked(_ context.Context, snap *Snapshot) []Finding {
	if snap.Pending == nil {
		return []Finding{skippedCheck("consumer.flow_blocked",
			"inflight facts were not collected")}
	}
	var out []Finding
	for _, c := range snap.Consumers {
		if c.MaxAckPending <= 0 {
			continue
		}
		pf := snap.Pending[c.Stream+"\x00"+c.Name]
		if pf.InflightCount < c.MaxAckPending {
			continue
		}
		out = append(out, Finding{
			ID: "consumer.flow_blocked", Severity: SevInfo,
			Title: fmt.Sprintf("inflight pinned at max_ack_pending (%d)", c.MaxAckPending),
			Detail: "Delivery is wedged at the cap, not by demand: another worker or a " +
				"higher cap converts backlog into progress immediately.",
			Fix: []string{
				fmt.Sprintf("messq consumer edit %s %s --max-ack-pending <cap>",
					c.Stream, c.Name),
			},
			Subject: Subject{Stream: c.Stream, Consumer: c.Name},
			Evidence: map[string]any{
				"inflight": pf.InflightCount, "max_ack_pending": c.MaxAckPending,
			},
			Docs: docsAnchor("consumer.flow_blocked"),
		})
	}
	return out
}

// sampleSubjectsLookBack bounds the filter-matching probe per stream.
const sampleSubjectsLookBack = 200

func evalConsumerFilterMatchesNothing(_ context.Context, snap *Snapshot) []Finding {
	counts := streamConsumers(snap)
	var out []Finding
	for _, st := range snap.Streams {
		streamName := st.Name
		if counts[streamName] == 0 || st.SampleSubjects == nil {
			continue
		}
		for i := range snap.Consumers {
			c := &snap.Consumers[i]
			if c.Stream != streamName {
				continue
			}
			matched := 0
			for _, subj := range st.SampleSubjects {
				if subjectMatchesAny(c.Filters, subj) {
					matched++
				}
			}
			if matched > 0 {
				continue
			}
			top := topSubjects(st.SampleSubjects, 3)
			out = append(out, Finding{
				ID: "consumer.filter_matches_nothing", Severity: SevWarn,
				Title: fmt.Sprintf("filters match 0 of the stream's last %d subjects",
					len(st.SampleSubjects)),
				Detail: fmt.Sprintf("Most common live subjects: %s — the filter set "+
					"selects none of them.", strings.Join(top, ", ")),
				Fix: []string{
					fmt.Sprintf("messq consumer edit %s %s --filter <subject>",
						streamName, c.Name),
				},
				Subject: Subject{Stream: streamName, Consumer: c.Name},
				Evidence: map[string]any{
					"samples": len(st.SampleSubjects), "matched": matched,
				},
				Docs: docsAnchor("consumer.filter_matches_nothing"),
			})
		}
	}
	return out
}

func topSubjects(subs []string, n int) []string {
	counter := map[string]int{}
	order := []string{}
	for _, s := range subs {
		if counter[s] == 0 {
			order = append(order, s)
		}
		counter[s]++
	}
	sortStringsByDescCount(order, counter)
	if len(order) < n {
		n = len(order)
	}
	return order[:n]
}

// subjectMatchesAny delegates to the same matcher grammar publishers hit.
func subjectMatchesAny(filters []string, subject string) bool {
	if len(filters) == 0 {
		return false
	}
	for _, f := range filters {
		if f == ">" { // fan-out wildcard matches everything by grammar
			return true
		}
	}
	// The one-hop structural check doctor may do without the subject package:
	// exact token equality plus trailing wildcards per token.
	stokens := strings.Split(subject, ".")
	for _, f := range filters {
		ftokens := strings.Split(f, ".")
		ok := true
		for i, ft := range ftokens {
			if ft == ">" { // multi-level tail
				ok = true
				break
			}
			if i >= len(stokens) || (stokens[i] != ft && ft != "*") {
				ok = false
				break
			}
		}
		multiLevel := len(ftokens) > 0 && ftokens[len(ftokens)-1] == ">"
		if ok && (multiLevel || len(ftokens) == len(stokens)) {
			return true
		}
	}
	return false
}

func sortStrings(s []string) {
	for i := 1; i < len(s); i++ {
		for j := i; j > 0 && s[j] < s[j-1]; j-- {
			s[j], s[j-1] = s[j-1], s[j]
		}
	}
}

func sortStringsByDescCount(order []string, counter map[string]int) {
	for i := 1; i < len(order); i++ {
		for j := i; j > 0 &&
			(counter[order[j]] > counter[order[j-1]] ||
				(counter[order[j]] == counter[order[j-1]] && order[j] < order[j-1])); j-- {
			order[j], order[j-1] = order[j-1], order[j]
		}
	}
}
