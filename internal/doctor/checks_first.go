// SPDX-License-Identifier: Apache-2.0

package doctor

import (
	"context"
	"fmt"
	"time"
)

func init() {
	defaultRegistry.Register(Check{
		ID:      "consumer.max_deliver_unlimited",
		Summary: "a consumer redelivers forever because max_deliver=0",
		Explain: "max_deliver=0 disables the delivery ceiling: one poison message loops " +
			"through the worker until someone notices. The §7 default is a small bound plus " +
			"a DLQ, so terminal failures park instead of churning.",
		Needs: SourceEither,
		Eval:  evalConsumerMaxDeliver,
	})
	defaultRegistry.Register(Check{
		ID:      "consumer.max_deliver_unlimited_no_dlq",
		Summary: "max_deliver=0 AND dead_policy=drop loses messages silently",
		Explain: "With unlimited redelivery and drop-on-dead there is no ceiling and no " +
			"parking lot; a message that kills the worker is retried forever and anything " +
			"else that fails permanently vanishes. This combination is refused advice.",
		Needs: SourceEither,
		Eval:  evalConsumerMaxDeliver,
	})
	defaultRegistry.Register(Check{
		ID:      "stream.no_consumers",
		Summary: "a stream holds messages nobody consumes",
		Explain: "Messages with no consumer are retained (and billed in bytes) while doing " +
			"nothing. If the stream is deliberate, keep an eye on retention; otherwise " +
			"remove it once its consumers are gone for good.",
		Needs: SourceDataDir,
		Eval:  evalStreamNoConsumers,
	})
	defaultRegistry.Register(Check{
		ID:      "stream.typo_suspect",
		Summary: "a young, tiny, unconsumed stream looks like a typo of another stream",
		Explain: "A publisher that mistypes a subject creates a new stream nobody reads; " +
			"the messages are accepted (2xx!) but never delivered. The suspect profile: " +
			"at most 10 messages, no consumers, under a week old, name within edit " +
			"distance 2 of an existing stream.",
		Needs: SourceDataDir,
		Eval:  evalStreamTypoSuspect,
	})
	defaultRegistry.Register(Check{
		ID:      "server.restored",
		Summary: "this data directory was restored from a backup snapshot",
		Explain: "Everything published after the snapshot's read transaction began is gone, " +
			"and deliveries INFLIGHT at snapshot time redeliver after the restore with their " +
			"attempts intact — workers holding pre-restore tokens get stale_ack 409s named " +
			"future_attempt. That is the documented safe direction.",
		Needs: SourceEither,
		Eval:  evalServerRestored,
	})
}

// evalConsumerMaxDeliver emits both max-deliver findings; the combined
// no-DLQ case escalates to fail.
func evalConsumerMaxDeliver(_ context.Context, snap *Snapshot) []Finding {
	var out []Finding
	for _, c := range snap.Consumers {
		if c.MaxDeliver != 0 {
			continue
		}
		out = append(out, Finding{
			ID:       "consumer.max_deliver_unlimited",
			Severity: SevWarn,
			Subject:  Subject{Stream: c.Stream, Consumer: c.Name},
			Title:    "redelivers forever: max_deliver=0 means unlimited",
			Detail: fmt.Sprintf("consumer %s/%s has max_deliver=0, so a poison message is "+
				"retried without end.", c.Stream, c.Name),
			Fix: []string{fmt.Sprintf("messq consumer edit %s %s --max-deliver 5", c.Stream, c.Name)},
			Evidence: map[string]any{
				"max_deliver": c.MaxDeliver, "dead_policy": c.DeadPolicy,
			},
			Docs: docsAnchor("consumer.max_deliver_unlimited"),
		})
		if c.DeadPolicy == "drop" {
			out = append(out, Finding{
				ID:       "consumer.max_deliver_unlimited_no_dlq",
				Severity: SevFail,
				Subject:  Subject{Stream: c.Stream, Consumer: c.Name},
				Title:    "unlimited retries combined with silent loss on dead",
				Detail: fmt.Sprintf("consumer %s/%s has max_deliver=0 and dead_policy=drop: "+
					"there is neither a delivery ceiling nor a DLQ parking lot.",
					c.Stream, c.Name),
				Fix: []string{
					fmt.Sprintf("messq consumer edit %s %s --dead-policy dlq --max-deliver 5",
						c.Stream, c.Name),
				},
				Evidence: map[string]any{
					"max_deliver": c.MaxDeliver, "dead_policy": c.DeadPolicy,
				},
				Docs: docsAnchor("consumer.max_deliver_unlimited_no_dlq"),
			})
		}
	}
	return out
}

// evalServerRestored reports restore provenance as info.
func evalServerRestored(_ context.Context, snap *Snapshot) []Finding {
	if snap.Restored == nil {
		return nil
	}
	return []Finding{{
		ID:       "server.restored",
		Severity: SevInfo,
		Title:    "data directory was restored from a backup snapshot",
		Detail: fmt.Sprintf("snapshot taken at %s from node %s; publishes after that moment "+
			"are gone and INFLIGHT deliveries redelivered at restore.",
			time.UnixMilli(snap.Restored.SnapshotAtMS).UTC().Format(time.RFC3339),
			snap.Restored.SourceNodeID),
		NoFix: "informational — run messq trace for duplicates caused by the restore",
		Evidence: map[string]any{
			"snapshot_at_ms": snap.Restored.SnapshotAtMS,
			"source_node_id": snap.Restored.SourceNodeID,
		},
		Docs: docsAnchor("server.restored"),
	}}
}

// streamConsumers counts consumers per stream from the collected state.
func streamConsumers(snap *Snapshot) map[string]int {
	counts := make(map[string]int)
	for _, c := range snap.Consumers {
		counts[c.Stream]++
	}
	return counts
}

// evalStreamNoConsumers flags streams holding messages with zero consumers for
// over a week.
func evalStreamNoConsumers(_ context.Context, snap *Snapshot) []Finding {
	if snap.Now.IsZero() {
		return nil // collector did not timestamp the run: skip rather than guess ages
	}
	counts := streamConsumers(snap)
	var out []Finding
	const week = 7 * 24 * time.Hour
	for _, s := range snap.Streams {
		if s.Msgs == 0 || counts[s.Name] > 0 {
			continue
		}
		age := snap.Now.Sub(time.UnixMilli(s.CreatedAtMS))
		if age < week || age < 0 {
			continue
		}
		out = append(out, Finding{
			ID:       "stream.no_consumers",
			Severity: SevInfo,
			Subject:  Subject{Stream: s.Name},
			Title: fmt.Sprintf("holds %d messages and has had no consumers for %dd",
				s.Msgs, int(age.Hours()/24)),
			Detail: fmt.Sprintf("stream %s retains %d messages (%d bytes) nobody consumes. "+
				"If the workers are gone for good, remove the stream or trim retention.",
				s.Name, s.Msgs, s.Bytes),
			Fix:      []string{fmt.Sprintf("messq stream rm %s --confirm %s   # or set retention", s.Name, s.Name)},
			Evidence: map[string]any{"msgs": s.Msgs, "bytes": s.Bytes, "age_days": int(age.Hours() / 24)},
			Docs:     docsAnchor("stream.no_consumers"),
		})
	}
	return out
}

// evalStreamTypoSuspect flags young tiny unconsumed streams whose names sit
// within edit distance 2 of an existing stream.
func evalStreamTypoSuspect(_ context.Context, snap *Snapshot) []Finding {
	if snap.Now.IsZero() {
		return nil
	}
	counts := streamConsumers(snap)
	const (
		maxMsgs = int64(10)
		maxAge  = 7 * 24 * time.Hour
		maxDist = 2
	)
	var out []Finding
	for _, s := range snap.Streams {
		if s.Msgs > maxMsgs || s.Msgs == 0 || counts[s.Name] > 0 {
			continue
		}
		age := snap.Now.Sub(time.UnixMilli(s.CreatedAtMS))
		if age >= maxAge || age < 0 {
			continue
		}
		best, bestDist := "", -1
		for _, other := range snap.Streams {
			if other.Name == s.Name {
				continue
			}
			d := levenshtein(s.Name, other.Name)
			if d <= maxDist && (bestDist < 0 || d < bestDist) {
				best, bestDist = other.Name, d
			}
		}
		if bestDist < 0 {
			continue
		}
		out = append(out, Finding{
			ID:       "stream.typo_suspect",
			Severity: SevInfo,
			Subject:  Subject{Stream: s.Name},
			Title: fmt.Sprintf("possible typo of %q — %d messages, 0 consumers, %dd old",
				best, s.Msgs, int(age.Hours()/24)),
			Detail: fmt.Sprintf("stream %q accepted %d message(s) nobody will ever consume; "+
				"its name sits %d edits from existing stream %q, whose publishers may have "+
				"meant it.", s.Name, s.Msgs, bestDist, best),
			Fix: []string{fmt.Sprintf("messq stream rm %s --confirm %s", s.Name, s.Name)},
			Evidence: map[string]any{
				"msgs": s.Msgs, "nearest_stream": best, "edit_distance": bestDist,
			},
			Docs: docsAnchor("stream.typo_suspect"),
		})
	}
	return out
}

// levenshtein is the classic insert/delete/substitute edit distance — the
// naive reference the differential fuzz in the test plan compares against.
func levenshtein(a, b string) int {
	ar, br := []rune(a), []rune(b)
	prev := make([]int, len(br)+1)
	cur := make([]int, len(br)+1)
	for j := range prev {
		prev[j] = j
	}
	for i := 1; i <= len(ar); i++ {
		cur[0] = i
		for j := 1; j <= len(br); j++ {
			cost := 1
			if ar[i-1] == br[j-1] {
				cost = 0
			}
			cur[j] = min(prev[j]+1, min(cur[j-1]+1, prev[j-1]+cost))
		}
		prev, cur = cur, prev
	}
	return prev[len(br)]
}
