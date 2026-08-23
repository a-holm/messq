// SPDX-License-Identifier: Apache-2.0

package crash

import (
	"fmt"
	"io"
	"strings"

	"github.com/a-holm/messq/internal/testutil/ledger"
)

// The vacuity guards are the answer to "is this test testing anything?". A kill/restart
// oracle nobody has watched fail is an oracle nobody should trust, so the guards make the
// things that would let it pass vacuously — a kill that lands before any publish, a blind
// classifier that dumps everything to UNKNOWN, an oracle that never actually sees the kill
// window — into explicit, reported failures.

// Report is the sweep summary printed at the end: per-cycle results, the verdict
// histogram, the survivorship counts, the wal-tail observation and every violation.
type Report struct {
	Cycles          int
	OK              int64
	Unknown         int64
	Failed          int64
	UnknownPresent  int64 // UNKNOWN records whose message was present after recovery
	UnknownAbsent   int64 // UNKNOWN records whose message was absent after recovery
	WALTailObserved bool
	Violations      []Violation
	Results         []CycleResult
}

// livenessOKFloor is the minimum average OK publishes per cycle the sweep must produce. It
// is a vacuity catcher, not a throughput gate: the sweep must have acknowledged at least one
// publish per cycle on average, or the kill is landing before any commit. The 50/cycle
// figure the milestone plan sketched assumed the group-commit writer is wired into `messq
// serve`; it is not (issue #7 shipped the serve skeleton without Store.NewWriter, so the
// daemon runs the one-fsync-per-message solo path — recorded in docs/perf/M2-baseline.md).
// Until that follow-up lands, a 50/cycle floor would flake under the race detector, so the
// floor is 1 and the "load is substantial" signal belongs to the KILL-LANDS and SURVIVORSHIP
// guards. Raise it back to 50 when the writer is wired.
const livenessOKFloor = 1

// Guards computes the vacuity-guard violations for the whole sweep. The guard values are
// printed by [Report.Print] regardless, so a sweep can report them even when none fire.
func (r Report) Guards() []Violation {
	var vs []Violation
	if r.OK < int64(r.Cycles)*livenessOKFloor {
		vs = append(vs, Violation{
			Rule: "LIVENESS",
			Detail: fmt.Sprintf("sweep produced %d OK over %d cycles (avg %.1f/cycle), want >= %d/cycle",
				r.OK, r.Cycles, float64(r.OK)/float64(max(r.Cycles, 1)), livenessOKFloor),
		})
	}
	total := r.OK + r.Unknown + r.Failed
	if total > 0 {
		ratio := float64(r.Unknown) / float64(total)
		switch {
		case ratio < 0.01:
			vs = append(vs, Violation{
				Rule:   "KILL-LANDS-LOW",
				Detail: fmt.Sprintf("UNKNOWN is %.1f%% of %d records, want >= 1%% — the kill never lands mid-flight", ratio*100, total),
			})
		case ratio > 0.20:
			vs = append(vs, Violation{
				Rule:   "KILL-LANDS-HIGH",
				Detail: fmt.Sprintf("UNKNOWN is %.1f%% of %d records, want <= 20%% — the oracle has gone blind", ratio*100, total),
			})
		}
	}
	if r.UnknownPresent == 0 || r.UnknownAbsent == 0 {
		vs = append(vs, Violation{
			Rule: "SURVIVORSHIP",
			Detail: fmt.Sprintf("%d UNKNOWN present, %d absent — both outcomes must occur to prove the kill window is real",
				r.UnknownPresent, r.UnknownAbsent),
		})
	}
	if !r.WALTailObserved {
		vs = append(vs, Violation{
			Rule:   "WAL-TAIL",
			Detail: "no kill ever observed a non-empty -wal, so recovery never had durable-but-unreplayed frames to replay",
		})
	}
	return vs
}

// Print renders the report in the shape a human reads during a failure investigation: one
// line per cycle, then the guard values and the verdict histogram. It builds into a string
// so a write failure (a full pipe, a closed writer) is the caller's to handle, exactly once.
func (r Report) Print(w io.Writer) error {
	var b strings.Builder
	for _, res := range r.Results {
		fmt.Fprintf(&b, "crash: cycle %3d  seed=%d  strategy=%-14s ok=%d  unknown=%d  failed=%d  wal_tail=%t\n",
			res.Cycle, res.Seed, res.Strategy, res.OK, res.Unknown, res.Failed, res.WALTail)
	}
	total := r.OK + r.Unknown + r.Failed
	fmt.Fprintf(&b, "crash: %d cycles, %d violations\n", r.Cycles, len(r.Violations))
	if total > 0 {
		fmt.Fprintf(&b, "crash: ledger %d records — OK %d (%.1f%%) · UNKNOWN %d (%.1f%%) · FAILED %d\n",
			total, r.OK, pct(r.OK, total), r.Unknown, pct(r.Unknown, total), r.Failed)
	}
	if r.Unknown > 0 {
		fmt.Fprintf(&b, "crash: of %d UNKNOWN: %d present after recovery, %d absent\n",
			r.Unknown, r.UnknownPresent, r.UnknownAbsent)
	}
	fmt.Fprintf(&b, "crash: wal_tail_observed=%t\n", r.WALTailObserved)
	for _, g := range r.Guards() {
		fmt.Fprintf(&b, "crash: guard %s: %s\n", g.Rule, g.Detail)
	}
	_, err := io.WriteString(w, b.String())
	return err
}

func pct(part, whole int64) float64 {
	if whole == 0 {
		return 0
	}
	return 100 * float64(part) / float64(whole)
}

// summarize builds the sweep report from the per-cycle results, the replayed ledger, the
// final state snapshot and the wal-tail observation. Survivorship is computed here, once,
// against the final state: an UNKNOWN record is "present" when its dedup key survives into
// the recovered state.
func summarize(results []CycleResult, recs map[string]ledger.Record, state *StateSnapshot) Report {
	r := Report{Cycles: len(results), Results: results}
	for _, res := range results {
		r.OK += res.OK
		r.Unknown += res.Unknown
		r.Failed += res.Failed
		if res.WALTail {
			r.WALTailObserved = true
		}
	}
	for key, rec := range recs {
		if rec.Verdict != ledger.Unknown {
			continue
		}
		if _, present := state.dedup[key]; present {
			r.UnknownPresent++
		} else {
			r.UnknownAbsent++
		}
	}
	return r
}
