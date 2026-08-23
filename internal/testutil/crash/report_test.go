// SPDX-License-Identifier: Apache-2.0

package crash

import (
	"testing"

	"github.com/a-holm/messq/internal/testutil/ledger"
)

// guardNames collects the rule names of the guard violations, for the table assertions.
func guardNames(vs []Violation) map[string]bool {
	out := make(map[string]bool, len(vs))
	for _, v := range vs {
		out[v.Rule] = true
	}
	return out
}

// TestGuardsFire proves each vacuity guard bites, and that a healthy report is clean.
func TestGuardsFire(t *testing.T) {
	// Liveness: an average below 50 OK/cycle means the load was vacuous.
	if g := guardNames((Report{Cycles: 2, Results: []CycleResult{{OK: 10}, {OK: 20}}}).Guards()); !g["LIVENESS"] {
		t.Errorf("LIVENESS did not fire for avg 15 OK/cycle: %v", g)
	}
	// Kill-lands low: UNKNOWN below 1% means the kill never lands mid-flight.
	if g := guardNames((Report{OK: 1000, Unknown: 5}).Guards()); !g["KILL-LANDS-LOW"] {
		t.Errorf("KILL-LANDS-LOW did not fire for 0.5%% UNKNOWN: %v", g)
	}
	// Kill-lands high: UNKNOWN above 20% means the oracle has gone blind.
	if g := guardNames((Report{OK: 10, Unknown: 90}).Guards()); !g["KILL-LANDS-HIGH"] {
		t.Errorf("KILL-LANDS-HIGH did not fire for 90%% UNKNOWN: %v", g)
	}
	// Survivorship: both outcomes must be observed.
	if g := guardNames((Report{OK: 100, Unknown: 10, UnknownPresent: 10, UnknownAbsent: 0}).Guards()); !g["SURVIVORSHIP"] {
		t.Errorf("SURVIVORSHIP did not fire when no UNKNOWN was absent: %v", g)
	}
	// Wal-tail: a non-empty -wal must be observed at kill time at least once.
	if g := guardNames((Report{OK: 100, Unknown: 5, UnknownPresent: 3, UnknownAbsent: 2, WALTailObserved: false}).Guards()); !g["WAL-TAIL"] {
		t.Errorf("WAL-TAIL did not fire when no kill saw a non-empty -wal: %v", g)
	}
}

// TestGuardsClean proves a healthy report produces no guard violations.
func TestGuardsClean(t *testing.T) {
	r := Report{
		Cycles:          2,
		OK:              200,
		Unknown:         20,
		Failed:          1,
		UnknownPresent:  12,
		UnknownAbsent:   8,
		WALTailObserved: true,
		Results:         []CycleResult{{OK: 100, Unknown: 10}, {OK: 100, Unknown: 10}},
	}
	if vs := r.Guards(); len(vs) != 0 {
		t.Fatalf("healthy report has guard violations: %+v", vs)
	}
}

// TestSummarizeSurvivorship proves the report's survivorship split counts UNKNOWN records
// by whether their dedup key survived into the final state.
func TestSummarizeSurvivorship(t *testing.T) {
	state := &StateSnapshot{
		msgAt:  map[seqKey]Message{},
		dedup:  map[string]seqKey{"present-key": {"crash", 1}},
		next:   map[string]int64{"crash": 4},
		maxSeq: map[string]int64{"crash": 3},
	}
	recs := map[string]ledger.Record{
		"present-key": {Key: "present-key", Stream: "crash", Verdict: ledger.Unknown},
		"absent-key":  {Key: "absent-key", Stream: "crash", Verdict: ledger.Unknown},
		"ok-key":      {Key: "ok-key", Stream: "crash", Verdict: ledger.OK, Seq: 3, ID: "id3", Size: 4},
	}
	results := []CycleResult{{Cycle: 0, OK: 1, Unknown: 2, WALTail: true}}
	r := summarize(results, recs, state)
	if r.UnknownPresent != 1 || r.UnknownAbsent != 1 {
		t.Errorf("survivorship = present %d absent %d, want 1 and 1", r.UnknownPresent, r.UnknownAbsent)
	}
	if !r.WALTailObserved {
		t.Errorf("wal-tail observation was not propagated from the cycle result")
	}
}
