// SPDX-License-Identifier: Apache-2.0

package verify

import (
	"context"
	"strings"
	"testing"
)

// V7 (#27 G3): stream_stats equals a full scan per stream, deep-only. Drift is the bug
// that silently disables discard=new's exact O(1) check, so it is checked, not trusted.

// vByID collects a report's violations for one check ID.
func vByID(rep Report, id string) []Violation {
	var out []Violation
	for _, v := range rep.Violations {
		if v.ID == id {
			out = append(out, v)
		}
	}
	return out
}

func TestV7GreenAfterPublish(t *testing.T) {
	dir := migratedDir(t)
	rep := runVerify(t, dir, Options{Deep: true})
	for _, c := range rep.Checks {
		if c.ID == V7 && !c.OK {
			t.Fatalf("V7 reported violations on a clean store: %v", c.Violations)
		}
	}
}

func TestV7ReportsCounterDrift(t *testing.T) {
	dir := migratedDir(t)
	db := writable(t, dir)
	if _, err := db.ExecContext(context.Background(),
		`UPDATE stream_stats SET msgs = msgs + 3, bytes = bytes + 300 WHERE stream = 'orders'`); err != nil {
		t.Fatalf("tamper stats: %v", err)
	}
	rep := runVerify(t, dir, Options{Deep: true})
	vs := vByID(rep, V7)
	if len(vs) == 0 {
		t.Fatal("V7 reported nothing after tampered counters")
	}
	detail := vs[0].Detail
	for _, want := range []string{"orders", "stats", "scan"} {
		if !strings.Contains(detail, want) {
			t.Fatalf("V7 detail %q does not name %q", detail, want)
		}
	}
}

func TestV7ReportsMissingStatsRow(t *testing.T) {
	dir := migratedDir(t)
	db := writable(t, dir)
	if _, err := db.ExecContext(context.Background(), `DELETE FROM stream_stats WHERE stream = 'orders'`); err != nil {
		t.Fatalf("delete stats row: %v", err)
	}
	rep := runVerify(t, dir, Options{Deep: true})
	vs := vByID(rep, V7)
	if len(vs) == 0 {
		t.Fatal("V7 reported nothing after the stats row vanished")
	}
	if !strings.Contains(vs[0].Detail, "orders") {
		t.Fatalf("V7 detail %q does not name the affected stream", vs[0].Detail)
	}
}

func TestV7SkippedUnlessDeep(t *testing.T) {
	dir := migratedDir(t)
	db := writable(t, dir)
	if _, err := db.ExecContext(context.Background(),
		`UPDATE stream_stats SET msgs = msgs + 99 WHERE stream = 'orders'`); err != nil {
		t.Fatalf("tamper stats: %v", err)
	}
	rep := runVerify(t, dir, Options{})
	for _, c := range rep.Checks {
		if c.ID == V7 && !c.Skipped {
			t.Fatalf("V7 ran without --deep: %+v", c)
		}
	}
	if len(vByID(rep, V7)) != 0 {
		t.Fatal("V7 produced violations while skipped")
	}
}
