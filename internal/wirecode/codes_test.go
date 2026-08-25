// SPDX-License-Identifier: Apache-2.0

package wirecode

import (
	"net/http"
	"slices"
	"testing"
)

// TestProducedCodesHaveHTTStatuses: every code the daemon can emit today carries the
// HTTP status the API layer actually sends. A row that drifts from statusFor is a
// silent contract break.
func TestProducedCodesHaveHTTPStatuses(t *testing.T) {
	for _, c := range All() {
		e := Table[c]
		switch e.Kind {
		case Produced:
			if e.Status == 0 {
				t.Errorf("produced code %q has no HTTP status", c)
			}
		case Planned:
			if e.Status == 0 {
				t.Errorf("planned code %q must still pin its planned status", c)
			}
		case Reserved:
			if e.Owner == "" {
				t.Errorf("reserved code %q names no owning issue", c)
			}
		case NeverOverHTTP:
			if e.Status != 0 {
				t.Errorf("never-over-HTTP code %q must not carry an HTTP status", c)
			}
		}
	}
}

// TestNeverOverHTTPSet pins exactly which closed-set members can never appear in an
// HTTP error envelope: two startup-only refusals and one client-side sentinel
// (brief-issue-18 §8 Q3). #14 consumes this set as its neverOverHTTP source; the wire
// freeze test fails if a handler ever maps a sentinel in this set to an envelope.
// want and got are compared as sorted slices (both sides come out of All()'s
// ordering); a map literal keyed by Code would trip the exhaustive gate.
func TestNeverOverHTTPSet(t *testing.T) {
	want := []Code{Locked, SchemaNewer, Unavailable}
	if got := NeverOverHTTPSet(); !slices.Equal(want, got) {
		t.Errorf("NeverOverHTTPSet() = %v, want %v", got, want)
	}
}

// TestReservedCodesNameTheirIssue: a reserved code without an owner is a promise
// nobody holds — the freeze cannot ever be discharged.
func TestReservedCodesNameTheirIssue(t *testing.T) {
	for _, c := range All() {
		if Table[c].Kind != Reserved {
			continue
		}
		if Table[c].Owner == "" {
			t.Errorf("reserved code %q has no owning issue", c)
		}
	}
}

// TestStatusesMatchPlan pins the statuses PLAN.md and the issues state verbatim:
// 507 disk_full (PLAN section on degraded writes), 429 rate_limited (#39),
// 401/403 auth denials (#16).
func TestStatusesMatchPlan(t *testing.T) {
	// A struct slice, not a map literal: exhaustive demands every Code key in a
	// map[Code]… literal, which would defeat the point of pinning a subset.
	cases := []struct {
		code Code
		want int
	}{
		{DiskFull, http.StatusInsufficientStorage},
		{RateLimited, http.StatusTooManyRequests},
		{Unauthorized, http.StatusUnauthorized},
		{Forbidden, http.StatusForbidden},
		{NotFound, http.StatusNotFound},
		{Conflict, http.StatusConflict},
		{BadRequest, http.StatusBadRequest},
		{TooLarge, http.StatusRequestEntityTooLarge},
	}
	for _, tc := range cases {
		if got := Table[tc.code].Status; got != tc.want {
			t.Errorf("code %q status = %d, want %d", tc.code, got, tc.want)
		}
	}
}

// TestAllIsSortedAndTotal: the frozen enum iterates deterministically and covers every
// table row.
func TestAllIsSortedAndTotal(t *testing.T) {
	all := All()
	if len(all) != len(Table) {
		t.Fatalf("All() = %d codes, Table has %d rows", len(all), len(Table))
	}
	for i := 1; i < len(all); i++ {
		if all[i-1] >= all[i] {
			t.Fatalf("All() is not sorted: %v", all)
		}
	}
}
