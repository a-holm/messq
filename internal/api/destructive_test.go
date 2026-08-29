// SPDX-License-Identifier: Apache-2.0

package api

import (
	"strings"
	"testing"

	"github.com/a-holm/messq/internal/obs"
)

// Issue #28 slice 2: destructiveness is a route attribute and the registry is
// walked by a test, so a new destructive verb cannot ship undisciplined
// (PLAN §8's fourth CLI rule, mechanised). A Destructive route must declare the
// ?confirm= target whose name the operator types, the audit-class event its
// execution commits (exactly one per execution, #28 Decision 6), and be a
// mutating route. Dry-run acceptance (?dry_run=1) is part of the handler
// contract each destructive verb implements when it lands; at the registry
// level the attribute marks the routes the discipline applies to.

// destructiveFindings lists every discipline violation of one route entry. It
// lives beside the walker so TestDestructiveDisciplinePredicate can prove each
// requirement bites on synthetic entries — permanent evidence, independent of
// whatever routes the registry currently declares.
func destructiveFindings(rt Route) []string {
	if !rt.Destructive {
		return nil // the discipline applies to destructive routes only
	}
	var out []string
	if !rt.Mutating {
		out = append(out, "a destructive route must also be Mutating")
	}
	if rt.Confirm == "" {
		out = append(out, "declares no Confirm target (?confirm=<name> is required when impact is non-empty)")
	}
	if rt.Audit == obs.KindInvalid {
		out = append(out, "declares no Audit event (exactly one audit-class event per execution)")
	} else {
		if _, err := obs.ParseKind(rt.Audit.String()); err != nil {
			out = append(out, "Audit event is not a member of the closed vocabulary")
		}
	}
	return out
}

func TestDestructiveDiscipline(t *testing.T) {
	t.Parallel()

	for _, rt := range (*Server)(nil).routes() {
		if !rt.Destructive {
			continue
		}
		for _, f := range destructiveFindings(rt) {
			t.Errorf("%s %s (%s): %s", rt.Method, rt.Pattern, rt.Name, f)
		}
	}
}

func TestDestructiveDisciplinePredicate(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		rt          Route
		wantStrings []string // substrings expected among the findings
	}{
		{
			name: "complete destructive route is clean",
			rt: Route{
				Method: "POST", Pattern: "/v1/streams/{stream}/replay", Name: "replay",
				Mutating: true, Destructive: true,
				Confirm: "{stream}", Audit: obs.AdminAction,
			},
		},
		{
			name: "missing confirm target",
			rt: Route{
				Method: "POST", Pattern: "/v1/streams/{s}/seek", Name: "seek_consumer",
				Mutating: true, Destructive: true,
				Audit: obs.ConsumerSeek,
			},
			wantStrings: []string{"no Confirm target"},
		},
		{
			name: "missing audit event",
			rt: Route{
				Method: "POST", Pattern: "/v1/streams/{s}/purge", Name: "purge_stream",
				Mutating: true, Destructive: true,
				Confirm: "{stream}",
			},
			wantStrings: []string{"no Audit event"},
		},
		{
			name: "audit kind outside the closed vocabulary",
			rt: Route{
				Method: "DELETE", Pattern: "/v1/streams/{stream}", Name: "delete_stream",
				Mutating: true, Destructive: true,
				Confirm: "{stream}", Audit: obs.Kind(200),
			},
			wantStrings: []string{"closed vocabulary"},
		},
		{
			name: "destructive but not mutating",
			rt: Route{
				Method: "POST", Pattern: "/x", Name: "bad",
				Destructive: true,
				Confirm:     "{stream}", Audit: obs.AdminAction,
			},
			wantStrings: []string{"must also be Mutating"},
		},
		{
			name: "non-destructive routes are outside the discipline",
			rt:   Route{Method: "GET", Pattern: "/v1/info", Name: "info"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := destructiveFindings(tt.rt)
			if len(tt.wantStrings) == 0 {
				if len(got) > 0 {
					t.Fatalf("route %+v produced findings %v, want none", tt.rt, got)
				}
				return
			}
			if len(got) == 0 {
				t.Fatalf("route %+v produced no findings, want %v", tt.rt, tt.wantStrings)
			}
			joined := strings.Join(got, "; ")
			for _, want := range tt.wantStrings {
				if !strings.Contains(joined, want) {
					t.Fatalf("findings %q do not name %q", joined, want)
				}
			}
		})
	}
}

// TestDestructiveRoutesDeclareKnownVocabulary pins the audit-event table for
// every destructive route that exists: the walk above enforces DECLARATION;
// this test makes the declared kinds human-reviewable in one place as the
// surface grows (#29 adds dlq redrive here).

// destructiveAuditKinds is §9.2's subset a destructive execution may commit:
// the domain event where one exists, admin.action only where the vocabulary has
// no name for the verb (replay today), and dlq.redrive for #29.
var destructiveAuditKinds = []obs.Kind{
	obs.StreamDelete, obs.StreamPurge, obs.ConsumerDelete,
	obs.ConsumerSeek, obs.AdminAction, obs.DLQRedrive,
}

func TestDestructiveRoutesDeclareKnownVocabulary(t *testing.T) {
	t.Parallel()

	for _, rt := range (*Server)(nil).routes() {
		if !rt.Destructive || rt.Audit == obs.KindInvalid {
			continue
		}
		known := false
		for _, k := range destructiveAuditKinds {
			if rt.Audit == k {
				known = true
				break
			}
		}
		if !known {
			t.Errorf("%s %s declares audit event %q, which is not one of the destructive-verb kinds",
				rt.Method, rt.Pattern, rt.Audit.String())
		}
	}
}
