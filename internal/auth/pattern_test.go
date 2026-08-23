// SPDX-License-Identifier: Apache-2.0

package auth_test

import (
	"errors"
	"strings"
	"testing"

	"github.com/a-holm/messq/internal/auth"
	"github.com/a-holm/messq/internal/errs"
)

// TestParsePatternMatching walks the exact/prefix/* grammar through its match behaviour. The
// two wildcard forms are the whole pattern "*" (every stream) and a trailing "*" (a prefix
// match); a bare literal is exact equality, which is why "orders" deliberately does not cover
// "orders.dlq" — the dead-letter stream is a real stream with a real name (issue #16, D3).
func TestParsePatternMatching(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		in      string
		matches []string
		misses  []string
	}{
		{name: "star", in: "*", matches: []string{"orders", "payments", "orders.dlq", "orders.eu.1"}, misses: []string{}},
		{name: "exact", in: "orders", matches: []string{"orders"}, misses: []string{"orders.dlq", "orders2", "order", "payments"}},
		{name: "prefix", in: "orders*", matches: []string{"orders", "orders.dlq", "orders2", "orders.eu.1"}, misses: []string{"order", "orderx", "payments", ""}},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			p, err := auth.ParsePattern(tc.in)
			if err != nil {
				t.Fatalf("ParsePattern(%q) error = %v, want nil", tc.in, err)
			}
			for _, s := range tc.matches {
				if !p.Match(s) {
					t.Errorf("pattern %q did not match %q", tc.in, s)
				}
			}
			for _, s := range tc.misses {
				if p.Match(s) {
					t.Errorf("pattern %q matched %q, want a miss", tc.in, s)
				}
			}
		})
	}
}

// TestParsePatternCaseSensitive pins that stream names are compared byte-for-byte: the matcher
// is a string matcher, not a normalising one, so "Orders" and "orders" are different streams.
func TestParsePatternCaseSensitive(t *testing.T) {
	t.Parallel()

	p, err := auth.ParsePattern("orders")
	if err != nil {
		t.Fatalf("ParsePattern: %v", err)
	}
	if p.Match("Orders") {
		t.Error(`pattern "orders" matched "Orders"; stream matching is case-sensitive`)
	}
}

// TestParsePatternRejectsGrammarErrors pins the operator-facing grammar. Stream patterns are
// deliberately NOT the subject matcher from internal/subject: ">" is a subject-token wildcard
// and has no meaning in a stream name, and "*" may only be the whole pattern or its trailing
// character. The matcher shares no code and no fixtures with internal/subject — that boundary
// is enforced by scripts/layers.sh, which forbids internal/auth from reaching internal/subject
// transitively, so a shared helper would break the gate rather than a test.
func TestParsePatternRejectsGrammarErrors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		in       string
		fragment string
	}{
		{in: "", fragment: "empty"},
		{in: "orders.>", fragment: "not subject patterns"},
		{in: ">", fragment: "not subject patterns"},
		{in: "or*ers", fragment: "trailing character"},
		{in: "*orders", fragment: "trailing character"},
		{in: "**", fragment: "trailing character"},
		{in: "orders**", fragment: "trailing character"},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(strings.ReplaceAll(tc.in, "*", "_"), func(t *testing.T) {
			t.Parallel()
			_, err := auth.ParsePattern(tc.in)
			if err == nil {
				t.Fatalf("ParsePattern(%q) = nil error, want a grammar error", tc.in)
			}
			if !errors.Is(err, errs.ErrBadRequest) {
				t.Fatalf("ParsePattern(%q) error = %v, want errors.Is ErrBadRequest", tc.in, err)
			}
			if !strings.Contains(err.Error(), tc.fragment) {
				t.Errorf("ParsePattern(%q) error = %q, want it to contain %q", tc.in, err, tc.fragment)
			}
		})
	}
}

// TestParsePatternStringRoundTrips asserts the compiled pattern renders exactly the text it
// was built from, so a pattern survives a render/parse cycle unchanged.
func TestParsePatternStringRoundTrips(t *testing.T) {
	t.Parallel()

	for _, in := range []string{"*", "orders", "orders*"} {
		p, err := auth.ParsePattern(in)
		if err != nil {
			t.Fatalf("ParsePattern(%q): %v", in, err)
		}
		if got := p.String(); got != in {
			t.Errorf("String() = %q, want %q", got, in)
		}
	}
}
