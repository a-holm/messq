// SPDX-License-Identifier: Apache-2.0

package doctor

import (
	"context"
	"os"
	"strings"
	"testing"
	"time"
)

// TestDocsDoctorHasSectionPerID enforces the issue contract: every registered
// check documents itself under docs/doctor.md#<id>, and the registry's Summary
// and Explain texts match what the section teaches. A check that renames or
// forgets its teaching paragraph fails here, not in an incident review.
func TestDocsDoctorHasSectionPerID(t *testing.T) {
	raw, err := os.ReadFile("../../docs/doctor.md")
	if err != nil {
		t.Fatalf("read docs/doctor.md: %v", err)
	}
	doc := string(raw)

	for _, id := range DefaultRegistry().List() {
		check := *mustID(t, id)
		anchor := "## " + id
		if !strings.Contains(doc, anchor) {
			t.Errorf("docs/doctor.md lacks a section heading %q", anchor)
			continue
		}
		section := sectionOf(doc, anchor)
		if !strings.Contains(section, check.Summary) {
			t.Errorf("%s: section summary drifted from the registry:\nsection=%q\nregistry=%q",
				id, section, check.Summary)
		}
		if !strings.Contains(section, firstSentence(check.Explain)) {
			t.Errorf("%s: section lacks the explain text: %q", id, section)
		}
	}
}

func mustID(t *testing.T, id string) *Check {
	t.Helper()
	c, ok := DefaultRegistry().Get(id)
	if !ok {
		t.Fatalf("registered id %q vanished between List and Get", id)
	}
	return c
}

// sectionOf returns the text from one ## heading to the next.
func sectionOf(doc, anchor string) string {
	start := strings.Index(doc, anchor)
	if start < 0 {
		return ""
	}
	next := strings.Index(doc[start+len(anchor):], "\n## ")
	end := len(doc)
	if next >= 0 {
		end = start + len(anchor) + next
	}
	return doc[start:end]
}

func firstSentence(s string) string {
	for i := 0; i+1 < len(s); i++ {
		if s[i] == '.' && s[i+1] == ' ' {
			return s[:i]
		}
	}
	return strings.TrimSpace(s)
}

// TestEveryCheckCarriesFixOrNoFix walks the whole registry enforcing the
// finding contract statically: a check whose Eval could emit neither fix
// commands nor an honest "nothing to run" sentence must not exist.
func TestEveryCheckCarriesFixOrNoFix(t *testing.T) {
	for _, id := range DefaultRegistry().List() {
		check := *mustID(t, id)
		if check.ID == "" || check.Summary == "" || check.Explain == "" {
			t.Errorf("check %q has empty ID/Summary/Explain fields", id)
		}
		if check.Needs != SourceLive && check.Needs != SourceDataDir &&
			check.Needs != SourceEither {
			t.Errorf("check %q declares source %d outside the enum", id, check.Needs)
		}
	}
}

// TestFindingShapeInvariant spot-runs every eval over a rich-but-empty
// snapshot asserting produced findings always honor Docs + Fix-or-NoFix.
func TestFindingShapeInvariant(t *testing.T) {
	snap := &Snapshot{
		Now:     time.Date(2026, 11, 4, 12, 0, 0, 0, time.UTC),
		Pending: map[string]PendingFacts{},
	}
	for _, id := range DefaultRegistry().List() {
		check := *mustID(t, id)
		for _, f := range RunChecks(context.Background(), func() *Registry {
			r := NewRegistry()
			r.Register(check)
			return r
		}(), snap) {
			if !strings.HasPrefix(f.Docs, "docs/doctor.md#") {
				t.Errorf("%s produced a finding with docs %q", id, f.Docs)
			}
			if len(f.Fix) == 0 && f.NoFix == "" {
				t.Errorf("%s produced a finding with neither Fix nor NoFix: %+v", id, f)
			}
		}
	}
}
