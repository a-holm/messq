// SPDX-License-Identifier: Apache-2.0

package docsguard_test

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/a-holm/messq/internal/docsguard"
	"github.com/a-holm/messq/internal/errs"
)

const (
	repoRoot = "../.."
	specPath = repoRoot + "/docs/SEMANTICS.md"
	planPath = repoRoot + "/docs/PLAN.md"
	adrDir   = repoRoot + "/docs/adr"
)

func read(t *testing.T, path string) []byte {
	t.Helper()
	body, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	return body
}

func transitions(t *testing.T) []docsguard.Transition {
	t.Helper()
	ts, err := docsguard.ParseTransitions(read(t, specPath))
	if err != nil {
		t.Fatalf("ParseTransitions: %v", err)
	}
	return ts
}

func TestSpec_TransitionIDsUniqueAndComplete(t *testing.T) {
	t.Parallel()

	ts := transitions(t)
	if err := docsguard.CheckTransitions(ts); err != nil {
		t.Fatal(err)
	}

	got := map[string]bool{}
	for _, tr := range ts {
		got[tr.ID] = true
	}
	for _, want := range []string{"T1", "T2", "T3", "T3a", "T3b", "T4", "T5", "T6", "T7", "T8", "T9", "T10", "T11"} {
		if !got[want] {
			t.Errorf("docs/SEMANTICS.md S6.1 has no row for %s", want)
		}
	}
}

// TestSpec_TransitionTableMirrorsPlan is the check PLAN.md section 11.1 depends on: the
// conformance suite mirrors section 5.1 one row to one row, and it does that by mirroring this
// document, so the two tables must carry the same IDs in the same order.
func TestSpec_TransitionTableMirrorsPlan(t *testing.T) {
	t.Parallel()

	planIDs, err := docsguard.ParsePlanTransitionIDs(read(t, planPath))
	if err != nil {
		t.Fatalf("ParsePlanTransitionIDs: %v", err)
	}
	if err := docsguard.CheckMirrorsPlan(transitions(t), planIDs); err != nil {
		t.Fatal(err)
	}
}

func TestSpec_TransitionEventsAreInVocabulary(t *testing.T) {
	t.Parallel()

	vocab, err := docsguard.ParseSpecVocabulary(read(t, specPath))
	if err != nil {
		t.Fatalf("ParseSpecVocabulary: %v", err)
	}
	if err := docsguard.CheckTransitionEvents(transitions(t), vocab); err != nil {
		t.Fatal(err)
	}
}

// TestSpec_EveryMentionedEventIsInTheVocabulary covers prose as well as tables: an event name
// invented in a paragraph is the same drift as one invented in a row.
func TestSpec_EveryMentionedEventIsInTheVocabulary(t *testing.T) {
	t.Parallel()

	spec := read(t, specPath)
	vocab, err := docsguard.ParseSpecVocabulary(spec)
	if err != nil {
		t.Fatalf("ParseSpecVocabulary: %v", err)
	}
	if err := docsguard.CheckDocumentEvents(spec, vocab); err != nil {
		t.Fatal(err)
	}
	if names := docsguard.DocumentEventNames(spec, vocab); len(names) < len(vocab)/2 {
		t.Fatalf("the document mentions %d event names, which is too few to be checking anything", len(names))
	}
}

func TestSpec_EventVocabularyMatchesPlan(t *testing.T) {
	t.Parallel()

	spec, err := docsguard.ParseSpecVocabulary(read(t, specPath))
	if err != nil {
		t.Fatalf("ParseSpecVocabulary: %v", err)
	}
	plan, err := docsguard.ParsePlanVocabulary(read(t, planPath))
	if err != nil {
		t.Fatalf("ParsePlanVocabulary: %v", err)
	}
	if err := docsguard.CheckVocabulary(spec, plan); err != nil {
		t.Fatal(err)
	}
}

func TestSpec_InvariantsComplete(t *testing.T) {
	t.Parallel()

	is, err := docsguard.ParseInvariants(read(t, specPath))
	if err != nil {
		t.Fatalf("ParseInvariants: %v", err)
	}
	if err := docsguard.CheckInvariants(is); err != nil {
		t.Fatal(err)
	}
	if len(is) != 11 {
		t.Fatalf("docs/SEMANTICS.md S15 holds %d invariants, want the eleven of PLAN.md section 5.2", len(is))
	}
}

// TestSpec_ErrorOutcomesKnown is the cross-check the merged sentinel set makes cheap: the
// document and internal/errs must describe the same closed set, in both directions.
func TestSpec_ErrorOutcomesKnown(t *testing.T) {
	t.Parallel()

	outcomes, err := docsguard.ParseErrorOutcomes(read(t, specPath))
	if err != nil {
		t.Fatalf("ParseErrorOutcomes: %v", err)
	}
	if err := docsguard.CheckErrorOutcomes(outcomes, errs.All()); err != nil {
		t.Fatal(err)
	}
}

func TestSpec_BoundsRegisterComplete(t *testing.T) {
	t.Parallel()

	bs, err := docsguard.ParseBounds(read(t, specPath))
	if err != nil {
		t.Fatalf("ParseBounds: %v", err)
	}
	if err := docsguard.CheckBounds(bs); err != nil {
		t.Fatal(err)
	}
}

func TestADR_WellFormed(t *testing.T) {
	t.Parallel()

	adrs, err := docsguard.ParseADRs(adrDir)
	if err != nil {
		t.Fatalf("ParseADRs: %v", err)
	}
	if err := docsguard.CheckADRs(adrs); err != nil {
		t.Fatal(err)
	}
}

func TestADR_EveryDecisionClaimed(t *testing.T) {
	t.Parallel()

	adrs, err := docsguard.ParseADRs(adrDir)
	if err != nil {
		t.Fatalf("ParseADRs: %v", err)
	}
	decisions, err := docsguard.ParsePlanDecisions(read(t, planPath))
	if err != nil {
		t.Fatalf("ParsePlanDecisions: %v", err)
	}
	if err := docsguard.CheckDecisionsClaimed(adrs, decisions); err != nil {
		t.Fatal(err)
	}
}

func TestDocs_NoBrokenRelativeLinks(t *testing.T) {
	t.Parallel()

	files, err := docsguard.MarkdownFiles(repoRoot)
	if err != nil {
		t.Fatalf("MarkdownFiles: %v", err)
	}
	if len(files) < 5 {
		t.Fatalf("found %d markdown files under %s, which cannot be right", len(files), repoRoot)
	}
	for _, f := range files {
		broken, err := docsguard.BrokenLinksIn(f)
		if err != nil {
			t.Fatalf("BrokenLinksIn(%s): %v", f, err)
		}
		for _, target := range broken {
			t.Errorf("%s links to %s, which does not exist", f, target)
		}
	}
}

// TestDocsguard_CatchesSabotage is the negative half. Each fixture breaks one rule, and the
// checker that owns that rule must reject it. A refactor that stops catching one of these fails
// here rather than silently letting the documents rot.
func TestDocsguard_CatchesSabotage(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name  string
		check func(t *testing.T) error
	}{
		{"duplicate transition ID", func(t *testing.T) error {
			return docsguard.CheckTransitions(sabotageTransitions(t, "spec-duplicate-transition.md"))
		}},
		{"missing transition row", func(t *testing.T) error {
			return docsguard.CheckTransitions(sabotageTransitions(t, "spec-missing-transition.md"))
		}},
		{"unknown event name", func(t *testing.T) error {
			vocab, err := docsguard.ParseSpecVocabulary(read(t, specPath))
			if err != nil {
				t.Fatalf("ParseSpecVocabulary: %v", err)
			}
			return docsguard.CheckTransitionEvents(sabotageTransitions(t, "spec-unknown-event.md"), vocab)
		}},
		{"unknown event name in prose", func(t *testing.T) error {
			vocab, err := docsguard.ParseSpecVocabulary(read(t, specPath))
			if err != nil {
				t.Fatalf("ParseSpecVocabulary: %v", err)
			}
			return docsguard.CheckDocumentEvents(read(t, fixture("spec-unknown-event-in-prose.md")), vocab)
		}},
		{"missing invariant row", func(t *testing.T) error {
			is, err := docsguard.ParseInvariants(read(t, fixture("spec-missing-invariant.md")))
			if err != nil {
				t.Fatalf("ParseInvariants: %v", err)
			}
			return docsguard.CheckInvariants(is)
		}},
		{"undocumented sentinel", func(t *testing.T) error {
			outcomes, err := docsguard.ParseErrorOutcomes(read(t, fixture("spec-unknown-sentinel.md")))
			if err != nil {
				t.Fatalf("ParseErrorOutcomes: %v", err)
			}
			return docsguard.CheckErrorOutcomes(outcomes, errs.All())
		}},
		{"event vocabulary drift", func(t *testing.T) error {
			spec, err := docsguard.ParseSpecVocabulary(read(t, fixture("spec-vocabulary-drift.md")))
			if err != nil {
				t.Fatalf("ParseSpecVocabulary: %v", err)
			}
			plan, err := docsguard.ParsePlanVocabulary(read(t, planPath))
			if err != nil {
				t.Fatalf("ParsePlanVocabulary: %v", err)
			}
			return docsguard.CheckVocabulary(spec, plan)
		}},
		{"ADR number gap", func(t *testing.T) error {
			return docsguard.CheckADRs(sabotageADRs(t, "adr-gap"))
		}},
		{"ADR missing heading", func(t *testing.T) error {
			return docsguard.CheckADRs(sabotageADRs(t, "adr-missing-heading"))
		}},
		{"decision claimed twice", func(t *testing.T) error {
			return docsguard.CheckDecisionsClaimed(sabotageADRs(t, "adr-double-claim"), []int{7})
		}},
		{"decision unclaimed", func(t *testing.T) error {
			return docsguard.CheckDecisionsClaimed(sabotageADRs(t, "adr-gap"), []int{7})
		}},
		{"broken relative link", func(t *testing.T) error {
			broken, err := docsguard.BrokenLinksIn(fixture("spec-broken-link.md"))
			if err != nil {
				t.Fatalf("BrokenLinksIn: %v", err)
			}
			if len(broken) == 0 {
				return nil
			}
			return errBroken
		}},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()

			if err := c.check(t); err == nil {
				t.Fatalf("the %s fixture was accepted; the checker does not bite", c.name)
			}
		})
	}
}

// errBroken lets the link fixture report through the same error-or-nil shape as every other row.
var errBroken = os.ErrNotExist

func fixture(name string) string { return filepath.Join("testdata", name) }

func sabotageTransitions(t *testing.T, name string) []docsguard.Transition {
	t.Helper()
	ts, err := docsguard.ParseTransitions(read(t, fixture(name)))
	if err != nil {
		t.Fatalf("ParseTransitions(%s): %v", name, err)
	}
	return ts
}

func sabotageADRs(t *testing.T, dir string) []docsguard.ADR {
	t.Helper()
	adrs, err := docsguard.ParseADRs(fixture(dir))
	if err != nil {
		t.Fatalf("ParseADRs(%s): %v", dir, err)
	}
	return adrs
}
