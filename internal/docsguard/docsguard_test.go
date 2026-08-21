// SPDX-License-Identifier: Apache-2.0

package docsguard_test

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/a-holm/messq/internal/docsguard"
	"github.com/a-holm/messq/internal/errs"
)

const (
	repoRoot = "../.."
	specPath = repoRoot + "/docs/SEMANTICS.md"
	planPath = repoRoot + "/docs/PLAN.md"
	adrDir   = repoRoot + "/docs/adr"
	errsPath = repoRoot + "/internal/errs/errs.go"
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

func planTransitions(t *testing.T) []docsguard.Transition {
	t.Helper()
	ts, err := docsguard.ParsePlanTransitions(read(t, planPath))
	if err != nil {
		t.Fatalf("ParsePlanTransitions: %v", err)
	}
	return ts
}

func clarifications(t *testing.T) []docsguard.Clarification {
	t.Helper()
	cs, err := docsguard.ParseClarifications(read(t, specPath))
	if err != nil {
		t.Fatalf("ParseClarifications: %v", err)
	}
	return cs
}

// TestSpec_TransitionTableMirrorsPlan is the check PLAN.md section 11.1 depends on: the
// conformance suite mirrors section 5.1 one row to one row, and it does that by mirroring this
// document, so the two tables must carry the same IDs in the same order.
func TestSpec_TransitionTableMirrorsPlan(t *testing.T) {
	t.Parallel()

	if err := docsguard.CheckMirrorsPlan(transitions(t), planTransitions(t)); err != nil {
		t.Fatal(err)
	}
}

// TestSpec_TransitionEventsMirrorPlan pins each event to the transition PLAN.md gives it.
// Moving one from T4 to T8 leaves both tables well formed and every name in the vocabulary.
func TestSpec_TransitionEventsMirrorPlan(t *testing.T) {
	t.Parallel()

	if err := docsguard.CheckEventsMirrorPlan(transitions(t), planTransitions(t)); err != nil {
		t.Fatal(err)
	}
}

// TestSpec_NoPlanSymbolIsDroppedSilently is what makes a flipped comparison or a deleted bound a
// build failure. A symbol may leave a cell only when an S1.5 entry citing that transition quotes
// it, which is S1.4's "nothing is resolved silently" made mechanical.
func TestSpec_NoPlanSymbolIsDroppedSilently(t *testing.T) {
	t.Parallel()

	if err := docsguard.CheckNoDroppedSymbols(transitions(t), planTransitions(t), clarifications(t)); err != nil {
		t.Fatal(err)
	}
}

// TestSpec_InvariantStatementsAreVerbatim catches a swapped pair of rows, which leaves the IDs
// contiguous and every cell populated while I5 stops saying what I5 says.
func TestSpec_InvariantStatementsAreVerbatim(t *testing.T) {
	t.Parallel()

	spec, err := docsguard.ParseInvariants(read(t, specPath))
	if err != nil {
		t.Fatalf("ParseInvariants: %v", err)
	}
	plan, err := docsguard.ParsePlanInvariants(read(t, planPath))
	if err != nil {
		t.Fatalf("ParsePlanInvariants: %v", err)
	}
	if err := docsguard.CheckInvariantStatements(spec, plan); err != nil {
		t.Fatal(err)
	}
}

// TestDocs_NoDanglingCrossReferences resolves every S, A, T, I and C citation in the
// specification and in every ADR against what the specification defines. Renumbering a section
// without fixing its callers is the failure this exists for.
func TestDocs_NoDanglingCrossReferences(t *testing.T) {
	t.Parallel()

	spec := read(t, specPath)
	idx, err := docsguard.BuildIDIndex(spec)
	if err != nil {
		t.Fatalf("BuildIDIndex: %v", err)
	}
	if refErr := docsguard.CheckReferences(specPath, spec, idx); refErr != nil {
		t.Error(refErr)
	}

	adrs, err := docsguard.ParseADRs(adrDir)
	if err != nil {
		t.Fatalf("ParseADRs: %v", err)
	}
	for _, a := range adrs {
		if refErr := docsguard.CheckReferences(a.Path, read(t, a.Path), idx); refErr != nil {
			t.Error(refErr)
		}
	}
}

// TestSentinelRegistryMatchesSource keeps errs.All() equal to what internal/errs declares. A
// sentinel added to the var block but never to the registry would otherwise be invisible to
// every mapping test that iterates All.
func TestSentinelRegistryMatchesSource(t *testing.T) {
	t.Parallel()

	declared, err := docsguard.ParseSentinels(errsPath)
	if err != nil {
		t.Fatalf("ParseSentinels: %v", err)
	}
	registered := errs.All()
	if len(declared) != len(registered) {
		t.Fatalf("internal/errs declares %d sentinels and registers %d", len(declared), len(registered))
	}
	for i := range declared {
		if declared[i].Message != registered[i].Error() {
			t.Errorf("sentinel %d: source declares %q, All() returns %q",
				i+1, declared[i].Message, registered[i].Error())
		}
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
// document and internal/errs must describe the same closed set, sentinel by sentinel, message by
// message, in declaration order.
func TestSpec_ErrorOutcomesKnown(t *testing.T) {
	t.Parallel()

	outcomes, err := docsguard.ParseErrorOutcomes(read(t, specPath))
	if err != nil {
		t.Fatalf("ParseErrorOutcomes: %v", err)
	}
	sentinels, err := docsguard.ParseSentinels(errsPath)
	if err != nil {
		t.Fatalf("ParseSentinels: %v", err)
	}
	if err := docsguard.CheckErrorOutcomes(outcomes, sentinels); err != nil {
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
		want  string
		check func(t *testing.T) error
	}{
		{"duplicate transition ID", "appears more than once", func(t *testing.T) error {
			return docsguard.CheckTransitions(sabotageTransitions(t, "spec-duplicate-transition.md"))
		}},
		{"missing transition row", "the table must be contiguous", func(t *testing.T) error {
			return docsguard.CheckTransitions(sabotageTransitions(t, "spec-missing-transition.md"))
		}},
		{"unknown event name", "not in the closed event vocabulary", func(t *testing.T) error {
			vocab, err := docsguard.ParseSpecVocabulary(read(t, specPath))
			if err != nil {
				t.Fatalf("ParseSpecVocabulary: %v", err)
			}
			return docsguard.CheckTransitionEvents(sabotageTransitions(t, "spec-unknown-event.md"), vocab)
		}},
		{"unknown event name in prose", "not in the closed event vocabulary", func(t *testing.T) error {
			return docsguard.CheckDocumentEvents(read(t, fixture("spec-unknown-event-in-prose.md")), vocabulary(t))
		}},
		// Pins the underscore in the candidate filter. Every merged event name that carries one,
		// msg.ack_dup and msg.ack_stale, is in the vocabulary, so narrowing the filter to
		// letters only would break nothing else and would stop catching this class.
		{"unknown event name with an underscore", "not in the closed event vocabulary", func(t *testing.T) error {
			return docsguard.CheckDocumentEvents(read(t, fixture("spec-underscore-event.md")), vocabulary(t))
		}},
		{"missing invariant row", "the register must be contiguous", func(t *testing.T) error {
			is, err := docsguard.ParseInvariants(read(t, fixture("spec-missing-invariant.md")))
			if err != nil {
				t.Fatalf("ParseInvariants: %v", err)
			}
			return docsguard.CheckInvariants(is)
		}},
		{"undocumented sentinel", "internal/errs declares", func(t *testing.T) error {
			return docsguard.CheckErrorOutcomes(sabotageOutcomes(t, "spec-unknown-sentinel.md"), sentinels(t))
		}},
		{"sentinel paired with another sentinel's message", "is documented as", func(t *testing.T) error {
			return docsguard.CheckErrorOutcomes(sabotageOutcomes(t, "spec-swapped-sentinel-message.md"), sentinels(t))
		}},
		{"flipped comparison in a guard cell", "drops", func(t *testing.T) error {
			return docsguard.CheckNoDroppedSymbols(
				sabotageTransitions(t, "spec-flipped-guard.md"), planTransitions(t), clarifications(t))
		}},
		{"column name deleted from an effect cell", "drops", func(t *testing.T) error {
			return docsguard.CheckNoDroppedSymbols(
				sabotageTransitions(t, "spec-dropped-symbol.md"), planTransitions(t), clarifications(t))
		}},
		{"event moved to the wrong transition", "PLAN.md section 5.1 gives it", func(t *testing.T) error {
			return docsguard.CheckEventsMirrorPlan(sabotageTransitions(t, "spec-moved-event.md"), planTransitions(t))
		}},
		{"swapped invariant rows", "does not restate PLAN.md section 5.2 verbatim", func(t *testing.T) error {
			is, err := docsguard.ParseInvariants(read(t, fixture("spec-swapped-invariants.md")))
			if err != nil {
				t.Fatalf("ParseInvariants: %v", err)
			}
			plan, err := docsguard.ParsePlanInvariants(read(t, planPath))
			if err != nil {
				t.Fatalf("ParsePlanInvariants: %v", err)
			}
			return docsguard.CheckInvariantStatements(is, plan)
		}},
		{"dangling cross-reference", "does not define", func(t *testing.T) error {
			idx, err := docsguard.BuildIDIndex(read(t, specPath))
			if err != nil {
				t.Fatalf("BuildIDIndex: %v", err)
			}
			return docsguard.CheckReferences("fixture", read(t, fixture("spec-dangling-reference.md")), idx)
		}},
		{"event vocabulary drift", "event vocabulary", func(t *testing.T) error {
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
		{"ADR number gap", "numbering must be contiguous", func(t *testing.T) error {
			return docsguard.CheckADRs(sabotageADRs(t, "adr-gap"))
		}},
		{"ADR missing heading", "is missing the heading", func(t *testing.T) error {
			return docsguard.CheckADRs(sabotageADRs(t, "adr-missing-heading"))
		}},
		{"decision claimed twice", "is claimed by both", func(t *testing.T) error {
			return docsguard.CheckDecisionsClaimed(sabotageADRs(t, "adr-double-claim"), []int{7})
		}},
		{"decision unclaimed", "has no ADR", func(t *testing.T) error {
			return docsguard.CheckDecisionsClaimed(sabotageADRs(t, "adr-gap"), []int{7})
		}},
		{"broken relative link", "file does not exist", func(t *testing.T) error {
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

			err := c.check(t)
			if err == nil {
				t.Fatalf("the %s fixture was accepted; the checker does not bite", c.name)
			}
			// Asserting the message, not only the failure: a fixture that goes stale and
			// trips a different checker would otherwise still look like a working gate.
			if !strings.Contains(err.Error(), c.want) {
				t.Fatalf("the %s fixture failed with %q, which does not mention %q", c.name, err, c.want)
			}
		})
	}
}

// errBroken lets the link fixture report through the same error-or-nil shape as every other row.
var errBroken = errors.New("file does not exist")

func fixture(name string) string { return filepath.Join("testdata", name) }

func vocabulary(t *testing.T) []string {
	t.Helper()
	v, err := docsguard.ParseSpecVocabulary(read(t, specPath))
	if err != nil {
		t.Fatalf("ParseSpecVocabulary: %v", err)
	}
	return v
}

func sentinels(t *testing.T) []docsguard.Sentinel {
	t.Helper()
	s, err := docsguard.ParseSentinels(errsPath)
	if err != nil {
		t.Fatalf("ParseSentinels: %v", err)
	}
	return s
}

func sabotageOutcomes(t *testing.T, name string) []docsguard.ErrorOutcome {
	t.Helper()
	out, err := docsguard.ParseErrorOutcomes(read(t, fixture(name)))
	if err != nil {
		t.Fatalf("ParseErrorOutcomes(%s): %v", name, err)
	}
	return out
}

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
