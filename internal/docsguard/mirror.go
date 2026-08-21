// SPDX-License-Identifier: Apache-2.0

package docsguard

import (
	"errors"
	"fmt"
	"regexp"
	"sort"
	"strconv"
	"strings"
)

// Headings on the PLAN.md side. The specification mirrors these three tables, so each one is
// addressed by its plan section number rather than by a literal at the call site.
const (
	planTransHeading = "5.1 Transition rules"
	planInvHeading   = "5.2 Invariants"
	planDecisionRE   = `(?m)^###\s+D(\d+)\s`
)

// Clarification is one row of the register in docs/SEMANTICS.md S1.5. Its whole text is what a
// dropped-symbol check searches, because a register entry that does not quote what it overrides
// has not recorded the override.
type Clarification struct {
	ID   string
	Text string
}

// Cites reports whether the entry names the given transition. The boundary matters: T1 must not
// match inside T11.
func (c Clarification) Cites(transitionID string) bool {
	re := regexp.MustCompile(`\b` + regexp.QuoteMeta(transitionID) + `\b`)
	return re.MatchString(c.Text)
}

// ParseClarifications reads the S1.5 register.
func ParseClarifications(md []byte) ([]Clarification, error) {
	t, err := firstTableAfter(md, "S1.5")
	if err != nil {
		return nil, err
	}
	if len(t.header) != 4 {
		return nil, fmt.Errorf("docsguard: S1.5 table has %d columns, want 4", len(t.header))
	}
	out := make([]Clarification, 0, len(t.rows))
	for _, r := range t.rows {
		out = append(out, Clarification{ID: untick(r[0]), Text: strings.Join(r, " ")})
	}
	return out, nil
}

// ParsePlanTransitions reads PLAN.md section 5.1. The plan's table has no guard-failure column,
// so OnFailure is empty on every row.
func ParsePlanTransitions(md []byte) ([]Transition, error) {
	t, err := firstTableAfter(md, planTransHeading)
	if err != nil {
		return nil, err
	}
	if len(t.header) != 6 {
		return nil, fmt.Errorf("docsguard: PLAN.md section 5.1 table has %d columns, want 6", len(t.header))
	}
	out := make([]Transition, 0, len(t.rows))
	for _, r := range t.rows {
		out = append(out, Transition{
			ID: untick(r[0]), From: r[1], Trigger: r[2], Guard: r[3], Effect: r[4], Event: r[5],
		})
	}
	return out, nil
}

// ParsePlanInvariants reads PLAN.md section 5.2, whose table is ID and statement only.
func ParsePlanInvariants(md []byte) ([]Invariant, error) {
	t, err := firstTableAfter(md, planInvHeading)
	if err != nil {
		return nil, err
	}
	if len(t.header) != 2 {
		return nil, fmt.Errorf("docsguard: PLAN.md section 5.2 table has %d columns, want 2", len(t.header))
	}
	out := make([]Invariant, 0, len(t.rows))
	for _, r := range t.rows {
		out = append(out, Invariant{ID: untick(r[0]), Statement: r[1]})
	}
	return out, nil
}

// ParsePlanDecisions reads the D-numbers PLAN.md section 2 adjudicates, so that the ADR claim
// check is driven by the plan rather than by a constant that can fall behind it.
func ParsePlanDecisions(md []byte) ([]int, error) {
	matches := regexp.MustCompile(planDecisionRE).FindAllStringSubmatch(string(md), -1)
	if len(matches) == 0 {
		return nil, errors.New("docsguard: PLAN.md declares no D-numbered decisions")
	}
	out := make([]int, 0, len(matches))
	for _, m := range matches {
		n, err := strconv.Atoi(m[1])
		if err != nil {
			return nil, fmt.Errorf("docsguard: PLAN.md decision %q: %w", m[1], err)
		}
		out = append(out, n)
	}
	sort.Ints(out)
	return out, nil
}

// CheckMirrorsPlan verifies that the specification's transition table carries the same IDs as
// PLAN.md section 5.1, in the same order.
func CheckMirrorsPlan(spec, plan []Transition) error {
	if len(spec) != len(plan) {
		return fmt.Errorf("docsguard: the specification has %d transitions, PLAN.md section 5.1 has %d",
			len(spec), len(plan))
	}
	for i := range spec {
		if spec[i].ID != plan[i].ID {
			return fmt.Errorf("docsguard: transition %d is %s in the specification and %s in PLAN.md section 5.1",
				i+1, spec[i].ID, plan[i].ID)
		}
	}
	return nil
}

// CheckEventsMirrorPlan verifies that each transition emits exactly the events PLAN.md section
// 5.1 gives it. Moving an event from one transition to another is otherwise invisible: both
// tables stay well formed and every name stays in the vocabulary.
func CheckEventsMirrorPlan(spec, plan []Transition) error {
	if err := CheckMirrorsPlan(spec, plan); err != nil {
		return err
	}
	for i := range spec {
		got, want := spec[i].Events(), plan[i].Events()
		if strings.Join(got, ",") != strings.Join(want, ",") {
			return fmt.Errorf("docsguard: transition %s emits %v, PLAN.md section 5.1 gives it %v",
				spec[i].ID, got, want)
		}
	}
	return nil
}

// symbolRE matches the load-bearing names of a transition cell: a flag, a reserved header, a
// dotted name, a snake_case identifier, or a relational operator. Prose words are deliberately
// not symbols, because no tractable grammar tells a reworded sentence from a changed rule.
var symbolRE = regexp.MustCompile(
	`--[a-z][a-z0-9-]*|messq-[a-z0-9-]+|[a-z][a-z0-9_]*\.[a-z][a-z0-9_]*|[a-z][a-z0-9]*(?:_[a-z0-9]+)+|>=|<=|!=|>|<`)

// unicodeOps is applied before tokenizing so that the plan's mathematical notation and the
// specification's ASCII compare as the same symbols.
// The arrow and the identity sign become whitespace rather than ASCII spellings: "→ DEAD" is
// prose, and rendering it "-> DEAD" would put a relational operator into a cell that has none.
var unicodeOps = strings.NewReplacer(
	"≥", ">=", "≤", "<=", "≠", "!=", "∧", " and ", "∨", " or ", "¬", " not ",
	"→", " ", "≡", " ", "`", " ", "*", " ", "—", " ",
)

// symbols returns the set of load-bearing names in a cell.
func symbols(cell string) map[string]bool {
	normalized := strings.ToLower(unicodeOps.Replace(cell))
	out := map[string]bool{}
	for _, tok := range symbolRE.FindAllString(normalized, -1) {
		// A flag and the setting it writes are the same thing, so --max-ack-wait and
		// max_ack_wait must not read as two different symbols.
		if strings.HasPrefix(tok, "--") {
			tok = strings.ReplaceAll(strings.TrimPrefix(tok, "--"), "-", "_")
		}
		out[tok] = true
	}
	return out
}

// CheckNoDroppedSymbols verifies that nothing PLAN.md section 5.1 names in a Trigger, Guard or
// Effect cell has vanished from the specification's corresponding cell. A symbol may be dropped
// only when a clarification that cites the transition also quotes it, which is what makes an
// override visible instead of silent.
//
// The converse is deliberately not checked. A symbol the specification adds is registered in
// S1.5 by review, because separating a new constraint from a reworded one needs a grammar this
// package does not have. A transition whose symbol is already registered as overridden is
// likewise guarded by review from then on: the register excuses that symbol, not just its first
// removal.
func CheckNoDroppedSymbols(spec, plan []Transition, clarifications []Clarification) error {
	if err := CheckMirrorsPlan(spec, plan); err != nil {
		return err
	}
	for i := range spec {
		for _, col := range []struct{ name, specCell, planCell string }{
			{"Trigger", spec[i].Trigger, plan[i].Trigger},
			{"Guard", spec[i].Guard, plan[i].Guard},
			{"Effect", spec[i].Effect, plan[i].Effect},
		} {
			have := symbols(col.specCell)
			for want := range symbols(col.planCell) {
				if have[want] {
					continue
				}
				if excused(spec[i].ID, want, clarifications) {
					continue
				}
				return fmt.Errorf(
					"docsguard: transition %s drops %q from its %s cell; PLAN.md section 5.1 names it and no S1.5 entry citing %s quotes it",
					spec[i].ID, want, col.name, spec[i].ID)
			}
		}
	}
	return nil
}

// excused reports whether a clarification citing the transition quotes the missing symbol.
func excused(transitionID, symbol string, clarifications []Clarification) bool {
	for _, c := range clarifications {
		if c.Cites(transitionID) && symbols(c.Text)[symbol] {
			return true
		}
	}
	return false
}

// CheckInvariantStatements verifies that the specification's Statement column is PLAN.md section
// 5.2 verbatim. Comparing the text rather than the count is what catches two rows swapped: the
// IDs stay contiguous and every cell stays populated, but I5 stops saying what I5 says.
func CheckInvariantStatements(spec, plan []Invariant) error {
	if len(spec) != len(plan) {
		return fmt.Errorf("docsguard: the specification has %d invariants, PLAN.md section 5.2 has %d",
			len(spec), len(plan))
	}
	for i := range spec {
		if spec[i].ID != plan[i].ID {
			return fmt.Errorf("docsguard: invariant %d is %s in the specification and %s in PLAN.md section 5.2",
				i+1, spec[i].ID, plan[i].ID)
		}
		if collapse(spec[i].Statement) != collapse(plan[i].Statement) {
			return fmt.Errorf("docsguard: %s does not restate PLAN.md section 5.2 verbatim\n  spec: %s\n  plan: %s",
				spec[i].ID, spec[i].Statement, plan[i].Statement)
		}
	}
	return nil
}

// collapse folds runs of whitespace so that a rewrapped line is not a difference.
func collapse(s string) string { return strings.Join(strings.Fields(s), " ") }
