// SPDX-License-Identifier: Apache-2.0

package docsguard

import (
	"errors"
	"fmt"
	"regexp"
	"strconv"
	"strings"
)

// Section headings this package addresses by number. They are constants rather than literals at
// the call sites so that renumbering the specification breaks one line per table.
const (
	transitionHeading = "S6.1"
	invariantHeading  = "S15."
	errorHeading      = "S13."
	boundsHeading     = "A1."
	specVocabHeading  = "S2.4"
	planVocabHeading  = "9.2 Event vocabulary"
)

// Transition is one row of the delivery state machine, docs/SEMANTICS.md S6.1.
type Transition struct {
	ID        string
	From      string
	Trigger   string
	Guard     string
	Effect    string
	Event     string
	OnFailure string
}

// Events returns the event names the transition emits, taken from the backticked spans of the
// Event cell. A cell that emits nothing yields no names.
func (t Transition) Events() []string { return ticked(t.Event) }

// Invariant is one row of the invariant register, docs/SEMANTICS.md S15.
type Invariant struct {
	ID              string
	Statement       string
	Predicate       string
	CheckedBy       string
	FirstRequiredAt string
}

// ErrorOutcome is one row of the semantic outcome table, docs/SEMANTICS.md S13.
type ErrorOutcome struct {
	Sentinel string
	Message  string
	RaisedBy string
}

// Bound is one row of the bounds register, docs/SEMANTICS.md A1. It is the concrete form of
// invariant I11, which has no runtime predicate.
type Bound struct {
	Name    string
	SetBy   string
	Default string
	Owner   string
}

var (
	transitionIDRE = regexp.MustCompile(`^T(\d+)([a-z]?)$`)
	invariantIDRE  = regexp.MustCompile(`^I(\d+)$`)
	sentinelRE     = regexp.MustCompile(`^errs\.Err[A-Za-z]+$`)
	decisionRE     = regexp.MustCompile(`^D(\d+)$`)
)

// ParseTransitions reads the S6.1 table. Issue #13 imports it so that the conformance suite can
// assert every transition ID appears in at least one test name.
func ParseTransitions(md []byte) ([]Transition, error) {
	t, err := firstTableAfter(md, transitionHeading)
	if err != nil {
		return nil, err
	}
	if len(t.header) != 7 {
		return nil, fmt.Errorf("docsguard: %s table has %d columns, want 7", transitionHeading, len(t.header))
	}
	out := make([]Transition, 0, len(t.rows))
	for _, r := range t.rows {
		out = append(out, Transition{
			ID: untick(r[0]), From: r[1], Trigger: r[2], Guard: r[3],
			Effect: r[4], Event: r[5], OnFailure: r[6],
		})
	}
	return out, nil
}

// ParseInvariants reads the S15 table.
func ParseInvariants(md []byte) ([]Invariant, error) {
	t, err := firstTableAfter(md, invariantHeading)
	if err != nil {
		return nil, err
	}
	if len(t.header) != 5 {
		return nil, fmt.Errorf("docsguard: %s table has %d columns, want 5", invariantHeading, len(t.header))
	}
	out := make([]Invariant, 0, len(t.rows))
	for _, r := range t.rows {
		out = append(out, Invariant{
			ID: untick(r[0]), Statement: r[1], Predicate: r[2],
			CheckedBy: r[3], FirstRequiredAt: r[4],
		})
	}
	return out, nil
}

// ParseErrorOutcomes reads the S13 table.
func ParseErrorOutcomes(md []byte) ([]ErrorOutcome, error) {
	t, err := firstTableAfter(md, errorHeading)
	if err != nil {
		return nil, err
	}
	if len(t.header) != 3 {
		return nil, fmt.Errorf("docsguard: %s table has %d columns, want 3", errorHeading, len(t.header))
	}
	out := make([]ErrorOutcome, 0, len(t.rows))
	for _, r := range t.rows {
		out = append(out, ErrorOutcome{Sentinel: untick(r[0]), Message: r[1], RaisedBy: r[2]})
	}
	return out, nil
}

// ParseBounds reads the A1 bounds register.
func ParseBounds(md []byte) ([]Bound, error) {
	t, err := firstTableAfter(md, boundsHeading)
	if err != nil {
		return nil, err
	}
	if len(t.header) != 4 {
		return nil, fmt.Errorf("docsguard: %s table has %d columns, want 4", boundsHeading, len(t.header))
	}
	out := make([]Bound, 0, len(t.rows))
	for _, r := range t.rows {
		out = append(out, Bound{Name: r[0], SetBy: r[1], Default: r[2], Owner: r[3]})
	}
	return out, nil
}

// ParseSpecVocabulary reads the closed event vocabulary embedded in docs/SEMANTICS.md S2.4.
func ParseSpecVocabulary(md []byte) ([]string, error) {
	lines, err := firstFenceAfter(md, specVocabHeading)
	if err != nil {
		return nil, err
	}
	return fields(lines), nil
}

// ParsePlanVocabulary reads the closed event vocabulary of PLAN.md section 9.2, which is the
// reference the embedded copy must equal.
func ParsePlanVocabulary(md []byte) ([]string, error) {
	lines, err := firstFenceAfter(md, planVocabHeading)
	if err != nil {
		return nil, err
	}
	return fields(lines), nil
}

// CheckTransitions verifies that the transition table is a usable index: IDs unique, base
// numbers contiguous from T1, a suffixed ID only where its base exists, and no empty cell.
func CheckTransitions(ts []Transition) error {
	if len(ts) == 0 {
		return errors.New("docsguard: the transition table is empty")
	}
	seen := map[string]bool{}
	bases := map[int]bool{}
	suffixed := map[string]int{}
	maxBase := 0
	for _, t := range ts {
		m := transitionIDRE.FindStringSubmatch(t.ID)
		if m == nil {
			return fmt.Errorf("docsguard: transition ID %q does not match T<n> or T<n><letter>", t.ID)
		}
		if seen[t.ID] {
			return fmt.Errorf("docsguard: transition %s appears more than once", t.ID)
		}
		seen[t.ID] = true

		n, err := strconv.Atoi(m[1])
		if err != nil {
			return fmt.Errorf("docsguard: transition %s: %w", t.ID, err)
		}
		if m[2] == "" {
			bases[n] = true
			if n > maxBase {
				maxBase = n
			}
		} else {
			suffixed[t.ID] = n
		}

		for name, cell := range map[string]string{
			"From": t.From, "Trigger": t.Trigger, "Guard": t.Guard,
			"Effect": t.Effect, "Event": t.Event, "On guard failure": t.OnFailure,
		} {
			if cell == "" {
				return fmt.Errorf("docsguard: transition %s has an empty %s cell", t.ID, name)
			}
		}
	}
	for n := 1; n <= maxBase; n++ {
		if !bases[n] {
			return fmt.Errorf("docsguard: transition T%d is missing; the table must be contiguous from T1", n)
		}
	}
	for id, n := range suffixed {
		if !bases[n] {
			return fmt.Errorf("docsguard: transition %s has no base row T%d", id, n)
		}
	}
	return nil
}

// CheckTransitionEvents verifies that every event a transition emits is a member of the closed
// vocabulary. An event name invented in the table is the drift this catches.
func CheckTransitionEvents(ts []Transition, vocabulary []string) error {
	known := map[string]bool{}
	for _, v := range vocabulary {
		known[v] = true
	}
	for _, t := range ts {
		for _, e := range t.Events() {
			if !known[e] {
				return fmt.Errorf("docsguard: transition %s emits %q, which is not in the closed event vocabulary", t.ID, e)
			}
		}
	}
	return nil
}

// eventShapeRE matches a backticked span that could be an event name. The namespace decides
// whether it is one; the shape only keeps table names and Go identifiers out of the candidate
// set.
var eventShapeRE = regexp.MustCompile(`^[a-z]+\.[a-z_]+$`)

// DocumentEventNames returns every backticked span anywhere in md whose namespace, meaning the
// text before its first dot, is a namespace of the vocabulary. Deriving the namespaces from the
// vocabulary is what keeps the filter honest: `msg.acked` is a candidate because `msg` is a
// namespace, while `messages.id` and `consumers.generation` never are.
func DocumentEventNames(md []byte, vocabulary []string) []string {
	namespaces := map[string]bool{}
	for _, v := range vocabulary {
		if i := strings.IndexByte(v, '.'); i > 0 {
			namespaces[v[:i]] = true
		}
	}

	var out []string
	for _, span := range ticked(stripFences(string(md))) {
		if !eventShapeRE.MatchString(span) {
			continue
		}
		if namespaces[span[:strings.IndexByte(span, '.')]] {
			out = append(out, span)
		}
	}
	return out
}

// CheckDocumentEvents verifies that every event name the document mentions, in a table or in
// prose, is a member of the closed vocabulary.
func CheckDocumentEvents(md []byte, vocabulary []string) error {
	known := map[string]bool{}
	for _, v := range vocabulary {
		known[v] = true
	}
	for _, name := range DocumentEventNames(md, vocabulary) {
		if !known[name] {
			return fmt.Errorf("docsguard: the document mentions %q, which is not in the closed event vocabulary", name)
		}
	}
	return nil
}

// CheckInvariants verifies that the register is contiguous from I1 and that every cell carries
// something a reader can act on.
func CheckInvariants(is []Invariant) error {
	if len(is) == 0 {
		return errors.New("docsguard: the invariant register is empty")
	}
	seen := map[int]bool{}
	maxN := 0
	for _, inv := range is {
		m := invariantIDRE.FindStringSubmatch(inv.ID)
		if m == nil {
			return fmt.Errorf("docsguard: invariant ID %q does not match I<n>", inv.ID)
		}
		n, err := strconv.Atoi(m[1])
		if err != nil {
			return fmt.Errorf("docsguard: invariant %s: %w", inv.ID, err)
		}
		if seen[n] {
			return fmt.Errorf("docsguard: invariant %s appears more than once", inv.ID)
		}
		seen[n] = true
		if n > maxN {
			maxN = n
		}

		for name, cell := range map[string]string{
			"Statement": inv.Statement, "Predicate": inv.Predicate,
			"Checked by": inv.CheckedBy, "First required at": inv.FirstRequiredAt,
		} {
			if cell == "" {
				return fmt.Errorf("docsguard: invariant %s has an empty %s cell", inv.ID, name)
			}
		}
	}
	for n := 1; n <= maxN; n++ {
		if !seen[n] {
			return fmt.Errorf("docsguard: invariant I%d is missing; the register must be contiguous from I1", n)
		}
	}
	return nil
}

// CheckErrorOutcomes verifies that the S13 table is internal/errs's closed set: the same
// sentinels, in declaration order, each paired with the message it actually carries. Comparing
// the pairing rather than two sets is what stops a row from naming errs.ErrConflict against
// errs.ErrNotFound's message, which reads perfectly well and is wrong.
func CheckErrorOutcomes(outcomes []ErrorOutcome, sentinels []Sentinel) error {
	if len(outcomes) != len(sentinels) {
		return fmt.Errorf("docsguard: the error outcome table has %d rows, internal/errs declares %d sentinels",
			len(outcomes), len(sentinels))
	}
	seen := map[string]bool{}
	for i, o := range outcomes {
		if !sentinelRE.MatchString(o.Sentinel) {
			return fmt.Errorf("docsguard: error outcome %q does not name a sentinel as errs.ErrX", o.Sentinel)
		}
		if o.RaisedBy == "" {
			return fmt.Errorf("docsguard: error outcome %s does not say when it is raised", o.Sentinel)
		}
		if seen[o.Sentinel] {
			return fmt.Errorf("docsguard: error outcome %s has more than one row", o.Sentinel)
		}
		seen[o.Sentinel] = true

		want := "errs." + sentinels[i].Name
		if o.Sentinel != want {
			return fmt.Errorf("docsguard: error outcome row %d names %s; internal/errs declares %s there",
				i+1, o.Sentinel, want)
		}
		if o.Message != sentinels[i].Message {
			return fmt.Errorf("docsguard: %s is documented as %q and carries %q",
				o.Sentinel, o.Message, sentinels[i].Message)
		}
	}
	return nil
}

// CheckVocabulary verifies that the copy embedded in the specification is the plan's list,
// word for word and in the same order.
func CheckVocabulary(spec, plan []string) error {
	if len(spec) == 0 {
		return errors.New("docsguard: the embedded event vocabulary is empty")
	}
	if len(spec) != len(plan) {
		return fmt.Errorf("docsguard: the embedded event vocabulary has %d names, PLAN.md section 9.2 has %d",
			len(spec), len(plan))
	}
	for i := range spec {
		if spec[i] != plan[i] {
			return fmt.Errorf("docsguard: event vocabulary differs at position %d: specification has %q, PLAN.md has %q",
				i, spec[i], plan[i])
		}
	}
	return nil
}

// CheckBounds verifies that every bound names what sets it, a default and the issue that owns
// it. Invariant I11 has no runtime predicate, so this table is what makes it checkable.
func CheckBounds(bs []Bound) error {
	if len(bs) == 0 {
		return errors.New("docsguard: the bounds register is empty")
	}
	for _, b := range bs {
		if b.Name == "" || b.SetBy == "" || b.Default == "" || b.Owner == "" {
			return fmt.Errorf("docsguard: bound %q has an empty cell", b.Name)
		}
		if !strings.HasPrefix(b.Owner, "#") && !strings.Contains(b.Owner, "#") {
			return fmt.Errorf("docsguard: bound %q does not name an owning issue", b.Name)
		}
	}
	return nil
}
