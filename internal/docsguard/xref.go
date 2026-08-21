// SPDX-License-Identifier: Apache-2.0

package docsguard

import (
	"fmt"
	"regexp"
	"sort"
	"strconv"
	"strings"
)

// IDIndex is everything docs/SEMANTICS.md defines that another document may cite.
type IDIndex struct {
	Sections       map[string]bool // S6, S6.1, A1
	Transitions    map[string]bool // T1, T3a
	Invariants     map[string]bool // I1
	Clarifications map[string]bool // C1
	ReservedFromI  int             // the first invariant number declared reserved, 0 when none
}

var (
	sectionHeadRE = regexp.MustCompile(`^([SA]\d+(?:\.\d+)*)[.\s]`)
	sectionCiteRE = regexp.MustCompile(`\b([SA]\d+(?:\.\d+)*)\b`)
	transCiteRE   = regexp.MustCompile(`\b(T\d+[a-z]?)\b`)
	invCiteRE     = regexp.MustCompile(`\b(I\d+)\b`)
	clarCiteRE    = regexp.MustCompile(`\b(C\d+)\b`)
	// grammarRuleRE removes the subject-grammar rule IDs before anything is resolved. They live
	// in docs/generated/subject-rules.md and collide with this document's section numbers, which
	// is why S1.3 requires them to be written "grammar rule S3".
	grammarRuleRE = regexp.MustCompile(`grammar rules? S\d+(?: through S\d+)?`)
)

// BuildIDIndex reads every ID docs/SEMANTICS.md defines.
func BuildIDIndex(md []byte) (IDIndex, error) {
	idx := IDIndex{
		Sections:       map[string]bool{},
		Transitions:    map[string]bool{},
		Invariants:     map[string]bool{},
		Clarifications: map[string]bool{},
	}

	for _, line := range strings.Split(string(md), "\n") {
		h := headingRE.FindStringSubmatch(line)
		if h == nil {
			continue
		}
		if m := sectionHeadRE.FindStringSubmatch(h[2] + " "); m != nil {
			idx.Sections[m[1]] = true
		}
	}

	transitions, err := ParseTransitions(md)
	if err != nil {
		return IDIndex{}, err
	}
	for _, t := range transitions {
		idx.Transitions[t.ID] = true
	}

	invariants, err := ParseInvariants(md)
	if err != nil {
		return IDIndex{}, err
	}
	highest := 0
	for _, i := range invariants {
		idx.Invariants[i.ID] = true
		if n, convErr := strconv.Atoi(strings.TrimPrefix(i.ID, "I")); convErr == nil && n > highest {
			highest = n
		}
	}
	idx.ReservedFromI = highest + 1

	clarifications, err := ParseClarifications(md)
	if err != nil {
		return IDIndex{}, err
	}
	for _, c := range clarifications {
		idx.Clarifications[c.ID] = true
	}
	return idx, nil
}

// DanglingReferences returns every ID cited in md that the index does not define. Invariant
// numbers at or above the reserved floor are references to the reserved space, not dangling.
func DanglingReferences(md []byte, idx IDIndex) []string {
	body := grammarRuleRE.ReplaceAllString(stripFences(string(md)), " ")

	bad := map[string]bool{}
	for _, m := range sectionCiteRE.FindAllStringSubmatch(body, -1) {
		if !idx.Sections[m[1]] {
			bad[m[1]] = true
		}
	}
	for _, m := range transCiteRE.FindAllStringSubmatch(body, -1) {
		if !idx.Transitions[m[1]] {
			bad[m[1]] = true
		}
	}
	for _, m := range clarCiteRE.FindAllStringSubmatch(body, -1) {
		if !idx.Clarifications[m[1]] {
			bad[m[1]] = true
		}
	}
	for _, m := range invCiteRE.FindAllStringSubmatch(body, -1) {
		if idx.Invariants[m[1]] {
			continue
		}
		n, err := strconv.Atoi(strings.TrimPrefix(m[1], "I"))
		if err == nil && idx.ReservedFromI > 0 && n >= idx.ReservedFromI {
			continue
		}
		bad[m[1]] = true
	}

	out := make([]string, 0, len(bad))
	for id := range bad {
		out = append(out, id)
	}
	sort.Strings(out)
	return out
}

// CheckReferences is DanglingReferences as a predicate, so a caller can report one document.
func CheckReferences(path string, md []byte, idx IDIndex) error {
	if bad := DanglingReferences(md, idx); len(bad) > 0 {
		return fmt.Errorf("docsguard: %s cites %s, which docs/SEMANTICS.md does not define",
			path, strings.Join(bad, ", "))
	}
	return nil
}
