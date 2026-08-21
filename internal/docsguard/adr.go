// SPDX-License-Identifier: Apache-2.0

package docsguard

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
)

// requiredHeadings are the second-level headings every record carries, in order. A record may
// add its own headings between them; it may not drop one of these.
var requiredHeadings = []string{"Context", "Decision", "Alternatives", "Consequences", "Revisit trigger"}

var (
	adrFileRE     = regexp.MustCompile(`^(\d{4})-[a-z0-9]+(?:-[a-z0-9]+)*\.md$`)
	adrTitleRE    = regexp.MustCompile(`^#\s+(\d{4})\.\s+(\S.*)$`)
	adrFieldRE    = regexp.MustCompile(`^-\s+([A-Za-z][A-Za-z ]*):\s*(.*)$`)
	adrDateRE     = regexp.MustCompile(`^\d{4}-\d{2}-\d{2}$`)
	supersededRE  = regexp.MustCompile(`^Superseded by (\d{4})$`)
	plainStatuses = map[string]bool{"Proposed": true, "Accepted": true, "Deprecated": true}
)

// ADR is one architecture decision record.
type ADR struct {
	Number      int
	Title       string
	Status      string
	Date        string
	Adjudicates string
	RelatesTo   string
	Headings    []string
	Path        string
}

// ParseADRs reads every record in dir. The template and the index are not records and are
// skipped; anything else that does not match the filename pattern is an error, because a record
// nobody can link to is worse than no record.
func ParseADRs(dir string) ([]ADR, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}

	var out []ADR
	for _, e := range entries {
		name := e.Name()
		if e.IsDir() || !strings.HasSuffix(name, ".md") || name == "README.md" || name == "0000-template.md" {
			continue
		}
		m := adrFileRE.FindStringSubmatch(name)
		if m == nil {
			return nil, fmt.Errorf("docsguard: %s does not match NNNN-kebab-case-title.md", name)
		}
		number, err := strconv.Atoi(m[1])
		if err != nil {
			return nil, fmt.Errorf("docsguard: %s: %w", name, err)
		}

		path := filepath.Join(dir, name)
		body, err := os.ReadFile(path)
		if err != nil {
			return nil, err
		}
		adr, err := parseADR(path, body)
		if err != nil {
			return nil, err
		}
		if adr.Number != number {
			return nil, fmt.Errorf("docsguard: %s is titled %04d, which does not match its filename", name, adr.Number)
		}
		out = append(out, adr)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Number < out[j].Number })
	return out, nil
}

func parseADR(path string, body []byte) (ADR, error) {
	adr := ADR{Path: path}
	fieldsDone := false
	for _, line := range strings.Split(string(body), "\n") {
		if m := adrTitleRE.FindStringSubmatch(line); m != nil && adr.Title == "" {
			n, err := strconv.Atoi(m[1])
			if err != nil {
				return ADR{}, fmt.Errorf("docsguard: %s: %w", path, err)
			}
			adr.Number, adr.Title = n, strings.TrimSpace(m[2])
			continue
		}
		if h := headingRE.FindStringSubmatch(line); h != nil {
			if len(h[1]) == 2 {
				adr.Headings = append(adr.Headings, strings.TrimSpace(h[2]))
			}
			fieldsDone = true
			continue
		}
		if fieldsDone {
			continue
		}
		m := adrFieldRE.FindStringSubmatch(line)
		if m == nil {
			continue
		}
		value := strings.TrimSpace(m[2])
		switch strings.TrimSpace(m[1]) {
		case "Status":
			adr.Status = value
		case "Date":
			adr.Date = value
		case "Adjudicates":
			adr.Adjudicates = value
		case "Relates to":
			adr.RelatesTo = value
		}
	}
	if adr.Title == "" {
		return ADR{}, fmt.Errorf("docsguard: %s has no `# NNNN. Title` heading", path)
	}
	return adr, nil
}

// CheckADRs verifies the shape of the register: contiguous numbering from 0001, a valid status,
// an ISO date, an Adjudicates field, a Relates to field, and the required headings in order.
func CheckADRs(adrs []ADR) error {
	if len(adrs) == 0 {
		return errors.New("docsguard: no ADRs found")
	}
	numbers := map[int]bool{}
	for _, a := range adrs {
		if numbers[a.Number] {
			return fmt.Errorf("docsguard: ADR number %04d is used twice", a.Number)
		}
		numbers[a.Number] = true
	}
	for n := 1; n <= len(adrs); n++ {
		if !numbers[n] {
			return fmt.Errorf("docsguard: ADR %04d is missing; numbering must be contiguous from 0001", n)
		}
	}

	for _, a := range adrs {
		if err := checkADRFields(a); err != nil {
			return err
		}
		if err := checkADRHeadings(a); err != nil {
			return err
		}
		if m := supersededRE.FindStringSubmatch(a.Status); m != nil {
			target, err := strconv.Atoi(m[1])
			if err != nil {
				return fmt.Errorf("docsguard: %s: %w", a.Path, err)
			}
			if !numbers[target] {
				return fmt.Errorf("docsguard: %s is superseded by %04d, which does not exist", a.Path, target)
			}
		}
	}
	return nil
}

func checkADRFields(a ADR) error {
	if !plainStatuses[a.Status] && !supersededRE.MatchString(a.Status) {
		return fmt.Errorf("docsguard: %s has status %q, want Proposed, Accepted, Deprecated or Superseded by NNNN",
			a.Path, a.Status)
	}
	if !adrDateRE.MatchString(a.Date) {
		return fmt.Errorf("docsguard: %s has date %q, want YYYY-MM-DD", a.Path, a.Date)
	}
	if a.Adjudicates == "" {
		return fmt.Errorf("docsguard: %s has no Adjudicates field; use a D-number or none", a.Path)
	}
	if a.Adjudicates != "none" && !decisionRE.MatchString(a.Adjudicates) {
		return fmt.Errorf("docsguard: %s adjudicates %q, want a D-number or none", a.Path, a.Adjudicates)
	}
	if a.RelatesTo == "" {
		return fmt.Errorf("docsguard: %s has no Relates to field", a.Path)
	}
	return nil
}

func checkADRHeadings(a ADR) error {
	next := 0
	for _, h := range a.Headings {
		if next < len(requiredHeadings) && h == requiredHeadings[next] {
			next++
		}
	}
	if next != len(requiredHeadings) {
		return fmt.Errorf("docsguard: %s is missing the heading %q, or carries it out of order; want %v",
			a.Path, requiredHeadings[next], requiredHeadings)
	}
	return nil
}

// CheckDecisionsClaimed verifies that every decision PLAN.md section 2 adjudicates is claimed by
// exactly one record, and that no record claims a decision the plan does not have.
func CheckDecisionsClaimed(adrs []ADR, decisions []int) error {
	claims := map[int]string{}
	for _, a := range adrs {
		if a.Adjudicates == "none" {
			continue
		}
		m := decisionRE.FindStringSubmatch(a.Adjudicates)
		if m == nil {
			return fmt.Errorf("docsguard: %s adjudicates %q, want a D-number or none", a.Path, a.Adjudicates)
		}
		n, err := strconv.Atoi(m[1])
		if err != nil {
			return fmt.Errorf("docsguard: %s: %w", a.Path, err)
		}
		if prev, dup := claims[n]; dup {
			return fmt.Errorf("docsguard: D%d is claimed by both %s and %s", n, prev, a.Path)
		}
		claims[n] = a.Path
	}

	declared := map[int]bool{}
	for _, d := range decisions {
		declared[d] = true
		if _, ok := claims[d]; !ok {
			return fmt.Errorf("docsguard: D%d has no ADR; every decision of PLAN.md section 2 needs one", d)
		}
	}
	for n, path := range claims {
		if !declared[n] {
			return fmt.Errorf("docsguard: %s claims D%d, which PLAN.md section 2 does not declare", path, n)
		}
	}
	return nil
}
