// SPDX-License-Identifier: Apache-2.0

package main

import (
	"bufio"
	"fmt"
	"io"
	"math"
	"regexp"
	"strconv"
	"strings"
)

// floor is one line of coverage.floors: a package path relative to the module root, the
// minimum statement coverage it must reach, and the reason the floor exists.
type floor struct {
	Pkg  string
	Min  float64
	Note string
}

// floorLine matches a floor line and captures the leading field, the number and the tail, so a
// rewrite can replace the number without disturbing the column alignment of the file.
var floorLine = regexp.MustCompile(`^(\s*\S+\s+)([0-9]+(?:\.[0-9]+)?)(\s.*)?$`)

// parseFloors reads a coverage.floors file. Blank lines and lines whose first non-space
// character is '#' are ignored. A duplicate package is an error: two floors for one package
// means one of them is dead, and which one wins would be a parsing accident.
func parseFloors(r io.Reader) ([]floor, error) {
	var floors []floor
	seen := map[string]int{}

	scanner := bufio.NewScanner(r)
	for lineNo := 1; scanner.Scan(); lineNo++ {
		text := scanner.Text()
		if strings.TrimSpace(text) == "" || strings.HasPrefix(strings.TrimSpace(text), "#") {
			continue
		}

		fields := strings.Fields(text)
		if len(fields) < 2 {
			return nil, fmt.Errorf("line %d: want '<package> <floor%%> [note]', got %q", lineNo, text)
		}

		min, err := strconv.ParseFloat(fields[1], 64)
		if err != nil {
			return nil, fmt.Errorf("line %d: floor %q is not a number", lineNo, fields[1])
		}
		if min < 0 || min > 100 {
			return nil, fmt.Errorf("line %d: floor %.1f is outside 0..100", lineNo, min)
		}
		if first, dup := seen[fields[0]]; dup {
			return nil, fmt.Errorf("line %d: package %s already has a floor on line %d", lineNo, fields[0], first)
		}
		seen[fields[0]] = lineNo

		floors = append(floors, floor{
			Pkg:  fields[0],
			Min:  min,
			Note: strings.TrimSpace(strings.Join(fields[2:], " ")),
		})
	}
	if err := scanner.Err(); err != nil {
		return nil, err
	}
	return floors, nil
}

// lowering is a floor that a proposed coverage.floors sets below the value it has in the
// merge base, or drops entirely.
type lowering struct {
	Pkg       string
	From, To  float64
	Removed   bool
	Explained bool
}

func (l lowering) String() string {
	if l.Removed {
		return fmt.Sprintf("%s floor removed (was %.1f); floors ratchet upward only", l.Pkg, l.From)
	}
	return fmt.Sprintf("%s floor lowered %.1f -> %.1f; floors ratchet upward only", l.Pkg, l.From, l.To)
}

// compareFloors reports every floor that base sets higher than proposed, or that proposed
// drops. Raising a floor and adding a package are always allowed.
func compareFloors(base, proposed []floor) []lowering {
	byPkg := make(map[string]floor, len(proposed))
	for _, f := range proposed {
		byPkg[f.Pkg] = f
	}

	var out []lowering
	for _, b := range base {
		p, ok := byPkg[b.Pkg]
		switch {
		case !ok:
			out = append(out, lowering{Pkg: b.Pkg, From: b.Min, Removed: true})
		case p.Min < b.Min:
			out = append(out, lowering{Pkg: b.Pkg, From: b.Min, To: p.Min})
		}
	}
	return out
}

// ratchetSlack is how far actual coverage must exceed a floor before `make cover-ratchet`
// raises it. Without it every run that moves coverage by a tenth of a point would produce a
// churn commit.
const ratchetSlack = 1.0

// raise is one floor that the ratchet moves up.
type raise struct {
	Pkg      string
	From, To float64
}

// ratchet computes the new floors for the packages whose measured coverage clears their floor
// by at least ratchetSlack. New floors are whole percent, rounded down, so a floor is never
// set above a number the suite has actually reached. It never returns a lower floor.
func ratchet(floors []floor, measured map[string]float64) []raise {
	var out []raise
	for _, f := range floors {
		actual, ok := measured[f.Pkg]
		if !ok || actual < f.Min+ratchetSlack {
			continue
		}
		to := math.Floor(actual)
		if to <= f.Min {
			continue
		}
		out = append(out, raise{Pkg: f.Pkg, From: f.Min, To: to})
	}
	return out
}

// applyRaises rewrites the floor values in a coverage.floors file, leaving comments, blank
// lines, notes and column alignment untouched.
func applyRaises(content string, raises []raise) string {
	byPkg := make(map[string]raise, len(raises))
	for _, r := range raises {
		byPkg[r.Pkg] = r
	}

	lines := strings.Split(content, "\n")
	for i, line := range lines {
		trimmed := strings.TrimSpace(line)
		if trimmed == "" || strings.HasPrefix(trimmed, "#") {
			continue
		}
		m := floorLine.FindStringSubmatch(line)
		if m == nil {
			continue
		}
		r, ok := byPkg[strings.TrimSpace(m[1])]
		if !ok {
			continue
		}
		lines[i] = m[1] + strconv.FormatFloat(r.To, 'f', 1, 64) + m[3]
	}
	return strings.Join(lines, "\n")
}
