// SPDX-License-Identifier: Apache-2.0

package docsguard

import (
	"fmt"
	"regexp"
	"strings"
)

// headingRE matches an ATX heading and captures its level and its text.
var headingRE = regexp.MustCompile(`^(#{1,6})\s+(.*)$`)

// tickRE matches a backticked span. Every identifier this package extracts from a table cell is
// backticked in the document, which is what lets prose share a cell with an event name.
var tickRE = regexp.MustCompile("`([^`]+)`")

// table is a parsed markdown table: the header cells and one slice of cells per body row.
type table struct {
	header []string
	rows   [][]string
}

// splitRow turns "| a | b |" into ["a", "b"]. A trailing backslash never appears in these
// documents, so an escaped pipe is not a case that needs handling; a cell containing "|" would
// show up as an extra column and fail the width check in the caller.
func splitRow(line string) []string {
	trimmed := strings.TrimSpace(line)
	trimmed = strings.TrimPrefix(trimmed, "|")
	trimmed = strings.TrimSuffix(trimmed, "|")
	parts := strings.Split(trimmed, "|")
	cells := make([]string, len(parts))
	for i, p := range parts {
		cells[i] = strings.TrimSpace(p)
	}
	return cells
}

// isSeparator reports whether a row is the |---|---| line under a table header.
func isSeparator(cells []string) bool {
	for _, c := range cells {
		if c == "" || strings.Trim(c, ":-") != "" {
			return false
		}
	}
	return len(cells) > 0
}

// firstTableAfter returns the first markdown table that follows a heading whose text starts
// with prefix. It is how a section number in the document becomes an addressable table: the
// caller names "S6.1" and gets the transition rows.
func firstTableAfter(md []byte, prefix string) (table, error) {
	lines, err := linesAfterHeading(md, prefix)
	if err != nil {
		return table{}, err
	}

	for i, line := range lines {
		if !strings.HasPrefix(strings.TrimSpace(line), "|") {
			continue
		}
		header := splitRow(line)
		if i+1 >= len(lines) || !isSeparator(splitRow(lines[i+1])) {
			return table{}, fmt.Errorf("docsguard: table under %q has no separator row", prefix)
		}
		t := table{header: header}
		for _, row := range lines[i+2:] {
			if !strings.HasPrefix(strings.TrimSpace(row), "|") {
				break
			}
			cells := splitRow(row)
			if len(cells) != len(header) {
				return table{}, fmt.Errorf("docsguard: table under %q: row %q has %d cells, header has %d",
					prefix, cells[0], len(cells), len(header))
			}
			t.rows = append(t.rows, cells)
		}
		if len(t.rows) == 0 {
			return table{}, fmt.Errorf("docsguard: table under %q has no rows", prefix)
		}
		return t, nil
	}
	return table{}, fmt.Errorf("docsguard: no table found under a heading starting with %q", prefix)
}

// firstFenceAfter returns the contents of the first fenced code block that follows a heading
// whose text starts with prefix.
func firstFenceAfter(md []byte, prefix string) ([]string, error) {
	lines, err := linesAfterHeading(md, prefix)
	if err != nil {
		return nil, err
	}

	open := false
	var body []string
	for _, line := range lines {
		if strings.HasPrefix(strings.TrimSpace(line), "```") {
			if open {
				return body, nil
			}
			open = true
			continue
		}
		if open {
			body = append(body, line)
		}
	}
	return nil, fmt.Errorf("docsguard: no closed fenced block found under a heading starting with %q", prefix)
}

// linesAfterHeading returns the lines from the matching heading to the next heading of the same
// or a higher level. Bounding the search at the next heading is what stops a missing table from
// silently matching the next section's table.
func linesAfterHeading(md []byte, prefix string) ([]string, error) {
	all := strings.Split(string(md), "\n")
	start, level := -1, 0
	for i, line := range all {
		m := headingRE.FindStringSubmatch(line)
		if m == nil {
			continue
		}
		if start < 0 {
			if strings.HasPrefix(m[2], prefix) {
				start, level = i+1, len(m[1])
			}
			continue
		}
		if len(m[1]) <= level {
			return all[start:i], nil
		}
	}
	if start < 0 {
		return nil, fmt.Errorf("docsguard: no heading starts with %q", prefix)
	}
	return all[start:], nil
}

// stripFences removes fenced code blocks, including their fence lines. A fence is a run of three
// backticks, so leaving them in would shift the pairing of every inline span after the first
// fence and turn prose into one long "code" match.
func stripFences(s string) string {
	var kept []string
	inFence := false
	for _, line := range strings.Split(s, "\n") {
		if strings.HasPrefix(strings.TrimSpace(line), "```") {
			inFence = !inFence
			continue
		}
		if !inFence {
			kept = append(kept, line)
		}
	}
	return strings.Join(kept, "\n")
}

// ticked returns every backticked span in s, in order.
func ticked(s string) []string {
	matches := tickRE.FindAllStringSubmatch(s, -1)
	out := make([]string, 0, len(matches))
	for _, m := range matches {
		out = append(out, m[1])
	}
	return out
}

// untick strips one surrounding pair of backticks.
func untick(s string) string {
	return strings.Trim(strings.TrimSpace(s), "`")
}

// fields splits a fenced block into whitespace-separated words, which is the shape both event
// vocabulary blocks have.
func fields(lines []string) []string {
	var out []string
	for _, l := range lines {
		out = append(out, strings.Fields(l)...)
	}
	return out
}
