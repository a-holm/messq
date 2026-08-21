// SPDX-License-Identifier: Apache-2.0

package main

import (
	"bufio"
	"fmt"
	"io"
	"path"
	"regexp"
	"sort"
	"strconv"
	"strings"
)

// block is one coverage block: a contiguous run of statements the compiler counts as a unit.
type block struct {
	File      string
	StartLine int
	EndLine   int
	NumStmt   int
}

// pkgCover is the statement coverage of one package, aggregated over its blocks. The unit is
// statements, not lines: `go tool cover -func` reports statements and so does every number in
// coverage.floors.
type pkgCover struct {
	Pkg       string
	Covered   int64
	Total     int64
	Uncovered []block
}

// Pct is statement coverage in percent. A package with no statements is 0, and callers must
// treat that case as "nothing to measure" rather than as a failing percentage.
func (p *pkgCover) Pct() float64 {
	if p.Total == 0 {
		return 0
	}
	return float64(p.Covered) / float64(p.Total) * 100
}

// profileLine matches one entry of a Go coverage profile:
// `import/path/file.go:12.34,56.78 3 1` is file, start line.col, end line.col, statements, count.
var profileLine = regexp.MustCompile(`^(.+):(\d+)\.(\d+),(\d+)\.(\d+) (\d+) (\d+)$`)

// parseProfile aggregates a coverage profile by package. File names lose the module prefix as
// they are read, so both the package keys and the uncovered ranges printed on a failure read
// the way coverage.floors and an editor do.
//
// A block that appears more than once (profiles concatenated from several runs) keeps its
// highest count, which is what `go tool cover` does: the statement was covered if any run
// covered it.
func parseProfile(r io.Reader, modulePath string) (map[string]*pkgCover, error) {
	counts := map[block]int64{}
	order := []block{}

	scanner := bufio.NewScanner(r)
	scanner.Buffer(make([]byte, 0, 64*1024), 4*1024*1024)
	for lineNo := 1; scanner.Scan(); lineNo++ {
		text := scanner.Text()
		if text == "" {
			continue
		}
		if strings.HasPrefix(text, "mode:") {
			continue
		}

		m := profileLine.FindStringSubmatch(text)
		if m == nil {
			return nil, fmt.Errorf("line %d: not a coverage profile line: %q", lineNo, text)
		}
		// The regexp already guarantees each of these is a run of digits, so the only way
		// ParseInt fails here is overflow, which is a corrupt profile.
		nums := make([]int64, 0, 4)
		for _, field := range []string{m[2], m[4], m[6], m[7]} {
			n, err := strconv.ParseInt(field, 10, 64)
			if err != nil {
				return nil, fmt.Errorf("line %d: %q is not a usable number: %w", lineNo, field, err)
			}
			nums = append(nums, n)
		}
		startLine, endLine, numStmt, count := int(nums[0]), int(nums[1]), int(nums[2]), nums[3]

		b := block{File: relPkg(m[1], modulePath), StartLine: startLine, EndLine: endLine, NumStmt: numStmt}
		if prev, seen := counts[b]; seen {
			if count > prev {
				counts[b] = count
			}
			continue
		}
		counts[b] = count
		order = append(order, b)
	}
	if err := scanner.Err(); err != nil {
		return nil, err
	}

	out := map[string]*pkgCover{}
	for _, b := range order {
		pkg := path.Dir(b.File)
		p, ok := out[pkg]
		if !ok {
			p = &pkgCover{Pkg: pkg}
			out[pkg] = p
		}
		p.Total += int64(b.NumStmt)
		if counts[b] > 0 {
			p.Covered += int64(b.NumStmt)
		} else {
			p.Uncovered = append(p.Uncovered, b)
		}
	}
	for _, p := range out {
		sort.Slice(p.Uncovered, func(i, j int) bool {
			if p.Uncovered[i].File != p.Uncovered[j].File {
				return p.Uncovered[i].File < p.Uncovered[j].File
			}
			return p.Uncovered[i].StartLine < p.Uncovered[j].StartLine
		})
	}
	return out, nil
}

// relPkg turns an absolute import path into the module-relative form used by coverage.floors.
// An import path from outside the module is left alone so it is still reported under a name a
// developer recognises.
func relPkg(importPath, modulePath string) string {
	if modulePath == "" {
		return importPath
	}
	if importPath == modulePath {
		return "."
	}
	if rest, ok := strings.CutPrefix(importPath, modulePath+"/"); ok {
		return rest
	}
	return importPath
}
