// SPDX-License-Identifier: Apache-2.0

package main

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strings"
)

// status is the outcome of one floor.
type status int

const (
	// statusOK means the package met its floor.
	statusOK status = iota
	// statusPending means the package exists but declares no function bodies yet, so there
	// is nothing to measure. It is not a pass on merit and it is not a failure.
	statusPending
	// statusFail means the floor is unmet, or the package is gone, or the package has code
	// that no test binary linked in.
	statusFail
)

// pkgState is what the working tree says about a floored package, as opposed to what the
// coverage profile says. The two together are what distinguishes "no code yet" from "the
// tests that covered this package were deleted".
type pkgState struct {
	Exists        bool
	HasStatements bool
}

// result is one floor's verdict.
type result struct {
	Floor     floor
	Status    status
	Pct       float64
	Covered   int64
	Total     int64
	Reason    string
	Uncovered []block
}

// check decides every floor. Absence from the profile is a failure whenever the package
// declares functions: deleting a package's tests must not silently satisfy its floor.
func check(profile map[string]*pkgCover, floors []floor, state map[string]pkgState) []result {
	out := make([]result, 0, len(floors))
	for _, f := range floors {
		st := state[f.Pkg]
		cov, inProfile := profile[f.Pkg]

		switch {
		case !st.Exists:
			out = append(out, result{
				Floor:  f,
				Status: statusFail,
				Reason: "package directory does not exist; remove the floor or restore the package",
			})

		case inProfile && cov.Total > 0:
			r := result{
				Floor:     f,
				Pct:       cov.Pct(),
				Covered:   cov.Covered,
				Total:     cov.Total,
				Uncovered: cov.Uncovered,
			}
			if r.Pct+floatSlop < f.Min {
				r.Status = statusFail
			} else {
				r.Status = statusOK
			}
			out = append(out, r)

		case st.HasStatements:
			out = append(out, result{
				Floor:  f,
				Status: statusFail,
				Reason: "declares functions but contributes no block to the coverage profile: no test binary links it in",
			})

		default:
			out = append(out, result{
				Floor:  f,
				Status: statusPending,
				Reason: "no coverable statements yet",
			})
		}
	}
	return out
}

// floatSlop absorbs the last-bit noise of covered/total*100 so that a package whose coverage
// is exactly its floor passes. 89.99 against a floor of 90.0 still fails.
const floatSlop = 1e-9

// scanPackage reports what the working tree holds for one floored package. Test files are not
// consulted: a floor is about the coverage of production code.
func scanPackage(root, pkg string) (pkgState, error) {
	dir := filepath.Join(root, filepath.FromSlash(pkg))
	entries, err := os.ReadDir(dir)
	if os.IsNotExist(err) {
		return pkgState{}, nil
	}
	if err != nil {
		return pkgState{}, err
	}

	state := pkgState{Exists: true}
	fset := token.NewFileSet()
	for _, e := range entries {
		name := e.Name()
		if e.IsDir() || !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}
		file, err := parser.ParseFile(fset, filepath.Join(dir, name), nil, parser.SkipObjectResolution)
		if err != nil {
			return pkgState{}, fmt.Errorf("%s: %w", filepath.Join(pkg, name), err)
		}
		for _, decl := range file.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if ok && fn.Body != nil {
				state.HasStatements = true
				return state, nil
			}
		}
	}
	return state, nil
}
