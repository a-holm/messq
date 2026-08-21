// SPDX-License-Identifier: Apache-2.0

package clock

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"slices"
	"strings"
	"testing"
)

// wallClock is the set forbidigo bans outside this package. The lint rule is what keeps the
// rest of the tree clean; this test is what narrows the allowance from "the seam package" to
// "the seam", which is the claim the package documentation makes.
var wallClock = []string{
	"Now", "Since", "Until", "NewTimer", "NewTicker", "After", "Tick", "AfterFunc", "Sleep",
}

func TestSystemIsTheOnlyWallClockReader(t *testing.T) {
	t.Parallel()

	entries, err := os.ReadDir(".")
	if err != nil {
		t.Fatal(err)
	}

	fset := token.NewFileSet()
	inspected := 0
	for _, entry := range entries {
		name := entry.Name()
		if entry.IsDir() || !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}
		inspected++

		file, err := parser.ParseFile(fset, name, nil, parser.SkipObjectResolution)
		if err != nil {
			t.Fatal(err)
		}
		for _, used := range timePackageCalls(file) {
			if !slices.Contains(wallClock, used) {
				continue
			}
			if name != "system.go" {
				t.Errorf("%s calls time.%s; system.go is the only file allowed to read the clock", name, used)
			}
		}
	}

	if inspected < 2 {
		t.Fatalf("inspected %d files; the scan is broken, not the package", inspected)
	}
}

// timePackageCalls returns the time package selectors the file references.
func timePackageCalls(file *ast.File) []string {
	var out []string
	ast.Inspect(file, func(n ast.Node) bool {
		sel, ok := n.(*ast.SelectorExpr)
		if !ok {
			return true
		}
		ident, ok := sel.X.(*ast.Ident)
		if !ok || ident.Name != "time" {
			return true
		}
		out = append(out, sel.Sel.Name)
		return true
	})
	return out
}
