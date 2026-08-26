// SPDX-License-Identifier: Apache-2.0

package clock

import (
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
)

// wallClock is the set forbidigo bans outside this package. The lint rule keeps the rest of
// the tree clean at package granularity; this test narrows the allowance to named files, which
// is the claim doc.go and CONTRIBUTING.md make. It walks the whole repository, because a scan
// of internal/clock alone could not see the one caller that lives elsewhere.
var wallClock = []string{
	"Now", "Since", "Until", "NewTimer", "NewTicker", "After", "Tick", "AfterFunc", "Sleep",
}

// wallClockCallers is every file in the repository allowed to call them, and why. An entry
// that no longer calls one is a failure too: an allow-list nobody prunes stops being one.
var wallClockCallers = map[string]string{
	"internal/clock/system.go": "the seam itself, which every other package reads the clock through",
	"internal/tools/vulngate/main.go": "a build-gate command rather than the daemon: its -now flag is that command's " +
		"own seam, and the single call carries a //nolint:forbidigo saying so",
	"pkg/client/clock.go": "the client-side mirror of this seam (issue #22): pkg/client is public and " +
		"layers.sh forbids it importing internal/clock, so it defines its own Clock interface over " +
		"the same two calls; tests inject fakes or run under testing/synctest",
}

// skipDirs are not source. testdata holds fixtures, which are inputs to the sabotage matrix and
// deliberately contain the calls this test forbids; scripts/spdx.sh prunes the same three.
var skipDirs = []string{".git", "dist", "testdata"}

func TestWallClockCallsAreConfinedToTheSeam(t *testing.T) {
	t.Parallel()

	root := repoRoot(t)
	fset := token.NewFileSet()
	scanned := 0
	seen := map[string]bool{}

	err := filepath.WalkDir(root, func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() {
			if slices.Contains(skipDirs, entry.Name()) {
				return fs.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(entry.Name(), ".go") {
			return nil
		}

		rel, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		rel = filepath.ToSlash(rel)
		scanned++

		file, err := parser.ParseFile(fset, path, nil, parser.SkipObjectResolution)
		if err != nil {
			return err
		}
		local, imported := timeImportName(file)
		if !imported {
			return nil
		}

		for _, used := range wallClockCalls(file, local) {
			reason, allowed := wallClockCallers[rel]
			if !allowed {
				t.Errorf("%s calls %s.%s; the wall clock is read through internal/clock", rel, local, used)
				continue
			}
			seen[rel] = true
			t.Logf("%s calls %s.%s, allowed: %s", rel, local, used, reason)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}

	// A walk that reached nothing would pass silently, which is the one way this test could
	// be worse than no test at all.
	if scanned < 20 {
		t.Fatalf("scanned %d Go files under %s; the walk is broken, not the tree", scanned, root)
	}
	for path, reason := range wallClockCallers {
		if !seen[path] {
			t.Errorf("%s is allowed to read the wall clock (%s) but no longer does; drop the allowance", path, reason)
		}
	}
}

// repoRoot returns the module root, which is two directories above this package.
func repoRoot(t *testing.T) string {
	t.Helper()

	root, err := filepath.Abs(filepath.Join("..", ".."))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(root, "go.mod")); err != nil {
		t.Fatalf("no go.mod under %s: %v", root, err)
	}
	return root
}

// timeImportName returns the name a file binds the time package to, honouring an alias, and
// whether it imports it at all. Without this an `import stdtime "time"` would walk straight
// past the scan, which is the same hole forbidigo closes with analyze-types.
func timeImportName(file *ast.File) (string, bool) {
	for _, spec := range file.Imports {
		if spec.Path.Value != `"time"` {
			continue
		}
		if spec.Name == nil {
			return "time", true
		}
		return spec.Name.Name, true
	}
	return "", false
}

// wallClockCalls returns the banned time package selectors a file references under local.
func wallClockCalls(file *ast.File, local string) []string {
	if local == "." {
		// A dot import puts Now in the file scope with no selector to match, so the file
		// has to justify itself rather than slip through. Nothing in the tree does it.
		return wallClock
	}

	var out []string
	ast.Inspect(file, func(n ast.Node) bool {
		sel, ok := n.(*ast.SelectorExpr)
		if !ok {
			return true
		}
		ident, ok := sel.X.(*ast.Ident)
		if !ok || ident.Name != local {
			return true
		}
		if slices.Contains(wallClock, sel.Sel.Name) {
			out = append(out, sel.Sel.Name)
		}
		return true
	})
	return out
}
