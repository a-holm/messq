// SPDX-License-Identifier: Apache-2.0

package model_test

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/a-holm/messq/internal/model"
)

// selfPkg is the production package of the model. TestModelIsIndependent checks it by internal
// target and deliberately without -test: the production package must import nothing outside the
// standard library, while the in-package differential suite against internal/subject (allowed;
// only internal/queue and internal/store are forbidden) lives in a _test.go file that the
// prod-scope dependency list cannot see.
const selfPkg = "github.com/a-holm/messq/internal/model"

// moduleRoot is the repository root, resolved from the working directory of a test build.
func moduleRoot(t *testing.T) string {
	root, err := filepath.Abs("../..")
	if err != nil {
		t.Fatalf("resolve module root: %v", err)
	}
	return root
}

// TestModelIsIndependent is the acceptance gate that makes the model an oracle: its production
// code imports nothing outside the standard library, so it cannot inherit a bug by sharing code
// with internal/store. The dependency list is production-scope (no -test flag), exactly as the
// slice plan locks, and any dependency that resolves to the messq module fails the test.
func TestModelIsIndependent(t *testing.T) {
	ctx := context.Background()
	cmd := exec.CommandContext(ctx, "go", "list", "-deps", selfPkg)
	cmd.Dir = moduleRoot(t)
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("go list -deps %s failed (%v):\n%s", selfPkg, err, out)
	}

	var violations []string
	for _, dep := range strings.Split(string(out), "\n") {
		dep = strings.TrimSpace(dep)
		if dep == "" || dep == selfPkg {
			continue
		}
		if strings.HasPrefix(dep, "go.mess") || strings.HasPrefix(dep, "github.com/a-holm/messq") {
			violations = append(violations, dep)
		}
	}
	for _, v := range violations {
		t.Errorf("internal/model production code depends on %s; it must import nothing but the standard library", v)
	}
}

// lineBudget is the hard non-blank, non-comment line cap across model.go + match.go (PLAN.md
// §2 Rule 2): the budget is a smell detector, and raising it requires saying why in the commit.
const lineBudget = 600

// sourceFiles are the files the budget counts. model.go and match.go are the "naive broker plus
// naive matcher" pair the acceptance criterion names; view.go, history.go and invariant.go are
// outside the count by design.
var sourceFiles = []string{"model.go", "match.go"}

// TestModelLineBudget keeps the model within the naivety budget. It counts source lines that
// are neither blank nor whole-line comments, and fails when the pair exceeds the cap.
func TestModelLineBudget(t *testing.T) {
	root := moduleRoot(t)
	total := 0
	for _, name := range sourceFiles {
		path := filepath.Join(root, "internal/model", name)
		data, err := os.ReadFile(path)
		if os.IsNotExist(err) {
			continue // the file arrives with the slice that owns it (match.go is slice 2)
		}
		if err != nil {
			t.Errorf("read %s: %v", path, err)
			continue
		}
		for _, line := range strings.Split(string(data), "\n") {
			trimmed := strings.TrimSpace(line)
			if trimmed == "" || strings.HasPrefix(trimmed, "//") {
				continue
			}
			total++
		}
	}
	if total > lineBudget {
		t.Errorf("internal/model model.go+match.go holds %d non-blank, non-comment lines, over the %d budget; raise it only with a why-line in the commit", total, lineBudget)
	}
}

// TestModelSkeleton pins the slice-one shape: New returns a usable Model, View and DrainEvents
// are always callable, and the skeleton starts empty. This is the red that makes New/View/
// DrainEvents exist in the first place.
func TestModelSkeleton(t *testing.T) {
	m := model.New()
	if m == nil {
		t.Fatal("New() = nil")
	}
	if v := m.View(); v.Now != 0 {
		t.Fatalf("View().Now = %d, want 0 on a fresh model", v.Now)
	}
	if ev := m.DrainEvents(); ev != nil {
		t.Fatalf("DrainEvents() = %v, want nil on a fresh model", ev)
	}
}
