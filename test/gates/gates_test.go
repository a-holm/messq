// SPDX-License-Identifier: Apache-2.0

//go:build gatecheck

package gates

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// makeTimeout bounds one sabotage run. A gate that hangs is a failed gate, not a stalled suite.
const makeTimeout = 5 * time.Minute

// gate is one row of the sabotage matrix: a mutation, the make target that must notice it, and
// the message the failure must carry. Asserting only the exit code is not enough, because a
// build that fails for an unrelated reason would look like a working gate.
type gate struct {
	id      string
	name    string
	target  string
	want    string
	wantOK  bool // the mutation must be accepted rather than rejected
	prepare func(t *testing.T, root string)
}

func TestGates(t *testing.T) {
	for _, g := range matrix() {
		t.Run(g.id+"_"+strings.ReplaceAll(g.name, " ", "_"), func(t *testing.T) {
			t.Parallel()

			root := scratchCopy(t)
			if g.prepare != nil {
				g.prepare(t, root)
			}

			code, output := runMake(t, root, g.target)

			if g.wantOK {
				if code != 0 {
					t.Fatalf("gates: %s %-40s %-20s exit=%d, want 0\n%s", g.id, g.name, "make "+g.target, code, output)
				}
				if !strings.Contains(output, g.want) {
					t.Fatalf("gates: %s %-40s output does not contain %q\n%s", g.id, g.name, g.want, output)
				}
				t.Logf("gates: %-3s %-42s make %-20s exit=0  ok\n       %s", g.id, g.name, g.target, matched(output, g.want))
				return
			}

			if code == 0 {
				t.Fatalf("gates: %s %-40s make %s exited 0; the gate does not bite\n%s", g.id, g.name, g.target, output)
			}
			if !strings.Contains(output, g.want) {
				t.Fatalf("gates: %s %-40s make %s failed with exit=%d but without %q; it failed for the wrong reason\n%s",
					g.id, g.name, g.target, code, g.want, output)
			}
			t.Logf("gates: %-3s %-42s make %-20s exit=%d  ok\n       %s", g.id, g.name, g.target, code, matched(output, g.want))
		})
	}
}

func matrix() []gate {
	return []gate{
		{
			id: "B1", name: "an unsabotaged copy lints clean", target: "lint",
			want: "0 issues", wantOK: true,
		},
		{
			id: "B2", name: "an unsabotaged copy meets its floors", target: "cover",
			want: "PENDING", wantOK: true,
		},
		{
			id: "G1", name: "time.Sleep in a test", target: "lint",
			want:    "time.Sleep is banned",
			prepare: install("sleep_test.go", "internal/cli/sabotage_sleep_test.go"),
		},
		{
			id: "G2", name: "time.Now inside the state machine", target: "lint",
			want:    "Wall-clock access outside internal/clock",
			prepare: install("wallclock.go", "internal/queue/sabotage_wallclock.go"),
		},
		{
			id: "G3", name: "prometheus.MustRegister outside internal/obs", target: "lint",
			want: "custom registry only",
			prepare: install(
				"prometheus_stub.go", "internal/sabotageprom/prometheus/prometheus.go",
				"prometheus_caller.go", "internal/api/sabotage_prometheus.go",
			),
		},
		{
			id: "G4", name: "a switch missing an enum case", target: "lint",
			want:    "missing cases in switch",
			prepare: install("exhaustive.go", "internal/queue/sabotage_exhaustive.go"),
		},
		{
			id: "G5", name: "an unchecked error return", target: "lint",
			want:    "is not checked (errcheck)",
			prepare: install("errcheck.go", "internal/store/sabotage_errcheck.go"),
		},
		{
			id: "G6", name: "a nolint with no explanation", target: "lint",
			want:    "should provide explanation",
			prepare: install("nolint.go", "internal/store/sabotage_nolint.go"),
		},
		{
			id: "G7", name: "a result set loop with no rows.Err()", target: "lint",
			want:    "rows.Err must be checked",
			prepare: install("rowserr.go", "internal/store/sabotage_rowserr.go"),
		},
		{
			id: "G8", name: "a result set that is never closed", target: "lint",
			want:    "Rows/Stmt/NamedStmt was not closed",
			prepare: install("sqlclose.go", "internal/store/sabotage_sqlclose.go"),
		},
		{
			id: "G9", name: "fmt.Println in a library package", target: "lint",
			want:    "Data goes to the injected stdout writer",
			prepare: install("print.go", "internal/queue/sabotage_print.go"),
		},
		{
			id: "G10", name: "os.Exit in a library package", target: "lint",
			want:    "Only a command entry point may exit",
			prepare: install("exit.go", "internal/api/sabotage_exit.go"),
		},
		{
			id: "G11", name: "an invalid workflow expression", target: "lint",
			want:    "does not exist in this workflow",
			prepare: install("workflow.yml", ".github/workflows/sabotage.yml"),
		},
		{
			id: "G12", name: "a floored package below its floor", target: "cover",
			want: "< 90.0%",
			prepare: install(
				"uncovered.go", "internal/queue/sabotage_classify.go",
				"uncovered_test.go", "internal/queue/sabotage_classify_test.go",
			),
		},
		{
			id: "G13", name: "a floored package deleted outright", target: "cover",
			want:    "package directory does not exist",
			prepare: remove("internal/queue"),
		},
		{
			id: "G14", name: "a lowered coverage floor", target: "cover-ratchet-check",
			want:    "floors ratchet upward only",
			prepare: lowerFloor("lower internal/store to 80"),
		},
		{
			id: "G15", name: "a lowered floor the head commit explains", target: "cover-ratchet-check",
			want:    "the head commit explains the lowering",
			wantOK:  true,
			prepare: lowerFloor("coverage-floor-lowered: the sweeper moved to internal/queue"),
		},
		{
			id: "G16", name: "an expired vulnerability suppression", target: "vuln",
			want:    "suppression expired",
			prepare: install("expired-allow", ".govulncheck-allow"),
		},
		{
			id: "G17", name: "a data race in a test", target: "test",
			want:    "DATA RACE",
			prepare: install("race_test.go", "internal/cli/sabotage_race_test.go"),
		},
		{
			id: "G18", name: "a source file with no licence header", target: "spdx",
			want:    "missing 'SPDX-License-Identifier: Apache-2.0'",
			prepare: install("nospdx.go", "internal/queue/sabotage_nospdx.go"),
		},
		{
			id: "G19", name: "a test import that crosses a layer", target: "layers",
			want:    "internal/queue or its tests depends on",
			prepare: install("layers_test.go", "internal/queue/sabotage_layers_test.go"),
		},
	}
}

// matched returns the first line of output that carries the fragment the gate asserts on. The
// transcript is the evidence, so it prints what the gate actually said rather than only that
// something matched.
func matched(output, want string) string {
	for line := range strings.Lines(output) {
		if strings.Contains(line, want) {
			return strings.TrimSpace(line)
		}
	}
	return ""
}

// install copies fixture files from testdata into the scratch tree. Arguments are
// fixture, destination pairs.
func install(pairs ...string) func(*testing.T, string) {
	if len(pairs)%2 != 0 {
		panic("install needs fixture and destination pairs")
	}
	return func(t *testing.T, root string) {
		t.Helper()
		for i := 0; i < len(pairs); i += 2 {
			content, err := os.ReadFile(filepath.Join("testdata", pairs[i]))
			if err != nil {
				t.Fatal(err)
			}
			dest := filepath.Join(root, filepath.FromSlash(pairs[i+1]))
			if err := os.MkdirAll(filepath.Dir(dest), 0o755); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(dest, content, 0o644); err != nil {
				t.Fatal(err)
			}
		}
	}
}

// remove deletes paths from the scratch tree, directories included.
func remove(paths ...string) func(*testing.T, string) {
	return func(t *testing.T, root string) {
		t.Helper()
		for _, path := range paths {
			if err := os.RemoveAll(filepath.Join(root, filepath.FromSlash(path))); err != nil {
				t.Fatal(err)
			}
		}
	}
}

// lowerFloor turns the scratch copy into a repository whose origin/main holds the current
// floors, then lowers one and commits with the given message. The ratchet check compares
// against a merge base, so it needs real history rather than a bare file.
func lowerFloor(commitMessage string) func(*testing.T, string) {
	return func(t *testing.T, root string) {
		t.Helper()
		gitInit(t, root)

		path := filepath.Join(root, "coverage.floors")
		content, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		lowered := strings.Replace(string(content), "internal/store                     85.0", "internal/store                     80.0", 1)
		if lowered == string(content) {
			t.Fatal("coverage.floors no longer holds the internal/store 85.0 line the sabotage edits")
		}
		if err := os.WriteFile(path, []byte(lowered), 0o644); err != nil {
			t.Fatal(err)
		}

		git(t, root, "add", "coverage.floors")
		git(t, root, "commit", "-m", commitMessage)
	}
}

func gitInit(t *testing.T, root string) {
	t.Helper()
	git(t, root, "init", "-q", "-b", "main")
	git(t, root, "add", "-A")
	git(t, root, "commit", "-q", "-m", "baseline")
	git(t, root, "update-ref", "refs/remotes/origin/main", "HEAD")
}

func git(t *testing.T, root string, args ...string) {
	t.Helper()
	cmd := exec.CommandContext(t.Context(), "git", args...)
	cmd.Dir = root
	cmd.Env = append(childEnv(),
		"GIT_AUTHOR_NAME=gates", "GIT_AUTHOR_EMAIL=gates@example.invalid",
		"GIT_COMMITTER_NAME=gates", "GIT_COMMITTER_EMAIL=gates@example.invalid",
		"GIT_CONFIG_GLOBAL=/dev/null", "GIT_CONFIG_SYSTEM=/dev/null",
	)
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("git %s: %v\n%s", strings.Join(args, " "), err, out)
	}
}

// scratchCopy mirrors the working tree into a temporary directory. It copies what git tracks
// plus what git would track, so a sabotage never touches the developer's checkout and an
// uncommitted change to a gate is still exercised.
func scratchCopy(t *testing.T) string {
	t.Helper()

	repo, err := filepath.Abs(filepath.Join("..", ".."))
	if err != nil {
		t.Fatal(err)
	}

	cmd := exec.CommandContext(t.Context(), "git", "ls-files", "--cached", "--others", "--exclude-standard", "-z")
	cmd.Dir = repo
	listing, err := cmd.Output()
	if err != nil {
		t.Fatalf("git ls-files: %v", err)
	}

	root := t.TempDir()
	for _, name := range strings.Split(strings.TrimRight(string(listing), "\x00"), "\x00") {
		if name == "" {
			continue
		}
		src := filepath.Join(repo, filepath.FromSlash(name))
		info, err := os.Stat(src)
		if err != nil {
			// A file staged for deletion is listed but gone. It is not part of the tree
			// under test either.
			if errors.Is(err, os.ErrNotExist) {
				continue
			}
			t.Fatal(err)
		}
		content, err := os.ReadFile(src)
		if err != nil {
			t.Fatal(err)
		}
		dest := filepath.Join(root, filepath.FromSlash(name))
		if err := os.MkdirAll(filepath.Dir(dest), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(dest, content, info.Mode().Perm()); err != nil {
			t.Fatal(err)
		}
	}
	return root
}

func runMake(t *testing.T, root, target string) (int, string) {
	t.Helper()

	ctx, cancel := context.WithTimeout(t.Context(), makeTimeout)
	defer cancel()

	cmd := exec.CommandContext(ctx, "make", target)
	cmd.Dir = root
	cmd.Env = childEnv()
	output, err := cmd.CombinedOutput()

	var exitErr *exec.ExitError
	switch {
	case err == nil:
		return 0, string(output)
	case errors.As(err, &exitErr):
		return exitErr.ExitCode(), string(output)
	default:
		t.Fatalf("make %s: %v\n%s", target, err, output)
		return 0, ""
	}
}

// childEnv drops the make variables the outer invocation exports. Without this, running the
// matrix from `make ci` hands the child a MAKEFLAGS it never asked for, and a parallel outer
// build would silently make every sabotage run parallel too.
func childEnv() []string {
	env := os.Environ()
	out := env[:0]
	for _, kv := range env {
		if strings.HasPrefix(kv, "MAKEFLAGS=") || strings.HasPrefix(kv, "MAKELEVEL=") || strings.HasPrefix(kv, "MFLAGS=") {
			continue
		}
		out = append(out, kv)
	}
	return out
}
