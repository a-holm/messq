// SPDX-License-Identifier: Apache-2.0

//go:build gatecheck

package gates

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/a-holm/messq/internal/clock"
)

// makeTimeout bounds one sabotage run. A gate that hangs is a failed gate, not a stalled suite.
// The cover and test rows run the full `go test -race` suite in a scratch copy; under the
// -parallel 8 load the process tests (SIGKILL re-exec, curl transcript, golden socket suite)
// push a single run past ten minutes on a cold-cache GitHub runner (measured 765s for a cover
// row, PR #55), so the bound is twenty.
const makeTimeout = 20 * time.Minute

// modDownloadTimeout bounds the single module-cache warm-up in TestMain. Downloading the whole
// universe of transitive dependencies on a cold GitHub runner can take a while, so the bound
// is a generous five minutes; on a warm cache (the local case) `go mod download` returns in
// seconds because the modules are already present, so this is effectively free.
const modDownloadTimeout = 5 * time.Minute

// TestMain warms the module cache before any row runs a `go` invocation.
//
// Every sabotage row (TestGates) copies the working tree into a scratch directory
// (scratchCopy) and runs its make target there. The scratch copies share THIS binary's process
// environment, including GOMODCACHE (the per-user module cache under GOPATH/pkg/mod), so when
// the GitHub actions/setup-go cache restore misses — as it does on a cold runner, which is the
// rule right after a main merge — the first eight rows all race to download the same modules
// at the same time, into the same cache, from the same network paths. What looks like robust
// parallelism is actually a thundering herd of `go` processes each fetching on first use. That
// is not a latent edge; it is exactly the failure the required gates job hit on main e41b480,
// and it shows up in two symptoms:
//
//   - G12 ("a floored package below its floor") ran its cover row while lines like
//     `go: downloading github.com/prometheus/client_golang v1.24.1 ...` polluted stderr, so
//     the run failed exit=2 but WITHOUT the expected "< 90.0%" message — the row was reported
//     as having "failed for the wrong reason".
//   - G15 ("a lowered floor a branch commit explains") PASSED its assertion, but the test
//     framework's TempDir cleanup failed mid-download ("testing.go:1466 TempDir RemoveAll
//     cleanup: unlinkat ...") because the row's own `go` was still writing modules into its
//     scratch tree when RemoveAll ran.
//
// The fix is a single warm-up HERE, in TestMain, BEFORE m.Run() fans the rows out in parallel.
// One `go mod download` in the real repository root makes every module the rows will need land
// in the shared cache while only one downloader is running. Because every scratch copy shares
// that same cache, every subsequent row `go` invocation finds its modules already present: no
// concurrent downloads, no stderr pollution, no TempDir-cleanup race. This must hold even when
// the setup-go cache restore misses, so this deliberately does NOT depend on the cache being
// warm — it warms the cache itself.
func TestMain(m *testing.M) {
	start := clock.System{}.Now()
	root := repoRoot()
	ctx, cancel := context.WithTimeout(context.Background(), modDownloadTimeout)
	cmd := exec.CommandContext(ctx, "go", "mod", "download")
	cmd.Dir = root
	cmd.Env = childEnv()
	out, err := cmd.CombinedOutput()
	cancel()
	fmt.Fprintf(os.Stderr, "gates: module-cache warm-up (%s) took %v\n", "go mod download", clock.System{}.Since(start).Round(time.Millisecond))
	if err != nil {
		// The warm-up failing is a real preflight failure, not a gate verdict. Do not shrug it
		// off: every row below would then race concurrent downloads again, and the whole point
		// of the gates selftest is to prove the gates bite, not to rediscover the cold-cache
		// herd. Fail loudly, with the downloader's own stderr. A test binary must not call
		// os.Exit directly (forbidigo), and returning skips m.Run, so a panic is the one
		// remaining way to stop the suite with a nonzero exit here.
		panic("gates: module-cache warm-up failed: " + err.Error() + "\n" + string(out))
	}
	m.Run() // the testing package exits with m.Run's code when TestMain returns
}

// repoRoot returns the repository root as an absolute path. The test binary runs from
// test/gates, so the workspace root is two levels up.
func repoRoot() string {
	root, err := filepath.Abs(filepath.Join("..", ".."))
	if err != nil {
		panic(err)
	}
	return root
}

// gate is one row of the sabotage matrix: a mutation, the make target that must notice it, and
// the message the failure must carry. Asserting only the exit code is not enough, because a
// build that fails for an unrelated reason would look like a working gate.
type gate struct {
	id     string
	name   string
	target string
	want   string
	// makeArgs are appended to the make invocation, for the gates whose input the repository
	// cannot produce on demand. They override a seam the Makefile already exposes.
	makeArgs []string
	wantOK   bool // the mutation must be accepted rather than rejected
	prepare  func(t *testing.T, root string)
}

func TestGates(t *testing.T) {
	for _, g := range matrix() {
		t.Run(g.id+"_"+strings.ReplaceAll(g.name, " ", "_"), func(t *testing.T) {
			t.Parallel()

			root := scratchCopy(t)
			if g.prepare != nil {
				g.prepare(t, root)
			}

			code, output := runMake(t, root, g.target, g.makeArgs...)

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
			want: "covergate: OK", wantOK: true,
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
			id: "G34", name: "Secret.Reveal outside internal/auth", target: "lint",
			want:    "single, greppable exit",
			prepare: install("reveal.go", "internal/api/sabotage_reveal.go"),
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
			prepare: remove("internal/docsguard"),
		},
		{
			id: "G14", name: "a lowered coverage floor", target: "cover-ratchet-check",
			want:    "floors ratchet upward only",
			prepare: lowerFloor(branchCheckout, "lower internal/store to 80"),
		},
		{
			id: "G15", name: "a lowered floor a branch commit explains", target: "cover-ratchet-check",
			want:    "a commit on this branch explains the lowering",
			wantOK:  true,
			prepare: lowerFloor(branchCheckout, "#45: move the sweeper\n\ncoverage-floor-lowered: internal/store, its sweeper moved out from under it"),
		},
		// The four rows below are the escape hatch itself. It is documented, it is named in the
		// failure message, and a rule that reads HEAD alone would leave it unreachable on every
		// pull request while looking correct in every local test.
		{
			id: "G20", name: "an explained lowering behind a pull-request merge commit", target: "cover-ratchet-check",
			want:    "a commit on this branch explains the lowering",
			wantOK:  true,
			prepare: lowerFloor(mergeCheckout, "#45: move the sweeper\n\ncoverage-floor-lowered: internal/store, its sweeper moved out from under it"),
		},
		{
			id: "G21", name: "an unexplained lowering behind a merge commit", target: "cover-ratchet-check",
			want:    "floors ratchet upward only",
			prepare: lowerFloor(mergeCheckout, "lower internal/store to 80"),
		},
		{
			id: "G22", name: "a trailer with no reason", target: "cover-ratchet-check",
			want:    "floors ratchet upward only",
			prepare: lowerFloor(branchCheckout, "coverage-floor-lowered:"),
		},
		{
			id: "G23", name: "the trailer mentioned in prose", target: "cover-ratchet-check",
			want:    "floors ratchet upward only",
			prepare: lowerFloor(branchCheckout, "note that coverage-floor-lowered: is the escape hatch"),
		},
		// G37 is the per-floor half of the same contract (#45): a trailer explains only the
		// floors its reason names, so explaining one package's move must not unlock an
		// unrelated cut of another.
		{
			id: "G37", name: "a trailer that names another floor", target: "cover-ratchet-check",
			want:    "no commit on this branch explains the lowering of internal/store",
			prepare: lowerFloor(branchCheckout, "#45: move the sweeper\n\ncoverage-floor-lowered: internal/id, its sweeper moved out from under it"),
		},
		// G38 and G39 pin the ratchet's two silent passes. Both fire on a repository the check
		// cannot compare against -- a first push whose origin has no shared history yet, a merge
		// base from before coverage.floors existed -- and both exit 0 by design: there is
		// nothing to compare, and failing loudly would lock every bootstrap out of CI. What
		// keeps that from rotting into a quiet shrug is the printed reason: these rows prove
		// each path says why it passed instead of passing silently.
		{
			id: "G38", name: "no merge base with origin/main", target: "cover-ratchet-check",
			want:    "cover-ratchet-check: no merge base with origin/main, nothing to compare",
			wantOK:  true,
			prepare: dropOriginMain,
		},
		{
			id: "G39", name: "a merge base from before coverage.floors", target: "cover-ratchet-check",
			want:    "cover-ratchet-check: the merge base has no coverage.floors, nothing to compare",
			wantOK:  true,
			prepare: baseWithoutFloors,
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
		{
			id: "G24", name: "a file gofumpt would rewrite", target: "fmt-check",
			want:    "not gofumpt-clean",
			prepare: install("unformatted.go", "internal/queue/sabotage_unformatted.go"),
		},
		{
			id: "G25", name: "a format verb that does not match its argument", target: "vet",
			want:    "wrong type string",
			prepare: install("vet.go", "internal/queue/sabotage_vet.go"),
		},
		{
			id: "G26", name: "a require go mod tidy would drop", target: "tidy-check",
			want: "example.invalid/forbidden",
			prepare: install(
				"forbidden-dep-go.mod", "go.mod",
				"unusedmod-go.mod", "unusedmod/go.mod",
				"unusedmod.go", "unusedmod/x.go",
			),
		},
		{
			id: "G27", name: "a direct dependency nobody voted for", target: "dep-budget",
			want: "new direct dependency example.invalid/forbidden",
			prepare: install(
				"forbidden-dep-go.mod", "go.mod",
				"unusedmod-go.mod", "unusedmod/go.mod",
				"unusedmod.go", "unusedmod/x.go",
			),
		},
		// G28 and G29 are the fail-open rows. Both guards used to report success when the tool
		// they depend on could not answer at all, which is the worst way for a gate to be wrong:
		// it is indistinguishable from a clean tree.
		{
			id: "G28", name: "a module graph dep-budget cannot read", target: "dep-budget",
			want:    "the module graph is unreadable",
			prepare: install("unreadable-go.mod", "go.mod"),
		},
		{
			id: "G29", name: "an import graph layers cannot compute", target: "layers",
			want:    "the tree does not load",
			prepare: install("two-package-names.go", "pkg/client/sabotage_two_names.go"),
		},
		// G30 and G31 tamper with the guard rather than with the code it guards.
		{
			id: "G30", name: "CGO_ENABLED=0 dropped from the release build", target: "static-check",
			want:    `build setting CGO_ENABLED is "1", want "0"`,
			prepare: patch("Makefile", "GOBUILD := CGO_ENABLED=0 go build", "GOBUILD := go build"),
		},
		{
			id: "G31", name: "-trimpath dropped from the release build", target: "static-check",
			want:    `build setting -trimpath is "<unset>", want "true"`,
			prepare: patch("Makefile", "go build -trimpath -ldflags", "go build -ldflags"),
		},
		{
			id: "G32", name: "a SARIF severity the gate has never seen", target: "vuln",
			want:     `carries SARIF level "fatal"`,
			makeArgs: []string{"VULNSCAN=cat sabotage.sarif"},
			prepare:  install("unknown-level.sarif", "sabotage.sarif"),
		},
		{
			id: "G33", name: "a leaf package importing up", target: "layers",
			want:    "internal/subject or its tests depends on",
			prepare: install("leaf_test.go", "internal/subject/sabotage_leaf_test.go"),
		},
		// G41 pins the #49 C nuance: layers parses package clauses and imports but not function
		// bodies, so a body-syntax error is a blind spot in a production file (make vet is the
		// gate there). In the test binary the body is parsed as part of the dependency edge
		// layers walks, so a body-syntax error inside a _test.go must still load-fail loudly.
		{
			id: "G41", name: "a body-syntax error in a test file", target: "layers",
			want:    "the tree does not load",
			prepare: install("syntax_error_test.go", "internal/queue/sabotage_syntax_test.go"),
		},
		// A green row: the seam has to be usable, or the ban on wall-clock access is a ban on
		// having a clock at all. It is what keeps the two internal/clock exclusions honest.
		{
			id: "B3", name: "the clock seam may read and block on the clock", target: "lint",
			want:    "0 issues",
			wantOK:  true,
			prepare: install("clock.go", "internal/clock/sabotage_seam.go"),
		},
		// B4, G35 and G36 are the #45 seam-defaults rows. VULNSCAN and TEST_COUNT exist so the
		// matrix can feed a gate input the repository cannot produce on demand, which also means a
		// reviewed workflow edit could override either into vacuousness: `VULNSCAN=true` pipes
		// nothing into vulngate and `TEST_COUNT=0` runs no tests at all, both reported as success.
		// The seam-defaults target asserts the runner's view of both; these rows are what keep that
		// assertion honest.
		{
			id: "B4", name: "the seams hold their defaults", target: "seam-defaults",
			want:   "hold their defaults",
			wantOK: true,
		},
		{
			id: "G35", name: "VULNSCAN overridden off its default", target: "seam-defaults",
			want:    "seam-defaults: VULNSCAN is",
			prepare: patch("Makefile", "VULNSCAN ?= $(GOVULNCHECK) -format sarif ./...", "VULNSCAN ?= cat sabotage.sarif"),
		},
		{
			id: "G36", name: "TEST_COUNT set so no test runs", target: "seam-defaults",
			want:    "TEST_COUNT is",
			prepare: patch("Makefile", "TEST_COUNT ?= 1", "TEST_COUNT ?= 0"),
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

// patch rewrites one file in the scratch tree. Arguments are file, old, new triples, and every
// old must appear exactly once: a sabotage that silently matched nothing would leave the tree
// intact and the gate would look like it bit when nothing had happened.
//
// This is how a guard is tampered with rather than merely violated. A gate that only ever sees
// bad source has never been asked what it does when someone edits the gate itself.
func patch(triples ...string) func(*testing.T, string) {
	if len(triples)%3 != 0 {
		panic("patch needs file, old, new triples")
	}
	return func(t *testing.T, root string) {
		t.Helper()
		for i := 0; i < len(triples); i += 3 {
			path := filepath.Join(root, filepath.FromSlash(triples[i]))
			content, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			if n := strings.Count(string(content), triples[i+1]); n != 1 {
				t.Fatalf("patch %s: %q appears %d times, want exactly 1", triples[i], triples[i+1], n)
			}
			updated := strings.Replace(string(content), triples[i+1], triples[i+2], 1)
			if err := os.WriteFile(path, []byte(updated), 0o644); err != nil {
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
// checkout names the shape of the repository the ratchet gate runs in.
type checkout int

const (
	// branchCheckout is what a developer has: HEAD is the branch commit itself.
	branchCheckout checkout = iota
	// mergeCheckout is what a pull-request runner has: actions/checkout resolves
	// refs/pull/N/merge, so HEAD is GitHub's synthetic "Merge X into Y" commit and the branch's
	// own commits are its second parent. A rule that reads HEAD alone is unreachable here.
	mergeCheckout
)

// lowerFloor lowers a floor on a branch, commits it with commitMessage, and arranges the
// working tree into the requested checkout shape.
func lowerFloor(shape checkout, commitMessage string) func(*testing.T, string) {
	return func(t *testing.T, root string) {
		t.Helper()
		gitInit(t, root)
		git(t, root, "checkout", "-q", "-b", "topic")

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
		git(t, root, "commit", "-q", "-m", commitMessage)

		if shape == mergeCheckout {
			// Exactly what GitHub builds for refs/pull/N/merge: the branch merged into the
			// base, with the base as the first parent.
			git(t, root, "checkout", "-q", "--detach", "refs/remotes/origin/main")
			git(t, root, "merge", "-q", "--no-ff", "-m", "Merge topic into main", "topic")
		}
	}
}

func gitInit(t *testing.T, root string) {
	t.Helper()
	git(t, root, "init", "-q", "-b", "main")
	git(t, root, "add", "-A")
	git(t, root, "commit", "-q", "-m", "baseline")
	git(t, root, "update-ref", "refs/remotes/origin/main", "HEAD")
}

// dropOriginMain is the shape of a repository's very first push: the branch has history, but
// origin has none to share with it, so there is no remote-tracking ref for main and merge-base
// has nothing to name. The ratchet's first guard answers for that shape.
func dropOriginMain(t *testing.T, root string) {
	t.Helper()
	gitInit(t, root)
	git(t, root, "update-ref", "-d", "refs/remotes/origin/main")
}

// baseWithoutFloors parks origin/main on a baseline commit made without coverage.floors, then
// brings the file back on the branch -- the shape of a repository that grew its floors after
// the history the pull request would be compared against. git show at the base then has
// nothing to extract, and the ratchet's second guard answers for that shape.
func baseWithoutFloors(t *testing.T, root string) {
	t.Helper()

	floors := filepath.Join(root, "coverage.floors")
	content, err := os.ReadFile(floors)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(floors); err != nil {
		t.Fatal(err)
	}

	// The baseline commit, and origin/main with it, now predate the floors.
	gitInit(t, root)

	if err := os.WriteFile(floors, content, 0o644); err != nil {
		t.Fatal(err)
	}
	git(t, root, "checkout", "-q", "-b", "topic")
	git(t, root, "add", "coverage.floors")
	git(t, root, "commit", "-q", "-m", "add the floors")
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

	// repo is the repository root; scratchCopy mirrors the working tree from there.
	repo := repoRoot()

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

func runMake(t *testing.T, root, target string, extra ...string) (int, string) {
	t.Helper()

	ctx, cancel := context.WithTimeout(t.Context(), makeTimeout)
	defer cancel()

	cmd := exec.CommandContext(ctx, "make", append([]string{target}, extra...)...)
	cmd.Dir = root
	cmd.Env = childEnv()
	output, err := cmd.CombinedOutput()

	// Diagnostic guard: if a row's make run still shows `go: downloading ...` on its stderr,
	// the module cache was raced despite the TestMain warm-up (e.g. a module added by this
	// row's own code, or a cache-restore miss). The warm-up in TestMain is the fix; this is not
	// a second verdict but a breadcrumb so a "failed for the wrong reason" row is
	// distinguishable from a gate that genuinely does not bite. The row's own assertion below
	// still decides.
	if strings.Contains(string(output), "go: downloading") {
		t.Logf("gates: %s: make output shows 'go: downloading ...' (module cache was cold for this row; see TestMain warm-up)", target)
	}

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

// leakedPrefixes name the variables a scratch copy must not inherit.
//
// GIT_ is the dangerous one. Git exports GIT_DIR to every hook, so `make ci` run from the
// pre-push hook would hand each sabotage a GIT_DIR pointing at the developer's own repository:
// the scratch copy's `git init` would target it, and `make cover-ratchet-check` would resolve
// its merge base and its head commit message there instead of in the tree under test.
//
// MAKE is the quiet one. Running the matrix from `make ci` hands the child a MAKEFLAGS it never
// asked for, so a parallel outer build would silently make every sabotage run parallel too.
var leakedPrefixes = []string{"GIT_", "MAKEFLAGS=", "MAKELEVEL=", "MFLAGS="}

// childEnv is the environment a scratch copy runs in.
func childEnv() []string {
	env := os.Environ()
	out := env[:0]
	for _, kv := range env {
		leaked := false
		for _, prefix := range leakedPrefixes {
			if strings.HasPrefix(kv, prefix) {
				leaked = true
				break
			}
		}
		if !leaked {
			out = append(out, kv)
		}
	}
	return out
}
