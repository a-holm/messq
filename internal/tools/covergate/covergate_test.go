// SPDX-License-Identifier: Apache-2.0

package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

const module = "github.com/a-holm/messq"

func TestParseFloors_Table(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		want    []floor
		wantErr string
	}{
		{
			name: "comments, blank lines and notes",
			input: "# package        floor%  note\n" +
				"\n" +
				"internal/queue   90.0    PLAN.md section 11: the pure state machine\n" +
				"   # indented comment\n" +
				"internal/store   85.0    PLAN.md section 11: durability\n",
			want: []floor{
				{Pkg: "internal/queue", Min: 90, Note: "PLAN.md section 11: the pure state machine"},
				{Pkg: "internal/store", Min: 85, Note: "PLAN.md section 11: durability"},
			},
		},
		{
			name:  "a floor needs no note",
			input: "internal/queue 90\n",
			want:  []floor{{Pkg: "internal/queue", Min: 90}},
		},
		{
			name:    "a duplicate package is rejected",
			input:   "internal/queue 90.0\ninternal/queue 95.0\n",
			wantErr: "already has a floor on line 1",
		},
		{
			name:    "a missing floor value is rejected",
			input:   "internal/queue\n",
			wantErr: "want '<package> <floor%> [note]'",
		},
		{
			name:    "a non-numeric floor is rejected",
			input:   "internal/queue ninety\n",
			wantErr: `floor "ninety" is not a number`,
		},
		{
			name:    "a floor above 100 is rejected",
			input:   "internal/queue 101\n",
			wantErr: "outside 0..100",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := parseFloors(strings.NewReader(tt.input))

			if tt.wantErr != "" {
				if err == nil {
					t.Fatalf("parseFloors() = %v, want error containing %q", got, tt.wantErr)
				}
				if !strings.Contains(err.Error(), tt.wantErr) {
					t.Fatalf("error = %q, want it to contain %q", err, tt.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("parseFloors() error = %v", err)
			}
			if len(got) != len(tt.want) {
				t.Fatalf("parseFloors() = %v, want %v", got, tt.want)
			}
			for i := range got {
				if got[i] != tt.want[i] {
					t.Errorf("floor %d = %+v, want %+v", i, got[i], tt.want[i])
				}
			}
		})
	}
}

func TestParseProfile_AggregatesByPackage(t *testing.T) {
	input := "mode: atomic\n" +
		module + "/internal/queue/apply.go:10.1,12.2 3 1\n" +
		module + "/internal/queue/apply.go:14.1,20.2 5 0\n" +
		module + "/internal/queue/state.go:4.1,5.2 2 7\n" +
		module + "/internal/store/db.go:1.1,2.2 4 0\n"

	got, err := parseProfile(strings.NewReader(input), module)
	if err != nil {
		t.Fatalf("parseProfile() error = %v", err)
	}

	queue, ok := got["internal/queue"]
	if !ok {
		t.Fatalf("packages = %v, want internal/queue", keys(got))
	}
	if queue.Total != 10 || queue.Covered != 5 {
		t.Errorf("internal/queue = %d/%d statements, want 5/10", queue.Covered, queue.Total)
	}
	if want := 50.0; queue.Pct() != want {
		t.Errorf("internal/queue Pct() = %v, want %v", queue.Pct(), want)
	}
	if len(queue.Uncovered) != 1 || queue.Uncovered[0].StartLine != 14 || queue.Uncovered[0].EndLine != 20 {
		t.Errorf("uncovered = %+v, want one block at 14-20", queue.Uncovered)
	}
	if got := queue.Uncovered[0].File; got != "internal/queue/apply.go" {
		t.Errorf("uncovered file = %q, want the module prefix stripped", got)
	}
	if store := got["internal/store"]; store.Pct() != 0 {
		t.Errorf("internal/store Pct() = %v, want 0", store.Pct())
	}
}

func TestParseProfile_DuplicateBlockKeepsHighestCount(t *testing.T) {
	line := module + "/internal/queue/apply.go:10.1,12.2 3 %d\n"
	input := "mode: atomic\n" +
		strings.ReplaceAll(line, "%d", "0") +
		strings.ReplaceAll(line, "%d", "2")

	got, err := parseProfile(strings.NewReader(input), module)
	if err != nil {
		t.Fatalf("parseProfile() error = %v", err)
	}
	queue := got["internal/queue"]
	if queue.Total != 3 || queue.Covered != 3 {
		t.Errorf("internal/queue = %d/%d, want 3/3: a block covered by any run is covered", queue.Covered, queue.Total)
	}
}

func TestParseProfile_MalformedLine(t *testing.T) {
	_, err := parseProfile(strings.NewReader("mode: atomic\nnot a profile line\n"), module)
	if err == nil || !strings.Contains(err.Error(), "not a coverage profile line") {
		t.Fatalf("error = %v, want a malformed-line error", err)
	}
}

func TestCheck_Table(t *testing.T) {
	const pkg = "internal/queue"
	floors := []floor{{Pkg: pkg, Min: 90.0, Note: "the pure state machine"}}

	tests := []struct {
		name       string
		profile    map[string]*pkgCover
		state      pkgState
		wantStatus status
		wantReason string
	}{
		{
			name:       "exactly at the floor passes",
			profile:    map[string]*pkgCover{pkg: {Pkg: pkg, Covered: 90, Total: 100}},
			state:      pkgState{Exists: true, HasStatements: true},
			wantStatus: statusOK,
		},
		{
			name:       "a hundredth below the floor fails",
			profile:    map[string]*pkgCover{pkg: {Pkg: pkg, Covered: 8999, Total: 10000}},
			state:      pkgState{Exists: true, HasStatements: true},
			wantStatus: statusFail,
		},
		{
			name:       "above the floor passes",
			profile:    map[string]*pkgCover{pkg: {Pkg: pkg, Covered: 95, Total: 100}},
			state:      pkgState{Exists: true, HasStatements: true},
			wantStatus: statusOK,
		},
		{
			name:       "code with no profile entry fails",
			profile:    map[string]*pkgCover{},
			state:      pkgState{Exists: true, HasStatements: true},
			wantStatus: statusFail,
			wantReason: "no test binary links it in",
		},
		{
			name:       "no code yet is pending, not a pass on merit",
			profile:    map[string]*pkgCover{},
			state:      pkgState{Exists: true},
			wantStatus: statusPending,
			wantReason: "no coverable statements yet",
		},
		{
			name:       "a floor for a package that does not exist fails",
			profile:    map[string]*pkgCover{},
			state:      pkgState{},
			wantStatus: statusFail,
			wantReason: "does not exist",
		},
		{
			name:       "a package present with zero statements is pending",
			profile:    map[string]*pkgCover{pkg: {Pkg: pkg}},
			state:      pkgState{Exists: true},
			wantStatus: statusPending,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := check(tt.profile, floors, map[string]pkgState{pkg: tt.state})

			if len(got) != 1 {
				t.Fatalf("check() returned %d results, want 1", len(got))
			}
			if got[0].Status != tt.wantStatus {
				t.Errorf("status = %v, want %v (reason %q)", got[0].Status, tt.wantStatus, got[0].Reason)
			}
			if tt.wantReason != "" && !strings.Contains(got[0].Reason, tt.wantReason) {
				t.Errorf("reason = %q, want it to contain %q", got[0].Reason, tt.wantReason)
			}
		})
	}
}

func TestCompareFloors_Table(t *testing.T) {
	base := []floor{{Pkg: "internal/queue", Min: 90}, {Pkg: "internal/store", Min: 85}}

	tests := []struct {
		name     string
		proposed []floor
		want     string
	}{
		{
			name:     "unchanged is allowed",
			proposed: base,
		},
		{
			name:     "raising is allowed",
			proposed: []floor{{Pkg: "internal/queue", Min: 92}, {Pkg: "internal/store", Min: 85}},
		},
		{
			name:     "adding a package is allowed",
			proposed: append(append([]floor{}, base...), floor{Pkg: "internal/api", Min: 70}),
		},
		{
			name:     "lowering is refused",
			proposed: []floor{{Pkg: "internal/queue", Min: 90}, {Pkg: "internal/store", Min: 80}},
			want:     "internal/store floor lowered 85.0 -> 80.0; floors ratchet upward only",
		},
		{
			name:     "removing is refused",
			proposed: []floor{{Pkg: "internal/queue", Min: 90}},
			want:     "internal/store floor removed (was 85.0); floors ratchet upward only",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := compareFloors(base, tt.proposed)

			if tt.want == "" {
				if len(got) != 0 {
					t.Fatalf("compareFloors() = %v, want none", got)
				}
				return
			}
			if len(got) != 1 {
				t.Fatalf("compareFloors() = %v, want exactly one lowering", got)
			}
			if got[0].String() != tt.want {
				t.Errorf("lowering = %q, want %q", got[0], tt.want)
			}
		})
	}
}

func TestRatchet_RaisesOnlyPastTheSlack(t *testing.T) {
	floors := []floor{
		{Pkg: "internal/queue", Min: 90},
		{Pkg: "internal/store", Min: 85},
		{Pkg: "internal/api", Min: 70},
		{Pkg: "internal/obs", Min: 60},
	}
	measured := map[string]float64{
		"internal/queue": 90.7,  // inside the slack, no churn commit
		"internal/store": 91.4,  // clears the slack, floor becomes 91
		"internal/api":   100.0, // clears the slack, floor becomes 100
		"internal/obs":   10.0,  // a regression must never lower the floor
	}

	got := ratchet(floors, measured)

	want := []raise{
		{Pkg: "internal/store", From: 85, To: 91},
		{Pkg: "internal/api", From: 70, To: 100},
	}
	if len(got) != len(want) {
		t.Fatalf("ratchet() = %v, want %v", got, want)
	}
	for i := range got {
		if got[i] != want[i] {
			t.Errorf("raise %d = %+v, want %+v", i, got[i], want[i])
		}
	}
}

func TestRatchet_MissingFromProfileIsLeftAlone(t *testing.T) {
	floors := []floor{{Pkg: "internal/queue", Min: 90}}

	if got := ratchet(floors, map[string]float64{}); len(got) != 0 {
		t.Errorf("ratchet() = %v, want none: an unmeasured package keeps its floor", got)
	}
}

func TestApplyRaises_PreservesCommentsAndNotes(t *testing.T) {
	content := "# package        floor%  note\ninternal/queue   90.0    the pure state machine\ninternal/store   85.0    durability\n"

	got := applyRaises(content, []raise{{Pkg: "internal/store", From: 85, To: 91}})

	want := "# package        floor%  note\ninternal/queue   90.0    the pure state machine\ninternal/store   91.0    durability\n"
	if got != want {
		t.Errorf("applyRaises() =\n%q\nwant\n%q", got, want)
	}
}

func TestScanPackage_Table(t *testing.T) {
	root := t.TempDir()
	write := func(pkg, name, body string) {
		t.Helper()
		dir := filepath.Join(root, filepath.FromSlash(pkg))
		if err := os.MkdirAll(dir, 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(dir, name), []byte(body), 0o644); err != nil {
			t.Fatal(err)
		}
	}

	write("internal/empty", "doc.go", "// Package empty is a placeholder.\npackage empty\n")
	write("internal/decls", "types.go", "package decls\n\ntype State int\n\nconst A State = 1\n")
	write("internal/code", "apply.go", "package code\n\nfunc Apply() int { return 1 }\n")
	write("internal/testonly", "x_test.go", "package testonly\n\nfunc TestX() {}\n")
	write("internal/testonly", "doc.go", "// Package testonly is a placeholder.\npackage testonly\n")

	tests := []struct {
		pkg               string
		wantExists        bool
		wantHasStatements bool
	}{
		{pkg: "internal/empty", wantExists: true},
		{pkg: "internal/decls", wantExists: true},
		{pkg: "internal/code", wantExists: true, wantHasStatements: true},
		{pkg: "internal/testonly", wantExists: true},
		{pkg: "internal/missing"},
	}

	for _, tt := range tests {
		t.Run(tt.pkg, func(t *testing.T) {
			got, err := scanPackage(root, tt.pkg)
			if err != nil {
				t.Fatalf("scanPackage() error = %v", err)
			}
			if got.Exists != tt.wantExists || got.HasStatements != tt.wantHasStatements {
				t.Errorf("scanPackage(%q) = %+v, want {Exists:%v HasStatements:%v}",
					tt.pkg, got, tt.wantExists, tt.wantHasStatements)
			}
		})
	}
}

func TestRun_CheckReportsUncoveredRangesAndNextCommand(t *testing.T) {
	root := t.TempDir()
	writeFile(t, root, "go.mod", "module "+module+"\n\ngo 1.25.0\n")
	writeFile(t, root, "internal/queue/apply.go", "package queue\n\nfunc Apply() int {\n\treturn 1\n}\n")
	writeFile(t, root, "coverage.floors", "internal/queue 90.0 the pure state machine\n")
	writeFile(t, root, "cover.out", "mode: atomic\n"+
		module+"/internal/queue/apply.go:10.1,12.2 3 1\n"+
		module+"/internal/queue/apply.go:118.1,131.2 5 0\n")

	var stdout, stderr strings.Builder
	code := run([]string{
		"-root", root,
		"-profile", filepath.Join(root, "cover.out"),
		"-floors", filepath.Join(root, "coverage.floors"),
	}, &stdout, &stderr)

	if code != exitViolation {
		t.Fatalf("run() = %d, want %d (stdout: %s stderr: %s)", code, exitViolation, stdout.String(), stderr.String())
	}
	for _, want := range []string{
		"FAIL",
		"internal/queue",
		"37.50% < 90.0%",
		"internal/queue/apply.go:118-131",
		"next: go tool cover -html=",
	} {
		if !strings.Contains(stdout.String(), want) {
			t.Errorf("stdout does not contain %q:\n%s", want, stdout.String())
		}
	}
}

func TestRun_CheckIsGreenOnAPendingPackage(t *testing.T) {
	root := t.TempDir()
	writeFile(t, root, "go.mod", "module "+module+"\n\ngo 1.25.0\n")
	writeFile(t, root, "internal/queue/doc.go", "// Package queue is a placeholder.\npackage queue\n")
	writeFile(t, root, "coverage.floors", "internal/queue 90.0 the pure state machine\n")
	writeFile(t, root, "cover.out", "mode: atomic\n")

	var stdout, stderr strings.Builder
	code := run([]string{
		"-root", root,
		"-profile", filepath.Join(root, "cover.out"),
		"-floors", filepath.Join(root, "coverage.floors"),
	}, &stdout, &stderr)

	if code != exitOK {
		t.Fatalf("run() = %d, want %d (stdout: %s stderr: %s)", code, exitOK, stdout.String(), stderr.String())
	}
	if !strings.Contains(stdout.String(), "PENDING") {
		t.Errorf("stdout = %q, want a PENDING line", stdout.String())
	}
}

func TestRun_CompareRefusesALoweredFloor(t *testing.T) {
	root := t.TempDir()
	writeFile(t, root, "go.mod", "module "+module+"\n\ngo 1.25.0\n")
	writeFile(t, root, "base.floors", "internal/store 85.0 durability\n")
	writeFile(t, root, "coverage.floors", "internal/store 80.0 durability\n")

	args := []string{
		"-root", root,
		"-floors", filepath.Join(root, "coverage.floors"),
		"-compare-floors", filepath.Join(root, "base.floors"),
	}

	var stdout, stderr strings.Builder
	if code := run(args, &stdout, &stderr); code != exitViolation {
		t.Fatalf("run() = %d, want %d (stdout: %s)", code, exitViolation, stdout.String())
	}
	if !strings.Contains(stdout.String(), "floors ratchet upward only") {
		t.Errorf("stdout = %q, want the ratchet message", stdout.String())
	}
	if !strings.Contains(stdout.String(), "no commit on this branch explains the lowering of internal/store") {
		t.Errorf("stdout = %q, want the unexplained-lowering message", stdout.String())
	}
}

// TestRun_CompareMatchesTheTrailerPerFloor pins the #45 contract: a
// coverage-floor-lowered trailer explains only the floors it names. One trailer naming any
// floor used to unlock every lowered floor on the branch, so a commit explaining a real move
// of internal/queue would silently authorize an unrelated cut of internal/store.
func TestRun_CompareMatchesTheTrailerPerFloor(t *testing.T) {
	root := t.TempDir()
	writeFile(t, root, "go.mod", "module "+module+"\n\ngo 1.25.0\n")
	writeFile(t, root, "base.floors", "internal/store 85.0 durability\ninternal/id 95.0 ids\n")
	proposed := "internal/store 80.0 durability\ninternal/id 95.0 ids\n"
	writeFile(t, root, "coverage.floors", proposed)

	args := []string{
		"-root", root,
		"-floors", filepath.Join(root, "coverage.floors"),
		"-compare-floors", filepath.Join(root, "base.floors"),
	}

	// No trailer at all: the lowering is refused.
	var stdout, stderr strings.Builder
	if code := run(args, &stdout, &stderr); code != exitViolation {
		t.Fatalf("run() with no trailer = %d, want %d (stdout: %s)", code, exitViolation, stdout.String())
	}

	// A trailer naming another floor does not explain this one.
	stdout.Reset()
	writeFile(t, root, "messages.txt", "#45: move the sweeper\n\ncoverage-floor-lowered: internal/id, its sweeper moved out from under it\n")
	if code := run(append(args, "-commit-messages", filepath.Join(root, "messages.txt")), &stdout, &stderr); code != exitViolation {
		t.Fatalf("run() with a trailer naming internal/id = %d, want %d (stdout: %s)", code, exitViolation, stdout.String())
	}
	if !strings.Contains(stdout.String(), "no commit on this branch explains the lowering of internal/store") {
		t.Errorf("stdout = %q, want the unexplained-lowering message for internal/store", stdout.String())
	}

	// A trailer naming the floor explains exactly that floor.
	stdout.Reset()
	writeFile(t, root, "messages.txt", "#45: move the sweeper\n\ncoverage-floor-lowered: internal/store, its sweeper moved out from under it\n")
	if code := run(append(args, "-commit-messages", filepath.Join(root, "messages.txt")), &stdout, &stderr); code != exitOK {
		t.Fatalf("run() with a trailer naming internal/store = %d, want %d (stdout: %s)", code, exitOK, stdout.String())
	}
	if !strings.Contains(stdout.String(), "a commit on this branch explains the lowering") {
		t.Errorf("stdout = %q, want the acceptance line", stdout.String())
	}

	// One trailer may name several floors, separated by whitespace.
	stdout.Reset()
	writeFile(t, root, "base.floors", "internal/store 85.0 durability\ninternal/id 95.0 ids\n")
	writeFile(t, root, "coverage.floors", "internal/store 80.0 durability\ninternal/id 90.0 ids\n")
	writeFile(t, root, "messages.txt", "#45: split the primitives\n\ncoverage-floor-lowered: internal/store internal/id, both moved to their own packages\n")
	if code := run(append(args, "-commit-messages", filepath.Join(root, "messages.txt")), &stdout, &stderr); code != exitOK {
		t.Fatalf("run() with one trailer naming two floors = %d, want %d (stdout: %s)", code, exitOK, stdout.String())
	}
}

func TestRun_RatchetRewritesTheFileAndIsANoOpTheSecondTime(t *testing.T) {
	root := t.TempDir()
	writeFile(t, root, "go.mod", "module "+module+"\n\ngo 1.25.0\n")
	writeFile(t, root, "coverage.floors", "internal/queue   90.0    the pure state machine\n")
	writeFile(t, root, "cover.out", "mode: atomic\n"+
		module+"/internal/queue/apply.go:10.1,12.2 96 1\n"+
		module+"/internal/queue/apply.go:20.1,22.2 4 0\n")

	args := []string{
		"-root", root,
		"-profile", filepath.Join(root, "cover.out"),
		"-floors", filepath.Join(root, "coverage.floors"),
		"-ratchet",
	}

	var stdout, stderr strings.Builder
	if code := run(args, &stdout, &stderr); code != exitOK {
		t.Fatalf("run() = %d, want %d (stderr: %s)", code, exitOK, stderr.String())
	}
	updated := readFile(t, root, "coverage.floors")
	if want := "internal/queue   96.0    the pure state machine\n"; updated != want {
		t.Fatalf("coverage.floors = %q, want %q", updated, want)
	}

	stdout.Reset()
	if code := run(args, &stdout, &stderr); code != exitOK {
		t.Fatalf("second run() = %d, want %d", code, exitOK)
	}
	if !strings.Contains(stdout.String(), "unchanged") {
		t.Errorf("stdout = %q, want an unchanged report", stdout.String())
	}
	if again := readFile(t, root, "coverage.floors"); again != updated {
		t.Errorf("coverage.floors changed on a no-op run: %q", again)
	}
}

func TestRun_BadInputExitsTwo(t *testing.T) {
	root := t.TempDir()
	writeFile(t, root, "go.mod", "module "+module+"\n\ngo 1.25.0\n")
	writeFile(t, root, "coverage.floors", "internal/queue not-a-number\n")

	var stdout, stderr strings.Builder
	code := run([]string{"-root", root, "-floors", filepath.Join(root, "coverage.floors")}, &stdout, &stderr)

	if code != exitUsage {
		t.Fatalf("run() = %d, want %d", code, exitUsage)
	}
	if !strings.Contains(stderr.String(), "is not a number") {
		t.Errorf("stderr = %q, want a parse error", stderr.String())
	}
}

func writeFile(t *testing.T, root, name, body string) {
	t.Helper()
	full := filepath.Join(root, filepath.FromSlash(name))
	if err := os.MkdirAll(filepath.Dir(full), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(full, []byte(body), 0o644); err != nil {
		t.Fatal(err)
	}
}

func readFile(t *testing.T, root, name string) string {
	t.Helper()
	content, err := os.ReadFile(filepath.Join(root, filepath.FromSlash(name)))
	if err != nil {
		t.Fatal(err)
	}
	return string(content)
}

func keys(m map[string]*pkgCover) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	return out
}
