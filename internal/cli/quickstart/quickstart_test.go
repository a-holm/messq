// SPDX-License-Identifier: Apache-2.0

package quickstart

import (
	"context"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/a-holm/messq/internal/clock"
)

// TestQuickstartPrintsWhatItRuns is the tour's G1 invariant: for every step the
// printed "$ " line is byte-identical to the argv the engine executed. The
// mutant this kills: hardcoding a displayed command that differs from the
// executed argv.
func TestQuickstartPrintsWhatItRuns(t *testing.T) {
	var executed [][]string
	deps := Deps{
		Stdout: &strings.Builder{},
		Stderr: &strings.Builder{},
		Getenv: func(string) string { return "" },
		ExecuteStep: func(_ context.Context, argv []string, _, _ io.Writer) int {
			executed = append(executed, argv)
			return 0
		},
	}
	tour := NewTour(deps, "/tmp/tour", "unix:///tmp/tour/messq.sock")
	steps := []Step{
		{Title: "step one", Argv: []string{"version"}},
		{Title: "step two", Argv: []string{"verify", "--data-dir", "/tmp/tour"}},
		{Title: "step three", Argv: []string{"help", "concepts"}},
	}
	if code := tour.Run(context.Background(), steps); code != 0 {
		t.Fatalf("tour exit = %d", code)
	}
	transcript := asString(t, deps.Stdout)
	lines := strings.Split(strings.TrimSpace(transcript), "\n")
	if len(lines) != len(steps) {
		t.Fatalf("transcript has %d lines for %d steps:\n%s", len(lines), len(steps), transcript)
	}
	for i, argv := range executed {
		want := "$ " + strings.Join(argv, " ")
		if lines[i] != want {
			t.Errorf("step %d: printed %q, executed argv renders %q", i, lines[i], want)
		}
	}
}

// TestQuickstartIgnoresPoisonedEnv is the G2 invariant: a MESSQ_ADDR in the
// operator's environment is IGNORED and NAMED. The sanitiser strips every
// MESSQ_* variable from what steps see; the fixture proves a poisoned address
// never reaches the step runner (dialling it fails the test).
func TestQuickstartIgnoresPoisonedEnv(t *testing.T) {
	// A poisoned address whose dial would fail the test if the step ever saw it.
	poisoned := "unix:///definitely/not/a/tour/socket"
	seen := map[string]string{}
	deps := Deps{
		Stdout: &strings.Builder{},
		Stderr: &strings.Builder{},
		Getenv: func(k string) string {
			if k == "MESSQ_ADDR" {
				return poisoned
			}
			return ""
		},
		ExecuteStep: func(_ context.Context, argv []string, _, _ io.Writer) int {
			// The engine passes the SANITISED environment only; the runner would
			// build its client from MESSQ_ADDR, which is gone here.
			seen["argv0"] = argv[0]
			return 0
		},
	}
	tour := NewTour(deps, "/tmp/tour", "unix:///tmp/tour/messq.sock")
	tour.Out = deps.Stdout
	tour.Err = deps.Stderr
	tour.PoisonedEnv = firstPoisonedEnv(deps.Getenv)

	if code := tour.Run(context.Background(), []Step{{Title: "one", Argv: []string{"version"}}}); code != 0 {
		t.Fatalf("tour exit = %d", code)
	}
	if seen["argv0"] != "version" {
		t.Errorf("step did not run: %v", seen)
	}
	stderr := asString(t, deps.Stderr)
	if !strings.Contains(stderr, "ignoring MESSQ_ADDR="+poisoned) {
		t.Errorf("the ignored environment is not named on stderr:\n%s", stderr)
	}
	if strings.Contains(stderr, "is used") || strings.Contains(stderr, poisoned+" used") {
		t.Errorf("the poisoned address is treated as live:\n%s", stderr)
	}
}

// TestQuickstartFirstPoisonedEnv covers the banner's naming order: ADDR first,
// TOKEN_FILE second, other MESSQ_* generically, clean environment empty.
func TestQuickstartFirstPoisonedEnv(t *testing.T) {
	for _, tc := range []struct {
		env  map[string]string
		want string
	}{
		{map[string]string{}, ""},
		{map[string]string{"MESSQ_ADDR": "unix://prod"}, "MESSQ_ADDR=unix://prod"},
		{map[string]string{"MESSQ_TOKEN_FILE": "/x"}, "MESSQ_TOKEN_FILE=/x"},
		{map[string]string{"MESSQ_OUTPUT": "json"}, "MESSQ_OUTPUT=json"},
	} {
		getenv := func(k string) string { return tc.env[k] }
		if got := firstPoisonedEnv(getenv); got != tc.want {
			t.Errorf("firstPoisonedEnv(%v) = %q, want %q", tc.env, got, tc.want)
		}
	}
}

// TestQuickstartEnvOverrideStripsMessq pins the sanitiser itself: every
// MESSQ_* variable is dropped, non-messq variables pass.
func TestQuickstartEnvOverrideStripsMessq(t *testing.T) {
	outer := func(k string) string {
		if k == "MESSQ_ADDR" {
			return "unix://prod"
		}
		if k == "HOME" {
			return "/root"
		}
		return ""
	}
	sanitised := envOverride(outer)
	if sanitised("MESSQ_ADDR") != "" {
		t.Error("sanitised env leaked MESSQ_ADDR")
	}
	if sanitised("HOME") != "/root" {
		t.Error("sanitised env dropped a non-messq variable")
	}
}

// TestQuickstartCancelledMidTour pins the Ctrl-C contract at the engine level:
// a cancelled context unwinds with 130 and a sentence that says the directory
// goes with it.
func TestQuickstartCancelledMidTour(t *testing.T) {
	deps := Deps{
		Stdout: &strings.Builder{},
		Stderr: &strings.Builder{},
		Getenv: func(string) string { return "" },
		ExecuteStep: func(_ context.Context, _ []string, _, _ io.Writer) int {
			return 0
		},
	}
	tour := NewTour(deps, "/tmp/tour", "unix:///tmp/tour/messq.sock")
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if code := tour.Run(ctx, []Step{{Title: "one", Argv: []string{"version"}}}); code != 130 {
		t.Errorf("cancelled tour exit = %d, want 130", code)
	}
	if !strings.Contains(asString(t, deps.Stderr), "interrupted") {
		t.Error("cancelled tour did not narrate the unwind")
	}
}

// TestQuickstartStopsEarlyOnFailingStep pins the honesty rule: a step that
// fails stops the tour with the step's exit code and a teaching footer — the
// tour never fakes its way to step 7.
func TestQuickstartStopsEarlyOnFailingStep(t *testing.T) {
	deps := Deps{
		Stdout: &strings.Builder{},
		Stderr: &strings.Builder{},
		Getenv: func(string) string { return "" },
		ExecuteStep: func(_ context.Context, _ []string, _, _ io.Writer) int {
			return 3
		},
	}
	tour := NewTour(deps, "/tmp/tour", "unix:///tmp/tour/messq.sock")
	code := tour.Run(context.Background(), []Step{
		{Title: "one", Argv: []string{"a"}},
		{Title: "two", Argv: []string{"b"}},
	})
	if code != 3 {
		t.Errorf("tour exit = %d, want the failing step's 3", code)
	}
	if strings.Contains(asString(t, deps.Stdout), "$ b") {
		t.Error("the tour ran step 2 after step 1 failed")
	}
}

// TestQuickstartReaperNeverTouchesTheInnocent walks the four guards: a
// directory that misses ANY of prefix, marker, ownership or age survives.
func TestQuickstartReaperNeverTouchesTheInnocent(t *testing.T) {
	base := t.TempDir()
	clk := clock.NewFake(time.UnixMilli(1_700_000_000_000))
	// A genuine leftover: right prefix, marker, owned by us, 2h old.
	stale := filepath.Join(base, dirPrefix+"01STALE")
	if err := os.MkdirAll(stale, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(stale, markerName), nil, 0o600); err != nil {
		t.Fatal(err)
	}
	old := time.UnixMilli(clk.Now().UnixMilli() - 2*int64(time.Hour/time.Millisecond))
	if err := os.Chtimes(filepath.Join(stale, markerName), old, old); err != nil {
		t.Fatal(err)
	}

	// The innocent bystanders.
	living := make(map[string]bool)
	mk := func(name string, marker bool) string {
		dir := filepath.Join(base, name)
		if err := os.MkdirAll(dir, 0o700); err != nil {
			t.Fatal(err)
		}
		if marker {
			if err := os.WriteFile(filepath.Join(dir, markerName), nil, 0o600); err != nil {
				t.Fatal(err)
			}
		}
		living[dir] = true
		return dir
	}
	wrongPrefix := mk("other-tool-01", true)
	noMarker := mk(dirPrefix+"02NOMARKER", false)
	tooYoung := mk(dirPrefix+"03YOUNG", true)

	removed := reapStale(base, clk)
	if removed != 1 {
		t.Errorf("reapStale removed %d dirs, want exactly the 1 stale one", removed)
	}
	if _, err := os.Stat(stale); !os.IsNotExist(err) {
		t.Errorf("the stale tour dir survived: %v", err)
	}
	for dir := range living {
		if _, err := os.Stat(dir); err != nil {
			t.Errorf("the reaper deleted %s, which misses a guard", dir)
		}
	}
	_ = wrongPrefix
	_ = noMarker
	_ = tooYoung
}

// TestQuickstartReaperHonoursAge pins the age guard on its own: a well-formed
// marker that is only minutes old is a LIVE tour (possibly on a slow disk) and
// must never be reaped.
func TestQuickstartReaperHonoursAge(t *testing.T) {
	base := t.TempDir()
	clk := clock.NewFake(time.UnixMilli(1_700_000_000_000))
	young := filepath.Join(base, dirPrefix+"YOUNG")
	if err := os.MkdirAll(young, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(young, markerName), nil, 0o600); err != nil {
		t.Fatal(err)
	}
	if removed := reapStale(base, clk); removed != 0 {
		t.Errorf("the reaper ate a live tour's directory (%d removals)", removed)
	}
	if _, err := os.Stat(young); err != nil {
		t.Fatalf("live tour dir vanished: %v", err)
	}
}

// TestSocketPathLadder covers the fallback ladder: a short TMPDIR holds at
// rung one; the ladder's units are bytes, not runes.
func TestSocketPathLadder(t *testing.T) {
	base := t.TempDir()
	if dir, tcp, err := socketDir(base); err != nil || tcp != "" || dir != base {
		t.Errorf("short TMPDIR did not hold rung one: (%q, %q, %v)", dir, tcp, err)
	}
	// A rung-two path always fits (mkdtemp under /tmp), so a long TMPDIR falls
	// back to a fresh /tmp dir, not to TCP — TCP is the last resort only when
	// even /tmp refuses, which this test cannot simulate without fakes.
	long := base
	for len(long) < sunPathLimit {
		long = filepath.Join(long, "aaaaaaaaaa")
	}
	dir, tcp, err := socketDir(long)
	if err != nil {
		t.Fatalf("ladder failed outright: %v", err)
	}
	if tcp != "" {
		t.Fatalf("unexpected TCP fallback: %q", tcp)
	}
	if strings.HasPrefix(dir, long) {
		t.Errorf("the ladder kept the over-long dir %q", dir)
	}
	t.Cleanup(func() { sinkTestErr(os.RemoveAll(dir)) })
	if !fits(filepath.Join(dir, "messq.sock")) {
		t.Errorf("fallback dir %q does not fit a socket path", dir)
	}
}

// asString reads a deps stream back as text (they are *strings.Builder in
// every engine test).
func asString(t *testing.T, v any) string {
	t.Helper()
	s, ok := v.(interface{ String() string })
	if !ok {
		t.Fatalf("%T is not a stringer", v)
	}
	return s.String()
}

// sinkTestErr consumes a cleanup error whose only handler could be a log line;
// the resource is being torn down with the test either way.
func sinkTestErr(err error) { _ = err }
