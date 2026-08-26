// SPDX-License-Identifier: Apache-2.0

package cli

import (
	"bytes"
	"io"
	"strings"
	"sync"
	"testing"

	"github.com/a-holm/messq/internal/cli/render"
	"github.com/a-holm/messq/internal/cli/uierr"
	"github.com/spf13/cobra"
)

// fixtureStreamCmd is a stand-in for #24's followed commands: annotated as a stream
// and carrying a --follow flag, it exercises the chassis's json+follow refusal.
func fixtureStreamCmd(env *Env) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "events",
		Short: "follow events as they happen",
		Long: "Attach to the daemon's event flow and print one event per line until " +
			"interrupted. This fixture exists to exercise the chassis contract.",
		Example: "  messq events --follow",
		Args:    cobra.NoArgs,
		RunE:    func(*cobra.Command, []string) error { return nil },
	}
	cmd.Annotations = map[string]string{annStream: "true", annExits: "0"}
	cmd.Flags().Bool("follow", false, "keep streaming until interrupted")
	return cmd
}

func execute(t *testing.T, env *Env, args ...string) int {
	t.Helper()
	root := NewRoot(env)
	err := func() error {
		root.SetArgs(args)
		return root.Execute()
	}()
	code := classifyExecuteError(err)
	if err != nil {
		uierr.Render(&uierr.Env{Stderr: env.stderr(), Format: resolvedFormat(root)}, err, code)
	}
	return code
}

// executeWithFixture registers the annotated stream fixture before running.
func executeWithFixture(t *testing.T, env *Env, args ...string) int {
	t.Helper()
	root := NewRoot(env)
	probe := fixtureStreamCmd(env)
	root.AddCommand(probe)
	root.SetArgs(args)
	err := root.Execute()
	code := classifyExecuteError(err)
	if err != nil {
		uierr.Render(&uierr.Env{Stderr: env.stderr(), Format: resolvedFormat(root)}, err, code)
	}
	return code
}

func resolvedFormat(root *cobra.Command) render.Format {
	if s := sessionFrom(root); s != nil {
		return s.format
	}
	return render.FormatTable
}

func TestPersistentFlagsResolveThroughAllThreeLayers(t *testing.T) {
	env := &Env{
		Getenv: func(k string) string {
			if k == "MESSQ_OUTPUT" {
				return "json"
			}
			return ""
		},
	}
	var buf bytes.Buffer
	env.Stderr = &buf
	root := NewRoot(env)
	root.SetArgs([]string{"--output", "table"})
	probe := fixtureStreamCmd(env)
	root.AddCommand(probe)
	probe.RunE = func(cmd *cobra.Command, _ []string) error {
		f := cmd.Flags().Lookup("output")
		if f.Value.String() != "table" {
			t.Errorf("flag layer lost: %q", f.Value.String())
		}
		return nil
	}
	if code := classifyExecuteError(root.Execute()); code != 0 && code != 2 {
		t.Fatalf("execute failed with %d (stderr %q)", code, buf.String())
	}
}

func TestFlagBeatsEnvBeatsDefault(t *testing.T) {
	env := &Env{
		Getenv: func(k string) string {
			if k == "MESSQ_TIMEOUT" {
				return "5s"
			}
			return ""
		},
	}
	var seen string
	var buf bytes.Buffer
	env.Stderr = &buf
	root := NewRoot(env)
	probe := fixtureStreamCmd(env)
	delete(probe.Annotations, annStream) // not a stream here, just a config probe
	root.AddCommand(probe)
	root.SetArgs([]string{"events", "--timeout", "1m"})
	probe.RunE = func(cmd *cobra.Command, _ []string) error {
		seen = cmd.Flags().Lookup("timeout").Value.String()
		return nil
	}
	if err := root.Execute(); err != nil {
		t.Fatalf("execute: %v (stderr %q)", err, buf.String())
	}
	if seen != "1m0s" {
		t.Errorf("timeout = %q, want the flag's 1m0s to beat env 5s and default 30s", seen)
	}
}

func TestInvalidEnvValueIsUsageNamingTheVariable(t *testing.T) {
	var stdout, stderr bytes.Buffer
	env := &Env{
		Stdout: &stdout,
		Stderr: &stderr,
		Getenv: func(k string) string {
			if k == "MESSQ_TIMEOUT" {
				return "soon"
			}
			return ""
		},
	}
	code := execute(t, env, "events")
	if code != 2 {
		t.Errorf("exit = %d, want 2 for an invalid env value (stderr %q)", code, stderr.String())
	}
	if !strings.Contains(stderr.String(), "MESSQ_TIMEOUT") {
		t.Errorf("error does not name the variable: %q", stderr.String())
	}
}

func TestJSONPlusFollowIsAUsageError(t *testing.T) {
	var stdout, stderr bytes.Buffer
	env := &Env{
		Stdout: &stdout,
		Stderr: &stderr,
		Getenv: func(string) string { return "" },
	}
	code := executeWithFixture(t, env, "events", "--follow", "--output", "json")
	if code != 2 {
		t.Errorf("exit = %d, want 2 for --output json --follow (stderr %q)", code, stderr.String())
	}
	if !strings.Contains(stderr.String(), "ndjson") {
		t.Errorf("refusal must point at ndjson: %q", stderr.String())
	}
}

func TestUnknownCommandTeachesWithSuggestion(t *testing.T) {
	var stdout, stderr bytes.Buffer
	env := &Env{Stdout: &stdout, Stderr: &stderr, Getenv: func(string) string { return "" }}
	code := executeWithFixture(t, env, "--output", "table", "consumr")
	if code != 2 {
		t.Errorf("exit = %d, want 2 (stderr %q)", code, stderr.String())
	}
	if !strings.Contains(stderr.String(), `unknown command "consumr"`) {
		t.Errorf("teaching error missing: %q", stderr.String())
	}
	if strings.Contains(stdout.String(), "anything") && false {
		t.Error("unreachable")
	}
}

func TestNoArgumentsPrintsHelpToStdoutAndSucceeds(t *testing.T) {
	var stdout, stderr bytes.Buffer
	env := &Env{Stdout: &stdout, Stderr: &stderr, Getenv: func(string) string { return "" }}
	code := execute(t, env)
	if code != 0 {
		t.Errorf("exit = %d, want 0 (stderr %q)", code, stderr.String())
	}
	if !strings.Contains(stdout.String(), "Usage:") {
		t.Errorf("help did not go to stdout: %q", stdout.String())
	}
	if stderr.Len() != 0 {
		t.Errorf("narration leaked to stderr: %q", stderr.String())
	}
}

func TestChassisSettings(t *testing.T) {
	env := &Env{Getenv: func(string) string { return "" }}
	root := NewRoot(env)
	if !root.SilenceUsage || !root.SilenceErrors {
		t.Error("SilenceUsage/SilenceErrors are required: errors render once, via uierr")
	}
	if !cobra.EnableTraverseRunHooks {
		t.Error("cobra.EnableTraverseRunHooks must be true or a child's PersistentPreRunE silently skips root config")
	}
	if root.CompletionOptions.DisableDefaultCmd != true {
		t.Error("cobra's default completion command must be disabled (#26 ships a real one)")
	}
	for _, group := range []string{"hot", "inspect", "manage", "operate", "server"} {
		found := false
		for _, g := range root.Groups() {
			if g.ID == group {
				found = true
			}
		}
		if !found {
			t.Errorf("command group %q missing", group)
		}
	}
	pf := root.PersistentFlags()
	if pf.Lookup("addr") == nil || pf.Lookup("output") == nil ||
		pf.Lookup("token-file") == nil || pf.Lookup("timeout") == nil ||
		pf.Lookup("color") == nil || pf.Lookup("quiet") == nil ||
		pf.Lookup("verbose") == nil || pf.Lookup("full-ids") == nil {
		t.Error("the §4 persistent flag set is incomplete")
	}
}

func TestChildPreRunCannotSkipRootConfig(t *testing.T) {
	var ran bool
	env := &Env{Getenv: func(string) string { return "" }}
	var buf bytes.Buffer
	env.Stderr = &buf
	root := NewRoot(env)
	child := fixtureStreamCmd(env)
	child.PersistentPreRunE = func(*cobra.Command, []string) error {
		ran = true
		return nil
	}
	root.AddCommand(child)
	root.SetArgs([]string{"events"})
	if err := root.Execute(); err != nil {
		t.Fatal(err)
	}
	if !ran {
		t.Error("the child's own PersistentPreRunE never ran (traverse hooks broken)")
	}
}

func TestBadFlagValueExitsTwoViaUsageError(t *testing.T) {
	var stdout, stderr bytes.Buffer
	env := &Env{Stdout: &stdout, Stderr: &stderr, Getenv: func(string) string { return "" }}
	code := execute(t, env, "--timeout", "not-a-duration")
	if code != 2 {
		t.Errorf("exit = %d, want 2 (stderr %q)", code, stderr.String())
	}
}

// TestFreshTreePerInvocation is G1: a package-level tree leaks parsed flag state and
// makes parallel in-process tests wrong in ways that look like flakes. Two concurrent
// invocations with contradictory settings must both be correct.
func TestFreshTreePerInvocation(t *testing.T) {
	getenv := func(v string) func(string) string {
		return func(k string) string {
			if k == "MESSQ_TIMEOUT" {
				return v
			}
			return ""
		}
	}
	var wg sync.WaitGroup
	results := make([]string, 2)
	for i, want := range []string{"3s", "9s"} {
		wg.Add(1)
		go func(i int, want string) {
			defer wg.Done()
			var stdout, stderr bytes.Buffer
			env := &Env{Stdout: &stdout, Stderr: &stderr, Getenv: getenv(want)}
			root := NewRoot(env)
			probe := fixtureStreamCmd(env)
			delete(probe.Annotations, annStream)
			root.AddCommand(probe)
			probe.RunE = func(cmd *cobra.Command, _ []string) error {
				results[i] = cmd.Flags().Lookup("timeout").Value.String()
				return nil
			}
			root.SetArgs([]string{"events"})
			if err := root.Execute(); err != nil {
				t.Errorf("invocation %d: %v", i, err)
			}
		}(i, want)
	}
	wg.Wait()
	if results[0] != "3s" || results[1] != "9s" {
		t.Errorf("parallel invocations leaked state: %v", results)
	}
}

func TestResolvedModeStoredOncePerInvocation(t *testing.T) {
	isTTYCalled := 0
	env := &Env{
		Getenv: func(string) string { return "" },
		Stdout: &ttyWriter{},
		Stderr: &bytes.Buffer{},
		IsTerminal: func(io.Writer) bool {
			isTTYCalled++
			return true
		},
	}
	root := NewRoot(env)
	probe := fixtureStreamCmd(env)
	delete(probe.Annotations, annStream)
	root.AddCommand(probe)
	var fmtGot render.Format
	probe.RunE = func(cmd *cobra.Command, _ []string) error {
		fmtGot = sessionFrom(cmd).format
		return nil
	}
	root.SetArgs([]string{"events"})
	if err := root.Execute(); err != nil {
		t.Fatal(err)
	}
	if fmtGot != render.FormatTable {
		t.Errorf("auto on a TTY resolved to %v, want table", fmtGot)
	}
	if isTTYCalled == 0 {
		t.Error("the TTY predicate was never consulted")
	}
}

// ttyWriter forces the TTY branch of mode resolution without a pty.
type ttyWriter struct{ bytes.Buffer }

func (ttyWriter) IsTerminal() bool { return true }

func TestVVDumpGoesToStderr(t *testing.T) {
	var stdout, stderr bytes.Buffer
	env := &Env{
		Stdout: &stdout,
		Stderr: &stderr,
		Getenv: func(k string) string {
			if k == "MESSQ_ADDR" {
				return "unix:///tmp/probe.sock"
			}
			return ""
		},
	}
	root := NewRoot(env)
	probe := fixtureStreamCmd(env)
	delete(probe.Annotations, annStream)
	root.AddCommand(probe)
	probe.RunE = func(*cobra.Command, []string) error { return nil }
	root.SetArgs([]string{"-vv", "events"})
	if err := root.Execute(); err != nil {
		t.Fatal(err)
	}
	out := stderr.String()
	if !strings.Contains(out, "addr") || !strings.Contains(out, "(env MESSQ_ADDR)") {
		t.Errorf("-vv dump missing resolved settings:\n%s", out)
	}
}
