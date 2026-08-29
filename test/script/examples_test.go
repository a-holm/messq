// SPDX-License-Identifier: Apache-2.0

package script

import (
	"context"
	"fmt"
	"io"
	"path/filepath"
	"strings"
	"testing"

	"github.com/a-holm/messq/internal/cli"
	"github.com/a-holm/messq/internal/cli/quickstart"
	"github.com/a-holm/messq/internal/clock"
	"github.com/spf13/cobra"
)

// TestExamplesExecute is the executable-docs harness for the CLI (issue §6):
// every cobra Example line that starts with `messq ` runs against a --dev
// in-process daemon and must exit 0. A line the harness cannot run carries a
// `# noexec: reason` suffix, and the reason must be non-empty — documentation
// that cannot run is documentation nobody checked.
//
// The harness executes examples in-process (cli.NewRoot + cli.ExecuteTree, the
// production funnel) with the tour's environment discipline: MESSQ_ADDR points
// at the throwaway daemon and nothing else is inherited.
func TestExamplesExecute(t *testing.T) {
	// One --dev daemon for the whole walk: commands that dial get a real,
	// fresh, throwaway daemon; nothing outside t.TempDir is touched.
	dir := filepath.Join(t.TempDir(), "data")
	daemon, err := quickstart.StartDaemon(dir, clock.System{})
	if err != nil {
		t.Fatalf("dev daemon: %v", err)
	}
	t.Cleanup(daemon.Stop)

	root := cli.NewRoot(&cli.Env{})
	walkExamples(t, root, daemon.Addr())
}

// walkExamples visits every command (hidden helpers included: their Example
// blocks still execute) and checks each line of its Example block.
func walkExamples(t *testing.T, cmd *cobra.Command, addr string) {
	t.Helper()
	for _, sub := range cmd.Commands() {
		walkExamples(t, sub, addr)
	}
	if strings.TrimSpace(cmd.Example) == "" {
		return
	}
	for i, line := range strings.Split(cmd.Example, "\n") {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		name := fmt.Sprintf("example[%s:%d]", cmd.CommandPath(), i+1)
		if reason, ok := noexecReason(line); ok {
			if reason == "" {
				t.Errorf("%s: example %q carries an empty noexec reason", name, line)
			}
			continue
		}
		if !strings.HasPrefix(line, "messq ") {
			t.Errorf("%s: example %q is not runnable and carries no `# noexec: reason`", name, line)
			continue
		}
		if code := runExample(t, addr, line); code != 0 {
			t.Errorf("%s: example %q exited %d; fix the example or annotate `# noexec: reason`",
				name, line, code)
		}
	}
}

// noexecReason reports the `# noexec: <reason>` suffix, if any.
func noexecReason(line string) (string, bool) {
	i := strings.Index(line, "# noexec:")
	if i < 0 {
		return "", false
	}
	return strings.TrimSpace(line[i+len("# noexec:"):]), true
}

// runExample executes one example line through the production funnel with the
// harness daemon's address injected and the machine face pinned.
func runExample(t *testing.T, addr, line string) int {
	t.Helper()
	argv := append([]string{"--addr", addr, "--output", "table"},
		strings.Fields(strings.TrimPrefix(line, "messq "))...)

	var out, errOut strings.Builder
	env := &cli.Env{
		Stdin:      strings.NewReader(""),
		Stdout:     &out,
		Stderr:     &errOut,
		Getenv:     func(string) string { return "" },
		IsTerminal: isTTYFalse,
		Width:      func() int { return 100 },
	}
	root := cli.NewRoot(env)
	root.SetArgs(argv)
	return cli.ExecuteTree(context.Background(), env, root, argv)
}

func isTTYFalse(io.Writer) bool { return false }
