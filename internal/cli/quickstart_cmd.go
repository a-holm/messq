// SPDX-License-Identifier: Apache-2.0

package cli

import (
	"context"
	"io"
	"strings"

	"github.com/a-holm/messq/internal/cli/quickstart"
	"github.com/spf13/cobra"
)

// quickstartExecuteStep is the tour's production step runner: one argv through
// the real command tree — a fresh NewRoot per step (the chassis's one-invocation
// rule), ExecuteTree through the single error/exit funnel, and the SANITISED
// environment the tour builds (every MESSQ_* ignored) so the tour's steps dial
// the tour's daemon and nothing else.
func quickstartExecuteStep(ctx context.Context, argv []string, stdout, stderr io.Writer) int {
	env := &Env{
		Stdin:      strings.NewReader(""),
		Stdout:     stdout,
		Stderr:     stderr,
		Getenv:     func(string) string { return "" }, // the tour's sanitised environment
		IsTerminal: IsTerminal,
		Width:      func() int { return 100 },
	}
	root := NewRoot(env)
	root.SetArgs(argv)
	return ExecuteTree(ctx, env, root, argv)
}

// newQuickstartCmds builds the tour and its hidden handler on the chassis.
func newQuickstartCmds(env *Env) []*cobra.Command {
	deps := quickstart.Deps{
		Stdout:     env.Stdout,
		Stderr:     env.Stderr,
		Getenv:     env.Getenv,
		IsTerminal: env.IsTerminal != nil && env.IsTerminal(env.Stdout),
	}
	return []*cobra.Command{
		quickstart.NewQuickstartCmd(deps, quickstartExecuteStep),
		quickstart.NewHandlerCmd(deps),
	}
}
