// SPDX-License-Identifier: Apache-2.0

package quickstart

import (
	"context"
	"fmt"
	"io"
	"os"

	"github.com/a-holm/messq/internal/cli/exit"
	"github.com/a-holm/messq/internal/cli/uierr"
	"github.com/a-holm/messq/internal/clock"
	"github.com/spf13/cobra"
)

// Deps extends the process seams with the ones the command needs beyond the
// tour: the exact-args runner every step executes through. Production passes
// the chassis Env's values; the ExecuteStep closure (wired in internal/cli's
// assemble) runs cli.NewRoot + cli.ExecuteTree — the real command tree, real
// client, real socket.
//
// NewQuickstartCmd builds `messq quickstart`: the step engine, the ephemeral
// daemon, the flags, the cleanup, the reaper.
func NewQuickstartCmd(deps Deps, executeStep func(ctx context.Context, argv []string, stdout, stderr io.Writer) int) *cobra.Command {
	deps.ExecuteStep = executeStep
	cmd := &cobra.Command{
		Use:   "quickstart",
		Short: "run the five-minute tour on a throwaway daemon",
		Long: "Run the guided tour: a throwaway daemon in a temp directory that you\n" +
			"publish to, ack from, nak with backoff, and deliberately let a message\n" +
			"time out into the DLQ — ending with the unprompted trace of the dead\n" +
			"message. The tour runs real commands, and prints exactly what it runs.\n\n" +
			"Nothing outside the tour's own directory is touched, and every MESSQ_*\n" +
			"variable from your environment is ignored: the tour never talks to your\n" +
			"real daemon. No config file, no --data-dir, no setup — that is the point.",
		Example: "  messq quickstart # noexec: the full tour needs the delivery commands (#13/#14)\n" +
			"  messq quickstart --output ndjson # noexec: one record per step, for machines; same dependency\n" +
			"  messq quickstart --keep # noexec: keeps a data dir; same dependency",
		Args: func(_ *cobra.Command, args []string) error {
			if len(args) == 0 {
				return nil
			}
			return uierr.Usage("unexpected argument %q: quickstart takes no arguments", args[0])
		},
		GroupID: "server",
		RunE: func(c *cobra.Command, _ []string) error {
			code := runQuickstart(c, deps)
			if code == exit.OK {
				return nil
			}
			// The tour printed its own transcript and footer; the funnel must
			// honour the code and add nothing.
			return &silentExit{code: code}
		},
	}
	fs := cmd.Flags()
	fs.Bool("keep", false, "keep the throwaway data dir and print the command that reopens it")
	fs.Bool("pause", deps.IsTerminal, "wait for Enter between steps (default: only on a terminal)")
	fs.String("output", "text", "output mode: text|ndjson — ndjson emits one record per step")
	fs.Bool("no-banner", false, "skip the banner (for asciinema recordings)")
	return cmd
}

// silentExit marks outcomes the tour has already rendered itself: the funnel
// honours the code and adds nothing.
type silentExit struct{ code int }

func (e *silentExit) Error() string { return fmt.Sprintf("exit %d", e.code) }
func (e *silentExit) ExitCode() int { return e.code }
func (e *silentExit) Silent() bool  { return true }

// runQuickstart is the tour's orchestration: reap stale dirs, prepare the
// ephemeral dir, start the in-process daemon, run the steps, clean up.
func runQuickstart(cmd *cobra.Command, deps Deps) int {
	stderr := deps.Stderr

	// The reaper runs FIRST: a crashed tour's leftovers (prefix + marker + uid +
	// age > 1h) go before this one starts. Failures here never block the tour.
	reapStale(os.TempDir(), clock.System{})

	keep := flagBool(cmd, "keep")
	noBanner := flagBool(cmd, "no-banner")
	output := flagString(cmd, "output")
	if output != "text" && output != "ndjson" {
		fmt.Fprintf(stderr, "messq quickstart: --output %q is not one of text|ndjson\n", output)
		return exit.Usage
	}

	dir, note, err := prepareDir(os.TempDir())
	if err != nil {
		fmt.Fprintf(stderr, "messq quickstart: %v\n", err)
		return exit.Error
	}

	daemon, err := StartDaemon(dir, clock.System{})
	if err != nil {
		fmt.Fprintf(stderr, "messq quickstart: %v\n", err)
		return exit.Error
	}
	tour := NewTour(deps, dir, daemon.Addr())
	tour.NoBanner = noBanner
	tour.PoisonedEnv = firstPoisonedEnv(deps.Getenv)
	if note != "" {
		fmt.Fprintln(stderr, note)
	}

	code := tour.Run(cmd.Context(), TourSteps())

	// Cleanup: stop the daemon first (it holds open files), then remove the dir
	// unless --keep — in which case the reopening command is printed.
	daemon.Stop()
	if keep {
		fmt.Fprintf(stderr, "  kept %s\n  messq serve --data-dir %s   reopens this daemon\n", dir, dir)
	} else {
		if rmErr := os.RemoveAll(dir); rmErr != nil {
			fmt.Fprintf(stderr, "  could not remove %s: %v\n", dir, rmErr)
		} else {
			fmt.Fprintf(stderr, "  removed %s (keep it next time with --keep)\n", dir)
		}
	}
	return code
}

// prepareDir creates the tour's ephemeral directory, applying the socket-path
// ladder (issue §8): a TMPDIR whose socket path would exceed the kernel limit
// loses to /tmp; the note ("" when the ladder held at the first rung) is what
// the banner prints about the choice.
func prepareDir(preferred string) (dir string, note string, err error) {
	dir, tcpAddr, lErr := socketDir(preferred)
	if lErr != nil {
		return "", "", lErr
	}
	if tcpAddr != "" {
		return "", "  (long TMPDIR: the tour daemon listens on " + tcpAddr + " instead of a unix socket)", nil
	}
	return dir, "", nil
}

// flagBool reads a boolean flag, treating an impossible read as false (the
// flags are declared by NewQuickstartCmd itself; a mismatch is a compile-time
// bug this helper turns into a harmless default).
func flagBool(cmd *cobra.Command, name string) bool {
	v, err := cmd.Flags().GetBool(name)
	return err == nil && v
}

// flagString reads a string flag with the same self-declared guarantee.
func flagString(cmd *cobra.Command, name string) string {
	v, err := cmd.Flags().GetString(name)
	if err != nil {
		return ""
	}
	return v
}
