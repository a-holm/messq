// SPDX-License-Identifier: Apache-2.0

package cli

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/signal"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/a-holm/messq/internal/buildinfo"
	"github.com/a-holm/messq/internal/cli/exit"
	"github.com/a-holm/messq/internal/cli/render"
	"github.com/a-holm/messq/internal/cli/uierr"
	"github.com/a-holm/messq/pkg/client"
	"github.com/spf13/cobra"
)

// Exit-code names kept from the #1 placeholder era; internal/cli/exit owns the
// documented contract and these aliases keep the pre-chassis command files stable
// until their owning issues migrate them onto it wholesale.
const (
	exitOK         = exit.OK
	exitError      = exit.Error
	exitUsage      = exit.Usage
	exitNotFound   = exit.NotFound
	exitConflict   = exit.Conflict
	exitEmpty      = exit.Empty
	exitPermission = exit.Denied
)

// usageError prints a teaching one-liner on stderr and reports the usage code. It
// remains the error face of the hand-parsed §8 flag sets (serve, verify) so their
// output bytes do not move before their owners migrate them.
func usageError(stderr io.Writer, msg string) int {
	fmt.Fprintf(stderr, "messq: %s\nrun 'messq help' for usage\n", msg)
	return exitUsage
}

// Run executes one messq invocation and returns the process exit code. Returning an
// int rather than an error keeps the exit-code contract testable without spawning a
// process (#1's signature, unchanged). Data goes to stdout, narration to stderr.
func Run(args []string, stdin io.Reader, stdout, stderr io.Writer) int {
	return RunEnv(context.Background(), DefaultEnv(stdin, stdout, stderr), args)
}

// RunEnv is the real entry point: it builds a FRESH command tree for this invocation,
// installs the signal contract, executes once, renders any failure exactly once via
// uierr on stderr, and returns the documented exit code.
func RunEnv(ctx context.Context, env *Env, args []string) int {
	if env == nil {
		env = &Env{}
	}
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	// First SIGINT/SIGTERM cancels the command context so long polls unwind and
	// in-flight work settles; a SECOND signal exits immediately — an operator who
	// hits ^C twice has said so. SIGPIPE stays deliberately unhandled: Go's
	// default die-on-write is correct for `messq events --follow | head -3`.
	// The watch records the first catch so the funnel can tell a signal-driven
	// unwind (exit 130, §7) from any other context cancellation.
	watch := &interruptWatch{}
	ctx = context.WithValue(ctx, interruptKey{}, watch)
	sigs := make(chan os.Signal, 2)
	signal.Notify(sigs, os.Interrupt, syscall.SIGTERM)
	defer signal.Stop(sigs)
	go func() {
		select {
		case <-ctx.Done():
			return
		case <-sigs:
			watch.fired.Store(true)
			cancel()
			select {
			case <-ctx.Done(): // graceful unwind won
				return
			case <-sigs:
				hardExit()
			}
		}
	}()

	newTree := NewRoot
	if env.build != nil {
		newTree = env.build
	}
	root := newTree(env)
	root.SetArgs(args)
	return ExecuteTree(ctx, env, root, args)
}

// ExecuteTree runs one assembled tree through the single error/exit funnel: execute
// once, render any failure exactly once via uierr on stderr (unless the failure
// rendered itself), and return the documented exit code. clitest uses it so harness
// runs take exactly the production path.
func ExecuteTree(ctx context.Context, env *Env, root *cobra.Command, args []string) int {
	if env == nil {
		env = &Env{}
	}
	root.SetArgs(args)
	err := root.ExecuteContext(ctx)
	if err == nil {
		return exit.OK
	}
	code := classifyExecuteError(err)
	face := taughtFace(root, err)
	if interruptedBySignal(ctx) && errors.Is(err, context.Canceled) {
		// The operator's first signal caused this unwind, not the daemon: §7 pins
		// "interrupted by signal" at 130 (128+SIGINT). Letting it classify as 6/1
		// invites cron retry loops against a command someone explicitly stopped
		// and leaks Go internals onto the error face.
		code = exitInterrupted
		face = &uierr.UserError{
			Code:    "interrupted",
			Summary: "interrupted by signal",
			Next:    []string{"messq --help"},
			Cause:   err, // keeps the unwind chain unwrappable and the exit field rendered
		}
	}
	if !rendersItself(err) {
		uierr.Render(&uierr.Env{Stderr: env.stderr(), Format: resolvedFormatOf(root)}, face, code)
	}
	return code
}

// exitInterrupted is §7's documented exception outside the 0–7 table: the shell
// convention for a process killed by SIGINT after it unwound gracefully.
const exitInterrupted = 130

// interruptWatch records that this invocation's own signal handler caught the first
// SIGINT/SIGTERM. It rides the context so clitest-driven trees (plain Background)
// keep today's behaviour and RunEnv stays the only producer of the state.
type interruptWatch struct{ fired atomic.Bool }

type interruptKey struct{}

func interruptedBySignal(ctx context.Context) bool {
	w, ok := ctx.Value(interruptKey{}).(*interruptWatch)
	return ok && w.fired.Load()
}

// taughtFace adapts a raw daemon envelope into the teaching type so rendering keeps
// the server's Message verbatim and its next[]/detail/trace_id intact (uierr's
// no-invented-text rule). Errors that already carry a *uierr.UserError — a command
// built its own teaching error, or adapted the envelope at the call site — pass
// through untouched; anything that is not a *client.Error has nothing to adapt.
func taughtFace(root *cobra.Command, err error) error {
	var ue *uierr.UserError
	if errors.As(err, &ue) {
		return err
	}
	var ce *client.Error
	if !errors.As(err, &ce) {
		return err
	}
	addr := ""
	if f := root.PersistentFlags().Lookup("addr"); f != nil {
		addr = f.Value.String()
	}
	return uierr.FromClient(ce, addr)
}

// preRendered is the marker for failures whose message already went to stderr in
// their owner's exact byte shape (serve's and verify's hand-parsed layers). The
// funnel honours the code and adds nothing — no second, drifting rendering.
type preRendered interface {
	error
	Silent() bool
}

func rendersItself(err error) bool {
	var p preRendered
	return errors.As(err, &p) && p.Silent()
}

type silentExit struct{ code int }

func (e *silentExit) Error() string { return fmt.Sprintf("exit %d", e.code) }
func (e *silentExit) ExitCode() int { return e.code }
func (e *silentExit) Silent() bool  { return true }

// hardExit restores the default signal disposition and re-raises SIGINT so the
// process dies with the shell-conventional 130 instead of unwinding half-torn state.
func hardExit() {
	signal.Reset(os.Interrupt, syscall.SIGTERM)
	if p, err := os.FindProcess(os.Getpid()); err == nil {
		if sigErr := p.Signal(syscall.SIGINT); sigErr != nil {
			// Nothing can report a failure here: stderr may be gone and the
			// fallback select below still stops the caller from returning.
			return
		}
	}
	// The re-raised signal lands asynchronously; block forever rather than return
	// into torn-down state. If delivery somehow fails, the caller still terminates.
	select {}
}

// resolvedFormatOf reports the mode this invocation resolved to, for the funnel's
// choice of error face. Before resolution (or without hooks) errors are human.
func resolvedFormatOf(root *cobra.Command) render.Format {
	if s := sessionFrom(root); s != nil {
		return s.format
	}
	return render.FormatTable
}

// assemble attaches every current command to the fresh tree. One file per command;
// new commands join here and nowhere else.
func assemble(root *cobra.Command, env *Env) {
	root.AddCommand(
		newVersionCmd(env),
		newServeCmd(env),
		newVerifyCmd(env),
		newAuthCmd(env),
		newBackupCmd(env),
		newDoctorCmd(env),
	)
}

func newVersionCmd(env *Env) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "version",
		Short: "print build information",
		Long: "Print the version, platform, Go toolchain and commit of this messq binary.\n" +
			"\n" +
			"The default output follows the global --output contract: a human line\n" +
			"on a terminal, one JSON document otherwise. Add --remote to also ask\n" +
			"the daemon at --addr for its build and report any skew between the\n" +
			"two binaries.",
		Example: "  messq version\n  messq version --output json | jq .commit",
		GroupID: "server",
		Args:    exactArgsMessage,
		RunE: func(cmd *cobra.Command, _ []string) error {
			format := render.FormatTable
			if s := sessionFrom(cmd); s != nil {
				format = s.format
			}
			out := env.stdoutOrDiscard()
			if format == render.FormatTable {
				fmt.Fprintln(out, buildinfo.Short())
			} else if err := json.NewEncoder(out).Encode(buildinfo.Get()); err != nil {
				return fmt.Errorf("write version json: %w", &exit.Err{Code: exit.Error})
			}
			return reportRemote(cmd, env, out, format)
		},
	}
	cmd.Flags().Bool("remote", false, "also ask the daemon for its build (2 s budget)")
	cmd.Annotations = map[string]string{annExits: "0,1,2,3,4,6,7"}
	return cmd
}

// remoteBudget bounds the opt-in /v1/info probe so `messq version --remote` can
// never hang a script that merely wanted to know what binary it is.
const remoteBudget = 2 * time.Second

// reportRemote asks the daemon for its build when --remote was given. Offline stays
// the default: plain `messq version` performs no network I/O at all. In machine
// modes the frozen document above is left untouched — skew narration goes to stderr,
// where jq pipelines never look.
func reportRemote(cmd *cobra.Command, env *Env, out io.Writer, format render.Format) error {
	remote, err := cmd.Flags().GetBool("remote")
	if err != nil || !remote {
		return nil
	}
	addr := cmd.Flags().Lookup("addr").Value.String()
	ctx, cancel := context.WithTimeout(cmd.Context(), remoteBudget)
	defer cancel()

	cl, err := client.New(addr)
	if err != nil {
		return err
	}
	info, err := cl.Info(ctx)
	if err != nil {
		return err
	}
	if format == render.FormatTable {
		fmt.Fprintf(out, "server      %s\n", render.Safe(info.Version))
		fmt.Fprintf(out, "durability  %s\n", render.Safe(info.Durability))
		fmt.Fprintf(out, "db_bytes    %d\n", info.DBBytes)
	}
	if info.Version == "" || info.Version == buildinfo.Get().Version {
		return nil
	}
	quiet := cmd.Flags().Lookup("quiet").Value.String() == "true"
	if !quiet {
		fmt.Fprintf(env.stderr(), "warning: daemon is %s, this CLI is %s — consider upgrading the older one\n",
			render.Safe(info.Version), render.Safe(buildinfo.Get().Version))
	}
	return nil
}

// exactArgsMessage refuses surplus arguments with a teaching message instead of
// cobra's bare "unknown command" phrasing.
func exactArgsMessage(cmd *cobra.Command, args []string) error {
	if len(args) == 0 {
		return nil
	}
	return uierr.Usage("unexpected argument %q: %s takes no arguments", args[0], cmd.Name())
}

func newServeCmd(env *Env) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "serve",
		Short: "run the messq daemon",
		Long: "Run the messq daemon against a data directory, serving the HTTP API over a\n" +
			"Unix socket or loopback TCP.\n" +
			"\n" +
			"serve keeps its own section-8 flag set and its own environment\n" +
			"fallbacks; it exits 74 when storage latched read-only, 75 when the\n" +
			"data directory is locked by another daemon, and 78 when the\n" +
			"configuration can never work — systemd's RestartPreventExitStatus\n" +
			"depends on those values.",
		Example:            "  messq serve --data-dir /var/lib/messq\n  messq serve --listen tcp://127.0.0.1:4390",
		GroupID:            "server",
		DisableFlagParsing: true, // #17's §8 flag set stays hand-parsed until #17 migrates it
		Args:               cobra.ArbitraryArgs,
		RunE: func(_ *cobra.Command, args []string) error {
			code := runServe(args, env.Getenv, env.stdoutOrDiscard(), env.stderr())
			if code == exit.OK {
				return nil
			}
			// runServe printed its own message in its owner's byte shape; the
			// funnel must honour the code without rendering a second copy.
			return &silentExit{code: code}
		},
	}
	cmd.Annotations = map[string]string{annExits: "0,1,74,75,78"}
	return cmd
}

func newVerifyCmd(env *Env) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "verify",
		Short: "check a data directory's invariants",
		Long: "Run the invariant checker over a messq data directory and report each check\n" +
			"as ok, violated or skipped.\n" +
			"\n" +
			"verify works on a stopped directory, a live one, or a backup copy,\n" +
			"and answers \"is my broker's state sound?\" in seconds. Its exit codes\n" +
			"(0 clean, 1 violations found, 2 usage, 3 directory missing, 7 not\n" +
			"permitted) are part of the crash-harness contract.",
		Example:            "  messq verify --data-dir /var/lib/messq --deep",
		GroupID:            "server",
		DisableFlagParsing: true, // #8's §8 flag set stays hand-parsed until #8 migrates it
		Args:               cobra.ArbitraryArgs,
		RunE: func(_ *cobra.Command, args []string) error {
			code := runVerify(args, env.Getenv, env.stdoutOrDiscard(), env.stderr())
			if code == exit.OK {
				return nil
			}
			return &silentExit{code: code}
		},
	}
	cmd.Annotations = map[string]string{annExits: "0,1,2,3,7"}
	return cmd
}
