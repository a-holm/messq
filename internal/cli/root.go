// SPDX-License-Identifier: Apache-2.0

package cli

import (
	"context"
	"io"
	"os"
	"strconv"
	"time"

	"github.com/a-holm/messq/internal/buildinfo"
	"github.com/a-holm/messq/internal/cli/help"
	"github.com/a-holm/messq/internal/clock"

	"github.com/a-holm/messq/internal/cli/conf"
	"github.com/a-holm/messq/internal/cli/exit"
	"github.com/a-holm/messq/internal/cli/render"
	"github.com/a-holm/messq/internal/cli/uierr"
	"github.com/spf13/cobra"
)

// The one process-wide cobra setting lives at package init, not inside NewRoot: a
// write there would race between parallel fresh trees (G1).
func init() {
	cobra.EnableTraverseRunHooks = true
}

// Env is the seam every command is built from (issue §2): writers, environment,
// clock, TTY predicate. Nothing inside internal/cli reads package-level os state, so
// every CLI behaviour is an in-process table test with no subprocess and no
// t.Setenv race. A fresh [NewRoot] per invocation keeps parsed flag state from
// leaking between calls (G1).
type Env struct {
	Stdin      io.Reader
	Stdout     io.Writer
	Stderr     io.Writer
	Getenv     func(string) string
	Now        func() time.Time     // the clock seam; affects rendering only
	IsTerminal func(io.Writer) bool // default: *os.File character-device probe
	Width      func() int           // terminal columns; 0 = unlimited

	// build replaces the assembled command tree for ONE RunEnv call. Zero value
	// keeps [NewRoot]. In-package tests use it to hang a scratch command onto the
	// real entry point (the subprocess interrupt probe); clitest drives trees
	// without RunEnv and does not need it.
	build func(*Env) *cobra.Command
}

// Annotations used across the tree.
const (
	// annStream marks a streaming command (`sub`, `events --follow`): auto mode
	// resolves it to ndjson on a pipe instead of json.
	annStream = "messq.stream"
	// annExits documents the exit codes a command can produce; the DX linter
	// checks each name against the contract.
	annExits = "messq.exits"
)

// invocation is one resolved configuration pass: computed once in the root's
// PersistentPreRunE, then a plain value any command can read via [sessionFrom].
type invocation struct {
	format render.Format
	colour bool
	env    *Env
}

type sessionKey struct{}

// sessionFrom returns the invocation resolved for this execution.
func sessionFrom(cmd *cobra.Command) *invocation {
	for c := cmd; c != nil; c = c.Parent() {
		if v, ok := c.Context().Value(sessionKey{}).(*invocation); ok {
			return v
		}
	}
	return nil
}

// NewRoot builds the command tree fresh for ONE invocation. Callers own the Env;
// run.go's entry points pair it with the render/classify funnel.
func NewRoot(env *Env) *cobra.Command {
	if env == nil {
		env = &Env{}
	}
	root := &cobra.Command{
		Use:   "messq",
		Short: "a lightweight, single-binary queue daemon",
		Long: "messq is a lightweight, single-binary message queue for Linux.\n" +
			"\n" +
			"The same binary runs the daemon (serve) and drives it as a client;\n" +
			"every command answers to the same flags, environment variables,\n" +
			"output modes and exit-code contract, so scripts written against one\n" +
			"command keep working against all of them.",
		SilenceUsage:               true,
		SilenceErrors:              true,
		SuggestionsMinimumDistance: 2,
		Example:                    "  messq version\n  messq serve --data-dir /var/lib/messq\n  messq verify --deep --data-dir /var/lib/messq",
		Version:                    buildinfo.Short(),
		CompletionOptions:          cobra.CompletionOptions{DisableDefaultCmd: true},
		Args:                       cobra.ArbitraryArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			if len(args) == 0 {
				cmd.SetOut(env.Stdout)
				return cmd.Help()
			}
			// With a RunE on the root, cobra hands an unknown first token here
			// as an argument — which is exactly where the teaching error wants
			// to be built: with did-you-mean candidates from the real tree.
			ue := uierr.Usage("unknown command %q", args[0])
			ue.Suggest = cmd.SuggestionsFor(args[0])
			return ue
		},
	}
	// A child's own PersistentPreRunE (serve) must not silently skip the root's
	// config resolution — the nastiest cobra footgun in this design. The flag is
	// a process-wide cobra setting: set once in init, never written per-invocation
	// (a write here would be a data race between parallel fresh trees).

	fs := root.PersistentFlags()
	fs.StringP("addr", "a", "unix:///run/messq/messq.sock", "daemon address (unix://, http://, https://)")
	fs.StringP("output", "o", "auto", "output mode: auto|table|json|ndjson")
	fs.String("token-file", "", "file holding the bearer token, mode 0600")
	fs.Duration("timeout", 30*time.Second, "per-request deadline (0 = none)")
	fs.String("color", render.ColourAuto, "colour: auto|always|never")
	fs.BoolP("quiet", "q", false, "suppress narration; data and errors unaffected")
	fs.CountP("verbose", "v", "narrate (-vv dumps each HTTP exchange and the resolved config)")
	fs.Bool("full-ids", false, "print full ULIDs instead of abbreviated ones")

	root.AddGroup(&cobra.Group{ID: "hot", Title: "Hot path"})
	root.AddGroup(&cobra.Group{ID: "inspect", Title: "Inspect"})
	root.AddGroup(&cobra.Group{ID: "manage", Title: "Manage"})
	root.AddGroup(&cobra.Group{ID: "operate", Title: "Operate"})
	root.AddGroup(&cobra.Group{ID: "server", Title: "Server"})

	root.SetVersionTemplate(buildinfo.Short() + "\n")
	root.SetOut(env.stdoutOrDiscard())
	root.SetErr(env.stderr())
	// One format for flag errors: a teaching usage failure → exit 2, usage text
	// on stderr via the normal funnel, never a raw pflag dump.
	root.SetFlagErrorFunc(func(_ *cobra.Command, err error) error {
		return uierr.Usage("%v", err)
	})
	root.PersistentPreRunE = resolveInvocation(env, root)
	assemble(root, env)

	// Issue #26 §4: the help topics ride the tree as additional help topics, and
	// the default help command is overridden so `messq help <topic>` renders and
	// `messq help nosuchtopic` is a teaching usage error (exit 2, suggestions,
	// the topic list) instead of cobra's bare exit-0 note.
	te := topicEnvAdaptor{env: env}
	root.SetHelpCommand(help.NewHelpCommand(te, root))
	root.AddCommand(help.NewTopicCommands(te)...)

	return root
}

// topicEnvAdaptor adapts cli.Env to help.TopicEnv without an import cycle in
// the other direction (help never imports cli).
type topicEnvAdaptor struct{ env *Env }

func (t topicEnvAdaptor) Stdout() io.Writer { return t.env.stdoutOrDiscard() }

// Colour for topics: topics are prose documentation, rendered plain unless a
// human terminal asks — the colour resolution runs once per invocation and the
// adaptor has no invocation at build time, so the safe default (plain) stands
// and the renderer's TTY colour path is exercised through clitest.
func (t topicEnvAdaptor) Colour() bool { return false }

// resolveInvocation returns the root hook that applies the three config layers and
// resolves the output contract once per execution. Running it in the ROOT hook (with
// cobra.EnableTraverseRunHooks) guarantees local flags of the executing command get
// their env fallback too.
//
// The resolved pass is stored on BOTH commands' contexts: the executing child reads
// it via sessionFrom(cmd) inside its RunE, while the funnel's error-face lookup
// (resolvedFormatOf) runs on the ROOT after Execute returns — cobra copies the
// parent's context into the child before hooks run, so a value stored only on the
// child is invisible there and machine modes would silently lose their error face.
func resolveInvocation(env *Env, root *cobra.Command) func(*cobra.Command, []string) error {
	return func(cmd *cobra.Command, _ []string) error {
		getenv := env.Getenv
		if getenv == nil {
			getenv = func(string) string { return "" }
		}
		if err := conf.ApplyEnv(cmd, getenv); err != nil {
			return uierr.Usage("%s", err)
		}

		format, err := render.Parse(cmd.Flags().Lookup("output").Value.String())
		if err != nil {
			return uierr.Usage("%s", err)
		}
		isTTY := env.IsTerminal != nil && env.IsTerminal(env.stdoutOrDiscard())
		streaming := cmd.Annotations[annStream] == "true"

		following := false
		if f := cmd.Flags().Lookup("follow"); f != nil && f.Changed {
			following = true
		}
		if following && format == render.FormatJSON {
			return uierr.Usage("--output json cannot follow: a followed stream has no final document; use --output ndjson")
		}

		sess := &invocation{
			format: render.Resolve(format, isTTY, streaming),
			colour: render.Colour(cmd.Flags().Lookup("color").Value.String(), getenv, isTTY),
			env:    env,
		}
		ctx := context.WithValue(cmd.Context(), sessionKey{}, sess)
		cmd.SetContext(ctx)
		root.SetContext(ctx)

		if n, pErr := strconv.Atoi(cmd.Flags().Lookup("verbose").Value.String()); pErr == nil && n >= 2 {
			conf.Dump(env.stderr(), cmd, getenv)
		}
		return nil
	}
}

// stderr tolerates a nil writer so hooks never panic on partial Envs.
func (e *Env) stderr() io.Writer {
	if e == nil || e.Stderr == nil {
		return io.Discard
	}
	return e.Stderr
}

func (e *Env) stdoutOrDiscard() io.Writer {
	if e == nil || e.Stdout == nil {
		return io.Discard
	}
	return e.Stdout
}

// DefaultEnv fills the seam from process state. Only the process entry points call
// it; everything below internal/cli receives an explicit Env. The wall clock is read
// through internal/clock — the one place allowed to touch it.
func DefaultEnv(stdin io.Reader, stdout, stderr io.Writer) *Env {
	return &Env{
		Stdin:      stdin,
		Stdout:     stdout,
		Stderr:     stderr,
		Getenv:     os.Getenv,
		Now:        (clock.System{}).Now,
		IsTerminal: IsTerminal,
		Width:      func() int { return 0 }, // COLUMNS wiring arrives with #26's goldens
	}
}

// IsTerminal reports whether w is an interactive terminal — the stdlib probe, kept
// injectable so tests force TTY-ness without a pty.
func IsTerminal(w io.Writer) bool {
	f, ok := w.(*os.File)
	if !ok {
		return false
	}
	info, err := f.Stat()
	if err != nil {
		return false
	}
	return info.Mode()&os.ModeCharDevice != 0
}

// classifyExecuteError maps a failed Execute onto the documented contract. It is the
// funnel's second half; the entry points pair it with uierr.Render.
func classifyExecuteError(err error) int {
	if err == nil {
		return exit.OK
	}
	code := exit.Of(err)
	if code == exit.OK { // a failed Execute must never report success
		return exit.Error
	}
	return code
}
