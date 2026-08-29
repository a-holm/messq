// SPDX-License-Identifier: Apache-2.0

// Package quickstart is the guided tour (issue #26 §1): a step engine that
// executes REAL argv through the REAL command tree against an ephemeral
// in-process daemon, ending in the unprompted trace of a message that timed
// out into the DLQ. The tour is a product surface: what it prints is what it
// runs — TestQuickstartPrintsWhatItRuns pins that — and it never talks to a
// pre-existing daemon: every MESSQ_* variable from the operator's environment
// is ignored, with one printed line saying so.
//
// The package takes its command-tree access through the [Deps.ExecuteStep]
// seam, so it can live beside internal/cli without an import cycle: the chassis
// wires the seam to cli.NewRoot + cli.ExecuteTree (the production path), and
// the engine's tests wire scratch trees.
package quickstart

import (
	"context"
	"fmt"
	"io"
	"strings"

	"github.com/a-holm/messq/internal/cli/exit"
)

// Step is one tour step: a teaching sentence, the argv that IS executed (and
// echoed verbatim after the "$ " prompt), and optional post-step narration.
type Step struct {
	Title string   // one teaching sentence, printed above the command
	Argv  []string // executed verbatim; also what is echoed after the "$ "
	Note  string   // optional post-step narration (rendered only when non-empty)
}

// Deps is the engine's access to the process: the streams it prints on, the
// outer environment it sanitises, and the step runner (the real command tree in
// production).
type Deps struct {
	Stdout io.Writer
	Stderr io.Writer
	// Getenv reads the operator's environment; the engine only looks at it to
	// NAME what it is ignoring, never to use it.
	Getenv func(string) string
	// ExecuteStep runs one argv through the real command tree with the given
	// (already sanitised) streams, and returns the documented exit code.
	ExecuteStep func(ctx context.Context, argv []string, stdout, stderr io.Writer) int
	// IsTerminal reports whether stdout is a TTY (the pause default).
	IsTerminal bool
}

// Tour runs steps against an ephemeral daemon.
type Tour struct {
	// Dir is the ephemeral data dir; the tour owns creating and (unless Keep)
	// removing it.
	Dir string
	// Addr is the daemon address the tour's steps dial.
	Addr string
	// Out is the transcript (stdout); Err is narration (stderr).
	Out io.Writer
	Err io.Writer

	// NoBanner suppresses the banner (ascinema recordings).
	NoBanner bool

	// PoisonedEnv, when non-empty, is the MESSQ_* value the operator had set
	// that the tour is ignoring; the banner names it once.
	PoisonedEnv string

	// deps carries the process seams.
	deps Deps
}

// NewTour builds a tour over the given deps.
func NewTour(deps Deps, dir, addr string) *Tour {
	return &Tour{deps: deps, Dir: dir, Addr: addr, Out: deps.Stdout, Err: deps.Stderr}
}

// StepRecord is one step's outcome in the ndjson face.
type StepRecord struct {
	Step      int    `json:"step"`
	Title     string `json:"title"`
	Command   string `json:"command"`
	Exit      int    `json:"exit"`
	ElapsedMS int64  `json:"elapsed_ms"`
}

// banner prints the tour header: what directory, what durability, and the one
// line about the ignored environment.
func (t *Tour) banner() {
	if t.NoBanner {
		return
	}
	fmt.Fprintf(t.err(), "  messq · quickstart\n")
	fmt.Fprintf(t.err(), "  throwaway daemon in %s (deleted when we finish)\n", t.Dir)
	fmt.Fprintf(t.err(), "  durability=full · nothing outside that directory is touched\n")
	if t.PoisonedEnv != "" {
		fmt.Fprintf(t.err(), "  ignoring %s from your environment — the tour never talks to your real daemon\n", t.PoisonedEnv)
	}
	fmt.Fprintf(t.err(), "\n")
}

func (t *Tour) err() io.Writer {
	if t.Err == nil {
		return io.Discard
	}
	return t.Err
}

func (t *Tour) out() io.Writer {
	if t.Out == nil {
		return io.Discard
	}
	return t.Out
}

// echo renders the command line exactly as it is executed — the invariant
// TestQuickstartPrintsWhatItRuns pins. A tour that lies about what it ran is
// worse than no tour.
func echo(argv []string) string {
	return "$ " + strings.Join(argv, " ")
}

// envOverride is the sanitised environment every tour step runs under:
// MESSQ_ADDR, MESSQ_TOKEN_FILE and every other MESSQ_* variable from the
// operator's environment are ignored — someone with MESSQ_ADDR=prod exported
// must not be able to point the tour at production.
func envOverride(getenv func(string) string) func(string) string {
	return func(key string) string {
		if strings.HasPrefix(key, "MESSQ_") {
			return ""
		}
		return getenv(key)
	}
}

// poisonedEnvKeys is the MESSQ_* list the tour checks when naming what it
// ignores — the variables the CLI actually reads. The scan is table-driven (not
// os.Environ) so the behaviour is testable and the banner stays deterministic.
var poisonedEnvKeys = []string{
	"MESSQ_ADDR", "MESSQ_TOKEN_FILE", "MESSQ_DATA_DIR", "MESSQ_LISTEN",
	"MESSQ_DURABILITY", "MESSQ_OUTPUT", "MESSQ_AUTH_FILE",
}

// firstPoisonedEnv finds the operator's MESSQ_* value worth naming in the
// banner (ADDR first, then the rest in table order), or "" for a clean
// environment.
func firstPoisonedEnv(getenv func(string) string) string {
	if getenv == nil {
		return ""
	}
	for _, k := range poisonedEnvKeys {
		if v := getenv(k); v != "" {
			if v == k || v == "set" {
				// A key listed without a value (the generic scan shape) names
				// the variable alone.
				return k
			}
			return k + "=" + v
		}
	}
	return ""
}

// Run executes the steps in order against the tour's daemon and returns the
// process exit code: 0 when every step held, the failing step's code otherwise,
// 130 when the context was cancelled mid-tour (the Ctrl-C contract).
func (t *Tour) Run(ctx context.Context, steps []Step) int {
	t.banner()

	for i, step := range steps {
		if err := ctx.Err(); err != nil {
			return t.cancelled()
		}
		fmt.Fprintf(t.err(), "  %d/%d  %s\n\n", i+1, len(steps), step.Title)

		// Echo BEFORE execution: the transcript is what the operator reads, and
		// it is byte-identical to the argv the engine runs.
		fmt.Fprintln(t.out(), echo(step.Argv))

		code := t.deps.ExecuteStep(ctx, step.Argv, t.out(), t.err())
		if step.Note != "" && code == exit.OK {
			fmt.Fprintf(t.err(), "  %s\n", step.Note)
		}
		fmt.Fprintln(t.err())
		if code != exit.OK {
			fmt.Fprintf(t.err(), "  the tour stopped early: %q exited %d\n", step.Argv[0], code)
			fmt.Fprintf(t.err(), "  every command above works outside the tour — try it against\n")
			fmt.Fprintf(t.err(), "  a real daemon: messq serve --dev\n")
			return code
		}
	}

	t.footer()
	return exit.OK
}

func (t *Tour) cancelled() int {
	fmt.Fprintf(t.err(), "  interrupted — the tour unwinds and the directory goes with it\n")
	return 130
}

// footer is the teaching close: what you now know, and the next commands —
// ending (per the issue) with the redrive that #29 makes executable.
func (t *Tour) footer() {
	fmt.Fprintf(t.err(), "  You now know: publish · ack · nak with backoff · redelivery · ack_wait timeout · DLQ · trace.\n\n")
	fmt.Fprintf(t.err(), "  Next\n")
	for _, h := range []string{
		"    messq help concepts                       the vocabulary, one paragraph each",
		"    messq help lifecycle                      the state machine you just walked through",
		"    messq serve --data-dir ./messq-data       a daemon that keeps your data",
	} {
		fmt.Fprintln(t.err(), h)
	}
}
