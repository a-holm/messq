// SPDX-License-Identifier: Apache-2.0

package cli

import (
	"context"
	"encoding/json"
	"fmt"
	"io"

	"github.com/a-holm/messq/internal/cli/exit"
	"github.com/a-holm/messq/internal/cli/render"
	"github.com/a-holm/messq/internal/cli/uierr"
)

// The uniform PLAN §8 envelope for destructive CLI verbs. Every destructive
// command — seek (#28 slice 5), replay (slice 6), stream purge / rm, consumer rm,
// and #29's dlq redrive — builds one [Destructive] value and hands it to [Run];
// the helper owns the gating ladder and THE renderer, so a preview cannot drift
// from its own apply output (G7: a second renderer anywhere breaks
// TestPreviewEqualsApply).
//
// The ladder, in order, after exactly one plan pass:
//
//  1. Plan ([Destructive.Preview]) runs FIRST — before any gate — so refusals are
//     computed against real data, never guessed (the body's Decision 1: the
//     preview is the truth, produced by the same numbers Apply applies).
//  2. An empty plan short-circuits: nothing to destroy, no --confirm/--yes
//     demanded, Apply never called; PLAN §8 scopes confirmation at "non-empty
//     impact". Both modes still render through the shared face.
//  3. --dry-run renders the plan face and returns 0 without touching Apply.
//  4. Confirmation BY NAME ([Destructive.Resource]) must repeat --confirm
//     verbatim — missing or wrong refuses at exit 2 with Next carrying the exact
//     copy-pasteable command ([Destructive.Command]). There is no interactive
//     prompt anywhere (PLAN §8).
//  5. Non-TTY automation additionally requires --yes (G7's policy): a piped run
//     cannot see any warning, so its consent is refused rather than implied.
//     Interactive terminals do not need --yes.
//  6. Apply runs once; its error propagates unchanged for the funnel
//     (classifyExecuteError + uierr.Render), and the result face prints nothing
//     after a failure.
//
// The renderer differences between the plan face and the apply face are FROZEN to
// exactly two things — the header tense (`<verb> <resource> — plan (dry-run)` vs
// `<verb> <resource> — applied`) and the machine documents' boolean
// `"applied":false|true` (single flag: dry_run is its negation). Everything else,
// all three faces alike, comes from this file alone.
type Destructive struct {
	// Verb is the CLI verb the operator typed ("seek"): narration and the
	// machine document's verb field.
	Verb string
	// Resource names what is destroyed BY NAME ("orders"); --confirm must
	// repeat it verbatim. Optional for stream-scoped verbs that confirm
	// against another name — leave empty only when Command says why.
	Resource string
	// Command is the FULL invocation with --confirm <name> appended — the
	// exact bytes printed as Next on a refusal, copy-pasteable as-is.
	Command string
	// DryRun is --dry-run: plan-only pass, nothing durable happens (I10 fold
	// safety: a preview writes nothing).
	DryRun bool
	// Confirm is the verbatim --confirm value ("": none given).
	Confirm string
	// Yes is --yes, the non-TTY automation gate.
	Yes bool
	// TTY reports whether stdout is an interactive terminal (Env.IsTerminal
	// resolved by the calling command).
	TTY bool
	// Preview is the plan. Exactly-once per Run; must observe real state and
	// write NOTHING durable. Its rows render identically in every face.
	Preview func(context.Context) (DestructiveImpact, error)
	// Apply performs the destruction. Called at most once, never on a dry
	// run, never behind a failed gate.
	Apply func(context.Context) error
	// Hints is the teaching footer (pause→work→resume runbook steps); data in
	// every face, never loose prose.
	Hints []Hint
}

// DestructiveRow is one quantity of the plan: what and how many. Labels are
// command-authored (never server-derived bytes), so they bypass render.Safe.
type DestructiveRow struct {
	Field string // e.g. "pending dropped"
	Value int64
}

// DestructiveImpact is the planned change a destructive verb reports. An empty
// Rows slice (or only-zero values — nothing is actually destroyed) counts as an
// empty plan for the §8 gate ladder.
type DestructiveImpact struct {
	Rows []DestructiveRow
}

// Empty reports whether running the verb would destroy nothing.
func (imp DestructiveImpact) Empty() bool {
	for _, r := range imp.Rows {
		if r.Value != 0 {
			return false
		}
	}
	return true
}

// Run drives one destructive invocation through the ladder and renders the result
// face into out. Errors that reach the operator (refusals) are *uierr.UserError;
// Preview/Apply failures propagate unchanged for the caller's funnel.
func (d *Destructive) Run(ctx context.Context, f render.Format, out io.Writer) error {
	imp, planErr := d.Preview(ctx)
	if planErr != nil {
		return planErr
	}
	if imp.Empty() {
		return d.renderFace(out, f, imp, false)
	}
	if d.DryRun {
		return d.renderFace(out, f, imp, false)
	}
	if d.Confirm != d.Resource {
		return &uierr.UserError{
			Code: "usage",
			Summary: fmt.Sprintf(
				"%s %s destroys real data: confirm it by name with --confirm %s",
				d.Verb, d.name(), d.name(),
			),
			Next: []string{d.Command},
			Exit: exit.Usage,
		}
	}
	if !d.TTY && !d.Yes {
		return &uierr.UserError{
			Code: "usage",
			Summary: fmt.Sprintf(
				"%s %s is running without an interactive terminal: non-TTY automation must accept the consequences explicitly with --yes",
				d.Verb, d.name(),
			),
			Next: []string{d.Command},
			Exit: exit.Usage,
		}
	}
	if err := d.Apply(ctx); err != nil {
		return err
	}
	return d.renderFace(out, f, imp, true)
}

// name is Resource, or "(the whole target)" when a stream-scoped verb left it
// empty; prose stays grammatical while the refusal's Next keeps the real bytes.
func (d *Destructive) name() string {
	if d.Resource == "" {
		return "(the whole target)"
	}
	return d.Resource
}

// renderFace dispatches the resolved format. FormatAuto reaching here is the same
// programming error it is in render.Emit: formats resolve once per invocation.
func (d *Destructive) renderFace(out io.Writer, f render.Format, imp DestructiveImpact, applied bool) error {
	switch f {
	case render.FormatTable:
		return d.writeTable(out, imp, applied)
	case render.FormatJSON, render.FormatNDJSON:
		return d.writeDocument(out, imp, applied)
	case render.FormatAuto:
		// unreachable: formats resolve before any face renders (render.Emit's
		// contract); the listed Auto case keeps the closed-set check total.
		return fmt.Errorf("render: format was not resolved (call render.Resolve first)")
	default:
		return fmt.Errorf("render: unknown format %d", int(f))
	}
}

// writeTable draws the human face through the house TableWriter: one head line
// whose tense carries plan-vs-applied, the plan rows, then the hints block.
func (d *Destructive) writeTable(out io.Writer, imp DestructiveImpact, applied bool) error {
	tw := render.NewTableWriter(out)
	tense := "applied"
	if !applied {
		tense = "plan (dry-run)"
	}
	line := d.Verb
	if d.Resource != "" {
		line += " " + d.Resource
	}
	if err := tw.WriteLine(fmt.Sprintf("%s — %s", line, tense)); err != nil {
		return err
	}
	if err := tw.WriteLine(""); err != nil {
		return err
	}
	for _, r := range imp.Rows {
		if err := tw.WriteLine(fmt.Sprintf("  %s\t%s", r.Field, render.Count(r.Value))); err != nil {
			return err
		}
	}
	if err := tw.Flush(); err != nil {
		return err
	}
	return WriteHints(out, d.Hints)
}

// writeDocument encodes the frozen machine shape: flat applied flag, the impact
// array, next[] inside the document (hints are data), and the identifying verb +
// resource fields. encoding/json sorts map keys deterministically.
func (d *Destructive) writeDocument(out io.Writer, imp DestructiveImpact, applied bool) error {
	doc := map[string]any{
		"applied": applied,
		"impact":  d.documentRows(imp),
		"verb":    d.Verb,
	}
	if d.Resource != "" {
		doc["resource"] = d.Resource
	}
	if next := d.documentNext(); next != nil {
		doc["next"] = next
	}
	enc := json.NewEncoder(out)
	return enc.Encode(doc)
}

func (d *Destructive) documentRows(imp DestructiveImpact) []map[string]any {
	rows := make([]map[string]any, 0, len(imp.Rows))
	for _, r := range imp.Rows {
		rows = append(rows, map[string]any{"field": r.Field, "value": r.Value})
	}
	return rows
}

func (d *Destructive) documentNext() []map[string]string {
	if len(d.Hints) == 0 {
		return nil
	}
	next := make([]map[string]string, 0, len(d.Hints))
	for _, h := range d.Hints {
		entry := map[string]string{"cmd": h.Cmd}
		if h.Why != "" {
			entry["why"] = h.Why
		}
		next = append(next, entry)
	}
	return next
}
