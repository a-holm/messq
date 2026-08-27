// SPDX-License-Identifier: Apache-2.0

package cli

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/a-holm/messq/internal/cli/exit"
	"github.com/a-holm/messq/internal/cli/render"
	"github.com/a-holm/messq/internal/cli/uierr"
)

// errSeekBomb marks the Apply bomb used by the refusal tests: an Apply call after
// any of the confirmation gates failed would mean the helper destroyed data the
// operator never approved.
var errSeekBomb = errors.New("apply must never be reached")

// seekSpec builds the canonical Destructive under test: two impact rows, the exact
// copy-pasteable command, and an Apply bomb counted by *calls. Every test overrides
// the few fields it drives.
type seekRecorder struct {
	d            Destructive
	previewCalls int
	applyCalls   int
}

func newSeekSpec() *seekRecorder {
	rec := &seekRecorder{}
	rec.d = Destructive{
		Verb:     "seek",
		Resource: "orders",
		Command:  "messq seek orders c1 --to seq:42 --confirm orders",
		Preview: func(context.Context) (DestructiveImpact, error) {
			rec.previewCalls++
			return DestructiveImpact{Rows: []DestructiveRow{
				{Field: "messages matched", Value: 12},
				{Field: "pending dropped", Value: 3},
			}}, nil
		},
		Apply: func(context.Context) error {
			rec.applyCalls++
			return errSeekBomb
		},
		Hints: []Hint{{Cmd: "messq consumer resume orders c1", Why: "operators pause workers before seeking"}},
	}
	return rec
}

// normalization folds the two documented differences between a plan face and an
// apply face — the verb tense in the header and the applied boolean in the machine
// documents — onto common markers. After folding, the two faces of one invocation
// must be byte-equal (PLAN §8: --dry-run previews the real run's renderer).
func foldPlanApply(s string) string {
	return strings.NewReplacer(
		"seek orders — plan (dry-run)", "[header]",
		"seek orders — applied", "[header]",
		`"applied":false`, "[applied]",
		`"applied":true`, "[applied]",
	).Replace(s)
}

func runSeek(t *testing.T, rec *seekRecorder, mutate func(*Destructive), f render.Format) (string, error) {
	t.Helper()
	spec := rec.d
	if mutate != nil {
		mutate(&spec)
	}
	var out bytes.Buffer
	err := spec.Run(context.Background(), f, &out)
	return out.String(), err
}

// TestPreviewEqualsApply is THE G7 test: --dry-run and the real run render through
// the same function, differing only by tense + applied. A second renderer or any
// prose drift between the two passes breaks the folded byte-equality below.
func TestPreviewEqualsApply(t *testing.T) {
	for _, tc := range []struct {
		face    render.Format
		docLine bool // machine faces emit exactly one document line
	}{
		{render.FormatTable, false},
		{render.FormatJSON, true},
		{render.FormatNDJSON, true},
	} {
		t.Run(tc.face.String(), func(t *testing.T) {
			rec := newSeekSpec()
			planOut, perr := runSeek(t, rec, func(d *Destructive) { d.DryRun = true }, tc.face)
			if perr != nil {
				t.Fatalf("dry run failed: %v", perr)
			}
			if rec.previewCalls != 1 || rec.applyCalls != 0 {
				t.Fatalf("dry run ran preview=%d apply=%d, want preview=1 apply=0", rec.previewCalls, rec.applyCalls)
			}

			appliedOut, aerr := runSeek(t, rec, func(d *Destructive) {
				d.Confirm = "orders"
				d.Yes = true
				d.Apply = func(context.Context) error { rec.applyCalls++; return nil }
			}, tc.face)
			if aerr != nil {
				t.Fatalf("apply failed: %v", aerr)
			}
			if rec.applyCalls != 1 {
				t.Fatalf("apply count = %d, want 1", rec.applyCalls)
			}

			if foldPlanApply(planOut) != foldPlanApply(appliedOut) {
				t.Fatalf("preview and apply drift after tense/applied folding:\n--- preview ---\n%q\n--- apply ---\n%q",
					planOut, appliedOut)
			}
			switch tc.face {
			case render.FormatTable:
				for _, want := range []string{"seek orders — plan (dry-run)", "messages matched", "pending dropped"} {
					if !strings.Contains(planOut, want) {
						t.Fatalf("table plan missing %q:\n%s", want, planOut)
					}
				}
				if !strings.Contains(appliedOut, "seek orders — applied") || strings.Contains(appliedOut, "dry-run") {
					t.Fatalf("table apply header wrong:\n%s", appliedOut)
				}
			case render.FormatJSON, render.FormatNDJSON:
				for _, want := range []string{`"applied":false`, `"field":"messages matched","value":12`} {
					if !strings.Contains(planOut, want) {
						t.Fatalf("%s plan document missing %q:\n%s", tc.face, want, planOut)
					}
				}
			case render.FormatAuto:
				t.Fatal("auto must be resolved before a face renders")
			}
			if !strings.Contains(appliedOut, `"applied":true`) && tc.face != render.FormatTable {
				t.Fatalf("%s apply document lost applied=true:\n%s", tc.face, appliedOut)
			}
		})
	}
}

// TestDestructiveExactFaces pins the full rendered bytes for one invocation so the
// copy cannot drift silently; goldens arrive when concrete verbs adopt the helper.
func TestDestructiveExactFaces(t *testing.T) {
	rec := newSeekSpec()
	out, err := runSeek(t, rec, func(d *Destructive) { d.DryRun = true }, render.FormatTable)
	if err != nil {
		t.Fatalf("dry run failed: %v", err)
	}
	want := "seek orders — plan (dry-run)\n" +
		"\n" +
		"  messages matched  12\n" +
		"  pending dropped   3\n" +
		"\n" +
		"  next  messq consumer resume orders c1  (operators pause workers before seeking)\n"
	if out != want {
		t.Fatalf("table plan bytes drifted:\n--- got ---\n%q\n--- want ---\n%q", out, want)
	}

	jsonRec := newSeekSpec()
	jout, jerr := runSeek(t, jsonRec, func(d *Destructive) { d.DryRun = true }, render.FormatJSON)
	if jerr != nil {
		t.Fatalf("json dry run failed: %v", jerr)
	}
	if wantJSON := `{"applied":false,"impact":[{"field":"messages matched","value":12},{"field":"pending dropped","value":3}],"next":[{"cmd":"messq consumer resume orders c1","why":"operators pause workers before seeking"}],"resource":"orders","verb":"seek"}` + "\n"; jout != wantJSON {
		t.Fatalf("json plan bytes drifted:\ngot  %q\nwant %q", jout, wantJSON)
	}
}

// TestDestructiveRequiresConfirmationByName freezes the G7 ladder: a non-empty
// impact with no --confirm refuses with exit 2 and Next carrying the exact
// copy-pasteable command; a wrong name refuses identically. Apply stays untouched.
func TestDestructiveRequiresConfirmationByName(t *testing.T) {
	for _, tc := range []struct {
		name       string
		setConfirm string
	}{
		{name: "missing"},
		{name: "mismatch", setConfirm: "ordres"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rec := newSeekSpec()
			_, err := runSeek(t, rec, func(d *Destructive) { d.Confirm = tc.setConfirm }, render.FormatTable)
			var ue *uierr.UserError
			if !errors.As(err, &ue) {
				t.Fatalf("got %v (%T), want *uierr.UserError", err, err)
			}
			if ue.Exit != exit.Usage {
				t.Fatalf("exit = %d, want %d", ue.Exit, exit.Usage)
			}
			if !strings.Contains(ue.Summary, "--confirm orders") {
				t.Fatalf("summary must teach --confirm orders, got: %s", ue.Summary)
			}
			if len(ue.Next) != 1 || ue.Next[0] != "messq seek orders c1 --to seq:42 --confirm orders" {
				t.Fatalf("Next = %v, want exactly the copy-pasteable command", ue.Next)
			}
			if rec.previewCalls != 1 {
				t.Fatalf("preview calls = %d, want exactly one (gates come after the plan)", rec.previewCalls)
			}
			if rec.applyCalls != 0 {
				t.Fatal("unconfirmed destruction executed Apply")
			}
		})
	}
}

// TestDestructiveNonTTYRequiresYes pins the automation gate: confirmation accepted,
// but stdout not a terminal and no --yes — refused at exit 2 naming --yes.
func TestDestructiveNonTTYRequiresYes(t *testing.T) {
	rec := newSeekSpec()
	_, err := runSeek(t, rec, func(d *Destructive) {
		d.Confirm = "orders"
		d.TTY = false
		d.Yes = false
	}, render.FormatTable)
	var ue *uierr.UserError
	if !errors.As(err, &ue) || ue.Exit != exit.Usage {
		t.Fatalf("got %v, want usage UserError", err)
	}
	if !strings.Contains(ue.Summary, "--yes") {
		t.Fatalf("summary must teach --yes, got: %s", ue.Summary)
	}
	if rec.applyCalls != 0 {
		t.Fatal("non-TTY destruction executed Apply")
	}

	// --yes satisfies the gate and the apply proceeds.
	okRec := newSeekSpec()
	_, err = runSeek(t, okRec, func(d *Destructive) {
		d.Confirm = "orders"
		d.Yes = true
		d.Apply = func(context.Context) error { okRec.applyCalls++; return nil }
	}, render.FormatTable)
	if err != nil {
		t.Fatalf("--yes pass failed: %v", err)
	}
	if okRec.applyCalls != 1 {
		t.Fatalf("apply calls = %d, want 1", okRec.applyCalls)
	}
}

// TestDestructiveTTYWithoutYesRuns pins that interactive terminals do NOT need
// --yes — the gate exists purely because piped/cron runs cannot see a warning.
func TestDestructiveTTYWithoutYesRuns(t *testing.T) {
	rec := newSeekSpec()
	_, err := runSeek(t, rec, func(d *Destructive) {
		d.Confirm = "orders"
		d.TTY = true
		d.Apply = func(context.Context) error { rec.applyCalls++; return nil }
	}, render.FormatTable)
	if err != nil {
		t.Fatalf("interactive pass failed: %v", err)
	}
	if rec.applyCalls != 1 {
		t.Fatalf("apply calls = %d, want 1", rec.applyCalls)
	}
}

// TestDestructiveEmptyImpactShortCircuits pins Decision-adjacent behaviour: an
// empty plan has nothing to destroy, so the confirm/--yes gates do not demand
// anything — but Apply still never runs, and both modes render the same plan face.
func TestDestructiveEmptyImpactShortCircuits(t *testing.T) {
	for _, tc := range []struct {
		name  string
		flags func(*Destructive)
	}{
		{name: "bare"},
		{name: "dry_run", flags: func(d *Destructive) { d.DryRun = true }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rec := newSeekSpec()
			rec.d.Preview = func(context.Context) (DestructiveImpact, error) {
				rec.previewCalls++
				return DestructiveImpact{}, nil
			}
			out, err := runSeek(t, rec, tc.flags, render.FormatTable)
			if err != nil {
				t.Fatalf("empty plan refused: %v", err)
			}
			if rec.applyCalls != 0 {
				t.Fatal("empty plan ran Apply")
			}
			if rec.previewCalls != 1 {
				t.Fatalf("preview calls = %d, want 1 (the plan itself always runs)", rec.previewCalls)
			}
			if !strings.Contains(out, "plan (dry-run)") {
				t.Fatalf("empty plan still renders through the shared face, got:\n%s", out)
			}
		})
	}
}

// TestDestructivePlanErrorSurfacesBeforeGates pins the plan-first ordering from
// both directions: a failing plan returns the error verbatim whether the operator
// asked for a dry run or a confirmed apply, and the gates never fire first.
func TestDestructivePlanErrorSurfacesBeforeGates(t *testing.T) {
	planErr := errors.New("stream vanished")
	for _, tc := range []struct {
		name  string
		flags func(*Destructive)
	}{
		{name: "dry_run_path", flags: func(d *Destructive) { d.DryRun = true }},
		{name: "apply_path_unconfirmed", flags: nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rec := newSeekSpec()
			rec.d.Preview = func(context.Context) (DestructiveImpact, error) {
				return DestructiveImpact{}, planErr
			}
			_, err := runSeek(t, rec, tc.flags, render.FormatJSON)
			if !errors.Is(err, planErr) {
				t.Fatalf("got %v, want the plan error verbatim", err)
			}
			if rec.applyCalls != 0 {
				t.Fatal("failed plan reached Apply")
			}
		})
	}
}

// TestDestructiveApplyErrorPropagates keeps the funnel contract: whatever Apply
// returns is handed back unchanged for classifyExecuteError/uierr.Render, and the
// result face prints nothing after a failure.
func TestDestructiveApplyErrorPropagates(t *testing.T) {
	rec := newSeekSpec()
	out, err := runSeek(t, rec, func(d *Destructive) { d.Confirm = "orders"; d.Yes = true }, render.FormatTable)
	if !errors.Is(err, errSeekBomb) {
		t.Fatalf("got %v, want the Apply error itself", err)
	}
	if out != "" {
		t.Fatalf("failed apply printed a result face:\n%s", out)
	}
}
