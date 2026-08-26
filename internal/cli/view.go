// SPDX-License-Identifier: Apache-2.0

package cli

import (
	"fmt"
	"io"
)

// View is the contract every command hands the renderer (issue #24 slice 2, PLAN §8):
// three faces rendered from one value, so "never a third mode" is enforceable by
// construction. A new command cannot ship with only one of the faces —
// TestEveryViewHasThreeFaces walks every view this package produces.
//
// Data goes to the faces; narration (warnings, findings, progress) goes to stderr
// outside this contract. Hints are data too — the renderer places them as the `next`
// block in the table face and as a next[] array in the JSON face — but they must never
// leak into the JSON document body itself, which the three-faces test pins.
type View interface {
	// Table writes the human face: aligned columns, relative times, prose.
	Table(w io.Writer) error
	// JSON returns the one frozen machine document. List commands return a value with
	// an items array; scalar commands return the single object.
	JSON() any
	// NDJSON returns one record per item for the streaming face, or nil for a scalar
	// command — the renderer then emits the JSON document itself as a single line
	// (`--output ndjson` on `consumer pause` is an NDJSON stream of one).
	NDJSON() []any
	// Hints returns the next-useful-command footer (§12's rule mechanised in
	// TestHintsResolve once #23 lands the cobra tree).
	Hints() []Hint
	// ExitCode returns the documented outcome: 0, or 4 conflict/stale, 5 empty/timeout,
	// and so on per PLAN §8's table.
	ExitCode() int
}

// Hint is one entry of the teaching footer. Cmd is the complete command line including
// the leading "messq", so a resolve test can hand it straight to the command tree;
// Why is the optional reason rendered only in the table face.
type Hint struct {
	Cmd string `json:"cmd"`
	Why string `json:"why,omitempty"`
}

// WriteHints renders the `next` block that closes every inspect command (§8's rule),
// exactly as it appears in the table face:
//
//	next  messq peek orders --seq 10496 --raw
//	      messq trace 01J8ZQ4P…
//
// The JSON face renders the same entries as a next[] array inside the document; hints
// are data, never loose prose. A view with no hints prints nothing.
func WriteHints(w io.Writer, hs []Hint) error {
	if len(hs) == 0 {
		return nil
	}
	if _, err := fmt.Fprintln(w); err != nil {
		return err
	}
	for i, h := range hs {
		prefix := "        "
		if i == 0 {
			prefix = "  next  "
		}
		if _, err := fmt.Fprintf(w, "%s%s", prefix, h.Cmd); err != nil {
			return err
		}
		if h.Why != "" {
			if _, err := fmt.Fprintf(w, "  (%s)", h.Why); err != nil {
				return err
			}
		}
		if _, err := fmt.Fprintln(w); err != nil {
			return err
		}
	}
	return nil
}
