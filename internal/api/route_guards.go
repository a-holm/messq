// SPDX-License-Identifier: Apache-2.0

package api

import (
	"fmt"
	"net/http"

	"github.com/a-holm/messq/internal/errs"
)

// The confirm handshake and dry-run gate (issue #15 §3/§12). Every destructive verb
// answers 409 confirm_required (with the BLAST RADIUS in the message and the exact
// command in next) when ?confirm= is absent, and 409 confirm_mismatch naming BOTH
// values when it is wrong — a silent accept is how "the wrong terminal" deletes prod.
// ?dry_run=1 on a route that does not declare DryRun refuses with
// dry_run_unsupported: an ignored preview flag is how someone deletes believing they
// previewed.

// confirmRequiredError says a destructive action lacks its name confirmation and what
// the action would remove.
type confirmRequiredError struct {
	kind  string // "stream" | "consumer"
	name  string
	blast string // rendered counts, e.g. `2 messages and 1 consumer`
}

func (e *confirmRequiredError) Error() string {
	return fmt.Sprintf("deleting %s %q removes %s — pass ?confirm=%s to go through with it",
		e.kind, e.name, e.blast, e.name)
}
func (*confirmRequiredError) Unwrap() error { return errs.ErrConflict }

// confirmMismatchError names both values because copy-paste from the wrong terminal is
// the real failure mode.
type confirmMismatchError struct {
	kind string
	got  string
	want string
}

func (e *confirmMismatchError) Error() string {
	return fmt.Sprintf("confirm parameter %q does not match the %s name %q",
		e.got, e.kind, e.want)
}
func (*confirmMismatchError) Unwrap() error { return errs.ErrConflict }

// confirmRefusal validates the API side of the handshake against the registry's
// Confirm declaration (which path value to match) and writes the appropriate envelope.
// ok=false means the handler must return; ok=true hands control to the store command.
func (s *Server) confirmRefusal(w http.ResponseWriter, kind, pathValue, gotConfirm, blast string, next []string) bool {
	if gotConfirm == "" {
		s.writeError(w, &confirmRequiredError{kind: kind, name: pathValue, blast: blast}, next...)
		return false
	}
	if gotConfirm != pathValue {
		s.writeError(w, &confirmMismatchError{kind: kind, got: gotConfirm, want: pathValue},
			next...)
		return false
	}
	return true
}

// checkDryRunGate enforces the registry's DryRun declaration for non-previewing
// routes: any dry_run query value present is a hard 400, never a silently ignored
// flag. Returns true when the request may proceed.
func (s *Server) checkDryRunGate(w http.ResponseWriter, r *http.Request) bool {
	if r.URL.Query().Get("dry_run") == "" {
		return true
	}
	s.writeError(w, errs.WithCode(errs.E(errs.ErrBadRequest, "api.dryRun",
		"?dry_run=1 previews nothing here; this route does not support dry runs"),
		string(CodeDryRunUnsupported)))
	return false
}
