// SPDX-License-Identifier: Apache-2.0

package errs_test

import (
	"errors"
	"fmt"
	"testing"

	"github.com/a-holm/messq/internal/errs"
)

// TestWithCodePreservesIs walks the whole sentinel registry (issue #14 §4): every
// sentinel must survive code attachment with errors.Is intact, because writeError's
// mapping consults the sentinel AFTER an attached code failed to match. One table over
// All() keeps the property from eroding as sentinels are added.
func TestWithCodePreservesIs(t *testing.T) {
	t.Parallel()

	for _, sentinel := range errs.All() {
		wrapped := fmt.Errorf("outer: %w", errs.WithCode(sentinel, "some_code"))
		if !errors.Is(wrapped, sentinel) {
			t.Errorf("WithCode(%v) lost errors.Is through wrapping", sentinel)
		}
	}
}

// TestWithCodeRoundTrip pins the attach/read pair: the code comes back exactly, an
// empty code is a no-op, and CodeOf on an untagged error reports absence rather than
// an empty string that could be mistaken for a real code.
func TestWithCodeRoundTrip(t *testing.T) {
	t.Parallel()

	base := errs.E(errs.ErrBadRequest, "api.test", "bad thing")
	tagged := errs.WithCode(base, "subject_mismatch")

	got, ok := errs.CodeOf(tagged)
	if !ok || got != "subject_mismatch" {
		t.Fatalf("CodeOf = (%q, %v), want (subject_mismatch, true)", got, ok)
	}
	if tagged.Error() != base.Error() {
		t.Errorf("WithCode changed the rendered text: %q != %q", tagged.Error(), base.Error())
	}
	if _, ok := errs.CodeOf(errors.New("plain")); ok {
		t.Error("CodeOf reported a code on an untagged error")
	}
	if got := errs.WithCode(nil, "x"); got != nil {
		t.Errorf("WithCode(nil) = %v, want nil", got)
	}
	// An empty code is a no-op: nothing attached, text unchanged.
	if _, attached := errs.CodeOf(errs.WithCode(base, "")); attached {
		t.Errorf("WithCode(err, \"\") attached something")
	}
	if noop := errs.WithCode(base, ""); noop.Error() != base.Error() {
		t.Errorf("WithCode(err, \"\") changed the rendered text")
	}
	// The innermost attachment wins: a double tag is a caller bug, but reading it
	// must still be deterministic.
	inner := errs.WithCode(base, "a")
	outer := errs.WithCode(inner, "b")
	if got, _ := errs.CodeOf(outer); got != "b" {
		t.Errorf("CodeOf(double tag) = %q, want the outermost %q", got, "b")
	}
}
