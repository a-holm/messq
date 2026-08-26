// SPDX-License-Identifier: Apache-2.0

package exit

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/a-holm/messq/internal/errs"
	"github.com/a-holm/messq/internal/wirecode"
	"github.com/a-holm/messq/pkg/client"
)

// errAt builds a daemon envelope error carrying one machine code.
func errAt(code string) *client.Error {
	return &client.Error{Code: code, Message: "canary " + code}
}

// codedError stands in for uierr.UserError (which cannot import this package's
// constants without a cycle): an ExitCoder whose Exit != 0 is the documented outcome.
type codedError struct {
	msg   string
	exit  int
	cause error
}

func (e *codedError) Error() string { return e.msg }
func (e *codedError) ExitCode() int { return e.exit }
func (e *codedError) Unwrap() error { return e.cause }

func TestOfEnvelopeCodes(t *testing.T) {
	tests := []struct {
		code string
		want int
	}{
		{"not_found", NotFound},
		{"bad_request", Usage},
		{"bad_subject", Usage},
		{"subject_mismatch", Usage},
		{"reserved_header", Usage},
		{"reserved_name", Usage},
		{"invalid_token", Usage},
		{"header_too_large", Usage},
		{"unsupported", Usage},
		{"unsupported_media_type", Usage},
		{"conflict", Conflict},
		{"stream_exists", Conflict},
		{"immutable_field", Conflict},
		{"would_lose_data", Conflict},
		{"stale_ack", Conflict},
		{"extend_capped", Conflict},
		{"paused", Conflict},
		{"too_large", Conflict},
		{"disk_full", Conflict},
		{"stream_full", Conflict},
		{"rate_limited", Conflict},
		{"flow_control", Conflict},
		{"unauthorized", Denied},
		{"forbidden", Denied},
		{"busy", Unreachable},
		{"too_many_waiters", Unreachable},
		{"shutting_down", Unreachable},
		{"unavailable", Unreachable},
		{"internal", Error},
		{"method_not_allowed", Error},
		{"read_only", Error},
		{"commit_unknown", Error},
		// Client-local refusals that never ride HTTP: a typoed --addr is the
		// operator's input error, so "retype it" is advice that can work — usage,
		// not a generic failure (the review's exit-1 finding).
		{"bad_address", Usage},
		{"config_error", Usage},
		// An unknown code from a newer daemon never crashes and is never
		// mis-bucketed as usage: it falls back to the kind bucket, which for an
		// unlisted code is internal (§7).
		{"future_code_from_v2", Error},
	}
	for _, tt := range tests {
		t.Run(tt.code, func(t *testing.T) {
			if got := Of(errAt(tt.code)); got != tt.want {
				t.Errorf("Of(code %q) = %d, want %d", tt.code, got, tt.want)
			}
			// Wrapped chains classify identically.
			wrapped := fmt.Errorf("publish orders: %w", errAt(tt.code))
			if got := Of(wrapped); got != tt.want {
				t.Errorf("Of(wrapped %q) = %d, want %d", tt.code, got, tt.want)
			}
		})
	}
}

func TestOfKinds(t *testing.T) {
	tests := []struct {
		kind client.Kind
		want int
	}{
		{client.KindOK, OK},
		{client.KindUsage, Usage},
		{client.KindNotFound, NotFound},
		{client.KindConflict, Conflict},
		{client.KindEmpty, Empty},
		{client.KindUnavailable, Unreachable},
		// A timeout classified by the client is a request that got no answer:
		// 6. A wait that expired is produced locally as an explicit Empty.
		{client.KindTimeout, Unreachable},
		{client.KindPermission, Denied},
		{client.KindInternal, Error},
	}
	for _, tt := range tests {
		if got, ok := ByKind[tt.kind]; !ok || got != tt.want {
			t.Errorf("ByKind[%v] = %d, %v; want %d", tt.kind, got, ok, tt.want)
		}
	}
}

func TestOfSentinels(t *testing.T) {
	tests := []struct {
		sentinel error
		want     int
	}{
		{errs.ErrNotFound, NotFound},
		{errs.ErrConflict, Conflict},
		{errs.ErrBadRequest, Usage},
		{errs.ErrBadSubject, Usage},
		{errs.ErrUnknownToken, Usage},
		{errs.ErrTooLarge, Conflict},
		{errs.ErrStreamFull, Conflict},
		{errs.ErrFlowControl, Conflict},
		{errs.ErrStaleAck, Conflict},
		{errs.ErrWrongGen, Conflict},
		{errs.ErrPaused, Conflict},
		{errs.ErrDiskFull, Conflict},
		{errs.ErrLocked, Conflict},
		{errs.ErrReadOnly, Error},
		{errs.ErrSchemaNewer, Error},
		{errs.ErrShuttingDown, Unreachable},
		{errs.ErrUnavailable, Unreachable},
		{errs.ErrUnauthorized, Denied},
		{errs.ErrForbidden, Denied},
	}
	for _, tt := range tests {
		wrapped := fmt.Errorf("operation failed: %w", tt.sentinel)
		if got := Of(wrapped); got != tt.want {
			t.Errorf("Of(%v) = %d, want %d", tt.sentinel, got, tt.want)
		}
	}
}

func TestOfOverridesAndFallthrough(t *testing.T) {
	t.Run("nil is OK", func(t *testing.T) {
		if got := Of(nil); got != OK {
			t.Errorf("Of(nil) = %d, want %d", got, OK)
		}
	})
	t.Run("explicit Err wins over everything", func(t *testing.T) {
		err := fmt.Errorf("serve: %w", &Err{Code: 78})
		if got := Of(err); got != 78 {
			t.Errorf("Of(exit.Err 78) = %d, want 78", got)
		}
		interrupted := fmt.Errorf("%w", &Err{Code: 130})
		if got := Of(interrupted); got != 130 {
			t.Errorf("Of(interrupt 130) = %d, want 130", got)
		}
	})
	t.Run("an ExitCoder with a documented outcome wins before classification", func(t *testing.T) {
		// The shape uierr.UserError uses: Exit == 0 means "ask the classifier",
		// anything else is the command's own documented outcome.
		expired := errors.New("wait expired")
		coded := &codedError{msg: "the long poll returned nothing in time", exit: Empty, cause: expired}
		if got := Of(coded); got != Empty {
			t.Errorf("Of(coded 5) = %d, want %d", got, Empty)
		}
		deferred := &codedError{msg: "decide later", exit: 0, cause: errors.New("internal")}
		if got := Of(deferred); got != Error {
			t.Errorf("Of(coded 0) = %d, want the classifier's %d", got, Error)
		}
	})
	t.Run("deadline exceeded is unreachable", func(t *testing.T) {
		got := Of(fmt.Errorf("fetch: %w", context.DeadlineExceeded))
		if got != Unreachable {
			t.Errorf("Of(DeadlineExceeded) = %d, want %d", got, Unreachable)
		}
	})
	t.Run("unknown error is generic failure", func(t *testing.T) {
		got := Of(errors.New("something odd happened"))
		if got != Error {
			t.Errorf("Of(plain error) = %d, want %d", got, Error)
		}
	})
}

// TestEveryWireCodeHasAnExitCode is one half of the contract's exhaustiveness: a
// code added to the closed enum without an exit mapping fails here.
func TestEveryWireCodeHasAnExitCode(t *testing.T) {
	for code := range wirecode.Table {
		got, ok := ByWireCode[code]
		if !ok {
			t.Errorf("wire code %q has no exit mapping; extend ByWireCode (issue #23 §7)", code)
			continue
		}
		if got < OK || got > Denied {
			t.Errorf("wire code %q maps to %d, outside the documented 0–%d table", code, got, Denied)
		}
	}
}

// TestEveryKindHasAnExitCode is the test #22 delegates here: the Kind enum and the
// exit table must move together.
func TestEveryKindHasAnExitCode(t *testing.T) {
	for k := client.KindOK; ; k++ {
		if _, ok := ByKind[k]; !ok {
			t.Errorf("client.Kind(%d) has no exit mapping; extend ByKind", uint8(k))
		}
		if k == client.KindInternal {
			break
		}
	}
}

// TestEverySentinelHasAnExitCode iterates the closed errs registry: every sentinel
// classifies into the documented table, and no sentinel is silently unmapped.
func TestEverySentinelHasAnExitCode(t *testing.T) {
	for _, s := range errs.All() {
		code := Of(fmt.Errorf("probe: %w", s))
		if code < OK || code > Denied {
			t.Errorf("sentinel %q classified as %d, outside the documented 0–%d table", s, code, Denied)
		}
	}
}

// TestValidRange pins the whole documented range so a typo cannot introduce an
// undocumented exit code through the maps.
func TestValidRange(t *testing.T) {
	for _, code := range []int{OK, Error, Usage, NotFound, Conflict, Empty, Unreachable, Denied} {
		if Name(code) == "" {
			t.Errorf("exit code %d has no name; the generated doc needs one", code)
		}
	}
	for code, name := range map[int]string{
		OK: "ok", Error: "error", Usage: "usage",
		NotFound: "not_found", Conflict: "conflict", Empty: "empty",
		Unreachable: "unreachable", Denied: "permission",
	} {
		if got := Name(code); got != name {
			t.Errorf("Name(%d) = %q, want %q", code, got, name)
		}
	}
	if got := Name(42); got != "" {
		t.Errorf("Name(42) = %q, want empty", got)
	}
}
