// SPDX-License-Identifier: Apache-2.0

// Package exit is the single source of truth for messq's CLI exit-code contract
// (PLAN.md section 8: "Exit codes are a documented contract (0 ok · 1 error · 2 usage ·
// 3 not found · 4 conflict/stale · 5 empty/timeout · 6 daemon unreachable · 7
// permission)"). docs/generated/exit-codes.md is generated from this table, and every
// failure of every command reaches [Of] through one funnel in internal/cli.
//
// The classifier is ordered, finer tables first:
//
//  1. an explicit [*Err] override wins over everything (serve's sysexits, interrupt 130)
//  2. a command-local exit coder ([ExitCoder], e.g. uierr.UserError with Exit != 0)
//  3. a *client.Error by wire code (finest), then by client.Classify kind
//  4. transport failures and context.DeadlineExceeded → Unreachable
//  5. the internal/errs sentinel table
//  6. anything else is a generic failure (1)
//
// Adding a sentinel, a wire code or a client.Kind without a mapping fails the build:
// TestEverySentinelHasAnExitCode, TestEveryWireCodeHasAnExitCode and
// TestEveryKindHasAnExitCode pin the three closed sets against these tables.
package exit

import (
	"context"
	"errors"
	"strconv"

	"github.com/a-holm/messq/internal/errs"
	"github.com/a-holm/messq/internal/wirecode"
	"github.com/a-holm/messq/pkg/client"
)

// The documented 0–7 contract. Names are stable; docs/generated/exit-codes.md renders them.
const (
	OK          = 0 // success
	Error       = 1 // generic runtime failure, incl. a bug in the daemon
	Usage       = 2 // bad flag, bad argument, request the daemon rejected as malformed
	NotFound    = 3 // stream / consumer / message / seq does not exist
	Conflict    = 4 // conflict, stale fence, precondition or limit refused the request
	Empty       = 5 // a wait expired before the request was satisfied (never "zero rows" — that is 0)
	Unreachable = 6 // no answer obtainable from the daemon
	Denied      = 7 // authentication or authorization refused
)

// Err is an explicit override: a command returns it when its outcome lives outside the
// semantic table by design. Two documented exceptions exist (issue §7): `messq serve`
// keeps #17's sysexits values 74 (storage latch), 75 (data dir locked) and 78 (config
// will never work) because `RestartPreventExitStatus=2 78` is an operational contract
// with systemd; and an interrupt exits 130 (128+SIGINT) per shell convention.
type Err struct{ Code int }

func (e *Err) Error() string { return "exit code " + strconv.Itoa(e.Code) }

// ExitCoder lets another package carry a documented outcome without importing this one
// (uierr.UserError implements it; Exit == 0 means "ask the classifier").
type ExitCoder interface {
	error
	ExitCode() int
}

// ByWireCode maps the closed §7 machine-code enum onto exits. Finer than the Kind
// bucket: where one Kind splits, the wire code decides. Sources: issue §7's wire-code
// table and SEMANTICS S13. Rows worth reading slowly:
//
//   - commit_unknown and read_only are deliberately NOT 6. Retrying either never
//     works blindly: commit_unknown means the publish may or may not be stored
//     (#8's UNKNOWN class), read_only is the fsync-gate latch an operator must clear.
//   - busy/too_many_waiters/shutting_down ARE 6 with their siblings: a retry loop
//     must treat "no daemon" and "daemon cannot serve you yet" identically.
//   - flow_control/rate_limited sit at 4 per §7 ("a limit refused the request");
//     issue #39 revisits the two 429 rows.
var ByWireCode = map[wirecode.Code]int{
	wirecode.NotFound:             NotFound,
	wirecode.StreamExists:         Conflict,
	wirecode.Conflict:             Conflict,
	wirecode.ImmutableField:       Conflict,
	wirecode.WouldLoseData:        Conflict,
	wirecode.ReservedName:         Usage,
	wirecode.BadRequest:           Usage,
	wirecode.ConsumerExists:       Conflict,
	wirecode.WouldChangeFilters:   Conflict,
	wirecode.ConfirmRequired:      Conflict,
	wirecode.ConfirmMismatch:      Conflict,
	wirecode.DryRunUnsupported:    Usage,
	wirecode.NotReady:             Unreachable, // 503-class: a retry loop sees the same daemon state
	wirecode.NotImplemented:       Unreachable, // mounted-but-unbacked knobs answer like #21-before-injection
	wirecode.BadSubject:           Usage,
	wirecode.SubjectMismatch:      Usage,
	wirecode.HeaderTooLarge:       Usage,
	wirecode.ReservedHeader:       Usage,
	wirecode.Unsupported:          Usage,
	wirecode.TooLarge:             Conflict,
	wirecode.ReadOnly:             Error,
	wirecode.ShuttingDown:         Unreachable,
	wirecode.Internal:             Error,
	wirecode.Unauthorized:         Denied,
	wirecode.Forbidden:            Denied,
	wirecode.InvalidToken:         Usage,
	wirecode.StaleAck:             Conflict,
	wirecode.Paused:               Conflict,
	wirecode.FlowControl:          Conflict,
	wirecode.StreamFull:           Conflict,
	wirecode.DiskFull:             Conflict,
	wirecode.MethodNotAllowed:     Error,
	wirecode.ExtendCapped:         Conflict,
	wirecode.UnsupportedMediaType: Usage,
	wirecode.CommitUnknown:        Error,
	wirecode.Busy:                 Unreachable,
	wirecode.TooManyWaiters:       Unreachable,
	wirecode.RateLimited:          Conflict,
	wirecode.Locked:               Conflict,
	wirecode.SchemaNewer:          Error,
	wirecode.Unavailable:          Unreachable,

	// Client-local refusals that never ride HTTP but still reach this classifier
	// as *client.Error from client.New. They sit in the finest table, not the
	// Kind fallback, because they ARE usage: retyping the address (or fixing the
	// refused option combination) genuinely works, which is exit.go's bar for
	// ever telling an operator to retype.
	wirecode.Code("bad_address"):  Usage,
	wirecode.Code("config_error"): Usage,
}

// ByKind maps the client's classification enum (#22 owns Kind, #23 owns policy) onto
// exits. An unknown future code degrades to KindInternal → Error, never to Usage: a
// newer daemon's new refusal must not tell the operator to retype the command.
//
// KindTimeout lands on Unreachable because Classify only produces it for a request
// that got NO answer (deadline before response, §7). A wait that expired while data
// trickled in is produced locally as Empty by the command that owned the wait.
//
// KindEmpty has no classifier producer today (#22 reserved it); its row keeps the map
// total so adding a producer cannot ship without a policy decision.
var ByKind = map[client.Kind]int{
	client.KindOK:          OK,
	client.KindUsage:       Usage,
	client.KindNotFound:    NotFound,
	client.KindConflict:    Conflict,
	client.KindPermission:  Denied,
	client.KindUnavailable: Unreachable,
	client.KindTimeout:     Unreachable,
	client.KindEmpty:       Empty,
	client.KindInternal:    Error,
}

// BySentinel maps the closed internal/errs registry (#3, S13) onto exits. These
// sentinels are the daemon-side vocabulary; they reach the CLI wrapped in command
// errors (e.g. verify's check results), not as envelopes. Reconciled with ByWireCode
// so both vocabularies answer identically for the same concept:
// ErrFlowControl→4 (§7's flow_control row), ErrReadOnly→1 (latch),
// ErrShuttingDown→6 (retryable-with-the-daemon family).
var BySentinel = map[error]int{
	errs.ErrNotFound:     NotFound,
	errs.ErrConflict:     Conflict,
	errs.ErrBadRequest:   Usage,
	errs.ErrBadSubject:   Usage,
	errs.ErrUnknownToken: Usage,
	errs.ErrTooLarge:     Conflict,
	errs.ErrStreamFull:   Conflict,
	errs.ErrFlowControl:  Conflict,
	errs.ErrStaleAck:     Conflict,
	errs.ErrWrongGen:     Conflict,
	errs.ErrPaused:       Conflict,
	errs.ErrDiskFull:     Conflict,
	errs.ErrLocked:       Conflict,
	errs.ErrReadOnly:     Error,
	errs.ErrSchemaNewer:  Error,
	errs.ErrShuttingDown: Unreachable,
	errs.ErrUnavailable:  Unreachable,
	errs.ErrUnauthorized: Denied,
	errs.ErrForbidden:    Denied,
}

// Of classifies any error into the documented table. It is total: nil is OK, anything
// unmapped is Error (1). Never mis-bucket an unknown as Usage — "retype the command"
// must only ever be advice that can work.
func Of(err error) int {
	if err == nil {
		return OK
	}
	var override *Err
	if errors.As(err, &override) {
		return override.Code
	}
	var coder ExitCoder
	if errors.As(err, &coder) && coder.ExitCode() != 0 {
		return coder.ExitCode()
	}
	var ce *client.Error
	if errors.As(err, &ce) {
		if code, ok := ByWireCode[wirecode.Code(ce.Code)]; ok {
			return code
		}
		// Unknown code from a newer daemon: fall back to the status-bucketed
		// Kind. Never Usage, never a crash; ByKind is total over the enum
		// (TestEveryKindHasAnExitCode).
		return ByKind[client.Classify(err)]
	}
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, client.ErrUnreachable) {
		return Unreachable
	}
	for sentinel, code := range BySentinel {
		if errors.Is(err, sentinel) {
			return code
		}
	}
	return Error
}

// names renders the generated doc and the -vv dump; it mirrors the constants above.
var names = map[int]string{
	OK:          "ok",
	Error:       "error",
	Usage:       "usage",
	NotFound:    "not_found",
	Conflict:    "conflict",
	Empty:       "empty",
	Unreachable: "unreachable",
	Denied:      "permission",
}

// Name returns the snake_case contract name of a documented exit code, or "" for an
// undocumented one (serve's sysexits render via their own overrides).
func Name(code int) string { return names[code] }
