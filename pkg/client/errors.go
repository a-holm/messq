// SPDX-License-Identifier: Apache-2.0

package client

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"
)

// The sentinels of the typed error model (issue §4). A wire *Error matches the sentinel
// for its machine code via errors.Is; an unknown code matches none, so a newer daemon
// degrades to a rendered message instead of breaking older clients (Decision 2).
var (
	ErrBadRequest      = errors.New("bad_request")
	ErrBadSubject      = errors.New("bad_subject")
	ErrSubjectMismatch = errors.New("subject_mismatch")
	// Reserved-header refusals are a bad_request family member AND their own
	// sentinel: the %w chain makes both errors.Is targets true.
	ErrReservedHeader       = fmt.Errorf("reserved_header: %w", ErrBadRequest)
	ErrReservedName         = errors.New("reserved_name")
	ErrInvalidToken         = errors.New("invalid_token")
	ErrUnauthorized         = errors.New("unauthorized")
	ErrForbidden            = errors.New("forbidden")
	ErrNotFound             = errors.New("not_found")
	ErrMethodNotAllowed     = errors.New("method_not_allowed")
	ErrConflict             = errors.New("conflict")
	ErrStreamExists         = errors.New("stream_exists")
	ErrImmutableField       = errors.New("immutable_field")
	ErrWouldLoseData        = errors.New("would_lose_data")
	ErrStaleAck             = errors.New("stale_ack")
	ErrExtendCapped         = errors.New("extend_capped")
	ErrPaused               = errors.New("paused")
	ErrTooLarge             = errors.New("too_large")
	ErrHeaderTooLarge       = errors.New("header_too_large")
	ErrUnsupportedMediaType = errors.New("unsupported_media_type")
	ErrUnsupported          = errors.New("unsupported")
	ErrFlowControl          = errors.New("flow_control")
	ErrRateLimited          = errors.New("rate_limited")
	ErrInternal             = errors.New("internal")
	ErrCommitUnknown        = errors.New("commit_unknown")
	ErrBusy                 = errors.New("busy")
	ErrTooManyWaiters       = errors.New("too_many_waiters")
	ErrReadOnly             = errors.New("read_only")
	ErrShuttingDown         = errors.New("shutting_down")
	ErrDiskFull             = errors.New("disk_full")
	ErrStreamFull           = errors.New("stream_full")

	// ErrUnreachable classifies transport-level failures: dial refused, missing socket,
	// EOF. It is also the code an *Error carries for them.
	ErrUnreachable = errors.New("unavailable")

	// Local refusals that never ride HTTP.
	ErrBadAddress = errors.New("bad address")
	ErrRedirect   = errors.New("redirect refused")
	ErrConfig     = errors.New("configuration error")

	// ErrLeaseLost is the cause a Worker cancels a handler's context with when the
	// broker no longer honours the token's lease (restart fencing, extend give-up).
	ErrLeaseLost = errors.New("lease lost")

	// errTimeout marks transport deadlines; it IS context.DeadlineExceeded so
	// callers' existing errors.Is checks keep working through our wrapper.
	errTimeout = context.DeadlineExceeded
)

// codeSentinels maps every machine code of the closed §7 enum onto its sentinel. The
// table is the whole errors.Is story; codes absent from it match no sentinel.
var codeSentinels = map[string]error{
	"bad_request":            ErrBadRequest,
	"bad_subject":            ErrBadSubject,
	"subject_mismatch":       ErrSubjectMismatch,
	"reserved_header":        ErrReservedHeader,
	"reserved_name":          ErrReservedName,
	"invalid_token":          ErrInvalidToken,
	"unauthorized":           ErrUnauthorized,
	"forbidden":              ErrForbidden,
	"not_found":              ErrNotFound,
	"method_not_allowed":     ErrMethodNotAllowed,
	"conflict":               ErrConflict,
	"stream_exists":          ErrStreamExists,
	"immutable_field":        ErrImmutableField,
	"would_lose_data":        ErrWouldLoseData,
	"stale_ack":              ErrStaleAck,
	"extend_capped":          ErrExtendCapped,
	"paused":                 ErrPaused,
	"too_large":              ErrTooLarge,
	"header_too_large":       ErrHeaderTooLarge,
	"unsupported_media_type": ErrUnsupportedMediaType,
	"unsupported":            ErrUnsupported,
	"flow_control":           ErrFlowControl,
	"rate_limited":           ErrRateLimited,
	"internal":               ErrInternal,
	"commit_unknown":         ErrCommitUnknown,
	"busy":                   ErrBusy,
	"too_many_waiters":       ErrTooManyWaiters,
	"read_only":              ErrReadOnly,
	"shutting_down":          ErrShuttingDown,
	"disk_full":              ErrDiskFull,
	"stream_full":            ErrStreamFull,
	"unavailable":            ErrUnreachable,
}

// Error is one failure of a messq request: the envelope the daemon wrote, plus the
// transport context around it. Unknown codes are preserved, never rejected.
type Error struct {
	Code       string         // closed enum member; UNKNOWN CODES ARE PRESERVED
	Message    string         // the daemon's human sentence
	Next       []string       // teaching "what to type next"; render verbatim
	Detail     map[string]any // machine-readable specifics
	TraceID    string
	RequestID  string        // Messq-Request-Id response header
	Status     int           // HTTP status; 0 for local/transport failures
	RetryAfter time.Duration // parsed from Retry-After when present

	err error // underlying transport error, if any
}

func (e *Error) Error() string {
	var b strings.Builder
	b.WriteString("messq: ")
	if e.Status != 0 {
		fmt.Fprintf(&b, "[%d] ", e.Status)
	}
	if e.Code != "" {
		b.WriteString(e.Code)
		b.WriteString(": ")
	}
	b.WriteString(e.Message)
	if e.TraceID != "" {
		fmt.Fprintf(&b, " (trace %s)", e.TraceID)
	}
	return b.String()
}

// Unwrap exposes the transport error behind unreachable/timeout classifications so
// errors.Is(err, context.DeadlineExceeded) keeps working through the wrapper.
func (e *Error) Unwrap() error { return e.err }

func (e *Error) Is(target error) bool {
	sentinel, ok := codeSentinels[e.Code]
	if !ok {
		return false
	}
	return sentinel == target || errors.Is(sentinel, target)
}

// Kind is #23's exit-code input: this package owns classification, the CLI owns policy.
type Kind uint8

const (
	KindOK Kind = iota
	KindUsage
	KindNotFound
	KindConflict
	KindPermission
	KindUnavailable
	KindTimeout
	// KindEmpty is reserved for "a well-formed answer that contains nothing" cases;
	// no mapping returns it today. It exists so #23's table is total over this enum.
	KindEmpty
	KindInternal
)

func (k Kind) String() string {
	switch k {
	case KindOK:
		return "ok"
	case KindUsage:
		return "usage"
	case KindNotFound:
		return "not_found"
	case KindConflict:
		return "conflict"
	case KindPermission:
		return "permission"
	case KindUnavailable:
		return "unavailable"
	case KindTimeout:
		return "timeout"
	case KindEmpty:
		return "empty"
	case KindInternal:
		return "internal"
	default:
		return fmt.Sprintf("kind(%d)", uint8(k))
	}
}

// kindByCode classifies every current enum member; the Classify test iterates the
// result. An unlisted (unknown/future) code lands in KindInternal via Classify.
var kindByCode = map[string]Kind{
	"bad_request":            KindUsage,
	"bad_subject":            KindUsage,
	"subject_mismatch":       KindUsage,
	"reserved_header":        KindUsage,
	"reserved_name":          KindUsage,
	"invalid_token":          KindUsage,
	"header_too_large":       KindUsage,
	"unsupported_media_type": KindUsage,
	"unsupported":            KindUsage,
	"method_not_allowed":     KindUsage,
	"too_large":              KindUsage,
	"not_found":              KindNotFound,
	"conflict":               KindConflict,
	"stream_exists":          KindConflict,
	"immutable_field":        KindConflict,
	"would_lose_data":        KindConflict,
	"stale_ack":              KindConflict,
	"extend_capped":          KindConflict,
	"paused":                 KindConflict,
	"unauthorized":           KindPermission,
	"forbidden":              KindPermission,
	"flow_control":           KindUnavailable,
	"rate_limited":           KindUnavailable,
	"commit_unknown":         KindUnavailable,
	"busy":                   KindUnavailable,
	"too_many_waiters":       KindUnavailable,
	"read_only":              KindUnavailable,
	"shutting_down":          KindUnavailable,
	"disk_full":              KindUnavailable,
	"stream_full":            KindUnavailable,
	"unavailable":            KindUnavailable,
	"internal":               KindInternal,
}

// Classify reduces any error to a Kind. nil is KindOK. A *Error maps by its code
// (unknown codes → KindInternal); anything else is either a timeout, an unreachable
// transport failure, or plain local misuse, which lands in KindInternal.
func Classify(err error) Kind {
	if err == nil {
		return KindOK
	}
	var e *Error
	if errors.As(err, &e) {
		if e.err != nil && errors.Is(e.err, errTimeout) {
			return KindTimeout
		}
		if k, ok := kindByCode[e.Code]; ok {
			return k
		}
		return KindInternal
	}
	switch {
	case errors.Is(err, errTimeout):
		return KindTimeout
	case errors.Is(err, ErrUnreachable):
		return KindUnavailable
	case errors.Is(err, ErrConfig), errors.Is(err, ErrBadAddress):
		return KindUsage
	case errors.Is(err, ErrLeaseLost):
		return KindConflict
	default:
		return KindInternal
	}
}

// unreachable wraps a transport failure as an *Error classified unavailable — or
// KindTimeout via Unwrap when the context deadline caused it — keeping the original
// error unwrappable.
func unreachable(op string, err error) *Error {
	code := "unavailable"
	msg := op + ": " + err.Error()
	if errors.Is(err, errTimeout) {
		msg = op + ": timed out"
	}
	return &Error{Code: code, Message: msg, Status: 0, err: err}
}

// retryAfter parses the Retry-After header in integer-seconds form (the only form the
// daemon emits). Anything unparsable is zero: no guidance.
func retryAfter(v string) time.Duration {
	if v == "" {
		return 0
	}
	n, err := strconv.Atoi(strings.TrimSpace(v))
	if err != nil || n < 0 {
		return 0
	}
	return time.Duration(n) * time.Second
}
