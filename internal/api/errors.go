// SPDX-License-Identifier: Apache-2.0

package api

import (
	"crypto/rand"
	"encoding/json"
	"errors"
	"net/http"
	"strconv"

	"github.com/a-holm/messq/internal/errs"
	"github.com/a-holm/messq/internal/id"
	"github.com/a-holm/messq/internal/queue"
	"github.com/a-holm/messq/internal/store"
)

// Code is one member of the closed machine-code enum of PLAN §7: part of the 1.0
// compatibility contract. Codes are refined from #3's sentinels with errs.WithCode or
// carried by typed errors; the sentinel→(status, code) tables below are the single
// source of truth, and the exhaustiveness tests iterate errs.All() and allCodes in
// both directions. The HTTP status mapping is defined here and FROZEN in #35 — this
// table only defines it coherently until then.
type Code string

// The enum. A wire code that merged before this issue (immutable_field,
// header_too_large, unsupported from #7/#9's provisional surface) is a member like any
// other; removing one would break shipped clients.
const (
	CodeBadRequest           Code = "bad_request"
	CodeBadSubject           Code = "bad_subject"
	CodeSubjectMismatch      Code = "subject_mismatch"
	CodeReservedHeader       Code = "reserved_header"
	CodeReservedName         Code = "reserved_name"
	CodeInvalidToken         Code = "invalid_token"
	CodeUnauthorized         Code = "unauthorized"
	CodeForbidden            Code = "forbidden"
	CodeNotFound             Code = "not_found"
	CodeMethodNotAllowed     Code = "method_not_allowed"
	CodeConflict             Code = "conflict"
	CodeStreamExists         Code = "stream_exists"
	CodeImmutableField       Code = "immutable_field"
	CodeWouldLoseData        Code = "would_lose_data"
	CodeStaleAck             Code = "stale_ack"
	CodeExtendCapped         Code = "extend_capped"
	CodePaused               Code = "paused"
	CodeTooLarge             Code = "too_large"
	CodeHeaderTooLarge       Code = "header_too_large"
	CodeUnsupportedMediaType Code = "unsupported_media_type"
	CodeUnsupported          Code = "unsupported"
	CodeFlowControl          Code = "flow_control"
	CodeRateLimited          Code = "rate_limited"
	CodeInternal             Code = "internal"
	CodeCommitUnknown        Code = "commit_unknown"
	CodeBusy                 Code = "busy"
	CodeTooManyWaiters       Code = "too_many_waiters"
	CodeReadOnly             Code = "read_only"
	CodeShuttingDown         Code = "shutting_down"
	CodeDiskFull             Code = "disk_full"
	CodeStreamFull           Code = "stream_full"
	CodeNotReady             Code = "not_ready"
	CodeNotImplemented       Code = "not_implemented"
	CodeConsumerExists       Code = "consumer_exists"
	CodeWouldChangeFilters   Code = "would_change_filters"
	CodeConfirmRequired      Code = "confirm_required"
	CodeConfirmMismatch      Code = "confirm_mismatch"
	CodeDryRunUnsupported    Code = "dry_run_unsupported"
)

// allCodes is the whole enum in declaration order. TestEveryCodeIsProduced iterates it;
// a member added without a producer scenario or an explicit reserved marker fails CI,
// so dead codes cannot accumulate.
var allCodes = []Code{
	CodeBadRequest,
	CodeBadSubject,
	CodeSubjectMismatch,
	CodeReservedHeader,
	CodeReservedName,
	CodeInvalidToken,
	CodeUnauthorized,
	CodeForbidden,
	CodeNotFound,
	CodeMethodNotAllowed,
	CodeConflict,
	CodeStreamExists,
	CodeImmutableField,
	CodeWouldLoseData,
	CodeStaleAck,
	CodeExtendCapped,
	CodePaused,
	CodeTooLarge,
	CodeHeaderTooLarge,
	CodeUnsupportedMediaType,
	CodeUnsupported,
	CodeFlowControl,
	CodeRateLimited,
	CodeInternal,
	CodeCommitUnknown,
	CodeBusy,
	CodeTooManyWaiters,
	CodeReadOnly,
	CodeShuttingDown,
	CodeDiskFull,
	CodeStreamFull,
	CodeNotReady,
	CodeNotImplemented,
	CodeConfirmRequired,
	CodeConfirmMismatch,
	CodeDryRunUnsupported,
	CodeConsumerExists,
	CodeWouldChangeFilters,
}

// isCodeMember reports whether c is in the enum. Attached codes are typed constants at
// every api call site, so this is a defensive check for foreign strings, not the primary
// closure mechanism.
func isCodeMember(c Code) bool {
	for _, m := range allCodes {
		if m == c {
			return true
		}
	}
	return false
}

// Envelope is the one wire shape every error shares (issue §7 / PLAN §7).
type Envelope struct {
	Error ErrorBody `json:"error"`
}

// ErrorBody carries the stable machine code, one human sentence, the suggested next
// commands, optional machine-readable specifics and the request's trace id.
type ErrorBody struct {
	Code    Code           `json:"code"`
	Message string         `json:"message"`
	Next    []string       `json:"next"`
	Detail  map[string]any `json:"detail,omitempty"`
	TraceID string         `json:"trace_id"`
}

// routerError is the classification for a response the mux itself produced (an
// unmatched path or a wrong method). Carrying the code AS the value keeps the two
// catch-all paths inside the enum without inventing sentinels for routing.
type routerError Code

func (e routerError) Error() string { return "router: " + string(e) }

// Sentinel defaults for the closed #3 registry. Order is irrelevant (matching is by
// identity); the exhaustiveness test fails when a new sentinel misses both tables.
type sentinelRow struct {
	sentinel error
	code     Code
}

var sentinelDefaults = []sentinelRow{
	{errs.ErrNotFound, CodeNotFound},
	{errs.ErrConflict, CodeConflict},
	{errs.ErrBadRequest, CodeBadRequest},
	{errs.ErrBadSubject, CodeBadSubject},
	{errs.ErrTooLarge, CodeTooLarge},
	{errs.ErrStreamFull, CodeStreamFull},
	{errs.ErrFlowControl, CodeFlowControl},
	{errs.ErrStaleAck, CodeStaleAck},
	{errs.ErrUnknownToken, CodeInvalidToken},
	{errs.ErrWrongGen, CodeStaleAck},
	{errs.ErrPaused, CodePaused},
	{errs.ErrDiskFull, CodeDiskFull},
	{errs.ErrReadOnly, CodeReadOnly},
	{errs.ErrShuttingDown, CodeShuttingDown},
	{errs.ErrUnauthorized, CodeUnauthorized},
	{errs.ErrForbidden, CodeForbidden},
}

// neverOverHTTP lists sentinels no HTTP response can carry, each with the reason. The
// exhaustiveness test requires every errs.All() member to be here or in
// sentinelDefaults, so a newly reachable sentinel cannot silently fall to internal.
var neverOverHTTP = []struct {
	sentinel error
	reason   string
}{
	{errs.ErrLocked, "startup condition: the daemon refuses to start, nothing serves"},
	{errs.ErrSchemaNewer, "startup condition: Open fails before any listener exists"},
	{errs.ErrUnavailable, "client-side dial failure: the client library owns it (#22)"},
}

// codeStatus maps every enum member onto its HTTP status. Members absent here are a
// mapping bug, not a default: statusOf panics in tests via TestEveryCodeHasStatus and
// falls back to 500 in production rather than writing a headerless response.
var codeStatus = map[Code]int{
	CodeBadRequest:       http.StatusBadRequest,
	CodeBadSubject:       http.StatusBadRequest,
	CodeSubjectMismatch:  http.StatusBadRequest,
	CodeReservedHeader:   http.StatusBadRequest,
	CodeReservedName:     http.StatusBadRequest,
	CodeInvalidToken:     http.StatusBadRequest,
	CodeUnauthorized:     http.StatusUnauthorized,
	CodeForbidden:        http.StatusForbidden,
	CodeNotFound:         http.StatusNotFound,
	CodeMethodNotAllowed: http.StatusMethodNotAllowed,
	CodeConflict:         http.StatusConflict,
	CodeStreamExists:     http.StatusConflict,
	CodeImmutableField:   http.StatusConflict,
	CodeWouldLoseData:    http.StatusConflict,
	CodeStaleAck:         http.StatusConflict,
	CodeExtendCapped:     http.StatusConflict,
	CodePaused:           http.StatusConflict,
	CodeTooLarge:         http.StatusRequestEntityTooLarge,
	// header_too_large is a publish-time user-header cap (#7), not a transport-header
	// condition; it kept its provisional 400 and the merged golden locks that.
	CodeHeaderTooLarge:       http.StatusBadRequest,
	CodeUnsupportedMediaType: http.StatusUnsupportedMediaType,
	CodeUnsupported:          http.StatusBadRequest,
	CodeFlowControl:          http.StatusTooManyRequests,
	CodeRateLimited:          http.StatusTooManyRequests,
	CodeInternal:             http.StatusInternalServerError,
	CodeCommitUnknown:        http.StatusServiceUnavailable,
	CodeBusy:                 http.StatusServiceUnavailable,
	CodeTooManyWaiters:       http.StatusServiceUnavailable,
	CodeReadOnly:             http.StatusServiceUnavailable,
	CodeShuttingDown:         http.StatusServiceUnavailable,
	CodeDiskFull:             http.StatusInsufficientStorage,
	CodeStreamFull:           http.StatusInsufficientStorage,
	CodeNotReady:             http.StatusServiceUnavailable,
	CodeNotImplemented:       http.StatusServiceUnavailable,
	CodeConsumerExists:       http.StatusConflict,
	CodeWouldChangeFilters:   http.StatusConflict,
	CodeConfirmRequired:      http.StatusConflict,
	CodeConfirmMismatch:      http.StatusConflict,
	CodeDryRunUnsupported:    http.StatusBadRequest,
}

// retryAfterSeconds is the integer-second Retry-After every 503 carries (issue §4);
// zero means the header is omitted. Keying off the status keeps any future 503 member
// honest without touching this function.
func retryAfterSeconds(c Code) int {
	if codeStatus[c] == http.StatusServiceUnavailable {
		return 1
	}
	return 0
}

// refineTyped carries the typed store/queue errors onto their refined codes. These run
// BEFORE the sentinel defaults because several wrap a broader sentinel
// (StreamExistsError wraps conflict, MismatchError wraps bad_subject): the specific
// condition must win. An attached code from errs.WithCode wins over everything.
func refineTyped(err error) (Code, bool) {
	var (
		existsErr       *store.StreamExistsError
		immErr          *store.ImmutableFieldError
		unsupErr        *unsupportedError
		loseErr         *queue.WouldLoseDataError
		mismatchErr     *queue.MismatchError
		reservedHdrr    *queue.ReservedHeaderError
		tooLargeErr     *queue.TooLargeError
		routerErr       routerError
		busyErr         busyError
		confirmReqErr   *confirmRequiredError
		confirmMisErr   *confirmMismatchError
		existsConsErr   *store.ConsumerExistsError
		filterChangeErr *consumerFilterChangeError
	)
	switch {
	case errors.As(err, &routerErr):
		return Code(routerErr), true
	case errors.As(err, &busyErr):
		return CodeBusy, true
	case errors.As(err, &confirmReqErr):
		return CodeConfirmRequired, true
	case errors.As(err, &confirmMisErr):
		return CodeConfirmMismatch, true
	case errors.As(err, &existsConsErr):
		return CodeConsumerExists, true
	case errors.As(err, &filterChangeErr):
		return CodeWouldChangeFilters, true
	case errors.As(err, &existsErr):
		return CodeStreamExists, true
	case errors.Is(err, queue.ErrReservedName):
		return CodeReservedName, true
	case errors.As(err, &immErr):
		return CodeImmutableField, true
	case errors.As(err, &unsupErr):
		return CodeUnsupported, true
	case errors.As(err, &loseErr):
		return CodeWouldLoseData, true
	case errors.As(err, &mismatchErr):
		return CodeSubjectMismatch, true
	case errors.As(err, &reservedHdrr):
		return CodeReservedHeader, true
	case errors.As(err, &tooLargeErr):
		if tooLargeErr.What == "body" {
			return CodeTooLarge, true
		}
		return CodeHeaderTooLarge, true
	case errors.Is(err, store.ErrCommitUnknown):
		// Orchestrator ruling 2026-08-24 (§8 Q1): store.ErrCommitUnknown EXISTS since
		// #6; commit_unknown/503 maps onto it directly. It stays a store-internal
		// error, deliberately outside errs.All()/S13.
		return CodeCommitUnknown, true
	default:
		return "", false
	}
}

// mapCode resolves an error onto its wire code: an attached code first, then the typed
// refinements, then the sentinel default. Anything else is internal — never a silent
// zero value.
func mapCode(err error) Code {
	if c, ok := errs.CodeOf(err); ok {
		if isCodeMember(Code(c)) {
			return Code(c)
		}
		return CodeInternal
	}
	if c, ok := refineTyped(err); ok {
		return c
	}
	for _, row := range sentinelDefaults {
		if errors.Is(err, row.sentinel) {
			return row.code
		}
	}
	return CodeInternal
}

// writeError renders err as the issue's error envelope. It is the ONLY place that
// consults the mapping tables and the only place an envelope is built, which is what
// makes the exhaustiveness tests meaningful (G1). Extra next commands the caller knows
// are appended after the ones the error already carries.
func (s *Server) writeError(w http.ResponseWriter, err error, next ...string) {
	code := mapCode(err)
	status, ok := codeStatus[code]
	if !ok {
		status = http.StatusInternalServerError
	}
	body := ErrorBody{
		Code:    code,
		Message: errorMessage(err),
		Next:    append(errs.NextOf(err), next...),
		TraceID: id.NewTraceID(rand.Reader).String(),
	}
	if ra := retryAfterSeconds(code); ra > 0 {
		w.Header().Set("Retry-After", strconv.Itoa(ra))
	}
	s.writeEnvelope(w, status, body)
}

// writeEnvelope writes a fully-formed error body. next is always present (empty array,
// never null) so clients parse one shape. Any body carrying a 503-class code gets the
// standard Retry-After automatically, so the probes' not_ready needs no special case.
func (s *Server) writeEnvelope(w http.ResponseWriter, status int, body ErrorBody) {
	if body.Next == nil {
		body.Next = []string{}
	}
	if ra := retryAfterSeconds(body.Code); ra > 0 {
		w.Header().Set("Retry-After", strconv.Itoa(ra))
	}
	s.writeJSON(w, status, Envelope{Error: body})
}

// writeJSON writes v as a JSON response with the given status. Encoding a response can
// only fail after the header is already sent, so the error is logged, not surfaced.
func (s *Server) writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(v); err != nil {
		s.logger.Warn("api: write response", "err", err)
	}
}

// errorMessage extracts the human sentence for the envelope: the teaching error's own
// message when the error is an errs.Error, otherwise the rendered text. The typed
// store/queue errors are not errs.Errors, so their full rendering — which names the
// differing fields or the at-risk counts — is what the client reads.
func errorMessage(err error) string {
	var te *errs.Error
	if errors.As(err, &te) {
		return te.Msg
	}
	return err.Error()
}
