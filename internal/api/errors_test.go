// SPDX-License-Identifier: Apache-2.0

package api

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/a-holm/messq/internal/errs"
	"github.com/a-holm/messq/internal/id"
	"github.com/a-holm/messq/internal/queue"
	"github.com/a-holm/messq/internal/store"
)

// The G1/G2 guarantees: the envelope is total (every non-success response comes from
// writeError) and the mapping is two-way exhaustive — every sentinel is mapped or named
// neverOverHTTP, and every enum member has a producing scenario or an explicit reserved
// marker naming the issue that will produce it.

// mappingServer builds the minimal server the envelope path needs; writeError consults
// no store state, so the mapping tests run without a database.
func mappingServer() *Server {
	return New(Config{Logger: discardLogger()})
}

// TestEverySentinelIsMapped iterates the closed #3 registry and fails on any sentinel
// that is neither mapped nor explicitly never reachable over HTTP. Mutant sensitivity:
// deleting one sentinelDefaults row turns this red by name.
func TestEverySentinelIsMapped(t *testing.T) {
	t.Parallel()

	defaults := make(map[error]Code, len(sentinelDefaults))
	for _, row := range sentinelDefaults {
		defaults[row.sentinel] = row.code
	}
	never := make(map[error]string, len(neverOverHTTP))
	for _, row := range neverOverHTTP {
		never[row.sentinel] = row.reason
	}

	for _, sentinel := range errs.All() {
		if code, ok := defaults[sentinel]; ok {
			if codeStatus[code] == 0 {
				t.Errorf("sentinel %v maps to %q which has no status", sentinel, code)
			}
			continue
		}
		if reason, ok := never[sentinel]; ok {
			if reason == "" {
				t.Errorf("%v is listed in neverOverHTTP without a reason", sentinel)
			}
			continue
		}
		t.Errorf("sentinel %v (%q) is neither mapped nor declared neverOverHTTP", sentinel, sentinel.Error())
	}
}

// TestEveryCodeHasATableStatus fails when an enum member carries no HTTP status — the
// production fallback is 500, but the table must be total so the fallback stays dead.
func TestEveryCodeHasATableStatus(t *testing.T) {
	t.Parallel()

	for _, c := range allCodes {
		if codeStatus[c] == 0 {
			t.Errorf("code %q has no entry in codeStatus", c)
		}
	}
}

// TestEnumDeclaredOnce guards the declaration order list against duplicates and drift
// from the const block (a member renamed in one place only changes the wire).
func TestEnumDeclaredOnce(t *testing.T) {
	t.Parallel()

	seen := make(map[Code]int, len(allCodes))
	for _, c := range allCodes {
		seen[c]++
	}
	for c, n := range seen {
		if n != 1 {
			t.Errorf("code %q appears %d times in allCodes", c, n)
		}
	}
	if len(seen) != len(allCodes) {
		t.Fatalf("allCodes holds %d entries but %d distinct codes", len(allCodes), len(seen))
	}
}

// produce returns the error a real code path hands to writeError for each produced
// member. Where the producing function already exists in a lower layer it is called
// directly, so the scenario cannot drift from reality.
//
//nolint:exhaustive // reserved members fall through to nil on purpose; the marker lookup below names them
func produce(c Code) error {
	switch c {
	case CodeBadRequest:
		return errs.E(errs.ErrBadRequest, "api.test", "invalid JSON body: eof")
	case CodeBadSubject:
		return errs.E(errs.ErrBadSubject, "api.test", "subject is not valid")
	case CodeSubjectMismatch:
		return &queue.MismatchError{Subject: "x.y", Accepted: []string{"orders.>"}}
	case CodeReservedHeader:
		return &queue.ReservedHeaderError{Key: "Messq-Foo"}
	case CodeReservedName:
		return queue.ValidateStreamName("orders.dlq")
	case CodeInvalidToken:
		_, err := queue.ParseToken("not-a-token")
		return err
	case CodeNotFound:
		return errs.E(errs.ErrNotFound, "api.test", "stream %q does not exist", "missing")
	case CodeMethodNotAllowed:
		return routerError(CodeMethodNotAllowed)
	case CodeConflict:
		return errs.E(errs.ErrConflict, "api.test", "already exists")
	case CodeStreamExists:
		return &store.StreamExistsError{Name: "orders", Diff: []string{"max_msg_size"}}
	case CodeImmutableField:
		return &store.ImmutableFieldError{Field: "start"}
	case CodeWouldLoseData:
		return &queue.WouldLoseDataError{Field: "max_msgs", AtRiskMsgs: 3, AtRiskBytes: 300}
	case CodeStaleAck:
		return errs.E(errs.ErrStaleAck, "store.Settle", "stale ack")
	case CodeExtendCapped:
		return errs.WithCode(errs.E(errs.ErrStaleAck, "store.Settle", "extend past --max-ack-wait"),
			string(CodeExtendCapped))
	case CodeTooLarge:
		return &queue.TooLargeError{What: "body", Size: 9000, Limit: 1024}
	case CodeHeaderTooLarge:
		return &queue.TooLargeError{What: "headers", Size: 9000, Limit: 4096}
	case CodeUnsupportedMediaType:
		return errs.WithCode(errs.E(errs.ErrBadRequest, "api.decode",
			"content type %q is not JSON", "text/plain"), string(CodeUnsupportedMediaType))
	case CodeUnsupported:
		return &unsupportedError{field: "ordered", issue: "#38"}
	case CodeInternal:
		return errors.New("unexpected condition")
	case CodeCommitUnknown:
		return fmt.Errorf("submit fetch: %w", store.ErrCommitUnknown)
	case CodeBusy:
		return errs.WithCode(errors.New("writer submit timed out after 5s"), string(CodeBusy))
	case CodeTooManyWaiters:
		return errs.WithCode(errors.New("too many parked waiters"), string(CodeTooManyWaiters))
	case CodeReadOnly:
		return errs.E(errs.ErrReadOnly, "store.Publish", "storage latched read-only after a write fault")
	case CodeShuttingDown:
		return errs.E(errs.ErrShuttingDown, "api.settle", "shutdown in progress")
	case CodeStreamFull:
		return errs.E(errs.ErrStreamFull, "store.Publish", "orders is at its limit and discard=new")
	default:
		return nil
	}
}

// reservedMarkers names members with NO current producer. Each marker names the issue
// that will produce it; adding a member without a producer or a marker here is what
// TestEveryCodeIsProduced rejects. A slice rather than a map so the exhaustive linter
// does not demand produced keys here too.
var reservedMarkers = []struct {
	code Code
	why  string
}{
	{CodeUnauthorized, "#16 auth middleware produces it once bearer tokens exist"},
	{CodeForbidden, "#16 role check produces it"},
	{CodePaused, "#15 pause/resume routes are its first non-fetch callers"},
	{CodeFlowControl, "fetch answers 200 + hold_reason=flow_control; the error form waits for #39"},
	{CodeRateLimited, "#39 per-consumer rate limiting"},
	{CodeDiskFull, "#27 fsyncgate degraded-writes latch"},
}

func reservedReason(c Code) (string, bool) {
	for _, m := range reservedMarkers {
		if m.code == c {
			return m.why, true
		}
	}
	return "", false
}

// TestEveryCodeIsProduced exercises every enum member through the real envelope path.
// Mutant sensitivity: deleting a produce() arm or a reservedMarkers entry turns this
// red; ADDING an enum constant without either turns it red too, so dead codes cannot
// accumulate in the contract.
func TestEveryCodeIsProduced(t *testing.T) {
	t.Parallel()

	srv := mappingServer()
	for _, c := range allCodes {
		t.Run(string(c), func(t *testing.T) {
			err := produce(c)
			if err == nil {
				if _, ok := reservedReason(c); ok {
					return // reserved: asserted by name, produced by a future issue
				}
				t.Fatalf("code %q has neither a producer nor a reserved marker", c)
			}
			rec := httptest.NewRecorder()
			srv.writeError(rec, err)

			wantStatus, ok := codeStatus[c]
			if !ok {
				t.Fatalf("code %q missing from codeStatus", c)
			}
			if rec.Code != wantStatus {
				t.Fatalf("status = %d, want %d (body %s)", rec.Code, wantStatus, rec.Body.String())
			}
			var env Envelope
			if err := json.Unmarshal(rec.Body.Bytes(), &env); err != nil {
				t.Fatalf("body is not an envelope (%v): %s", err, rec.Body.String())
			}
			if env.Error.Code != c {
				t.Errorf("code = %q, want %q", env.Error.Code, c)
			}
			if env.Error.Message == "" {
				t.Error("envelope message is empty")
			}
			if env.Error.Next == nil {
				t.Error("next must always marshal as an array, never null")
			}
			if env.Error.TraceID == "" {
				t.Error("trace_id is empty")
			}
			if _, err := id.ParseTraceID(env.Error.TraceID); err != nil {
				t.Errorf("trace_id %q does not parse: %v", env.Error.TraceID, err)
			}
			if ra := rec.Header().Get("Retry-After"); wantStatus == http.StatusServiceUnavailable && ra != "1" {
				t.Errorf("503 Retry-After = %q, want \"1\"", ra)
			}
			if ra := rec.Header().Get("Retry-After"); wantStatus != http.StatusServiceUnavailable && ra != "" {
				t.Errorf("non-503 carried Retry-After %q", ra)
			}
		})
	}
}

// TestAttachedCodeBeatsSentinelDefault pins the refinement precedence: WithCode wins
// over the sentinel default and over the typed refinements beneath it.
func TestAttachedCodeBeatsSentinelDefault(t *testing.T) {
	t.Parallel()

	base := errs.E(errs.ErrBadRequest, "api.test", "nope")
	tagged := errs.WithCode(base, string(CodeExtendCapped))
	if got := mapCode(tagged); got != CodeExtendCapped {
		t.Errorf("mapCode = %q, want the attached extend_capped", got)
	}
	if got := mapCode(base); got != CodeBadRequest {
		t.Errorf("mapCode = %q, want the sentinel default bad_request", got)
	}
}

// TestForeignAttachedCodeFallsToInternal refuses to emit a code that is not in the
// closed enum: an unknown attached string maps to internal rather than escaping onto
// the wire.
func TestForeignAttachedCodeFallsToInternal(t *testing.T) {
	t.Parallel()

	foreign := errs.WithCode(errs.E(errs.ErrBadRequest, "api.test", "x"), "totally_new_code")
	if got := mapCode(foreign); got != CodeInternal {
		t.Errorf("mapCode = %q, want internal for a foreign attached code", got)
	}
}
