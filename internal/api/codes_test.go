// SPDX-License-Identifier: Apache-2.0

package api

import (
	"errors"
	"net/http"
	"testing"

	"github.com/a-holm/messq/internal/errs"
	"github.com/a-holm/messq/internal/wirecode"
)

// The binding tests lock the API's local mapping tables onto internal/wirecode, the
// single source of truth for the closed code enum (brief-issue-18 §8 Q3). The API may
// keep its typed-error → code switch — #14 restructures it — but a code it emits or a
// status it sends that disagrees with the shared table is a build failure, not a
// documentation lie. Rebase note: #14 renamed the pre-existing lookup surface —
// wireCode → mapCode and the statusFor helper → the codeStatus map — so these tests
// bind onto the merged names.

// TestAPIStatusesMatchWirecodeTable: for every code marked Produced, the API's
// codeStatus mapping must send exactly the shared table's status.
func TestAPIStatusesMatchWirecodeTable(t *testing.T) {
	for _, c := range wirecode.All() {
		e := wirecode.Table[c]
		if e.Kind != wirecode.Produced {
			continue
		}
		if got := codeStatus[Code(c)]; got != e.Status {
			t.Errorf("codeStatus[%q] = %d, wirecode.Table says %d", c, got, e.Status)
		}
	}
}

// TestWireCodeNeverEmitsUnregisteredCodes: every sentinel and wrapper the API can
// classify must map to a registered code — an unregistered string on the wire is
// outside the closed enum by definition.
func TestWireCodeNeverEmitsUnregisteredCodes(t *testing.T) {
	inputs := []error{nil}
	for _, s := range errs.All() {
		inputs = append(inputs, s,
			errs.E(s, "api.test", "wrapped %v", s),
			errs.WithNext(errs.E(s, "api.test", "with next %v", s), "messq fix it"))
	}
	for _, err := range inputs {
		code := mapCode(err)
		e, ok := wirecode.Table[wirecode.Code(code)]
		if !ok {
			t.Errorf("mapCode(%v) = %q: unregistered code", err, code)
			continue
		}
		switch e.Kind {
		case wirecode.Produced, wirecode.Planned:
			// The only kinds allowed on the wire: shipped, or frozen for a route
			// whose issue is actively landing (#14's own envelope).
		case wirecode.NeverOverHTTP:
			t.Errorf("mapCode(%v) = %q: never-over-HTTP code leaked to the API layer", err, code)
		case wirecode.Reserved:
			t.Errorf("mapCode(%v) = %q: reserved code produced before its owning issue ships", err, code)
		}
	}
}

// TestWireCodeSentinelMapPinned pins the sentinel → code pairs PLAN names so a
// refactor cannot quietly reshuffle them.
func TestWireCodeSentinelMapPinned(t *testing.T) {
	cases := map[error]wirecode.Code{
		errs.ErrNotFound:     wirecode.NotFound,
		errs.ErrConflict:     wirecode.Conflict,
		errs.ErrBadRequest:   wirecode.BadRequest,
		errs.ErrBadSubject:   wirecode.BadSubject,
		errs.ErrTooLarge:     wirecode.TooLarge,
		errs.ErrReadOnly:     wirecode.ReadOnly,
		errs.ErrShuttingDown: wirecode.ShuttingDown,
		nil:                  wirecode.Internal,
	}
	for sentinel, want := range cases {
		if got := wirecode.Code(mapCode(sentinel)); got != want {
			t.Errorf("mapCode(%v) = %q, want %q", sentinel, got, want)
		}
	}
}

// TestUnknownSentinelIsInternal keeps the catch-all honest: an error nobody classified
// is 500 internal, never a borrowed 4xx.
func TestUnknownSentinelIsInternal(t *testing.T) {
	unregistered := errors.New("somebody forgot to classify this")
	if got := mapCode(unregistered); got != "internal" {
		t.Fatalf("mapCode(unclassified) = %q, want internal", got)
	}
	if got := codeStatus[CodeInternal]; got != http.StatusInternalServerError {
		t.Fatalf("codeStatus[internal] = %d, want 500", got)
	}
}
