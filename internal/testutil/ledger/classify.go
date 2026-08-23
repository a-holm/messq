// SPDX-License-Identifier: Apache-2.0

package ledger

import (
	"encoding/json"
	"fmt"
	"strings"
)

// The verdict classifier — the rule that turns a client observation into one of the three
// ledger verdicts. The FAILED allow-list is closed and small on purpose (issue §Design-3):
// adding a code to it is a claim about the daemon's internals and must be argued in review.
// The failure modes are asymmetric: a wrong UNKNOWN merely weakens the oracle, a wrong
// FAILED produces false failures, and a wrong OK is unthinkable. A code the daemon has
// never been observed to produce is NOT silently mapped to a verdict — it fails the cycle
// with "unclassified response code" so the mapping stays deliberate.

// failedCodes is the closed allow-list: errors the server can only produce BEFORE the
// command enters a commit batch, so the message must not exist.
var failedCodes = map[string]bool{
	"bad_request":      true, // 400
	"not_found":        true, // 404
	"too_large":        true, // 413
	"subject_mismatch": true, // 422 (the §7 envelope code; status 400)
	"disk_full":        true, // 507 — reserved for #32's ENOSPC, on the list from day one
}

// knownCodes is the full wire-code vocabulary the daemon's error envelope can carry at M2
// (internal/api.wireCode plus disk_full). A recognised code not on the FAILED allow-list is
// UNKNOWN; anything outside this set is a code the harness has never seen, which fails the
// cycle rather than becoming a verdict.
var knownCodes = map[string]bool{
	"stream_exists":    true,
	"reserved_name":    true,
	"would_lose_data":  true,
	"subject_mismatch": true,
	"reserved_header":  true,
	"too_large":        true,
	"header_too_large": true,
	"not_found":        true,
	"conflict":         true,
	"bad_subject":      true,
	"bad_request":      true,
	"read_only":        true,
	"shutting_down":    true,
	"internal":         true,
	"disk_full":        true,
}

// Classify maps a client observation to a verdict. status is the HTTP status (0 for a
// transport failure with no response); code is the error-envelope code (empty when there is
// none). A 2xx is a durability promise under `full` (D4/S11.1), so it is OK regardless of
// any code; a transport failure with no response is UNKNOWN — the kill window. An
// unrecognised non-empty code returns an error so the caller can fail the cycle.
func Classify(status int, code string) (Verdict, error) {
	if status/100 == 2 {
		return OK, nil
	}
	if code == "" {
		// No response at all: connection reset, EOF, write error, client timeout, or the
		// process died before replying. Either outcome is legal.
		return Unknown, nil
	}
	if failedCodes[code] {
		return Failed, nil
	}
	if knownCodes[code] {
		return Unknown, nil
	}
	return Unknown, fmt.Errorf("unclassified response code %q (status %d)", code, status)
}

// envelope is the daemon's error-envelope shape (internal/api.errorEnvelope): the classifier
// only needs the code, but decoding the whole object keeps the harness's wire shape in one
// place and fails loudly if the body is not an envelope at all.
type envelope struct {
	Error struct {
		Code string `json:"code"`
	} `json:"error"`
}

// ClassifyResponse decodes an error-envelope body and classifies it against the status.
// A body that is not a valid envelope is reported as an unclassified code, never as a
// verdict.
func ClassifyResponse(status int, body []byte) (Verdict, string, error) {
	var env envelope
	if err := json.Unmarshal(body, &env); err != nil {
		return Unknown, "", fmt.Errorf("unclassified response code (status %d): envelope does not parse: %w", status, err)
	}
	code := strings.TrimSpace(env.Error.Code)
	v, err := Classify(status, code)
	return v, code, err
}
