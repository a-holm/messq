// SPDX-License-Identifier: Apache-2.0

package api

import (
	"encoding/json"
	"errors"
	"net/http"
	"strings"

	"github.com/a-holm/messq/internal/errs"
)

// The JSON decoding policy (issue §10 / G12): MaxBytesReader BEFORE the first read,
// DisallowUnknownFields so a typo'd wait_ms is a rejected request instead of a silent
// zero (a 30-second long poll that becomes a hot loop is a support incident), trailing
// data rejected, and a non-JSON media type answered with unsupported_media_type. This
// strictness is forward-compatible by construction: new request fields are only ever
// added server-side, and an older server rejecting a newer client loudly is the wanted
// behaviour — stated in the compatibility section of PROTOCOL.md.

// decodeJSON decodes one JSON value of type T from the request body under the given
// byte cap. Errors carry their code refinement already, so writeError maps them
// without any caller-side classification.
func decodeJSON[T any](w http.ResponseWriter, r *http.Request, max int64) (T, error) {
	var v T
	err := decodeJSONInto(w, r, max, &v)
	return v, err
}

// decodeJSONInto is decodeJSON with a caller-seeded destination: handlers whose wire
// shape pre-seeds defaults (the consumer create form) pass their seeded struct.
func decodeJSONInto[T any](w http.ResponseWriter, r *http.Request, max int64, v *T) error {
	if ct := r.Header.Get("Content-Type"); ct != "" && !isJSONContentType(ct) && !isCurlFormDefault(ct) {
		return errs.WithCode(errs.E(errs.ErrBadRequest, "api.decode",
			"content type %q is not JSON; send application/json", ct),
			string(CodeUnsupportedMediaType))
	}
	r.Body = http.MaxBytesReader(w, r.Body, max)
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	if err := dec.Decode(v); err != nil {
		var maxErr *http.MaxBytesError
		if errors.As(err, &maxErr) {
			return errs.E(errs.ErrTooLarge, "api.decode",
				"request body exceeds the %d byte cap (--max-request-bytes)", max)
		}
		return errs.E(errs.ErrBadRequest, "api.decode", "invalid JSON body: %v", err)
	}
	if dec.More() {
		return errs.E(errs.ErrBadRequest, "api.decode",
			"trailing data after the JSON value; send exactly one JSON document")
	}
	return nil
}

// isCurlFormDefault reports whether a Content-Type is the one curl -d sends when no -H
// overrides it: application/x-www-form-urlencoded. PLAN §7's five-line shell worker —
// `curl … -d '{"batch":1,"wait_ms":5000}'` — relies on that default, so it is treated
// as "type unspecified" rather than as a typed non-JSON body. This is a deliberate,
// documented exception to the 415 policy (issue #14 §10 vs PLAN §7 worker, resolved for
// the executable transcript); any OTHER typed non-JSON media type is still refused.
func isCurlFormDefault(ct string) bool {
	media := ct
	if i := strings.IndexByte(media, ';'); i >= 0 {
		media = media[:i]
	}
	return strings.TrimSpace(strings.ToLower(media)) == "application/x-www-form-urlencoded"
}

// isJSONContentType reports whether a Content-Type value names JSON: application/json,
// its +suffix forms, or a generic application/* fallback. Parameters (charset) are
// ignored.
func isJSONContentType(ct string) bool {
	media := ct
	if i := strings.IndexByte(media, ';'); i >= 0 {
		media = media[:i]
	}
	media = strings.TrimSpace(strings.ToLower(media))
	switch {
	case media == "application/json":
		return true
	case strings.HasPrefix(media, "application/json+"), strings.HasPrefix(media, "application/vnd.messq."):
		return true // parameterised or vendored JSON spellings
	case media == "text/json":
		return true
	default:
		return false
	}
}
