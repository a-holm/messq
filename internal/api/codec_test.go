// SPDX-License-Identifier: Apache-2.0

package api

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/a-holm/messq/internal/errs"
)

// G12: decode strictness is a policy, not an accident — unknown fields, trailing data,
// non-JSON media types and oversized bodies are typed rejections, and a typo'd field
// name must never silently decode to a zero value.

type decodeFixture struct {
	Batch int `json:"batch"`
}

// postDecode pushes one body through the full chain into decodeJSON and returns the
// mapped (status, code) of whatever came out; code "" means the handler accepted it.
func postDecode(t *testing.T, srv *Server, contentType, body string) (int, Code) {
	t.Helper()

	req := httptest.NewRequestWithContext(context.Background(), http.MethodPost,
		"/v1/streams/x/consumers/y/fetch", strings.NewReader(body))
	if contentType != "" {
		req.Header.Set("Content-Type", contentType)
	}
	rec := httptest.NewRecorder()
	srv.chained(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		v, err := decodeJSON[decodeFixture](w, r, 1024)
		if err != nil {
			srv.writeError(w, err)
			return
		}
		if v.Batch != 16 {
			srv.writeError(w, errs.E(errs.ErrBadRequest, "test", "batch decoded as %d", v.Batch))
			return
		}
		w.WriteHeader(http.StatusOK)
	})).ServeHTTP(rec, req)

	if rec.Code == http.StatusOK {
		return http.StatusOK, ""
	}
	var env Envelope
	if err := json.Unmarshal(rec.Body.Bytes(), &env); err != nil {
		t.Fatalf("response not an envelope (%v): %q", err, rec.Body.String())
	}
	return rec.Code, env.Error.Code
}

func TestDecodeJSONAcceptsValidBody(t *testing.T) {
	t.Parallel()

	srv := mappingServer()
	if status, code := postDecode(t, srv, "application/json", `{"batch":16}`); status != 200 {
		t.Fatalf("valid body rejected: %d %s", status, code)
	}
	// An absent Content-Type is tolerated (the five-line shell worker's curl -d).
	if status, code := postDecode(t, srv, "", `{"batch":16}`); status != 200 {
		t.Fatalf("absent content type rejected: %d %s", status, code)
	}
}

func TestDecodeJSONRejectsUnknownField(t *testing.T) {
	t.Parallel()

	status, code := postDecode(t, mappingServer(), "application/json", `{"batch":16,"wait_ms":5}`)
	if status != 400 || code != CodeBadRequest {
		t.Fatalf("got (%d,%s), want (400,bad_request) — a typo'd field must not decode silently", status, code)
	}
}

func TestDecodeJSONRejectsTrailingData(t *testing.T) {
	t.Parallel()

	status, code := postDecode(t, mappingServer(), "application/json", `{"batch":16} {"batch":1}`)
	if status != 400 || code != CodeBadRequest {
		t.Fatalf("got (%d,%s), want (400,bad_request)", status, code)
	}
}

func TestDecodeJSONRejectsNonJSONContentType(t *testing.T) {
	t.Parallel()

	status, code := postDecode(t, mappingServer(), "text/plain", `{"batch":16}`)
	if status != 415 || code != CodeUnsupportedMediaType {
		t.Fatalf("got (%d,%s), want (415,unsupported_media_type)", status, code)
	}
}

func TestDecodeJSONMapsSizeTripToTooLarge(t *testing.T) {
	t.Parallel()

	body := `{"batch":16,"pad":"` + strings.Repeat("x", 2048) + `"}`
	status, code := postDecode(t, mappingServer(), "application/json", body)
	if status != 413 || code != CodeTooLarge {
		t.Fatalf("got (%d,%s), want (413,too_large)", status, code)
	}
}
