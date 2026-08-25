// SPDX-License-Identifier: Apache-2.0

package client

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
)

// This file is the ONE request/response path every wrapper rides (issue §6): request
// encoding, the bearer header, transport-error classification, response draining, and
// lenient decoding of both success bodies and the error envelope. Keeping it in one
// generic is what makes 25 control-plane wrappers three lines each.

// wireEnvelope is the one error shape every non-2xx carries (PLAN §7). Decoding is
// lenient: unknown fields are dropped, an unknown code is preserved verbatim.
type wireEnvelope struct {
	Error wireErrorBody `json:"error"`
}

type wireErrorBody struct {
	Code    string         `json:"code"`
	Message string         `json:"message"`
	Next    []string       `json:"next"`
	Detail  map[string]any `json:"detail,omitempty"`
	TraceID string         `json:"trace_id"`
}

// do performs one JSON-in/JSON-out request with the control-plane timeout applied.
func do[Res any](ctx context.Context, c *Client, method, path string, query url.Values, body any) (Res, error) {
	return send[Res](ctx, c, request{
		method:   method,
		path:     path,
		query:    query,
		jsonBody: body,
	})
}

// doUntimed is do without the control-plane timeout: Fetch owns its wait.
func doUntimed[Res any](ctx context.Context, c *Client, method, path string, query url.Values, body any) (Res, error) {
	return send[Res](ctx, c, request{
		method:           method,
		path:             path,
		query:            query,
		jsonBody:         body,
		noControlTimeout: true,
	})
}

type request struct {
	method           string
	path             string
	query            url.Values
	jsonBody         any // JSON-marshalled when non-nil
	rawBody          io.Reader
	rawLen           int64 // >= 0 sets Content-Length; -1 streams chunked
	rawContentType   string
	extraHeaders     [][2]string
	noControlTimeout bool
}

func opName(r request) string { return r.method + " " + r.path }

// send is the whole client-side HTTP story. Every branch drains and closes the body;
// every failure comes back as a typed *Error or a wrapped context error.
func send[Res any](ctx context.Context, c *Client, r request) (Res, error) {
	var zero Res
	if ctx == nil {
		ctx = context.Background()
	}
	if !r.noControlTimeout && c.requestTimeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, c.requestTimeout)
		defer cancel()
	}

	u := c.url(r.path)
	if len(r.query) > 0 {
		u += "?" + r.query.Encode()
	}

	var rdr io.Reader
	contentType := ""
	switch {
	case r.rawBody != nil:
		rdr = r.rawBody
		if r.rawLen >= 0 {
			rdr = io.LimitReader(r.rawBody, r.rawLen) // keep Content-Length honest
		}
		contentType = r.rawContentType
	case r.jsonBody == nil:
		// nothing to encode
	case isJSONRaw(r.jsonBody):
		raw := r.jsonBody.(jsonRaw) //nolint:errcheck // the case guard IS the type check
		rdr = strings.NewReader(string(raw))
		contentType = "application/json"
	default:
		b, err := json.Marshal(r.jsonBody)
		if err != nil {
			return zero, &Error{Code: "internal", Message: "encode request: " + err.Error(), err: err}
		}
		rdr = bytes.NewReader(b)
		contentType = "application/json"
	}
	req, err := http.NewRequestWithContext(ctx, r.method, u, rdr)
	if err != nil {
		return zero, &Error{Code: "internal", Message: "build request: " + err.Error(), err: err}
	}
	if r.rawLen >= 0 && r.rawBody != nil {
		req.ContentLength = r.rawLen
	}
	req.Header.Set("User-Agent", c.userAgent)
	if contentType != "" {
		req.Header.Set("Content-Type", contentType)
	}
	for _, kv := range r.extraHeaders {
		req.Header.Set(kv[0], kv[1])
	}
	if c.credential.token != "" {
		req.Header.Set("Authorization", "Bearer "+c.credential.token)
	}

	resp, err := c.hc.Do(req)
	if err != nil {
		// A refused redirect surfaces as *url.Error wrapping our own *Error; hand it
		// back directly so errors.Is(err, ErrRedirect) works at the call site.
		var ue *url.Error
		if errors.As(err, &ue) {
			var ce *Error
			if errors.As(ue.Err, &ce) {
				return zero, ce
			}
		}
		return zero, unreachable(opName(r), err)
	}

	data, rerr := readCapped(resp.Body, c.maxResponseBytes)
	drainErr := drain(resp.Body, c.maxResponseBytes)
	closeErr := resp.Body.Close()
	if rerr == nil {
		rerr = drainErr
	}
	if closeErr != nil && rerr == nil {
		rerr = unreachable(opName(r), closeErr)
	}
	if rerr != nil {
		if _, too := rerr.(*Error); too { //nolint:errorlint // single type switch on our own sentinel-carrying error
			return zero, rerr
		}
		return zero, unreachable(opName(r), rerr)
	}

	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		if len(data) == 0 {
			return zero, nil
		}
		var res Res
		if err := json.Unmarshal(data, &res); err != nil {
			return zero, &Error{
				Code:    "internal",
				Message: fmt.Sprintf("%s: response is not valid JSON (%d bytes): %v", opName(r), len(data), err),
				Status:  resp.StatusCode,
				err:     err,
			}
		}
		return res, nil
	}

	return zero, decodeFailure(opName(r), resp, data)
}

// plain performs a request whose response is NOT JSON (healthz text, peek data),
// applying the same header/credential/timeout policy and transport-error mapping as
// send. The caller owns draining and closing the returned body.
func (c *Client) plain(ctx context.Context, method, path string) (*http.Response, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if c.requestTimeout > 0 && method != "POST" || c.requestTimeout > 0 && method == "GET" {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, c.requestTimeout)
		defer cancel()
	}
	req, err := http.NewRequestWithContext(ctx, method, c.url(path), nil)
	if err != nil {
		return nil, &Error{Code: "internal", Message: "build request: " + err.Error(), err: err}
	}
	req.Header.Set("User-Agent", c.userAgent)
	if c.credential.token != "" {
		req.Header.Set("Authorization", "Bearer "+c.credential.token)
	}
	resp, err := c.hc.Do(req)
	if err != nil {
		var ue *url.Error
		if errors.As(err, &ue) {
			var ce *Error
			if errors.As(ue.Err, &ce) {
				return nil, ce
			}
		}
		return nil, unreachable(method+" "+path, err)
	}
	return resp, nil
}

// readCapped reads at most max bytes of a body; one byte more is a refusal wrapping
// ErrTooLarge — a rogue proxy must not OOM a worker (issue §9).
func readCapped(body io.Reader, max int64) ([]byte, error) {
	data, err := io.ReadAll(io.LimitReader(body, max+1))
	if err != nil {
		return nil, err
	}
	if int64(len(data)) > max {
		return nil, &Error{
			Code:    "too_large",
			Message: fmt.Sprintf("response body exceeds MaxResponseBytes (%d); raise WithMaxResponseBytes or fix what is answering", max),
			Status:  0,
			err:     ErrTooLarge,
		}
	}
	return data, nil
}

// drain empties whatever is left of a body so the connection returns to the idle
// pool, bounded by max so a hostile peer cannot make the drain unbounded.
func drain(body io.Reader, max int64) error {
	_, err := io.Copy(io.Discard, io.LimitReader(body, max))
	if err != nil {
		// A truncated drain only costs keep-alive reuse, never correctness.
		return nil //nolint:nilerr // deliberate: the request answer is already in hand
	}
	return nil
}

// decodeFailure builds the *Error for a non-2xx: envelope fields when the daemon
// wrote one, status-plus-snippet when something between here and there did not.
func decodeFailure(op string, resp *http.Response, data []byte) *Error {
	e := &Error{
		Status:     resp.StatusCode,
		RequestID:  resp.Header.Get("Messq-Request-Id"),
		RetryAfter: retryAfter(resp.Header.Get("Retry-After")),
		Code:       "internal",
		Message:    fmt.Sprintf("%s: unexpected status %d", op, resp.StatusCode),
	}
	var env wireEnvelope
	if err := json.Unmarshal(data, &env); err == nil && env.Error.Code != "" {
		e.Code = env.Error.Code
		e.Message = env.Error.Message
		e.Next = env.Error.Next
		e.Detail = env.Error.Detail
		e.TraceID = env.Error.TraceID
		if e.Message == "" {
			e.Message = e.Code
		}
		return e
	}
	// Not an envelope (a proxy's HTML 502, a truncated body): keep the status and a
	// short snippet — never panic, never lose the status.
	snippet := data
	if len(snippet) > 200 {
		snippet = snippet[:200]
	}
	e.Message = fmt.Sprintf("%s: status %d with a non-envelope body: %q",
		op, resp.StatusCode, strings.TrimSpace(string(snippet)))
	return e
}
