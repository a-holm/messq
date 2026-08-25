// SPDX-License-Identifier: Apache-2.0

package client

import (
	"errors"
	"net/http"
	"strings"
	"testing"
	"time"
)

func TestNewAcceptsAddressForms(t *testing.T) {
	t.Parallel()

	for _, addr := range []string{
		"unix:///run/messq/messq.sock",
		"/run/messq/messq.sock",
		"./messq.sock",
		"http://127.0.0.1:4390",
		"https://messq.example.com:4390",
		"tcp://127.0.0.1:4390",
		"tcp://[::1]:4390",
	} {
		c, err := New(addr)
		if err != nil {
			t.Errorf("New(%q): unexpected error %v", addr, err)
			continue
		}
		if c == nil {
			t.Errorf("New(%q): nil client", addr)
		}
	}
}

func TestNewRejectsAddressForms(t *testing.T) {
	t.Parallel()

	for _, addr := range []string{
		"",
		"ftp://example.com",
		"unix://",           // empty socket path
		"http://h/x",        // path prefix under http
		"https://h/y",       // path prefix under https
		"messq.sock",        // bare relative name, neither scheme nor ./path
		"[::1]:4390",        // bare IPv6 endpoint without a scheme
		"unix:///run/../..", // resolves outside any socket namespace? still a path — kept accepted below
	} {
		if addr == "unix:///run/../.." {
			continue // paths are taken verbatim; see TestNewUnixSocketPathVerbatim
		}
		_, err := New(addr)
		if !errors.Is(err, ErrBadAddress) {
			t.Errorf("New(%q): err = %v, want ErrBadAddress", addr, err)
			continue
		}
		for _, want := range []string{"unix", "http", "tcp"} {
			if !strings.Contains(err.Error(), want) {
				t.Errorf("New(%q): error %q does not name accepted form %q", addr, err, want)
			}
		}
	}
}

func TestNewUnixSocketHost(t *testing.T) {
	t.Parallel()

	c, err := New("/run/messq/messq.sock")
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if got := c.url("/v1/info"); got != "http://messq.invalid/v1/info" {
		t.Errorf("url(/v1/info) = %q, want the messq.invalid sentinel host", got)
	}
	if c.network != "unix" || c.dialAddr != "/run/messq/messq.sock" {
		t.Errorf("network/dialAddr = %q/%q, want unix//run/messq/messq.sock", c.network, c.dialAddr)
	}
}

func TestNewTCPRewritesToHTTP(t *testing.T) {
	t.Parallel()

	c, err := New("tcp://127.0.0.1:4390")
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if got := c.url("/healthz"); got != "http://127.0.0.1:4390/healthz" {
		t.Errorf("url(/healthz) = %q, want rewritten http form", got)
	}
}

func TestNewRefusesLongPollCuttingTimeout(t *testing.T) {
	t.Parallel()

	_, err := New("tcp://127.0.0.1:4390", WithHTTPClient(&http.Client{
		Timeout: 5 * time.Second,
	}))
	if !errors.Is(err, ErrConfig) {
		t.Fatalf("err = %v, want ErrConfig for a caller client whose Timeout cuts a long poll", err)
	}

	// A timeout longer than the default fetch wait, or none at all, is accepted.
	for _, hc := range []*http.Client{
		{Timeout: defaultFetchWait + time.Second},
		{Timeout: 0},
	} {
		if _, err := New("tcp://127.0.0.1:4390", WithHTTPClient(hc)); err != nil {
			t.Errorf("New(Timeout=%v): %v", hc.Timeout, err)
		}
	}
}

func TestNewDefaults(t *testing.T) {
	t.Parallel()

	c, err := New("tcp://127.0.0.1:4390")
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if c.maxResponseBytes != defaultMaxResponseBytes {
		t.Errorf("maxResponseBytes = %d, want %d", c.maxResponseBytes, defaultMaxResponseBytes)
	}
	if c.requestTimeout != defaultRequestTimeout {
		t.Errorf("requestTimeout = %v, want %v", c.requestTimeout, defaultRequestTimeout)
	}
	tr, ok := c.hc.Transport.(*http.Transport)
	if !ok {
		t.Fatalf("transport is %T, want *http.Transport", c.hc.Transport)
	}
	if tr.Proxy != nil {
		t.Error("Proxy is set; the default transport must NEVER consult proxy environment variables")
	}
	if tr.ForceAttemptHTTP2 {
		t.Error("ForceAttemptHTTP2 must stay false")
	}
	if tr.ResponseHeaderTimeout != 0 {
		t.Errorf("ResponseHeaderTimeout = %v, want 0 so long-poll headers arrive at wait_ms", tr.ResponseHeaderTimeout)
	}
	if c.hc.Timeout != 0 {
		t.Errorf("hc.Timeout = %v, want 0 (per-request deadlines instead)", c.hc.Timeout)
	}
	if c.userAgent == "" {
		t.Error("default user agent is empty")
	}
}

func TestClassifyEveryCode(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		code string
		want Kind
	}{
		{"bad_request", KindUsage},
		{"bad_subject", KindUsage},
		{"subject_mismatch", KindUsage},
		{"reserved_header", KindUsage},
		{"reserved_name", KindUsage},
		{"invalid_token", KindUsage},
		{"header_too_large", KindUsage},
		{"unsupported_media_type", KindUsage},
		{"unsupported", KindUsage},
		{"method_not_allowed", KindUsage},
		{"too_large", KindUsage},
		{"not_found", KindNotFound},
		{"conflict", KindConflict},
		{"stream_exists", KindConflict},
		{"immutable_field", KindConflict},
		{"would_lose_data", KindConflict},
		{"stale_ack", KindConflict},
		{"extend_capped", KindConflict},
		{"paused", KindConflict},
		{"unauthorized", KindPermission},
		{"forbidden", KindPermission},
		{"flow_control", KindUnavailable},
		{"rate_limited", KindUnavailable},
		{"commit_unknown", KindUnavailable},
		{"busy", KindUnavailable},
		{"too_many_waiters", KindUnavailable},
		{"read_only", KindUnavailable},
		{"shutting_down", KindUnavailable},
		{"disk_full", KindUnavailable},
		{"stream_full", KindUnavailable},
		{"internal", KindInternal},
		// An unknown code is preserved, never rejected (Decision 2); Classify still
		// owes #23 an answer for it.
		{"some_future_code", KindInternal},
	} {
		err := &Error{Code: tc.code}
		if got := Classify(err); got != tc.want {
			t.Errorf("Classify(%q) = %v, want %v", tc.code, got, tc.want)
		}
	}

	if got := Classify(nil); got != KindOK {
		t.Errorf("Classify(nil) = %v, want KindOK", got)
	}
	if got := Classify(errors.New("plain")); got != KindInternal {
		t.Errorf("Classify(plain) = %v, want KindInternal", got)
	}
}

func TestErrorIsMapsEverySentinel(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		code     string
		sentinel error
	}{
		{"bad_request", ErrBadRequest},
		{"bad_subject", ErrBadSubject},
		{"subject_mismatch", ErrSubjectMismatch},
		{"reserved_header", ErrReservedHeader},
		{"reserved_name", ErrReservedName},
		{"invalid_token", ErrInvalidToken},
		{"unauthorized", ErrUnauthorized},
		{"forbidden", ErrForbidden},
		{"not_found", ErrNotFound},
		{"method_not_allowed", ErrMethodNotAllowed},
		{"conflict", ErrConflict},
		{"stream_exists", ErrStreamExists},
		{"immutable_field", ErrImmutableField},
		{"would_lose_data", ErrWouldLoseData},
		{"stale_ack", ErrStaleAck},
		{"extend_capped", ErrExtendCapped},
		{"paused", ErrPaused},
		{"too_large", ErrTooLarge},
		{"header_too_large", ErrHeaderTooLarge},
		{"unsupported_media_type", ErrUnsupportedMediaType},
		{"unsupported", ErrUnsupported},
		{"flow_control", ErrFlowControl},
		{"rate_limited", ErrRateLimited},
		{"commit_unknown", ErrCommitUnknown},
		{"busy", ErrBusy},
		{"too_many_waiters", ErrTooManyWaiters},
		{"read_only", ErrReadOnly},
		{"shutting_down", ErrShuttingDown},
		{"disk_full", ErrDiskFull},
		{"stream_full", ErrStreamFull},
		{"unavailable", ErrUnreachable},
	} {
		err := &Error{Code: tc.code, Message: "m"}
		if !errors.Is(err, tc.sentinel) {
			t.Errorf("errors.Is(Error{Code:%q}, %#v) = false, want true", tc.code, tc.sentinel)
		}
	}

	// An unknown code matches NO sentinel — forward compatibility, Decision 2.
	err := &Error{Code: "brand_new_code"}
	for _, s := range []error{ErrBadRequest, ErrNotFound, ErrConflict, ErrStaleAck, ErrUnreachable} {
		if errors.Is(err, s) {
			t.Errorf("unknown code %q matched sentinel %#v", err.Code, s)
		}
	}
}
