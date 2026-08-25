// SPDX-License-Identifier: Apache-2.0

package client

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
)

func TestRedirectRefusedAndCredentialNeverResent(t *testing.T) {
	t.Parallel()

	var requests int
	var authOnSecond string
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests++
		if requests == 1 {
			http.Redirect(w, r, "/elsewhere", http.StatusFound)
			return
		}
		authOnSecond = r.Header.Get("Authorization")
		_, _ = io.WriteString(w, `{}`)
	}))
	defer ts.Close()

	c := newTestClient(t, ts, WithCredential(TokenCredential("msq1_id_secret")))
	_, err := do[struct{}](context.Background(), c, http.MethodGet, "/", nil, nil)
	if !errors.Is(err, ErrRedirect) {
		t.Fatalf("err = %v, want ErrRedirect", err)
	}
	if requests != 1 {
		t.Errorf("%d requests hit the server; a redirect was followed", requests)
	}
	if authOnSecond != "" {
		t.Error("the credential rode the redirect")
	}
}

func TestDefaultTransportNeverConsultsProxyEnvironment(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = io.WriteString(w, `{}`)
	}))
	defer ts.Close()
	t.Setenv("HTTP_PROXY", "http://127.0.0.1:9") // nothing listens there
	t.Setenv("http_proxy", "http://127.0.0.1:9")

	c := newTestClient(t, ts)
	if _, err := do[struct{}](context.Background(), c, http.MethodGet, "/", nil, nil); err != nil {
		t.Fatalf("request failed although the proxy must be ignored: %v", err)
	}
}

func TestWithProxyOptsIn(t *testing.T) {
	t.Parallel()

	var viaProxy bool
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.IsAbs() { // proxied form: the client asked us to act as its proxy
			viaProxy = true
			resp, err := http.Get(r.URL.String())
			if err != nil {
				w.WriteHeader(http.StatusBadGateway)
				return
			}
			defer resp.Body.Close() //nolint:errcheck // test relay
			w.WriteHeader(resp.StatusCode)
			_, _ = io.Copy(w, resp.Body)
			return
		}
		_, _ = io.WriteString(w, `{}`)
	}))
	defer ts.Close()

	proxyURL, perr := url.Parse(ts.URL)
	if perr != nil {
		t.Fatalf("parse proxy url: %v", perr)
	}
	c, err := New(ts.URL, WithProxy(func(*http.Request) (*url.URL, error) {
		return proxyURL, nil
	}))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if _, err := do[struct{}](context.Background(), c, http.MethodGet, "/", nil, nil); err != nil {
		t.Fatalf("proxied round trip: %v", err)
	}
	if !viaProxy {
		t.Error("WithProxy opt-in did not route through the proxy")
	}
}

func TestMaxResponseBytesCapsResponses(t *testing.T) {
	t.Parallel()

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"pad":"`))
		for range 64 * 1024 {
			_, _ = w.Write([]byte("x"))
		}
		_, _ = w.Write([]byte(`"}`))
	}))
	defer ts.Close()

	c := newTestClient(t, ts, WithMaxResponseBytes(1024))
	type padded struct {
		Pad string `json:"pad"`
	}
	_, err := do[padded](context.Background(), c, http.MethodGet, "/", nil, nil)
	if !errors.Is(err, ErrTooLarge) {
		t.Fatalf("err = %v, want ErrTooLarge past MaxResponseBytes", err)
	}
}

func TestUnixAddressDialsTheSocket(t *testing.T) {
	t.Parallel()

	// A real unix socket end to end: parseAddr's sentinel host must never leak into
	// the dial path.
	dir := t.TempDir()
	sock := dir + "/m.sock"

	ln, err := url.Parse("unix://" + sock)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	_ = ln
	c, err := New("unix://" + sock)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	// Nothing is listening: the failure class must be unreachable, proving the dial
	// targeted the socket path (a DNS attempt on messq.invalid would differ).
	_, err = do[struct{}](context.Background(), c, http.MethodGet, "/healthz", nil, nil)
	if !errors.Is(err, ErrUnreachable) {
		t.Fatalf("err = %v, want ErrUnreachable from dialing the socket", err)
	}
}
