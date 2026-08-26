// SPDX-License-Identifier: Apache-2.0

package client

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"runtime"
	"strings"
	"time"
)

// Package defaults (issue §9).
const (
	defaultFetchWait          = 30 * time.Second // WorkerConfig.Wait default; the refusal floor for caller clients
	defaultMaxResponseBytes   = int64(64 << 20)  // 64 MiB: a rogue proxy must not OOM a worker
	defaultRequestTimeout     = 30 * time.Second // control-plane only; never applied to Fetch
	defaultUserAgentPrefix    = "messq-client"
	unixHostSentinel          = "messq.invalid" // request host for unix:// targets; never resolved
	reasonLimit               = 4 << 10         // nak/term reason budget (#10's --max-reason-bytes)
	DefaultPublishBatchCap    = 1000            // the daemon's default --max-batch-messages
	defaultExtendWindow       = 250 * time.Millisecond
	defaultAckWindow          = 5 * time.Millisecond
	maxSettleTokensPerRequest = 256
	defaultBackoffInitial     = 100 * time.Millisecond
	defaultBackoffMax         = 30 * time.Second
	backoffJitter             = 0.2 // ±20 %, always
)

// Client is a messq client bound to one daemon address. Safe for concurrent use.
// Construct with [New]; the zero value is not usable.
type Client struct {
	hc               *http.Client
	base             string // scheme://host[:port] requests are built against
	network          string // "unix" or "tcp"
	dialAddr         string // socket path for unix; empty for tcp
	credential       Credential
	userAgent        string
	maxResponseBytes int64
	requestTimeout   time.Duration // control plane only
	clk              Clock
	proxy            func(*http.Request) (*url.URL, error)
}

// Option configures a [Client].
type Option func(*Client) error

// New binds a client to addr, one of:
//
//	unix:///run/messq/messq.sock   Unix socket (the daemon's default listener)
//	/run/messq/messq.sock          bare absolute path — same as above
//	./messq.sock                   explicit relative path
//	http://127.0.0.1:4390          plain HTTP
//	https://messq.example.com      TLS (bring your own *http.Client for custom roots)
//	tcp://127.0.0.1:4390           same as http:// — --listen speaks tcp://
//
// Anything else is refused with an error wrapping [ErrBadAddress] that names the
// accepted forms. New performs no I/O; it cannot detect a missing socket.
func New(addr string, opts ...Option) (*Client, error) {
	c := &Client{
		userAgent:        defaultUserAgentPrefix + " (go" + runtime.Version() + ")",
		maxResponseBytes: defaultMaxResponseBytes,
		requestTimeout:   defaultRequestTimeout,
		clk:              realClock{},
	}
	base, network, dial, err := parseAddr(addr)
	if err != nil {
		return nil, err
	}
	c.base, c.network, c.dialAddr = base, network, dial

	for _, opt := range opts {
		if err := opt(c); err != nil {
			return nil, &Error{Code: "config_error", Message: err.Error(), err: fmt.Errorf("%w: %w", ErrConfig, err)}
		}
	}
	if c.hc == nil {
		c.hc = newHTTPClient(c.network, c.dialAddr, c.proxy, nil)
	}
	return c, nil
}

// parseAddr splits an accepted address form into the URL base, dial network and dial
// address. See [New].
func parseAddr(addr string) (base, network, dial string, err error) {
	bad := func() (string, string, string, error) {
		return "", "", "", &Error{
			Code: "bad_address",
			Message: fmt.Sprintf("%q is not a messq address; accepted forms are "+
				`unix:///path/to.sock, /path/to.sock, ./path/to.sock, http://host:port, `+
				`https://host and tcp://host:port`, addr),
			err: fmt.Errorf("%w: %q", ErrBadAddress, addr),
		}
	}
	if addr == "" {
		return bad()
	}

	switch {
	case strings.HasPrefix(addr, "/"), strings.HasPrefix(addr, "./"), strings.HasPrefix(addr, "../"):
		return "http://" + unixHostSentinel, "unix", addr, nil
	}

	u, perr := url.Parse(addr)
	if perr != nil || u.Scheme == "" {
		return bad()
	}
	switch u.Scheme {
	case "unix":
		path := u.Path
		if path == "" {
			return bad()
		}
		return "http://" + unixHostSentinel, "unix", path, nil
	case "http", "https":
		if u.Host == "" || (u.Path != "" && u.Path != "/") || u.RawQuery != "" {
			return bad()
		}
		return u.Scheme + "://" + u.Host, "tcp", "", nil
	case "tcp":
		if u.Host == "" || u.Path != "" {
			return bad()
		}
		return "http://" + u.Host, "tcp", "", nil
	default:
		return bad()
	}
}

// url joins path onto the base, preserving any escaping done by the caller.
func (c *Client) url(path string) string { return c.base + path }

// newHTTPClient builds the package's transport policy: no proxy environment, no
// redirects (a credential must never ride one), no client timeout (per-request
// deadlines instead), idle pooling sized for concurrent long polls.
func newHTTPClient(network, dialAddr string, proxy func(*http.Request) (*url.URL, error), override *http.Transport) *http.Client {
	tr := override
	if tr == nil {
		var d net.Dialer
		tr = &http.Transport{
			DialContext: func(ctx context.Context, _, addr string) (net.Conn, error) {
				// Unix targets ignore the URL authority entirely; tcp targets keep
				// whatever host:port the transport resolved — including a proxy's,
				// which is what WithProxy opt-in must not break.
				if network == "unix" {
					addr = dialAddr
				}
				return d.DialContext(ctx, network, addr)
			},
			Proxy:                 proxy, // nil ⇒ NEVER ProxyFromEnvironment
			ForceAttemptHTTP2:     false,
			MaxIdleConns:          64,
			MaxIdleConnsPerHost:   64, // stdlib default of 2 causes a new conn per concurrent fetch
			IdleConnTimeout:       90 * time.Second,
			ResponseHeaderTimeout: 0, // a long poll's headers arrive at wait_ms
			ExpectContinueTimeout: time.Second,
		}
	}
	return &http.Client{
		Transport: tr,
		Timeout:   0,
		CheckRedirect: func(*http.Request, []*http.Request) error {
			return &Error{
				Code:    "redirect_refused",
				Message: "the daemon sent a redirect; messq clients refuse to follow one so credentials are never re-sent",
				err:     fmt.Errorf("%w", ErrRedirect),
			}
		},
	}
}

// WithHTTPClient supplies the caller's own *http.Client: they own transport, timeouts
// and TLS (#40). A client whose Timeout would cut a long poll short is refused at
// construction — a mysterious context deadline exceeded on every idle fetch is the bug
// this refusal prevents. Re-run your own check against WorkerConfig.Wait if you raise it.
func WithHTTPClient(hc *http.Client) Option {
	return func(c *Client) error {
		if hc == nil {
			return errors.New("WithHTTPClient(nil)")
		}
		if hc.Timeout > 0 && hc.Timeout <= defaultFetchWait {
			return fmt.Errorf(
				"http.Client.Timeout is %v; it must be 0 or greater than the fetch wait (%v), "+
					"otherwise every long poll dies mid-wait — pass Timeout: 0 and bound "+
					"requests with contexts instead", hc.Timeout, defaultFetchWait)
		}
		c.hc = hc
		return nil
	}
}

// WithCredential attaches a bearer credential (#16) sent as
// "Authorization: Bearer <token>" on every request.
func WithCredential(cred Credential) Option {
	return func(c *Client) error { c.credential = cred; return nil }
}

// WithUserAgent overrides the default user agent.
func WithUserAgent(ua string) Option {
	return func(c *Client) error {
		if ua == "" {
			return errors.New("WithUserAgent(\"\")")
		}
		c.userAgent = ua
		return nil
	}
}

// WithMaxResponseBytes caps how much of any response body is read before the client
// gives up with an error wrapping [ErrTooLarge]. Default 64 MiB.
func WithMaxResponseBytes(n int64) Option {
	return func(c *Client) error {
		if n <= 0 {
			return fmt.Errorf("MaxResponseBytes %d must be positive", n)
		}
		c.maxResponseBytes = n
		return nil
	}
}

// WithRequestTimeout bounds control-plane requests. It is NEVER applied to Fetch —
// a long poll owns its wait. Default 30s; zero disables.
func WithRequestTimeout(d time.Duration) Option {
	return func(c *Client) error {
		if d < 0 {
			return fmt.Errorf("request timeout %d must be >= 0", d)
		}
		c.requestTimeout = d
		return nil
	}
}

// WithProxy opts INTO proxy resolution (typically http.ProxyFromEnvironment). The
// default is nil: HTTP_PROXY silently rerouting a localhost broker call through a
// corporate proxy is a classic half-day debugging session, and nonsense over a socket.
func WithProxy(f func(*http.Request) (*url.URL, error)) Option {
	return func(c *Client) error { c.proxy = f; return nil }
}

// WithClock replaces the time seam. Production callers never need it; tests run under
// testing/synctest or supply their own Clock.
func WithClock(clk Clock) Option {
	return func(c *Client) error {
		if clk == nil {
			return errors.New("WithClock(nil)")
		}
		c.clk = clk
		return nil
	}
}
