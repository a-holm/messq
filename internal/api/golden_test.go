// SPDX-License-Identifier: Apache-2.0

package api

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/a-holm/messq/internal/clock"
	"github.com/a-holm/messq/internal/errs"
	"github.com/a-holm/messq/internal/queue"
	"github.com/a-holm/messq/internal/store"
	"github.com/google/go-cmp/cmp"
)

// updateGolden regenerates every testdata/*.golden file from live responses instead of
// comparing against them:
//
//	go test ./internal/api -run TestGoldenRoutes -update-golden
//
// A golden is the frozen wire contract of one §7 route (issue #7): the HTTP status and the
// response body with every volatile value — a freshly minted ULID, a trace id, a wall-clock
// timestamp, a db byte count, the build version — normalised to a placeholder. Field names
// are kept byte-for-byte, so renaming a JSON field changes the golden and fails CI, exactly
// like internal/store's schema golden. Regenerate only when a reviewed shape change is
// intentional, never to make a failing test pass.
var updateGolden = flag.Bool("update-golden", false, "rewrite testdata/*.golden from live responses")

// goldenCase is one §7 route or error code exercised over a real Unix socket.
type goldenCase struct {
	name  string
	raw   bool                                                         // raw: skip JSON normalisation (text bodies)
	setup func(t *testing.T, st *store.Store)                          // optional: prime store state
	do    func(t *testing.T, c *http.Client) (status int, body []byte) // the request under test
}

// TestGoldenRoutes drives every §7 route — success shapes and every error code the HTTP
// surface can emit — over a real Unix socket (net.Listen("unix", …) feeding an httptest
// server), and pins each response against its committed golden. This is the harness #18
// later generalises into the full contract-test suite.
func TestGoldenRoutes(t *testing.T) {
	orders := queue.DefaultConfig("orders")
	orders.Subjects = []string{"orders.>"}

	// peekByID carries the freshly minted ULID from the peek_by_id setup into its request,
	// since the id is random per run and must be read from the store rather than hard-coded.
	var peekByID string

	cases := []goldenCase{
		// ---- §7 success shapes ----
		{
			name: "healthz", raw: true,
			do: func(t *testing.T, c *http.Client) (int, []byte) {
				return doReq(t, c, http.MethodGet, "/healthz", "", nil)
			},
		},
		{
			name: "info",
			do: func(t *testing.T, c *http.Client) (int, []byte) {
				return doReq(t, c, http.MethodGet, "/v1/info", "", nil)
			},
		},
		{
			name: "stream_create",
			do: func(t *testing.T, c *http.Client) (int, []byte) {
				return doReq(t, c, http.MethodPost, "/v1/streams",
					`{"name":"orders","subjects":["orders.>"],"max_msg_size":262144,"dedup_window_ms":300000}`, nil)
			},
		},
		{
			name:  "stream_create_idempotent",
			setup: func(t *testing.T, st *store.Store) { mustCreateStream(t, st, orders) },
			do: func(t *testing.T, c *http.Client) (int, []byte) {
				return doReq(t, c, http.MethodPost, "/v1/streams",
					`{"name":"orders","subjects":["orders.>"]}`, nil)
			},
		},
		{
			name: "stream_list",
			setup: func(t *testing.T, st *store.Store) {
				mustCreateStream(t, st, orders)
				mustCreateStream(t, st, queue.DefaultConfig("shipments"))
			},
			do: func(t *testing.T, c *http.Client) (int, []byte) {
				return doReq(t, c, http.MethodGet, "/v1/streams", "", nil)
			},
		},
		{
			name:  "stream_get",
			setup: func(t *testing.T, st *store.Store) { mustCreateStream(t, st, orders) },
			do: func(t *testing.T, c *http.Client) (int, []byte) {
				return doReq(t, c, http.MethodGet, "/v1/streams/orders", "", nil)
			},
		},
		{
			name:  "stream_update",
			setup: func(t *testing.T, st *store.Store) { mustCreateStream(t, st, orders) },
			do: func(t *testing.T, c *http.Client) (int, []byte) {
				return doReq(t, c, http.MethodPatch, "/v1/streams/orders", `{"max_msgs":1000}`, nil)
			},
		},
		{
			name:  "stream_delete",
			setup: func(t *testing.T, st *store.Store) { mustCreateStream(t, st, orders) },
			do: func(t *testing.T, c *http.Client) (int, []byte) {
				return doReq(t, c, http.MethodDelete, "/v1/streams/orders?confirm=orders", "", nil)
			},
		},
		{
			name:  "publish",
			setup: func(t *testing.T, st *store.Store) { mustCreateStream(t, st, orders) },
			do: func(t *testing.T, c *http.Client) (int, []byte) {
				return doReq(t, c, http.MethodPost, "/v1/streams/orders/messages?subject=orders.eu.created",
					"hello", map[string]string{
						"Messq-Msg-Id":           "ord-1",
						"Messq-Trace-Id":         "4bf92f3577b34da6a3ce929d0e0e4736",
						"Messq-Header-Tenant-Id": "acme",
					})
			},
		},
		{
			name: "publish_dedup",
			setup: func(t *testing.T, st *store.Store) {
				mustCreateStream(t, st, orders)
				mustPublish(t, st, "orders", "orders.eu.created", "hello", "ord-dup")
			},
			do: func(t *testing.T, c *http.Client) (int, []byte) {
				return doReq(t, c, http.MethodPost, "/v1/streams/orders/messages?subject=orders.eu.created",
					"hello", map[string]string{"Messq-Msg-Id": "ord-dup"})
			},
		},
		{
			name:  "publish_batch",
			setup: func(t *testing.T, st *store.Store) { mustCreateStream(t, st, orders) },
			do: func(t *testing.T, c *http.Client) (int, []byte) {
				return doReq(t, c, http.MethodPost, "/v1/streams/orders/messages:batch",
					"{\"subject\":\"orders.eu.created\",\"body_b64\":\"aGVsbG8=\",\"msg_id\":\"b1\"}\n"+
						"{\"subject\":\"orders.us.created\",\"body\":\"world\",\"headers\":{\"Tenant\":\"acme\"}}\n",
					map[string]string{"Content-Type": "application/x-ndjson"})
			},
		},
		{
			name: "messages_list",
			setup: func(t *testing.T, st *store.Store) {
				mustCreateStream(t, st, orders)
				mustPublish(t, st, "orders", "orders.eu.created", "one", "")
				mustPublish(t, st, "orders", "orders.us.created", "two", "")
			},
			do: func(t *testing.T, c *http.Client) (int, []byte) {
				return doReq(t, c, http.MethodGet, "/v1/streams/orders/messages", "", nil)
			},
		},
		{
			name: "peek_seq",
			setup: func(t *testing.T, st *store.Store) {
				mustCreateStream(t, st, orders)
				mustPublish(t, st, "orders", "orders.eu.created", "hello", "")
			},
			do: func(t *testing.T, c *http.Client) (int, []byte) {
				return doReq(t, c, http.MethodGet, "/v1/streams/orders/messages/1", "", nil)
			},
		},
		{
			name: "peek_data", raw: true,
			setup: func(t *testing.T, st *store.Store) {
				mustCreateStream(t, st, orders)
				mustPublish(t, st, "orders", "orders.eu.created", "hello", "")
			},
			do: func(t *testing.T, c *http.Client) (int, []byte) {
				return doReq(t, c, http.MethodGet, "/v1/streams/orders/messages/1/data", "", nil)
			},
		},
		{
			name: "peek_by_id",
			setup: func(t *testing.T, st *store.Store) {
				mustCreateStream(t, st, orders)
				peekByID = mustPublishAck(t, st, "orders", "orders.eu.created", "hello", "").ID
			},
			do: func(t *testing.T, c *http.Client) (int, []byte) {
				return doReq(t, c, http.MethodGet, "/v1/messages/"+peekByID, "", nil)
			},
		},

		// ---- every error code the §7 surface emits ----
		{
			name: "error_not_found",
			do: func(t *testing.T, c *http.Client) (int, []byte) {
				return doReq(t, c, http.MethodGet, "/v1/streams/missing", "", nil)
			},
		},
		{
			name: "error_stream_exists",
			setup: func(t *testing.T, st *store.Store) {
				cfg := queue.DefaultConfig("orders")
				cfg.MaxMsgSize = 1000
				mustCreateStream(t, st, cfg)
			},
			do: func(t *testing.T, c *http.Client) (int, []byte) {
				return doReq(t, c, http.MethodPost, "/v1/streams",
					`{"name":"orders","max_msg_size":2000}`, nil)
			},
		},
		{
			name: "error_reserved_name",
			do: func(t *testing.T, c *http.Client) (int, []byte) {
				return doReq(t, c, http.MethodPost, "/v1/streams", `{"name":"orders.dlq"}`, nil)
			},
		},
		{
			name: "error_bad_request",
			do: func(t *testing.T, c *http.Client) (int, []byte) {
				return doReq(t, c, http.MethodPost, "/v1/streams", `{"name":`, nil)
			},
		},
		{
			name:  "error_bad_subject",
			setup: func(t *testing.T, st *store.Store) { mustCreateStream(t, st, orders) },
			do: func(t *testing.T, c *http.Client) (int, []byte) {
				return doReq(t, c, http.MethodPost, "/v1/streams/orders/messages?subject=orders.*.created", "x", nil)
			},
		},
		{
			name:  "error_subject_mismatch",
			setup: func(t *testing.T, st *store.Store) { mustCreateStream(t, st, orders) },
			do: func(t *testing.T, c *http.Client) (int, []byte) {
				return doReq(t, c, http.MethodPost, "/v1/streams/orders/messages?subject=other.a", "x", nil)
			},
		},
		{
			name: "error_too_large",
			setup: func(t *testing.T, st *store.Store) {
				cfg := queue.DefaultConfig("orders")
				cfg.MaxMsgSize = 10
				mustCreateStream(t, st, cfg)
			},
			do: func(t *testing.T, c *http.Client) (int, []byte) {
				return doReq(t, c, http.MethodPost, "/v1/streams/orders/messages?subject=orders.a",
					strings.Repeat("x", 100), nil)
			},
		},
		{
			name:  "error_header_too_large",
			setup: func(t *testing.T, st *store.Store) { mustCreateStream(t, st, orders) },
			do: func(t *testing.T, c *http.Client) (int, []byte) {
				return doReq(t, c, http.MethodPost, "/v1/streams/orders/messages?subject=orders.eu.created",
					"x", map[string]string{"Messq-Header-Big": strings.Repeat("x", 2000)})
			},
		},
		{
			name:  "error_reserved_header",
			setup: func(t *testing.T, st *store.Store) { mustCreateStream(t, st, orders) },
			do: func(t *testing.T, c *http.Client) (int, []byte) {
				return doReq(t, c, http.MethodPost, "/v1/streams/orders/messages?subject=orders.eu.created",
					"x", map[string]string{"Messq-Header-Messq-Foo": "bar"})
			},
		},
		{
			name: "error_would_lose_data",
			setup: func(t *testing.T, st *store.Store) {
				mustCreateStream(t, st, orders)
				mustPublish(t, st, "orders", "orders.eu.created", "hello", "")
				mustPublish(t, st, "orders", "orders.eu.created", "hello", "")
				mustPublish(t, st, "orders", "orders.eu.created", "hello", "")
			},
			do: func(t *testing.T, c *http.Client) (int, []byte) {
				return doReq(t, c, http.MethodPatch, "/v1/streams/orders", `{"max_msgs":1}`, nil)
			},
		},
		{
			name:  "error_conflict",
			setup: func(t *testing.T, st *store.Store) { mustCreateStream(t, st, orders) },
			do: func(t *testing.T, c *http.Client) (int, []byte) {
				return doReq(t, c, http.MethodDelete, "/v1/streams/orders?confirm=wrong-name", "", nil)
			},
		},
		{
			name:  "error_not_found_never_published",
			setup: func(t *testing.T, st *store.Store) { mustCreateStream(t, st, orders) },
			do: func(t *testing.T, c *http.Client) (int, []byte) {
				return doReq(t, c, http.MethodGet, "/v1/streams/orders/messages/5", "", nil)
			},
		},
		{
			name:  "error_batch_subject_mismatch",
			setup: func(t *testing.T, st *store.Store) { mustCreateStream(t, st, orders) },
			do: func(t *testing.T, c *http.Client) (int, []byte) {
				return doReq(t, c, http.MethodPost, "/v1/streams/orders/messages:batch",
					"{\"subject\":\"orders.eu.created\",\"body\":\"ok\"}\n{\"subject\":\"other.a\",\"body\":\"bad\"}\n",
					map[string]string{"Content-Type": "application/x-ndjson"})
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			clk := clock.NewFake(time.UnixMilli(1_700_000_000_000))
			st := openTestStore(t, clk, store.DurabilityFull)
			srv := New(st, clk, discardLogger(), time.Minute, queue.DefaultLimits(), defaultMaxBatchBytes)
			client := newUnixClient(t, srv.Handler())
			if tc.setup != nil {
				tc.setup(t, st)
			}
			status, body := tc.do(t, client)

			got := goldenContent(status, body, tc.raw)
			path := filepath.Join("testdata", tc.name+".golden")
			if *updateGolden {
				if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
					t.Fatalf("create testdata dir: %v", err)
				}
				if err := os.WriteFile(path, got, 0o644); err != nil {
					t.Fatalf("write golden: %v", err)
				}
				return
			}
			want, err := os.ReadFile(path)
			if err != nil {
				t.Fatalf("read golden (run with -update-golden to create it): %v", err)
			}
			if diff := cmp.Diff(string(want), string(got)); diff != "" {
				t.Fatalf("%s differs from golden (-golden +live):\n%s", tc.name, diff)
			}
		})
	}
}

// newUnixClient serves handler over a real Unix socket inside an httptest server and returns
// an http.Client that dials that socket, so every request below exercises the full HTTP
// stack over the socket a production daemon would bind. The client is deliberately custom:
// httptest.Server.Client only rewrites TCP listeners, so a Unix socket needs its own dialer.
func newUnixClient(t *testing.T, handler http.Handler) *http.Client {
	t.Helper()
	ln, err := (&net.ListenConfig{}).Listen(context.Background(), "unix", filepath.Join(t.TempDir(), "messq.sock"))
	if err != nil {
		t.Fatalf("listen unix: %v", err)
	}
	ts := httptest.NewUnstartedServer(handler)
	if closeErr := ts.Listener.Close(); closeErr != nil {
		t.Logf("close default listener: %v", closeErr)
	}
	ts.Listener = ln
	ts.Start()
	t.Cleanup(ts.Close)
	return &http.Client{
		Transport: &http.Transport{
			DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
				return (&net.Dialer{}).DialContext(ctx, "unix", ln.Addr().String())
			},
		},
	}
}

// doReq issues one request over the client's Unix socket and returns the status and body.
func doReq(t *testing.T, c *http.Client, method, path, body string, headers map[string]string) (int, []byte) {
	t.Helper()
	var rd io.Reader
	if body != "" {
		rd = strings.NewReader(body)
	}
	req, err := http.NewRequestWithContext(context.Background(), method, "http://messq"+path, rd)
	if err != nil {
		t.Fatalf("build request: %v", err)
	}
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	resp, err := c.Do(req)
	if err != nil {
		t.Fatalf("%s %s: %v", method, path, err)
	}
	defer func() {
		if cerr := resp.Body.Close(); cerr != nil {
			t.Errorf("close response body: %v", cerr)
		}
	}()
	b, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read response body: %v", err)
	}
	return resp.StatusCode, b
}

// goldenContent renders the pinned form of one response: the status on the first line, then
// the body — JSON-normalised unless the case is a raw text/byte response.
func goldenContent(status int, body []byte, raw bool) []byte {
	head := strconv.Itoa(status) + "\n"
	if raw {
		return []byte(head + string(body))
	}
	return []byte(head + string(normalizeJSON(body)))
}

// normalizeJSON rewrites the volatile values of a response body to stable placeholders while
// preserving every field name, so a golden pins the shape (and a renamed field fails CI)
// without being disturbed by a fresh ULID, a minted trace id, a wall-clock timestamp, a db
// byte count or the build version. Numbers are decoded as json.Number so re-marshalling
// cannot float-round an integer.
func normalizeJSON(body []byte) []byte {
	dec := json.NewDecoder(bytes.NewReader(body))
	dec.UseNumber()
	var v any
	if err := dec.Decode(&v); err != nil {
		panic("normalizeJSON: not JSON: " + err.Error() + ": " + string(body))
	}
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false) // keep the <ulid>/<ts>/… placeholders readable, not \u003c…
	enc.SetIndent("", "  ")
	if err := enc.Encode(normalizeValue(v)); err != nil {
		panic("normalizeJSON: marshal: " + err.Error())
	}
	return buf.Bytes()
}

// normalizeValue is the recursive half of normalizeJSON. Field names are matched exactly; a
// user header can never collide because canonical MIME keys are title-case while these are
// snake_case.
func normalizeValue(v any) any {
	switch x := v.(type) {
	case map[string]any:
		m := make(map[string]any, len(x))
		for k, val := range x {
			switch k {
			case "published_at", "created_at", "uptime_ms":
				m[k] = "<ts>"
			case "db_bytes":
				m[k] = "<db-bytes>"
			case "version":
				m[k] = "<version>"
			case "id", "node_id":
				m[k] = "<ulid>"
			case "trace_id":
				m[k] = "<trace-id>"
			default:
				m[k] = normalizeValue(val)
			}
		}
		return m
	case []any:
		out := make([]any, len(x))
		for i, val := range x {
			out[i] = normalizeValue(val)
		}
		return out
	default:
		return x
	}
}

func mustCreateStream(t *testing.T, st *store.Store, cfg queue.StreamConfig) {
	t.Helper()
	if _, _, err := st.CreateStream(context.Background(), cfg, "golden"); err != nil {
		t.Fatalf("create %s: %v", cfg.Name, err)
	}
}

func mustPublish(t *testing.T, st *store.Store, stream, subject, body, msgID string) {
	t.Helper()
	if _, err := st.Publish(context.Background(), store.PublishCmd{
		Stream: stream,
		Req:    queue.PublishReq{Subject: subject, Body: []byte(body), MsgID: msgID},
	}); err != nil {
		t.Fatalf("publish %s/%s: %v", stream, subject, err)
	}
}

// mustPublishAck publishes and returns the receipt, so a case can read the freshly minted ULID.
func mustPublishAck(t *testing.T, st *store.Store, stream, subject, body, msgID string) store.Ack {
	t.Helper()
	ack, err := st.Publish(context.Background(), store.PublishCmd{
		Stream: stream,
		Req:    queue.PublishReq{Subject: subject, Body: []byte(body), MsgID: msgID},
	})
	if err != nil {
		t.Fatalf("publish %s/%s: %v", stream, subject, err)
	}
	return ack
}

// TestWireCodeEnum pins the closed §7 error-code enum (issue #7 §7): every code the surface
// can emit maps to its wire code and HTTP status. The socket goldens above cover the eleven
// codes a request can reach directly; the three that only a store fault or a shutdown race
// can trigger — read_only, shutting_down, internal — are pinned here, alongside the rest, so
// a mapping regression fails CI by name. (#14 later closes the enum over the whole sentinel
// set; this test covers exactly the codes this issue defines.)
func TestWireCodeEnum(t *testing.T) {
	cases := []struct {
		name   string
		err    error
		code   string
		status int
	}{
		{"not_found", errs.E(errs.ErrNotFound, "t", "nope"), "not_found", http.StatusNotFound},
		{"stream_exists", &store.StreamExistsError{Name: "orders", Diff: []string{"max_msg_size"}}, "stream_exists", http.StatusConflict},
		{"reserved_name", queue.ErrReservedName, "reserved_name", http.StatusBadRequest},
		{"bad_request", errs.E(errs.ErrBadRequest, "t", "bad"), "bad_request", http.StatusBadRequest},
		{"bad_subject", errs.E(errs.ErrBadSubject, "t", "bad subject"), "bad_subject", http.StatusBadRequest},
		{"subject_mismatch", &queue.MismatchError{Subject: "x", Accepted: []string{"orders.>"}}, "subject_mismatch", http.StatusBadRequest},
		{"too_large", &queue.TooLargeError{What: "body", Size: 10, Limit: 1}, "too_large", http.StatusRequestEntityTooLarge},
		{"header_too_large", &queue.TooLargeError{What: "headers", Size: 10, Limit: 1}, "header_too_large", http.StatusBadRequest},
		{"reserved_header", &queue.ReservedHeaderError{Key: "Messq-Foo"}, "reserved_header", http.StatusBadRequest},
		{"would_lose_data", &queue.WouldLoseDataError{Field: "max_msgs", AtRiskMsgs: 1, AtRiskBytes: 2}, "would_lose_data", http.StatusConflict},
		{"conflict", errs.E(errs.ErrConflict, "t", "conflict"), "conflict", http.StatusConflict},
		{"read_only", errs.E(errs.ErrReadOnly, "t", "ro"), "read_only", http.StatusServiceUnavailable},
		{"shutting_down", errs.E(errs.ErrShuttingDown, "t", "closing"), "shutting_down", http.StatusServiceUnavailable},
		{"internal", errors.New("boom"), "internal", http.StatusInternalServerError},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := wireCode(tc.err); got != tc.code {
				t.Errorf("wireCode(%v) = %q, want %q", tc.err, got, tc.code)
			}
			if got := statusFor(tc.code); got != tc.status {
				t.Errorf("statusFor(%q) = %d, want %d", tc.code, got, tc.status)
			}
		})
	}
}
