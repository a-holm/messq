// SPDX-License-Identifier: Apache-2.0

package client

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
)

// TestAdminWrappersMatchRegistry drives every control-plane wrapper against a
// recording server and asserts the method/path/query of the route it must hit
// (internal/api's registry is the source of truth; #18's gates pin the other side).
func TestAdminWrappersMatchRegistry(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name       string
		call       func(c *Client) error
		wantMethod string
		wantPath   string
		wantQuery  string
	}{
		{
			name:       "healthz",
			call:       func(c *Client) error { return c.Healthz(context.Background()) },
			wantMethod: http.MethodGet, wantPath: "/healthz",
		},
		{
			name:       "info",
			call:       func(c *Client) error { _, err := c.Info(context.Background()); return err },
			wantMethod: http.MethodGet, wantPath: "/v1/info",
		},
		{
			name: "create_stream",
			call: func(c *Client) error {
				_, err := c.CreateStream(context.Background(), StreamConfig{Name: "orders"})
				return err
			},
			wantMethod: http.MethodPost, wantPath: "/v1/streams",
		},
		{
			name:       "list_streams",
			call:       func(c *Client) error { _, err := c.ListStreams(context.Background()); return err },
			wantMethod: http.MethodGet, wantPath: "/v1/streams",
		},
		{
			name:       "get_stream",
			call:       func(c *Client) error { _, err := c.GetStream(context.Background(), "orders"); return err },
			wantMethod: http.MethodGet, wantPath: "/v1/streams/orders",
		},
		{
			name: "update_stream",
			call: func(c *Client) error {
				_, err := c.UpdateStream(context.Background(), "orders", StreamPatch{})
				return err
			},
			wantMethod: http.MethodPatch, wantPath: "/v1/streams/orders",
		},
		{
			name: "delete_stream",
			call: func(c *Client) error {
				_, err := c.DeleteStream(context.Background(), "orders", Confirm("orders"))
				return err
			},
			wantMethod: http.MethodDelete, wantPath: "/v1/streams/orders", wantQuery: "confirm=orders",
		},
		{
			name: "delete_stream_dry_run",
			call: func(c *Client) error {
				_, err := c.DeleteStream(context.Background(), "orders", Confirm("orders"), DryRun())
				return err
			},
			wantMethod: http.MethodDelete, wantPath: "/v1/streams/orders", wantQuery: "confirm=orders&dry_run=1",
		},
		{
			name: "create_consumer",
			call: func(c *Client) error {
				_, err := c.CreateConsumer(context.Background(), "orders", ConsumerConfig{Name: "w"})
				return err
			},
			wantMethod: http.MethodPost, wantPath: "/v1/streams/orders/consumers",
		},
		{
			name: "list_consumers",
			call: func(c *Client) error {
				_, err := c.ListConsumers(context.Background(), "orders")
				return err
			},
			wantMethod: http.MethodGet, wantPath: "/v1/streams/orders/consumers",
		},
		{
			name: "get_consumer",
			call: func(c *Client) error {
				_, err := c.GetConsumer(context.Background(), "orders", "w")
				return err
			},
			wantMethod: http.MethodGet, wantPath: "/v1/streams/orders/consumers/w",
		},
		{
			name: "update_consumer",
			call: func(c *Client) error {
				_, err := c.UpdateConsumer(context.Background(), "orders", "w", ConsumerPatch{})
				return err
			},
			wantMethod: http.MethodPatch, wantPath: "/v1/streams/orders/consumers/w",
		},
		{
			name: "delete_consumer",
			call: func(c *Client) error {
				_, err := c.DeleteConsumer(context.Background(), "orders", "w")
				return err
			},
			wantMethod: http.MethodDelete, wantPath: "/v1/streams/orders/consumers/w",
		},
		{
			name: "list_messages",
			call: func(c *Client) error {
				_, err := c.ListMessages(context.Background(), "orders", ListOptions{Limit: 5})
				return err
			},
			wantMethod: http.MethodGet, wantPath: "/v1/streams/orders/messages", wantQuery: "limit=5",
		},
		{
			name: "peek_message",
			call: func(c *Client) error {
				_, err := c.PeekMessage(context.Background(), "orders", 7)
				return err
			},
			wantMethod: http.MethodGet, wantPath: "/v1/streams/orders/messages/7",
		},
		{
			name: "peek_message_data",
			call: func(c *Client) error {
				_, err := c.PeekMessageData(context.Background(), "orders", 7)
				return err
			},
			wantMethod: http.MethodGet, wantPath: "/v1/streams/orders/messages/7/data",
		},
		{
			name: "peek_by_id",
			call: func(c *Client) error {
				_, err := c.PeekByID(context.Background(), "01JACK")
				return err
			},
			wantMethod: http.MethodGet, wantPath: "/v1/messages/01JACK",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			var gotMethod, gotPath, gotQuery string
			ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				gotMethod, gotPath, gotQuery = r.Method, r.URL.Path, r.URL.RawQuery
				if r.Body != nil {
					_, _ = io.Copy(io.Discard, r.Body)
					_ = r.Body.Close()
				}
				w.Header().Set("Content-Type", "application/json")
				switch tc.name {
				case "healthz":
					_, _ = io.WriteString(w, "ok\n")
				case "info":
					_, _ = w.Write([]byte(`{"version":"test","uptime_ms":1,"durability":"safe","synchronous":2,"db_bytes":3,"node_id":"n"}`))
				case "peek_message_data":
					w.Header().Set("Content-Type", "application/octet-stream")
					_, _ = w.Write([]byte("raw-bytes"))
				case "list_streams", "list_consumers":
					_, _ = w.Write([]byte(`[]`))
				case "list_messages":
					_, _ = w.Write([]byte(`{"messages":[],"complete":true,"scanned_to_seq":0,"limit":5}`))
				default:
					_, _ = w.Write([]byte(`{}`))
				}
			}))
			defer ts.Close()

			c := newTestClient(t, ts)
			err := tc.call(c)
			if err != nil && !errors.Is(err, context.Canceled) {
				t.Fatalf("call failed: %v", err)
			}
			if gotMethod != tc.wantMethod || gotPath != tc.wantPath {
				t.Errorf("request = %s %s, want %s %s", gotMethod, gotPath, tc.wantMethod, tc.wantPath)
			}
			if gotQuery != tc.wantQuery {
				t.Errorf("query = %q, want %q", gotQuery, tc.wantQuery)
			}
		})
	}
}

func TestPeekMessageDataReturnsRawBytes(t *testing.T) {
	t.Parallel()

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/octet-stream")
		_, _ = w.Write([]byte{0x00, 0x01, 0xff})
	}))
	defer ts.Close()

	c := newTestClient(t, ts)
	data, err := c.PeekMessageData(context.Background(), "s", 1)
	if err != nil {
		t.Fatalf("PeekMessageData: %v", err)
	}
	if len(data) != 3 || data[2] != 0xff {
		t.Errorf("data = %x, want raw bytes un-decoded", data)
	}
}

func TestHealthzAcceptsPlainTextOK(t *testing.T) {
	t.Parallel()

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = io.WriteString(w, "ok\n") // plain text by design, not an envelope
	}))
	defer ts.Close()

	c := newTestClient(t, ts)
	if err := c.Healthz(context.Background()); err != nil {
		t.Errorf("Healthz: %v", err)
	}
}

func TestAdminBadNamesRefusedLocally(t *testing.T) {
	t.Parallel()

	var requests int
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		requests++
	}))
	defer ts.Close()

	c := newTestClient(t, ts)
	ctx := context.Background()
	for name, call := range map[string]func() error{
		"empty stream":     func() error { _, err := c.GetStream(ctx, ""); return err },
		"bad stream chars": func() error { _, err := c.GetStream(ctx, "a/b"); return err },
		"empty consumer":   func() error { _, err := c.GetConsumer(ctx, "orders", ""); return err },
		"new stream dot":   func() error { _, err := c.CreateStream(ctx, StreamConfig{Name: ".lead"}); return err },
	} {
		if err := call(); !errors.Is(err, ErrBadRequest) {
			t.Errorf("%s: err = %v, want ErrBadRequest", name, err)
		}
	}
	if requests != 0 {
		t.Errorf("%d round trips happened for locally refused names", requests)
	}
}

func TestDeleteStreamWithoutConfirmIsLocalError(t *testing.T) {
	t.Parallel()

	var requests int
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		requests++
	}))
	defer ts.Close()

	c := newTestClient(t, ts)
	if _, err := c.DeleteStream(context.Background(), "orders"); !errors.Is(err, ErrBadRequest) {
		t.Fatalf("err = %v, want a local refusal — the client never invents a confirmation", err)
	}
	if requests != 0 {
		t.Error("an unconfirmed delete reached the wire")
	}
}
