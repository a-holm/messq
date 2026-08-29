// SPDX-License-Identifier: Apache-2.0

package api

import (
	"encoding/json"
	"net/http"
	"testing"
	"time"

	"github.com/a-holm/messq/internal/clock"
	"github.com/a-holm/messq/internal/store"
)

// newDevServer opens a store on a fake clock with Dev mode on — the serve --dev
// daemon in a box (issue #26 §2).
func newDevServer(t *testing.T) (*store.Store, *Server) {
	t.Helper()
	clk := clock.NewFake(time.UnixMilli(1_700_000_000_000))
	st := openTestStore(t, clk, store.DurabilityRelaxed)
	return st, New(Config{
		Store:  st,
		Clock:  clk,
		Logger: discardLogger(),
		Dev:    true,
	})
}

// TestDevAutoCreatesStreamOnPublish pins the auto-create half of --dev: a
// publish to a stream nobody created succeeds, the stream carries the wildcard
// subject set, and the creating actor is dev-autocreate so messq events shows
// exactly how the state appeared.
func TestDevAutoCreatesStreamOnPublish(t *testing.T) {
	st, srv := newDevServer(t)

	rec := doJSON(t, srv.Handler(), http.MethodPost, "/v1/streams/orders/messages?subject=orders.created", "hello")
	if rec.Code != http.StatusOK && rec.Code != http.StatusCreated {
		t.Fatalf("dev publish to uncreated stream: status %d, body %s", rec.Code, rec.Body)
	}
	info, err := st.GetStream(t.Context(), "orders")
	if err != nil {
		t.Fatalf("stream was not auto-created: %v", err)
	}
	if len(info.Subjects) != 1 || info.Subjects[0] != ">" {
		t.Errorf("auto-created stream subjects = %v, want [\">\"]", info.Subjects)
	}
}

// TestDevAutoCreateCollapsesToOneEvent pins the concurrent-publish guarantee:
// the second publish to a just-created stream does not write a second
// stream.create event (ON CONFLICT DO NOTHING, one event).
func TestDevAutoCreateCollapsesToOneEvent(t *testing.T) {
	st, srv := newDevServer(t)
	h := srv.Handler()

	for range 3 {
		if rec := doJSON(t, h, http.MethodPost, "/v1/streams/orders/messages?subject=orders.created", "x"); rec.Code >= 300 {
			t.Fatalf("publish: status %d", rec.Code)
		}
	}
	events, err := st.Events(t.Context(), store.EventFilter{Stream: "orders"})
	if err != nil {
		t.Fatalf("read events: %v", err)
	}
	creates := 0
	for _, e := range events.Events {
		if e.Event == "stream.create" {
			creates++
		}
	}
	if creates != 1 {
		t.Errorf("stream.create events = %d, want exactly 1 (auto-create must collapse)", creates)
	}
}

// TestNoAutoCreateWithoutDev pins the negative: without --dev the same publish
// stays a 404 — auto-create is dev-only behaviour, never a default.
func TestNoAutoCreateWithoutDev(t *testing.T) {
	_, srv := newStreamsServer(t)
	rec := doJSON(t, srv.Handler(), http.MethodPost, "/v1/streams/orders/messages", "hello")
	if rec.Code != http.StatusNotFound {
		t.Fatalf("publish without --dev: status %d, want 404 (body %s)", rec.Code, rec.Body)
	}
}

// TestDevAutoCreatesConsumerOnFetch pins the consumer half: a fetch against a
// consumer nobody created auto-creates it with schema defaults.
func TestDevAutoCreatesConsumerOnFetch(t *testing.T) {
	st, srv := newDevServer(t)
	h := srv.Handler()

	// The stream must exist first (dev auto-create makes that one line).
	if rec := doJSON(t, h, http.MethodPost, "/v1/streams/orders/messages?subject=orders.created", "m"); rec.Code >= 300 {
		t.Fatalf("seed publish: status %d", rec.Code)
	}
	body := `{"wait_ms":0}`
	if rec := doJSON(t, h, http.MethodPost, "/v1/streams/orders/consumers/worker/fetch", body); rec.Code >= 300 {
		t.Fatalf("dev fetch from uncreated consumer: status %d, body %s", rec.Code, rec.Body)
	}
	if _, err := st.GetConsumer(t.Context(), "orders", "worker"); err != nil {
		t.Fatalf("consumer was not auto-created: %v", err)
	}
}

// TestInfoCarriesDev pins the additive self-report: "dev":true on a dev daemon
// and "dev":false otherwise — the flag doctor (#30) keys off.
func TestInfoCarriesDev(t *testing.T) {
	_, dev := newDevServer(t)
	_, plain := newStreamsServer(t)

	if rec := doJSON(t, dev.Handler(), http.MethodGet, "/v1/info", ""); rec.Code != 200 {
		t.Fatalf("dev info: status %d", rec.Code)
	} else if body := rec.Body.String(); !containsJSONBool(body, "dev", true) {
		t.Errorf("dev /v1/info is missing \"dev\":true: %s", body)
	}
	if rec := doJSON(t, plain.Handler(), http.MethodGet, "/v1/info", ""); rec.Code != 200 {
		t.Fatalf("info: status %d", rec.Code)
	} else if body := rec.Body.String(); !containsJSONBool(body, "dev", false) {
		t.Errorf("plain /v1/info is missing \"dev\":false: %s", body)
	}
}

// containsJSONBool reports whether body carries the top-level bool under key.
func containsJSONBool(body, key string, want bool) bool {
	m := map[string]any{}
	if err := json.Unmarshal([]byte(body), &m); err != nil {
		return false
	}
	got, ok := m[key].(bool)
	return ok && got == want
}
