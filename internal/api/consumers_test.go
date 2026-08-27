// SPDX-License-Identifier: Apache-2.0

package api

import (
	"context"
	"encoding/json"
	"net/http"
	"testing"

	"github.com/a-holm/messq/internal/queue"
	"github.com/a-holm/messq/internal/store"
)

func newConsumersServer(t *testing.T) (*store.Store, *Server) {
	t.Helper()
	st, srv := newStreamsServer(t)
	if _, _, err := st.CreateStream(context.Background(), queue.DefaultConfig("orders"), "test"); err != nil {
		t.Fatalf("create stream: %v", err)
	}
	return st, srv
}

func decodeConsumer(t *testing.T, body []byte) store.ConsumerInfo {
	t.Helper()
	var info store.ConsumerInfo
	if err := json.Unmarshal(body, &info); err != nil {
		t.Fatalf("decode consumer: %v (%s)", err, body)
	}
	return info
}

func TestCreateConsumerRoute(t *testing.T) {
	st, srv := newConsumersServer(t)
	_ = st
	h := srv.Handler()

	rec := doJSON(t, h, "POST", "/v1/streams/orders/consumers", `{"name":"worker","start":"new"}`)
	if rec.Code != http.StatusCreated {
		t.Fatalf("create status = %d, want 201 (%s)", rec.Code, rec.Body)
	}
	info := decodeConsumer(t, rec.Body.Bytes())
	if info.Stream != "orders" || info.Name != "worker" {
		t.Fatalf("info = %+v, want orders/worker", info)
	}
	if info.DeadPolicy != "dlq" || info.AckWaitMS != 30000 || info.MaxDeliver != 5 {
		t.Fatalf("defaults = %+v, want dlq/30000/5", info)
	}

	// Idempotent re-create → 200 changed:false, no second event.
	rec = doJSON(t, h, "POST", "/v1/streams/orders/consumers", `{"name":"worker","start":"new"}`)
	if rec.Code != http.StatusOK {
		t.Fatalf("idempotent status = %d, want 200 (%s)", rec.Code, rec.Body)
	}

	// Differing config on a taken name → 409 consumer_exists (#15 §6): a create that
	// silently rewrites someone's limits is a lost update, not idempotency. The
	// sparse PATCH route owns changes.
	rec = doJSON(t, h, "POST", "/v1/streams/orders/consumers", `{"name":"worker","start":"new","ack_wait_ms":60000}`)
	if rec.Code != http.StatusConflict {
		t.Fatalf("different-config status = %d, want 409 (%s)", rec.Code, rec.Body)
	}
	var env Envelope
	if err := json.Unmarshal(rec.Body.Bytes(), &env); err != nil {
		t.Fatalf("not an envelope: %v", err)
	}
	if env.Error.Code != "consumer_exists" {
		t.Fatalf("code = %q, want consumer_exists", env.Error.Code)
	}

	// The sparse PATCH applies the same change and it lands.
	patchRec := doJSON(t, h, "PATCH", "/v1/streams/orders/consumers/worker", `{"ack_wait_ms":60000}`)
	if patchRec.Code != http.StatusOK {
		t.Fatalf("patch status = %d, want 200 (%s)", patchRec.Code, patchRec.Body)
	}
	info = decodeConsumer(t, patchRec.Body.Bytes())
	if info.AckWaitMS != 60000 {
		t.Fatalf("ack_wait_ms after PATCH = %d, want 60000", info.AckWaitMS)
	}

	// Differing start on an otherwise-different document → 409 consumer_exists: the
	// declarative doc does not match, and the next hints at the PATCH/seek pair. (An
	// identical-config POST with a different start still reaches the store's
	// immutable_field check — the seek-only cursor move stays its own refusal.)
	rec = doJSON(t, h, "POST", "/v1/streams/orders/consumers", `{"name":"worker","start":"seq:3"}`)
	if rec.Code != http.StatusConflict {
		t.Fatalf("differing start status = %d, want 409 (%s)", rec.Code, rec.Body)
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &env); err != nil {
		t.Fatalf("decode error: %v", err)
	}
	if env.Error.Code != "consumer_exists" {
		t.Fatalf("code = %q, want consumer_exists", env.Error.Code)
	}
}

func TestCreateConsumerIdenticalExceptStartIsImmutableField(t *testing.T) {
	st, srv := newConsumersServer(t)
	h := srv.Handler()

	// Create with explicit ack_wait so a later identical body is achievable.
	rec := doJSON(t, h, "POST", "/v1/streams/orders/consumers",
		`{"name":"mover","start":"new","ack_wait_ms":45000}`)
	if rec.Code != http.StatusCreated {
		t.Fatalf("create status = %d (%s)", rec.Code, rec.Body)
	}
	_ = st

	// Identical config, different start: configs compare equal, so the request
	// reaches the store command, which refuses the cursor move as immutable.
	rec2 := doJSON(t, h, "POST", "/v1/streams/orders/consumers",
		`{"name":"mover","start":"seq:9","ack_wait_ms":45000}`)
	if rec2.Code != http.StatusConflict {
		t.Fatalf("status = %d, want 409 (%s)", rec2.Code, rec2.Body)
	}
	var env Envelope
	if err := json.Unmarshal(rec2.Body.Bytes(), &env); err != nil {
		t.Fatalf("decode error: %v", err)
	}
	if env.Error.Code != "immutable_field" {
		t.Fatalf("code = %q, want immutable_field", env.Error.Code)
	}
}

func TestCreateConsumerRejectsUnsupported(t *testing.T) {
	_, srv := newConsumersServer(t)
	h := srv.Handler()
	for _, feature := range []string{"ordered", "max_rate"} {
		body := `{"name":"worker","start":"new","` + feature + `":true}`
		if feature == "max_rate" {
			body = `{"name":"worker","start":"new","max_rate":100}`
		}
		rec := doJSON(t, h, "POST", "/v1/streams/orders/consumers", body)
		if rec.Code != http.StatusBadRequest {
			t.Fatalf("%s status = %d, want 400 (%s)", feature, rec.Code, rec.Body)
		}
		var env Envelope
		if err := json.Unmarshal(rec.Body.Bytes(), &env); err != nil {
			t.Fatalf("decode: %v", err)
		}
		if env.Error.Code != "unsupported" {
			t.Fatalf("%s code = %q, want unsupported", feature, env.Error.Code)
		}
	}
}

func TestConsumerCRUDRoutes(t *testing.T) {
	st, srv := newConsumersServer(t)
	h := srv.Handler()
	ctx := context.Background()

	if _, err := st.Publish(ctx, store.PublishCmd{Stream: "orders", Req: queue.PublishReq{Subject: "orders.1", Body: []byte("x")}}); err != nil {
		t.Fatalf("publish: %v", err)
	}

	rec := doJSON(t, h, "POST", "/v1/streams/orders/consumers", `{"name":"worker","start":"first","filters":[">"]}`)
	if rec.Code != http.StatusCreated {
		t.Fatalf("create: %d (%s)", rec.Code, rec.Body)
	}

	// List.
	rec = doJSON(t, h, "GET", "/v1/streams/orders/consumers", "")
	if rec.Code != http.StatusOK {
		t.Fatalf("list: %d (%s)", rec.Code, rec.Body)
	}
	var list []store.ConsumerInfo
	if err := json.Unmarshal(rec.Body.Bytes(), &list); err != nil || len(list) != 1 {
		t.Fatalf("list = %v / %d, want 1 consumer", err, len(list))
	}

	// Get single.
	rec = doJSON(t, h, "GET", "/v1/streams/orders/consumers/worker", "")
	if rec.Code != http.StatusOK {
		t.Fatalf("get: %d (%s)", rec.Code, rec.Body)
	}

	// PATCH.
	rec = doJSON(t, h, "PATCH", "/v1/streams/orders/consumers/worker", `{"max_deliver":7}`)
	if rec.Code != http.StatusOK {
		t.Fatalf("patch: %d (%s)", rec.Code, rec.Body)
	}
	if info := decodeConsumer(t, rec.Body.Bytes()); info.MaxDeliver != 7 {
		t.Fatalf("max_deliver = %d, want 7", info.MaxDeliver)
	}

	// Fetch: single message.
	rec = doJSON(t, h, "POST", "/v1/streams/orders/consumers/worker/fetch", `{"batch":10}`)
	if rec.Code != http.StatusOK {
		t.Fatalf("fetch: %d (%s)", rec.Code, rec.Body)
	}
	var fr store.FetchResult
	if err := json.Unmarshal(rec.Body.Bytes(), &fr); err != nil {
		t.Fatalf("decode fetch: %v", err)
	}
	if len(fr.Messages) != 1 || fr.Messages[0].Seq != 1 {
		t.Fatalf("fetch messages = %+v, want one seq 1", fr.Messages)
	}

	// DELETE without confirm → 409.
	rec = doJSON(t, h, "DELETE", "/v1/streams/orders/consumers/worker", "")
	if rec.Code != http.StatusConflict {
		t.Fatalf("delete no-confirm: %d (%s)", rec.Code, rec.Body)
	}
	// DELETE with confirm → 200.
	rec = doJSON(t, h, "DELETE", "/v1/streams/orders/consumers/worker?confirm=worker", "")
	if rec.Code != http.StatusOK {
		t.Fatalf("delete: %d (%s)", rec.Code, rec.Body)
	}
}
