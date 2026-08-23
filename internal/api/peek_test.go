// SPDX-License-Identifier: Apache-2.0

package api

import (
	"encoding/json"
	"errors"
	"net/http"
	"slices"
	"strings"
	"testing"

	"github.com/a-holm/messq/internal/errs"
	"github.com/a-holm/messq/internal/store"
)

// publishSeq publishes three messages with distinct subjects and returns nothing; it is
// the shared fixture for the listing tests.
func publishSeq(t *testing.T, st *store.Store, stream string, subjects ...string) {
	t.Helper()
	for _, subj := range subjects {
		publishForTest(t, st, stream, subj, "payload")
	}
}

func TestListMessages(t *testing.T) {
	st, srv := newStreamsServer(t)
	createStreamWith(t, st, ordersSubjectCfg())
	publishSeq(t, st, "orders", "orders.eu.created", "orders.us.created", "orders.eu.updated")

	rec := doJSON(t, srv.Handler(), http.MethodGet, "/v1/streams/orders/messages", "")
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200 (body %q)", rec.Code, rec.Body.String())
	}

	var page store.Page
	if err := json.Unmarshal(rec.Body.Bytes(), &page); err != nil {
		t.Fatalf("decode page: %v (%s)", err, rec.Body.String())
	}
	if len(page.Messages) != 3 {
		t.Fatalf("messages = %d, want 3", len(page.Messages))
	}
	if !page.Complete {
		t.Errorf("complete = %v, want true for a fully-scanned page", page.Complete)
	}
	if page.Limit != 1000 {
		t.Errorf("limit = %d, want the clamped default 1000", page.Limit)
	}
	if page.Messages[0].Seq != 1 || page.Messages[2].Seq != 3 {
		t.Errorf("asc seqs = %d..%d, want 1..3", page.Messages[0].Seq, page.Messages[2].Seq)
	}
}

func TestListMessagesLimitEcho(t *testing.T) {
	st, srv := newStreamsServer(t)
	createStreamWith(t, st, ordersSubjectCfg())
	publishSeq(t, st, "orders", "orders.a", "orders.b", "orders.c")

	rec := doJSON(t, srv.Handler(), http.MethodGet, "/v1/streams/orders/messages?limit=1", "")
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200 (body %q)", rec.Code, rec.Body.String())
	}
	var page store.Page
	if err := json.Unmarshal(rec.Body.Bytes(), &page); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if page.Limit != 1 {
		t.Errorf("limit = %d, want 1 (echoed, not clamped up)", page.Limit)
	}
	if len(page.Messages) != 1 {
		t.Errorf("messages = %d, want 1", len(page.Messages))
	}
}

func TestListMessagesIncludeBodyTightensLimit(t *testing.T) {
	st, srv := newStreamsServer(t)
	createStreamWith(t, st, ordersSubjectCfg())
	publishSeq(t, st, "orders", "orders.a", "orders.b")

	rec := doJSON(t, srv.Handler(), http.MethodGet, "/v1/streams/orders/messages?include_body=1", "")
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200 (body %q)", rec.Code, rec.Body.String())
	}
	var page store.Page
	if err := json.Unmarshal(rec.Body.Bytes(), &page); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if page.Limit != 100 {
		t.Errorf("limit = %d, want 100 (peek-max/10 with bodies)", page.Limit)
	}
}

func TestListMessagesSubjectFilter(t *testing.T) {
	st, srv := newStreamsServer(t)
	createStreamWith(t, st, ordersSubjectCfg())
	publishSeq(t, st, "orders", "orders.eu.created", "orders.us.created")

	rec := doJSON(t, srv.Handler(), http.MethodGet, "/v1/streams/orders/messages?subject=orders.eu.created", "")
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200 (body %q)", rec.Code, rec.Body.String())
	}
	var page store.Page
	if err := json.Unmarshal(rec.Body.Bytes(), &page); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(page.Messages) != 1 {
		t.Fatalf("messages = %d, want 1", len(page.Messages))
	}
	if page.Messages[0].Subject != "orders.eu.created" {
		t.Errorf("subject = %q, want orders.eu.created", page.Messages[0].Subject)
	}
}

func TestListMessagesOrderDesc(t *testing.T) {
	st, srv := newStreamsServer(t)
	createStreamWith(t, st, ordersSubjectCfg())
	publishSeq(t, st, "orders", "orders.a", "orders.b", "orders.c")

	rec := doJSON(t, srv.Handler(), http.MethodGet, "/v1/streams/orders/messages?order=desc", "")
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200 (body %q)", rec.Code, rec.Body.String())
	}
	var page store.Page
	if err := json.Unmarshal(rec.Body.Bytes(), &page); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(page.Messages) != 3 {
		t.Fatalf("messages = %d, want 3", len(page.Messages))
	}
	if page.Messages[0].Seq != 3 {
		t.Errorf("first seq = %d, want 3 (desc from head)", page.Messages[0].Seq)
	}
}

func TestListMessagesBadOrder(t *testing.T) {
	st, srv := newStreamsServer(t)
	createStreamWith(t, st, ordersSubjectCfg())

	rec := doJSON(t, srv.Handler(), http.MethodGet, "/v1/streams/orders/messages?order=sideways", "")
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400 (body %q)", rec.Code, rec.Body.String())
	}
	if env := decodeError(t, rec); env.Code != "bad_request" {
		t.Errorf("code = %q, want bad_request", env.Code)
	}
}

var messageJSONKeys = []string{
	"id", "published_at", "seq", "size", "stream", "subject", "trace_id",
}

func TestPeekMessage(t *testing.T) {
	st, srv := newStreamsServer(t)
	createStreamWith(t, st, ordersSubjectCfg())
	ack := publishForTest(t, st, "orders", "orders.eu.created", "hello")

	rec := doJSON(t, srv.Handler(), http.MethodGet, "/v1/streams/orders/messages/1", "")
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200 (body %q)", rec.Code, rec.Body.String())
	}
	if got := jsonKeys(t, rec.Body.Bytes()); !slices.Equal(got, messageJSONKeys) {
		t.Errorf("peek JSON keys = %v, want %v", got, messageJSONKeys)
	}

	var msg store.Message
	if err := json.Unmarshal(rec.Body.Bytes(), &msg); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if msg.ID != ack.ID || msg.Seq != 1 || msg.Subject != "orders.eu.created" {
		t.Errorf("msg = %+v, want id %q seq 1 subject orders.eu.created", msg, ack.ID)
	}
}

func TestPeekMessageData(t *testing.T) {
	st, srv := newStreamsServer(t)
	createStreamWith(t, st, ordersSubjectCfg())
	publishForTest(t, st, "orders", "orders.eu.created", "hello")

	rec := doJSON(t, srv.Handler(), http.MethodGet, "/v1/streams/orders/messages/1/data", "")
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200 (body %q)", rec.Code, rec.Body.String())
	}
	if got := rec.Body.String(); got != "hello" {
		t.Errorf("body = %q, want hello (raw bytes)", got)
	}
	if ct := rec.Header().Get("Content-Type"); ct != "application/octet-stream" {
		t.Errorf("Content-Type = %q, want application/octet-stream", ct)
	}
}

func TestPeekMessageByID(t *testing.T) {
	st, srv := newStreamsServer(t)
	createStreamWith(t, st, ordersSubjectCfg())
	ack := publishForTest(t, st, "orders", "orders.eu.created", "hello")

	rec := doJSON(t, srv.Handler(), http.MethodGet, "/v1/messages/"+ack.ID, "")
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200 (body %q)", rec.Code, rec.Body.String())
	}
	var msg store.Message
	if err := json.Unmarshal(rec.Body.Bytes(), &msg); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if msg.Stream != "orders" || msg.Seq != 1 {
		t.Errorf("msg = %+v, want orders/1", msg)
	}
}

func TestPeekMessageNeverPublished(t *testing.T) {
	st, srv := newStreamsServer(t)
	createStreamWith(t, st, ordersSubjectCfg())

	rec := doJSON(t, srv.Handler(), http.MethodGet, "/v1/streams/orders/messages/5", "")
	if rec.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want 404 (body %q)", rec.Code, rec.Body.String())
	}
	env := decodeError(t, rec)
	if env.Code != "not_found" {
		t.Errorf("code = %q, want not_found", env.Code)
	}
	if !strings.Contains(env.Message, "never_published") {
		t.Errorf("message %q does not name never_published", env.Message)
	}
}

func TestPeekMessageMissingStream(t *testing.T) {
	_, srv := newStreamsServer(t)

	rec := doJSON(t, srv.Handler(), http.MethodGet, "/v1/streams/missing/messages/1", "")
	if rec.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want 404 (body %q)", rec.Code, rec.Body.String())
	}
	if env := decodeError(t, rec); env.Code != "not_found" {
		t.Errorf("code = %q, want not_found", env.Code)
	}
}

// TestPeekMissErrorMapping pins the reason→detail rendering for both missing-seq
// incidents: expired names first_seq, never_published names last_seq, and both carry the
// requested seq in the message.
func TestPeekMissErrorMapping(t *testing.T) {
	cases := []struct {
		name         string
		miss         *store.PeekMissError
		seq          int64
		wantReason   string
		wantBoundary string
	}{
		{"expired", &store.PeekMissError{Stream: "orders", Reason: "expired", Boundary: 3}, 2, "expired", "first_seq 3"},
		{"never_published", &store.PeekMissError{Stream: "orders", Reason: "never_published", Boundary: 2}, 5, "never_published", "last_seq 2"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := peekMissError(tc.miss, tc.seq)
			if !errors.Is(err, errs.ErrNotFound) {
				t.Fatalf("peekMissError = %v, want ErrNotFound", err)
			}
			msg := err.Error()
			if !strings.Contains(msg, tc.wantReason) {
				t.Errorf("message %q does not name %q", msg, tc.wantReason)
			}
			if !strings.Contains(msg, tc.wantBoundary) {
				t.Errorf("message %q does not carry %q", msg, tc.wantBoundary)
			}
		})
	}
}
