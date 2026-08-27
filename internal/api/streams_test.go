// SPDX-License-Identifier: Apache-2.0

package api

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/a-holm/messq/internal/clock"
	"github.com/a-holm/messq/internal/errs"
	"github.com/a-holm/messq/internal/queue"
	"github.com/a-holm/messq/internal/store"
)

// newStreamsServer opens a store on a fake clock and wraps it in a Server with the
// default process limits, which is what the test store validates against too.
func newStreamsServer(t *testing.T) (*store.Store, *Server) {
	t.Helper()
	clk := clock.NewFake(time.UnixMilli(1_700_000_000_000))
	st := openTestStore(t, clk, store.DurabilityRelaxed)
	return st, New(Config{
		Store:  st,
		Clock:  clk,
		Logger: discardLogger(),
	})
}

// doJSON issues one request against the handler and returns the recorder.
func doJSON(t *testing.T, h http.Handler, method, path, body string) *httptest.ResponseRecorder {
	t.Helper()
	var rd io.Reader
	if body != "" {
		rd = strings.NewReader(body)
	}
	req := httptest.NewRequestWithContext(context.Background(), method, path, rd)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec
}

// jsonKeys returns the sorted top-level keys of a JSON object body.
func jsonKeys(t *testing.T, body []byte) []string {
	t.Helper()
	var m map[string]json.RawMessage
	if err := json.Unmarshal(body, &m); err != nil {
		t.Fatalf("body is not a JSON object: %v (%s)", err, body)
	}
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	slices.Sort(keys)
	return keys
}

// errorKeys returns the sorted keys of the {"error":{...}} inner object.
func errorKeys(t *testing.T, body []byte) []string {
	t.Helper()
	var outer map[string]json.RawMessage
	if err := json.Unmarshal(body, &outer); err != nil {
		t.Fatalf("body is not a JSON object: %v (%s)", err, body)
	}
	raw, ok := outer["error"]
	if !ok {
		t.Fatalf("body has no \"error\" key: %s", body)
	}
	var inner map[string]json.RawMessage
	if err := json.Unmarshal(raw, &inner); err != nil {
		t.Fatalf("\"error\" is not a JSON object: %v", err)
	}
	keys := make([]string, 0, len(inner))
	for k := range inner {
		keys = append(keys, k)
	}
	slices.Sort(keys)
	return keys
}

// testErrorBody is the test's decode shape for the error envelope; it mirrors the wire
// contract without depending on the production struct.
type testErrorBody struct {
	Code    string   `json:"code"`
	Message string   `json:"message"`
	Next    []string `json:"next"`
	TraceID string   `json:"trace_id"`
}

type testErrorEnvelope struct {
	Error testErrorBody `json:"error"`
}

func decodeError(t *testing.T, rec *httptest.ResponseRecorder) testErrorBody {
	t.Helper()
	var env testErrorEnvelope
	if err := json.Unmarshal(rec.Body.Bytes(), &env); err != nil {
		t.Fatalf("body is not a valid error envelope: %v (%s)", err, rec.Body.String())
	}
	return env.Error
}

// streamInfoKeys is the frozen StreamInfo wire shape (issue §7), sorted.
var streamInfoKeys = []string{
	"bytes", "created_at", "dedup_window_ms", "discard", "first_seq", "last_seq",
	"max_age_ms", "max_bytes", "max_msg_size", "max_msgs", "msgs", "name",
	"retention", "subjects",
}

var errorEnvelopeKeys = []string{"code", "message", "next", "trace_id"}

// streamUpdateKeys is the PATCH response shape: the frozen StreamInfo keys plus the
// narrowed_msgs report, sorted.
var streamUpdateKeys = []string{
	"bytes", "created_at", "dedup_window_ms", "discard", "first_seq", "last_seq",
	"max_age_ms", "max_bytes", "max_msg_size", "max_msgs", "msgs", "name",
	"narrowed_msgs", "retention", "subjects",
}

// assertTraceID pins the trace_id contract: 32 lowercase hex bytes, never all zero.
func assertTraceID(t *testing.T, traceID string) {
	t.Helper()
	if len(traceID) != 32 {
		t.Errorf("trace_id = %q (len %d), want 32 lowercase hex chars", traceID, len(traceID))
		return
	}
	for i := range len(traceID) {
		c := traceID[i]
		if (c < '0' || c > '9') && (c < 'a' || c > 'f') {
			t.Errorf("trace_id %q holds non-hex byte %q", traceID, c)
			return
		}
	}
	if traceID == "00000000000000000000000000000000" {
		t.Errorf("trace_id is the W3C all-zero id")
	}
}

func createOrders(t *testing.T, st *store.Store) {
	t.Helper()
	if _, _, err := st.CreateStream(context.Background(), queue.DefaultConfig("orders"), "test"); err != nil {
		t.Fatalf("create orders: %v", err)
	}
}

func publishForTest(t *testing.T, st *store.Store, stream, subject, body string) store.Ack {
	t.Helper()
	ack, err := st.Publish(context.Background(), store.PublishCmd{
		Stream: stream,
		Req:    queue.PublishReq{Subject: subject, Body: []byte(body)},
	})
	if err != nil {
		t.Fatalf("publish %s/%s: %v", stream, subject, err)
	}
	return ack
}

func TestCreateStream(t *testing.T) {
	st, srv := newStreamsServer(t)

	rec := doJSON(t, srv.Handler(), http.MethodPost, "/v1/streams",
		`{"name":"orders","subjects":["orders.>"],"max_msg_size":262144,"dedup_window_ms":300000}`)
	if rec.Code != http.StatusCreated {
		t.Fatalf("status = %d, want 201 (body %q)", rec.Code, rec.Body.String())
	}

	if got := jsonKeys(t, rec.Body.Bytes()); !slices.Equal(got, streamInfoKeys) {
		t.Errorf("create JSON keys = %v, want %v", got, streamInfoKeys)
	}

	var info store.StreamInfo
	if err := json.Unmarshal(rec.Body.Bytes(), &info); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if info.Name != "orders" {
		t.Errorf("name = %q, want orders", info.Name)
	}
	if !slices.Equal(info.Subjects, []string{"orders.>"}) {
		t.Errorf("subjects = %v, want [orders.>]", info.Subjects)
	}
	if info.Retention != "limits" {
		t.Errorf("retention = %q, want limits", info.Retention)
	}
	if info.MaxMsgs != 0 || info.MaxBytes != 0 {
		t.Errorf("max_msgs/max_bytes = %d/%d, want 0/0 (unlimited)", info.MaxMsgs, info.MaxBytes)
	}
	if want := int64((7 * 24 * time.Hour) / time.Millisecond); info.MaxAgeMS != want {
		t.Errorf("max_age_ms = %d, want %d (7-day default)", info.MaxAgeMS, want)
	}
	if info.MaxMsgSize != 262144 {
		t.Errorf("max_msg_size = %d, want 262144", info.MaxMsgSize)
	}
	if info.Discard != "old" {
		t.Errorf("discard = %q, want old", info.Discard)
	}
	if info.DedupWindowMS != 300000 {
		t.Errorf("dedup_window_ms = %d, want 300000", info.DedupWindowMS)
	}
	if info.CreatedAt <= 0 {
		t.Errorf("created_at = %d, want > 0", info.CreatedAt)
	}
	if info.FirstSeq != 0 || info.LastSeq != 0 || info.Msgs != 0 || info.Bytes != 0 {
		t.Errorf("empty-stream stats = %d/%d/%d/%d, want 0/0/0/0",
			info.FirstSeq, info.LastSeq, info.Msgs, info.Bytes)
	}

	if _, err := st.GetStream(context.Background(), "orders"); err != nil {
		t.Fatalf("get stream after create: %v", err)
	}
}

func TestCreateStreamIdempotent(t *testing.T) {
	_, srv := newStreamsServer(t)
	body := `{"name":"orders","subjects":["orders.>"],"max_msg_size":262144,"dedup_window_ms":300000}`

	first := doJSON(t, srv.Handler(), http.MethodPost, "/v1/streams", body)
	if first.Code != http.StatusCreated {
		t.Fatalf("first create status = %d, want 201 (%q)", first.Code, first.Body.String())
	}

	second := doJSON(t, srv.Handler(), http.MethodPost, "/v1/streams", body)
	if second.Code != http.StatusOK {
		t.Fatalf("re-post status = %d, want 200 (body %q)", second.Code, second.Body.String())
	}

	var a, b store.StreamInfo
	if err := json.Unmarshal(first.Body.Bytes(), &a); err != nil {
		t.Fatalf("decode first: %v", err)
	}
	if err := json.Unmarshal(second.Body.Bytes(), &b); err != nil {
		t.Fatalf("decode second: %v", err)
	}
	if !reflect.DeepEqual(a, b) {
		t.Errorf("idempotent re-post returned a different stream:\n first=%+v\n second=%+v", a, b)
	}
}

func TestCreateStreamConflict(t *testing.T) {
	_, srv := newStreamsServer(t)
	if rec := doJSON(t, srv.Handler(), http.MethodPost, "/v1/streams",
		`{"name":"orders","max_msg_size":1000}`); rec.Code != http.StatusCreated {
		t.Fatalf("first create status = %d, want 201 (%q)", rec.Code, rec.Body.String())
	}

	rec := doJSON(t, srv.Handler(), http.MethodPost, "/v1/streams",
		`{"name":"orders","max_msg_size":2000}`)
	if rec.Code != http.StatusConflict {
		t.Fatalf("status = %d, want 409 (body %q)", rec.Code, rec.Body.String())
	}
	env := decodeError(t, rec)
	if env.Code != "stream_exists" {
		t.Errorf("code = %q, want stream_exists", env.Code)
	}
	if !strings.Contains(env.Message, "max_msg_size") {
		t.Errorf("message %q does not name the differing field", env.Message)
	}
	if !slices.Equal(env.Next, []string{"messq stream edit orders"}) {
		t.Errorf("next = %v, want [messq stream edit orders]", env.Next)
	}
	assertTraceID(t, env.TraceID)
	if got := errorKeys(t, rec.Body.Bytes()); !slices.Equal(got, errorEnvelopeKeys) {
		t.Errorf("error JSON keys = %v, want %v", got, errorEnvelopeKeys)
	}
}

func TestCreateStreamReservedName(t *testing.T) {
	_, srv := newStreamsServer(t)

	rec := doJSON(t, srv.Handler(), http.MethodPost, "/v1/streams", `{"name":"orders.dlq"}`)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400 (body %q)", rec.Code, rec.Body.String())
	}
	env := decodeError(t, rec)
	if env.Code != "reserved_name" {
		t.Errorf("code = %q, want reserved_name", env.Code)
	}
	assertTraceID(t, env.TraceID)
}

func TestCreateStreamBadRequest(t *testing.T) {
	for _, tc := range []struct {
		name string
		body string
	}{
		{"malformed JSON", `{"name":`},
		{"empty subjects", `{"name":"orders","subjects":[]}`},
		{"bad retention", `{"name":"orders","retention":"forever"}`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, srv := newStreamsServer(t)
			rec := doJSON(t, srv.Handler(), http.MethodPost, "/v1/streams", tc.body)
			if rec.Code != http.StatusBadRequest {
				t.Fatalf("status = %d, want 400 (body %q)", rec.Code, rec.Body.String())
			}
			env := decodeError(t, rec)
			if env.Code != "bad_request" {
				t.Errorf("code = %q, want bad_request", env.Code)
			}
			assertTraceID(t, env.TraceID)
		})
	}
}

func TestListStreams(t *testing.T) {
	st, srv := newStreamsServer(t)
	for _, name := range []string{"shipments", "orders"} {
		if _, _, err := st.CreateStream(context.Background(), queue.DefaultConfig(name), "test"); err != nil {
			t.Fatalf("create %s: %v", name, err)
		}
	}

	rec := doJSON(t, srv.Handler(), http.MethodGet, "/v1/streams", "")
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200 (body %q)", rec.Code, rec.Body.String())
	}
	var list []store.StreamInfo
	if err := json.Unmarshal(rec.Body.Bytes(), &list); err != nil {
		t.Fatalf("decode list: %v (body %s)", err, rec.Body.String())
	}
	if len(list) != 2 {
		t.Fatalf("list length = %d, want 2 (%s)", len(list), rec.Body.String())
	}
	if list[0].Name != "orders" || list[1].Name != "shipments" {
		t.Errorf("list order = [%s %s], want [orders shipments]", list[0].Name, list[1].Name)
	}
	for _, info := range list {
		if got := jsonKeys(t, mustJSON(t, info)); !slices.Equal(got, streamInfoKeys) {
			t.Errorf("stream %s JSON keys = %v, want %v", info.Name, got, streamInfoKeys)
		}
	}
}

func TestGetStream(t *testing.T) {
	st, srv := newStreamsServer(t)
	createOrders(t, st)

	rec := doJSON(t, srv.Handler(), http.MethodGet, "/v1/streams/orders", "")
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200 (body %q)", rec.Code, rec.Body.String())
	}
	if got := jsonKeys(t, rec.Body.Bytes()); !slices.Equal(got, streamInfoKeys) {
		t.Errorf("get JSON keys = %v, want %v", got, streamInfoKeys)
	}
	var info store.StreamInfo
	if err := json.Unmarshal(rec.Body.Bytes(), &info); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if info.Name != "orders" {
		t.Errorf("name = %q, want orders", info.Name)
	}
}

func TestGetStreamNotFound(t *testing.T) {
	_, srv := newStreamsServer(t)

	rec := doJSON(t, srv.Handler(), http.MethodGet, "/v1/streams/missing", "")
	if rec.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want 404 (body %q)", rec.Code, rec.Body.String())
	}
	env := decodeError(t, rec)
	if env.Code != "not_found" {
		t.Errorf("code = %q, want not_found", env.Code)
	}
	assertTraceID(t, env.TraceID)
}

// testStreamUpdate is the PATCH response decode shape: the frozen StreamInfo plus the
// narrowed_msgs report.
type testStreamUpdate struct {
	store.StreamInfo
	NarrowedMsgs int64 `json:"narrowed_msgs"`
}

func TestUpdateStream(t *testing.T) {
	st, srv := newStreamsServer(t)
	createOrders(t, st)

	rec := doJSON(t, srv.Handler(), http.MethodPatch, "/v1/streams/orders", `{"max_msgs":1000}`)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200 (body %q)", rec.Code, rec.Body.String())
	}
	if got := jsonKeys(t, rec.Body.Bytes()); !slices.Equal(got, streamUpdateKeys) {
		t.Errorf("patch JSON keys = %v, want %v", got, streamUpdateKeys)
	}

	var res testStreamUpdate
	if err := json.Unmarshal(rec.Body.Bytes(), &res); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if res.MaxMsgs != 1000 {
		t.Errorf("max_msgs = %d, want 1000", res.MaxMsgs)
	}
	if res.NarrowedMsgs != 0 {
		t.Errorf("narrowed_msgs = %d, want 0 for a non-subject patch", res.NarrowedMsgs)
	}
}

func TestUpdateStreamSparseAbsentVsZero(t *testing.T) {
	st, srv := newStreamsServer(t)
	createOrders(t, st)

	patch := func(body string) testStreamUpdate {
		t.Helper()
		rec := doJSON(t, srv.Handler(), http.MethodPatch, "/v1/streams/orders", body)
		if rec.Code != http.StatusOK {
			t.Fatalf("status = %d, want 200 (body %q)", rec.Code, rec.Body.String())
		}
		var res testStreamUpdate
		if err := json.Unmarshal(rec.Body.Bytes(), &res); err != nil {
			t.Fatalf("decode: %v", err)
		}
		return res
	}

	if got := patch(`{"max_msgs":5}`); got.MaxMsgs != 5 {
		t.Fatalf("after set 5: max_msgs = %d, want 5", got.MaxMsgs)
	}
	if got := patch(`{}`); got.MaxMsgs != 5 {
		t.Errorf("empty patch zeroed max_msgs: got %d, want 5 (absent must not mean zero)", got.MaxMsgs)
	}
	if got := patch(`{"max_msgs":0}`); got.MaxMsgs != 0 {
		t.Errorf("after set 0: max_msgs = %d, want 0 (zero means unlimited)", got.MaxMsgs)
	}
}

func TestUpdateStreamWouldLoseData(t *testing.T) {
	st, srv := newStreamsServer(t)
	createOrders(t, st)
	for i := 0; i < 3; i++ {
		publishForTest(t, st, "orders", "orders.eu.created", "hello")
	}

	rec := doJSON(t, srv.Handler(), http.MethodPatch, "/v1/streams/orders", `{"max_msgs":1}`)
	if rec.Code != http.StatusConflict {
		t.Fatalf("status = %d, want 409 (body %q)", rec.Code, rec.Body.String())
	}
	env := decodeError(t, rec)
	if env.Code != "would_lose_data" {
		t.Errorf("code = %q, want would_lose_data", env.Code)
	}
	if !strings.Contains(env.Message, "would delete 2 messages (10 bytes)") {
		t.Errorf("message %q does not name the at-risk messages and bytes", env.Message)
	}
	if !slices.Equal(env.Next, []string{"messq stream edit orders --allow-data-loss"}) {
		t.Errorf("next = %v, want [messq stream edit orders --allow-data-loss]", env.Next)
	}
	assertTraceID(t, env.TraceID)

	ok := doJSON(t, srv.Handler(), http.MethodPatch, "/v1/streams/orders?allow_data_loss=1", `{"max_msgs":1}`)
	if ok.Code != http.StatusOK {
		t.Fatalf("allow_data_loss status = %d, want 200 (body %q)", ok.Code, ok.Body.String())
	}
	var res testStreamUpdate
	if err := json.Unmarshal(ok.Body.Bytes(), &res); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if res.MaxMsgs != 1 {
		t.Errorf("max_msgs = %d, want 1 after allow_data_loss", res.MaxMsgs)
	}
}

func TestUpdateStreamNarrowSubjects(t *testing.T) {
	st, srv := newStreamsServer(t)
	cfg := queue.DefaultConfig("orders")
	cfg.Subjects = []string{"a.>", "b.>"}
	if _, _, err := st.CreateStream(context.Background(), cfg, "test"); err != nil {
		t.Fatalf("create orders: %v", err)
	}
	for _, subj := range []string{"a.1", "a.2", "b.1"} {
		publishForTest(t, st, "orders", subj, "x")
	}

	rec := doJSON(t, srv.Handler(), http.MethodPatch, "/v1/streams/orders", `{"subjects":["a.>"]}`)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200 (body %q)", rec.Code, rec.Body.String())
	}
	var res testStreamUpdate
	if err := json.Unmarshal(rec.Body.Bytes(), &res); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if !slices.Equal(res.Subjects, []string{"a.>"}) {
		t.Errorf("subjects = %v, want [a.>]", res.Subjects)
	}
	if res.NarrowedMsgs != 1 {
		t.Errorf("narrowed_msgs = %d, want 1 (the b.1 message)", res.NarrowedMsgs)
	}
}

func TestUpdateStreamNotFound(t *testing.T) {
	_, srv := newStreamsServer(t)

	rec := doJSON(t, srv.Handler(), http.MethodPatch, "/v1/streams/missing", `{"max_msgs":1}`)
	if rec.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want 404 (body %q)", rec.Code, rec.Body.String())
	}
	if env := decodeError(t, rec); env.Code != "not_found" {
		t.Errorf("code = %q, want not_found", env.Code)
	}
}

type testDelete struct {
	Deleted store.DeleteResult `json:"deleted"`
}

func TestDeleteStream(t *testing.T) {
	st, srv := newStreamsServer(t)
	createOrders(t, st)
	publishForTest(t, st, "orders", "orders.eu.created", "abc")
	publishForTest(t, st, "orders", "orders.eu.created", "de")

	rec := doJSON(t, srv.Handler(), http.MethodDelete, "/v1/streams/orders?confirm=orders", "")
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200 (body %q)", rec.Code, rec.Body.String())
	}
	if got := jsonKeys(t, rec.Body.Bytes()); !slices.Equal(got, []string{"deleted"}) {
		t.Errorf("delete JSON keys = %v, want [deleted]", got)
	}

	var del testDelete
	if err := json.Unmarshal(rec.Body.Bytes(), &del); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if del.Deleted.Messages != 2 {
		t.Errorf("deleted.messages = %d, want 2", del.Deleted.Messages)
	}
	if del.Deleted.Bytes != 5 {
		t.Errorf("deleted.bytes = %d, want 5", del.Deleted.Bytes)
	}
	if del.Deleted.Consumers != 0 {
		t.Errorf("deleted.consumers = %d, want 0", del.Deleted.Consumers)
	}
	if _, err := st.GetStream(context.Background(), "orders"); !errors.Is(err, errs.ErrNotFound) {
		t.Errorf("get after delete = %v, want not_found", err)
	}
}

func TestDeleteStreamConfirmMismatch(t *testing.T) {
	for _, tc := range []struct {
		name     string
		path     string
		wantCode Code
	}{
		{"missing confirm", "/v1/streams/orders", CodeConfirmRequired},
		{"wrong confirm", "/v1/streams/orders?confirm=shipments", CodeConfirmMismatch},
	} {
		t.Run(tc.name, func(t *testing.T) {
			st, srv := newStreamsServer(t)
			createOrders(t, st)

			rec := doJSON(t, srv.Handler(), http.MethodDelete, tc.path, "")
			if rec.Code != http.StatusConflict {
				t.Fatalf("status = %d, want 409 (body %q)", rec.Code, rec.Body.String())
			}
			env := decodeError(t, rec)
			if env.Code != string(tc.wantCode) {
				t.Errorf("code = %q, want %s", env.Code, tc.wantCode)
			}
			assertTraceID(t, env.TraceID)
			if _, err := st.GetStream(context.Background(), "orders"); err != nil {
				t.Errorf("stream was deleted on a refused confirm: %v", err)
			}
		})
	}
}

func TestDeleteStreamNotFound(t *testing.T) {
	_, srv := newStreamsServer(t)

	rec := doJSON(t, srv.Handler(), http.MethodDelete, "/v1/streams/missing?confirm=missing", "")
	if rec.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want 404 (body %q)", rec.Code, rec.Body.String())
	}
	if env := decodeError(t, rec); env.Code != "not_found" {
		t.Errorf("code = %q, want not_found", env.Code)
	}
}

// TestErrorEnvelopeMapping pins the sentinel→wire-code and wire-code→status mapping for
// every code this slice can emit, including the store-level ones (read_only,
// shutting_down, internal) that no route can reach without fault injection.
func TestErrorEnvelopeMapping(t *testing.T) {
	srv := &Server{logger: discardLogger()}

	cases := []struct {
		name   string
		err    error
		code   string
		status int
	}{
		{"not_found", errs.E(errs.ErrNotFound, "store.GetStream", "stream %q does not exist", "orders"), "not_found", http.StatusNotFound},
		{"stream_exists", &store.StreamExistsError{Name: "orders", Diff: []string{"max_msgs", "subjects"}}, "stream_exists", http.StatusConflict},
		{"reserved_name", queue.ErrReservedName, "reserved_name", http.StatusBadRequest},
		{"bad_request", errs.ErrBadRequest, "bad_request", http.StatusBadRequest},
		{"bad_subject", errs.E(errs.ErrBadSubject, "", "a publish target holds no wildcard"), "bad_subject", http.StatusBadRequest},
		{"subject_mismatch", &queue.MismatchError{Subject: "other.a", Accepted: []string{"orders.>"}}, "subject_mismatch", http.StatusBadRequest},
		{"too_large", &queue.TooLargeError{What: "body", Size: 100, Limit: 10}, "too_large", http.StatusRequestEntityTooLarge},
		{"header_too_large", &queue.TooLargeError{What: "headers", Size: 5000, Limit: 4096}, "header_too_large", http.StatusBadRequest},
		{"reserved_header", &queue.ReservedHeaderError{Key: "Messq-Foo"}, "reserved_header", http.StatusBadRequest},
		{"would_lose_data", &queue.WouldLoseDataError{Field: "max_msgs", AtRiskMsgs: 2, AtRiskBytes: 10}, "would_lose_data", http.StatusConflict},
		{"conflict", errs.ErrConflict, "conflict", http.StatusConflict},
		{"read_only", errs.ErrReadOnly, "read_only", http.StatusServiceUnavailable},
		{"shutting_down", errs.ErrShuttingDown, "shutting_down", http.StatusServiceUnavailable},
		{"internal", errors.New("simulated infrastructure failure"), "internal", http.StatusInternalServerError},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rec := httptest.NewRecorder()
			srv.writeError(rec, tc.err)

			if rec.Code != tc.status {
				t.Fatalf("status = %d, want %d (body %q)", rec.Code, tc.status, rec.Body.String())
			}
			if got := errorKeys(t, rec.Body.Bytes()); !slices.Equal(got, errorEnvelopeKeys) {
				t.Errorf("error JSON keys = %v, want %v", got, errorEnvelopeKeys)
			}
			env := decodeError(t, rec)
			if env.Code != tc.code {
				t.Errorf("code = %q, want %q", env.Code, tc.code)
			}
			if env.Message == "" {
				t.Errorf("message is empty")
			}
			assertTraceID(t, env.TraceID)
		})
	}
}

// mustJSON re-marshals a value so the list test can assert its exact key set.
func mustJSON(t *testing.T, v any) []byte {
	t.Helper()
	raw, err := json.Marshal(v)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	return raw
}
