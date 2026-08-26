// SPDX-License-Identifier: Apache-2.0

package client

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestPublishRoundTrip(t *testing.T) {
	t.Parallel()

	var gotPath, gotSubject, gotMsgID, gotTraceID, gotTenant, gotBody, gotCT string
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.Path
		gotSubject = r.Header.Get("Messq-Subject")
		gotMsgID = r.Header.Get("Messq-Msg-Id")
		gotTraceID = r.Header.Get("Messq-Trace-Id")
		gotTenant = r.Header.Get("Messq-Header-Tenant")
		gotCT = r.Header.Get("Content-Type")
		b, _ := io.ReadAll(r.Body)
		gotBody = string(b)
		w.WriteHeader(http.StatusCreated)
		_, _ = w.Write([]byte(`{"stream":"orders","seq":42,"id":"01JACK","trace_id":"tr",` +
			`"duplicate":false,"published_at":1756100000000}`))
	}))
	defer ts.Close()

	c := newTestClient(t, ts)
	ack, err := c.Publish(context.Background(), "orders", Msg{
		Subject: "orders.west",
		Body:    []byte("hello"),
		Header:  map[string]string{"tenant": "acme"},
		MsgID:   "dedup-1",
		TraceID: "tr",
	})
	if err != nil {
		t.Fatalf("Publish: %v", err)
	}
	if gotPath != "/v1/streams/orders/messages" || gotSubject != "orders.west" ||
		gotMsgID != "dedup-1" || gotTraceID != "tr" || gotTenant != "acme" ||
		gotBody != "hello" || !strings.HasPrefix(gotCT, "application/octet-stream") {
		t.Errorf("publish wire shape = path:%s subject:%s msgid:%s trace:%s hdr:%s body:%q ct:%s",
			gotPath, gotSubject, gotMsgID, gotTraceID, gotTenant, gotBody, gotCT)
	}
	if ack.Seq != 42 || ack.ID != "01JACK" || ack.Duplicate {
		t.Errorf("ack = %+v", ack)
	}
}

func TestPublishRejectsReservedHeaderLocally(t *testing.T) {
	t.Parallel()

	var requests int
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		requests++
		_, _ = io.WriteString(w, `{}`)
	}))
	defer ts.Close()

	c := newTestClient(t, ts)
	for _, hdr := range []map[string]string{
		{"Messq-Trace-Id": "forged"},
		{"messq-trace-id": "forged-lowercase"},
		{"bad header": "space in key"},
	} {
		_, err := c.Publish(context.Background(), "orders", Msg{
			Subject: "orders.west",
			Header:  hdr,
		})
		if !errors.Is(err, ErrBadRequest) {
			t.Errorf("header %v: err = %v, want local ErrBadRequest", hdr, err)
		}
	}
	if requests != 0 {
		t.Errorf("%d round trips happened for a locally refused message", requests)
	}
}

func TestPublishEmptySubjectRefusedLocally(t *testing.T) {
	t.Parallel()

	var requests int
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		requests++
	}))
	defer ts.Close()
	c := newTestClient(t, ts)
	if _, err := c.Publish(context.Background(), "orders", Msg{}); !errors.Is(err, ErrBadRequest) {
		t.Errorf("err = %v, want ErrBadRequest", err)
	}
	if requests != 0 {
		t.Error("an empty subject reached the wire")
	}
}

func TestPublishZeroLengthBodyRoundTrips(t *testing.T) {
	t.Parallel()

	var gotLen int64
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		gotLen = int64(len(b))
		w.WriteHeader(http.StatusCreated)
		_, _ = w.Write([]byte(`{"stream":"s","seq":1,"id":"i","duplicate":false}`))
	}))
	defer ts.Close()

	c := newTestClient(t, ts)
	ack, err := c.Publish(context.Background(), "s", Msg{Subject: "s.a"})
	if err != nil {
		t.Fatalf("Publish(empty): %v", err)
	}
	if gotLen != 0 || ack.Seq != 1 {
		t.Errorf("len=%d ack=%+v", gotLen, ack)
	}
}

func TestPublishDuplicateIsSuccessNotError(t *testing.T) {
	t.Parallel()

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK) // dedup hit rides 200, not 201
		_, _ = w.Write([]byte(`{"stream":"s","seq":7,"id":"first-one","duplicate":true}`))
	}))
	defer ts.Close()

	c := newTestClient(t, ts)
	ack, err := c.Publish(context.Background(), "s", Msg{Subject: "s.a", Body: []byte("x"), MsgID: "m"})
	if err != nil {
		t.Fatalf("duplicate publish returned an error: %v", err)
	}
	if !ack.Duplicate || ack.Seq != 7 {
		t.Errorf("ack = %+v, want duplicate of the ORIGINAL seq", ack)
	}
}

func TestPublishCommitUnknownRetriedOnlyWithMsgID(t *testing.T) {
	t.Parallel()

	t.Run("with msg id the retry dedups", func(t *testing.T) {
		t.Parallel()

		var attempts int
		ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			attempts++
			if attempts == 1 {
				w.WriteHeader(http.StatusServiceUnavailable)
				_, _ = w.Write([]byte(`{"error":{"code":"commit_unknown","message":"commit may have landed"}}`))
				return
			}
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"stream":"s","seq":3,"id":"orig","duplicate":true}`))
		}))
		defer ts.Close()

		c := newTestClient(t, ts)
		ack, err := c.Publish(context.Background(), "s", Msg{Subject: "s.a", MsgID: "same"})
		if err != nil {
			t.Fatalf("retry failed: %v", err)
		}
		if attempts != 2 {
			t.Errorf("attempts = %d, want exactly one automatic retry", attempts)
		}
		if !ack.Duplicate {
			t.Errorf("ack = %+v, want Duplicate:true not a second seq", ack)
		}
	})

	t.Run("without msg id there is no retry and the error teaches", func(t *testing.T) {
		t.Parallel()

		var attempts int
		ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			attempts++
			w.WriteHeader(http.StatusServiceUnavailable)
			_, _ = w.Write([]byte(`{"error":{"code":"commit_unknown","message":"commit may have landed"}}`))
		}))
		defer ts.Close()

		c := newTestClient(t, ts)
		_, err := c.Publish(context.Background(), "s", Msg{Subject: "s.a"})
		if !errors.Is(err, ErrCommitUnknown) {
			t.Fatalf("err = %v, want ErrCommitUnknown", err)
		}
		var e *Error
		if !errors.As(err, &e) || len(e.Next) == 0 || !strings.Contains(strings.Join(e.Next, "; "), "Messq-Msg-Id") {
			t.Errorf("Next = %v, want the Messq-Msg-Id teaching hint", e.Next)
		}
		if attempts != 1 {
			t.Errorf("attempts = %d, want 1 — a retry without a dedup key can DUPLICATE", attempts)
		}
	})
}

func TestPublishBatchNeverSilentlySplits(t *testing.T) {
	t.Parallel()

	var requests int
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		requests++
	}))
	defer ts.Close()

	c := newTestClient(t, ts)
	msgs := make([]Msg, DefaultPublishBatchCap+1)
	for i := range msgs {
		msgs[i] = Msg{Subject: "s.a"}
	}
	_, err := c.PublishBatch(context.Background(), "s", msgs)
	if !errors.Is(err, ErrTooLarge) {
		t.Fatalf("err = %v, want ErrTooLarge over the cap", err)
	}
	if requests != 0 {
		t.Errorf("%d requests were made; an over-cap batch must be refused locally", requests)
	}

	// Explicit chunking is visible in caller code instead.
	chunks := MaxBatch(msgs, DefaultPublishBatchCap)
	if len(chunks) != 2 || len(chunks[0]) != DefaultPublishBatchCap || len(chunks[1]) != 1 {
		t.Errorf("MaxBatch = %d chunks (%d, %d), want 1000+1",
			len(chunks), len(chunks[0]), len(chunks[1]))
	}
}

func TestPublishBatchWireShape(t *testing.T) {
	t.Parallel()

	var lines []string
	var gotPath string
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.Path
		b, _ := io.ReadAll(r.Body)
		lines = strings.Split(strings.TrimSpace(string(b)), "\n")
		w.WriteHeader(http.StatusCreated)
		_, _ = w.Write([]byte(`{"stream":"s","results":[` +
			`{"stream":"s","seq":1,"id":"a","duplicate":false},` +
			`{"stream":"s","seq":2,"id":"b","duplicate":false}]}`))
	}))
	defer ts.Close()

	c := newTestClient(t, ts)
	acks, err := c.PublishBatch(context.Background(), "s", []Msg{
		{Subject: "s.a", Body: []byte("one")},
		{Subject: "s.b", Body: []byte("two"), MsgID: "m2"},
	})
	if err != nil {
		t.Fatalf("PublishBatch: %v", err)
	}
	if gotPath != "/v1/streams/s/messages:batch" {
		t.Errorf("path = %s", gotPath)
	}
	if len(lines) != 2 {
		t.Fatalf("got %d NDJSON lines, want 2", len(lines))
	}
	var first map[string]any
	if err := json.Unmarshal([]byte(lines[0]), &first); err != nil {
		t.Fatalf("line 1 is not JSON: %v", err)
	}
	if first["subject"] != "s.a" || first["body_b64"] != "b25l" {
		t.Errorf("line 1 = %v, want subject+body_b64", first)
	}
	if len(acks) != 2 || acks[1].Seq != 2 {
		t.Errorf("acks = %+v", acks)
	}
}

func TestFetchWireShape(t *testing.T) {
	t.Parallel()

	var gotPath, gotBody string
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.Path
		b, _ := io.ReadAll(r.Body)
		gotBody = string(b)
		_, _ = w.Write([]byte(`{"messages":[` +
			`{"stream":"orders","consumer":"w","seq":9,"subject":"orders.w",` +
			`"body_b64":"aGk=","attempt":1,"ack_token":"t9"}],"hold_reason":"",` +
			`"pending":4,"backlog":11,"batch":2,"max_bytes":1048576,"wait_ms":50}`))
	}))
	defer ts.Close()

	c := newTestClient(t, ts)
	res, err := c.Fetch(context.Background(), "orders", "w", FetchRequest{Batch: 2, Wait: 50 * time.Millisecond})
	if err != nil {
		t.Fatalf("Fetch: %v", err)
	}
	if gotPath != "/v1/streams/orders/consumers/w/fetch" {
		t.Errorf("path = %s", gotPath)
	}
	want := `{"batch":2,"max_bytes":0,"wait_ms":50}`
	if gotBody != want {
		t.Errorf("fetch body = %s, want %s", gotBody, want)
	}
	if len(res.Messages) != 1 || res.Messages[0].AckToken != "t9" ||
		string(res.Messages[0].Body) != "hi" {
		t.Fatalf("messages = %+v", res.Messages)
	}
	// Effective echoes come back, not the request's guesses.
	if res.Batch != 2 || res.MaxBytes != 1048576 || res.Wait != 50*time.Millisecond {
		t.Errorf("effective echo = %+v", res)
	}
	if res.Hold != HoldNone || res.Pending != 4 || res.Backlog != 11 {
		t.Errorf("hold/pending/backlog = %+v", res)
	}
}

func TestFetchHoldConstantsMatchWireValues(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		wire string
		want HoldReason
	}{
		{"", HoldNone},
		{"paused", HoldPaused},
		{"flow_control", HoldFlowControl},
		{"backoff", HoldBackoff},
		{"catching_up", HoldCatchingUp},
		{"empty", HoldEmpty},
		{"shutting_down", HoldShuttingDown},
	} {
		var w fetchResponseWire
		if err := json.Unmarshal([]byte(`{"messages":[],"hold_reason":"`+tc.wire+`"}`), &w); err != nil {
			t.Fatalf("decode %q: %v", tc.wire, err)
		}
		if got := w.export().Hold; got != tc.want {
			t.Errorf("hold_reason %q decoded to %q", tc.wire, got)
		}
	}
}

func TestAckSendsTokenArrayAndDecodesResults(t *testing.T) {
	t.Parallel()

	var gotBody string
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath := r.URL.Path
		if gotPath != "/v1/ack" {
			t.Errorf("path = %s", gotPath)
		}
		b, _ := io.ReadAll(r.Body)
		gotBody = string(b)
		_, _ = w.Write([]byte(`{"results":[{"token":"t1","result":"ok"},` +
			`{"token":"t2","result":"stale","reason":"nothing to do"}],"ok":1,"stale":1,"unknown":0}`))
	}))
	defer ts.Close()

	c := newTestClient(t, ts)
	res, err := c.Ack(context.Background(), "t1", "t2")
	if err != nil {
		t.Fatalf("Ack: %v", err)
	}
	if gotBody != `{"tokens":["t1","t2"]}` {
		t.Errorf("ack body = %s", gotBody)
	}
	if len(res.Results) != 2 || res.Results[0].Status != SettleOK ||
		res.Results[1].Status != SettleStale || res.Stale != 1 {
		t.Errorf("results = %+v", res)
	}
}

func TestNakCarriesDelayAndTruncatedReason(t *testing.T) {
	t.Parallel()

	var req struct {
		Token   string `json:"token"`
		DelayMS *int64 `json:"delay_ms"`
		Reason  string `json:"reason"`
	}
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		_ = json.Unmarshal(b, &req)
		_, _ = w.Write([]byte(`{"results":[{"token":"t1","result":"ok"}],"ok":1}`))
	}))
	defer ts.Close()

	c := newTestClient(t, ts)
	huge := strings.Repeat("r", reasonLimit*3)
	item, err := c.Nak(context.Background(), "t1", WithDelay(1500*time.Millisecond), WithReason(huge))
	if err != nil {
		t.Fatalf("Nak: %v", err)
	}
	if item.Status != SettleOK {
		t.Errorf("item = %+v", item)
	}
	if req.DelayMS == nil || *req.DelayMS != 1500 {
		t.Errorf("delay_ms = %v, want 1500", req.DelayMS)
	}
	if len(req.Reason) != reasonLimit {
		t.Errorf("reason length = %d, want truncated to %d", len(req.Reason), reasonLimit)
	}
}

func TestSingleStaleNakSurfacesTypedStaleAck(t *testing.T) {
	t.Parallel()

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusConflict)
		_, _ = w.Write([]byte(`{"error":{"code":"stale_ack","message":"token attempt moved on"}}`))
	}))
	defer ts.Close()

	c := newTestClient(t, ts)
	_, err := c.Nak(context.Background(), "orders/w/7/1/1")
	if !errors.Is(err, ErrStaleAck) {
		t.Fatalf("err = %v, want ErrStaleAck", err)
	}
}

func TestTermSendsReason(t *testing.T) {
	t.Parallel()

	var req struct {
		Token  string `json:"token"`
		Reason string `json:"reason"`
	}
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/term" {
			t.Errorf("path = %s", r.URL.Path)
		}
		b, _ := io.ReadAll(r.Body)
		_ = json.Unmarshal(b, &req)
		_, _ = w.Write([]byte(`{"results":[{"token":"t1","result":"ok"}],"ok":1}`))
	}))
	defer ts.Close()

	c := newTestClient(t, ts)
	if _, err := c.Term(context.Background(), "t1", "poison payload"); err != nil {
		t.Fatalf("Term: %v", err)
	}
	if req.Token != "t1" || req.Reason != "poison payload" {
		t.Errorf("term body = %+v", req)
	}
}

func TestExtendBatchesViaItemsForm(t *testing.T) {
	t.Parallel()

	var gotBody string
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/extend" {
			t.Errorf("path = %s", r.URL.Path)
		}
		b, _ := io.ReadAll(r.Body)
		gotBody = string(b)
		_, _ = w.Write([]byte(`{"results":[{"token":"t1","result":"ok"},` +
			`{"token":"t2","result":"unknown","reason":"no such delivery"}],"ok":1,"unknown":1}`))
	}))
	defer ts.Close()

	c := newTestClient(t, ts)
	res, err := c.Extend(context.Background(), "t1", "t2")
	if err != nil {
		t.Fatalf("Extend: %v", err)
	}
	// The tokens ARRAY is ack-only per #14; extends ride the items form.
	if !strings.Contains(gotBody, `"items"`) || strings.Contains(gotBody, `"tokens"`) {
		t.Errorf("extend body = %s, want the items form", gotBody)
	}
	if res.OK != 1 || res.Results[1].Status != SettleUnknown {
		t.Errorf("results = %+v", res)
	}
}

func TestBadNamesRefusedWithoutRoundTrip(t *testing.T) {
	t.Parallel()

	var requests int
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		requests++
	}))
	defer ts.Close()

	c := newTestClient(t, ts)
	ctx := context.Background()
	if _, err := c.Fetch(ctx, "../escape", "w", FetchRequest{}); !errors.Is(err, ErrBadRequest) {
		t.Errorf("bad stream name: err = %v", err)
	}
	if _, err := c.Ack(ctx, ""); !errors.Is(err, ErrBadRequest) {
		t.Errorf("empty token: err = %v", err)
	}
	if requests != 0 {
		t.Errorf("%d round trips happened for locally refused names", requests)
	}
}
