// SPDX-License-Identifier: Apache-2.0

package api

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/a-holm/messq/internal/clock"
	"github.com/a-holm/messq/internal/queue"
	"github.com/a-holm/messq/internal/store"
)

// G9: settle status semantics — a SINGLE token maps its outcome onto the HTTP status
// (200 / 409 stale_ack / 404), a BATCH always answers 200 with per-token results in
// request order and honest ok/stale/unknown counters, and a nak with immediate
// visibility wakes parked fetches.

type settleFixture struct {
	*fetchFixture
}

func newSettleFixture(t *testing.T) (*settleFixture, queue.Token) {
	t.Helper()

	f := &settleFixture{newFetchFixture(t, nil)}

	// One message claimed by the consumer, so a live token exists.
	if _, err := f.st.Publish(context.Background(), store.PublishCmd{
		Stream: "orders",
		Req:    queue.PublishReq{Subject: "orders.eu.created", Body: []byte(`{}`)},
	}); err != nil {
		t.Fatalf("publish: %v", err)
	}
	res, err := f.st.Fetch(context.Background(), store.FetchReq{Stream: "orders", Consumer: "w", Batch: 1})
	if err != nil || len(res.Messages) != 1 {
		t.Fatalf("claim: %v (%d messages)", err, len(res.Messages))
	}
	token, err := queue.ParseToken(res.Messages[0].AckToken)
	if err != nil {
		t.Fatalf("parse minted token: %v", err)
	}
	return f, token
}

func (f *settleFixture) post(t *testing.T, path, body string) (*httptest.ResponseRecorder, settleResponse) {
	t.Helper()
	req := httptest.NewRequestWithContext(context.Background(), http.MethodPost, path,
		strings.NewReader(body))
	rec := httptest.NewRecorder()
	f.srv.Handler().ServeHTTP(rec, req)
	var out settleResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &out); err != nil {
		t.Fatalf("%s %s: response not JSON (%v): %q", http.MethodPost, path, err, rec.Body.String())
	}
	return rec, out
}

func TestAckSingleTokenIsOK(t *testing.T) {
	t.Parallel()

	f, token := newSettleFixture(t)

	rec, out := f.post(t, "/v1/ack", `{"token":"`+token.String()+`"}`)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d (%s)", rec.Code, rec.Body.String())
	}
	if out.OK != 1 || len(out.Results) != 1 || out.Results[0].Result != "ok" {
		t.Fatalf("response %+v, want one ok result", out)
	}
}

func TestSecondAckIs409StaleAck(t *testing.T) {
	t.Parallel()

	f, token := newSettleFixture(t)

	rec, _ := f.post(t, "/v1/ack", `{"tokens":["`+token.String()+`"]}`)
	if rec.Code != http.StatusOK {
		t.Fatalf("first ack status = %d (%s)", rec.Code, rec.Body.String())
	}
	rec, out := f.post(t, "/v1/ack", `{"token":"`+token.String()+`"}`)
	if rec.Code != http.StatusConflict {
		t.Fatalf("second ack status = %d, want 409 stale_ack", rec.Code)
	}
	if envCode := envelopeCode(t, rec); envCode != CodeStaleAck {
		t.Errorf("code = %s, want stale_ack", envCode)
	}
	_ = out
}

func envelopeCode(t *testing.T, rec *httptest.ResponseRecorder) Code {
	t.Helper()
	var env Envelope
	if err := json.Unmarshal(rec.Body.Bytes(), &env); err != nil {
		t.Fatalf("not an envelope: %q", rec.Body.String())
	}
	return env.Error.Code
}

func TestBatchWithMixedOutcomesAlways200(t *testing.T) {
	t.Parallel()

	f, token := newSettleFixture(t)

	// A WELL-FORMED token naming nothing live (seq far ahead): unknown, not invalid.
	ghost := queue.Token{Stream: "orders", Consumer: "w", Seq: 987654321, Attempt: 1, Generation: token.Generation}

	body := `{"tokens":["` + token.String() + `","` + ghost.String() + `"]}`
	rec, out := f.post(t, "/v1/ack", body)
	if rec.Code != http.StatusOK {
		t.Fatalf("batch status = %d, want always-200 (%s)", rec.Code, rec.Body.String())
	}
	if len(out.Results) != 2 {
		t.Fatalf("%d results, want request order preserved (2)", len(out.Results))
	}
	if out.Results[0].Result != "ok" {
		t.Errorf("results[0] = %s, want ok (request order!)", out.Results[0].Result)
	}
	if out.Results[1].Result != "unknown" {
		t.Errorf("results[1] = %s, want unknown", out.Results[1].Result)
	}
	if out.OK != 1 || out.Stale != 0 || out.Unknown != 1 {
		t.Errorf("counters ok=%d stale=%d unknown=%d, want 1/0/1", out.OK, out.Stale, out.Unknown)
	}
}

func TestMalformedTokenSingleIs400InvalidToken(t *testing.T) {
	t.Parallel()

	f := &settleFixture{newFetchFixture(t, nil)}
	rec, _ := f.post(t, "/v1/term", `{"token":"not/a/token"}`)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", rec.Code)
	}
	if got := envelopeCode(t, rec); got != CodeInvalidToken {
		t.Errorf("code = %s, want invalid_token", got)
	}
}

func TestSettleBatchOverCapIs413NamingTheLimit(t *testing.T) {
	t.Parallel()

	f := &settleFixture{newFetchFixture(t, nil)}
	cap := f.st.MaxSettleBatch()

	items := make([]string, 0, cap+1)
	for i := 0; i <= cap; i++ {
		items = append(items, `{"token":"orders/w/1/1/1"}`)
	}
	rec, _ := f.post(t, "/v1/nak", `{"items":[`+strings.Join(items, ",")+`]}`)
	if rec.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("status = %d, want 413 (body %s)", rec.Code, rec.Body.String())
	}
	env := Envelope{}
	if err := json.Unmarshal(rec.Body.Bytes(), &env); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(env.Error.Message, "max-settle-batch") {
		t.Errorf("message %q does not name --max-settle-batch", env.Error.Message)
	}
}

func TestNakImmediateVisibilityWakesParkedFetch(t *testing.T) {
	t.Parallel()

	f, token := newSettleFixture(t)

	// Park a fetch BEFORE settling: only the settle-driven wake may complete it.
	done := f.do(t, `{"batch":1,"wait_ms":60000}`)
	f.waitForParked(1)

	delay0 := int64(0)
	rec, out := f.post(t, "/v1/nak", settleBody(token, delay0))
	if rec.Code != http.StatusOK {
		t.Fatalf("nak status = %d (%s)", rec.Code, rec.Body.String())
	}
	if out.OK != 1 {
		t.Fatalf("nak ok counter = %d, want 1", out.OK)
	}

	select {
	case fetchRec := <-done:
		got := decodeFetchResponse(t, fetchRec)
		if len(got.Messages) != 1 {
			t.Fatalf("nak did not make the message visible again: %d messages", len(got.Messages))
		}
	case <-clock.System{}.NewTimer(5 * time.Second).C():
		t.Fatal("immediate-visibility nak did not wake the parked fetch")
	}
}

func settleBody(token queue.Token, delayMS int64) string {
	return `{"items":[{"token":"` + token.String() + `","delay_ms":` +
		strconvI64(delayMS) + `}]}`
}

func strconvI64(v int64) string {
	return strings.TrimSpace(jsonNumber(v))
}

func jsonNumber(v int64) string {
	b, err := json.Marshal(v)
	if err != nil {
		return "0" // int64 marshalling cannot fail; the fallback keeps the shape
	}
	return string(b)
}
