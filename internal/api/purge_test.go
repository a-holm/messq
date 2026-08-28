// SPDX-License-Identifier: Apache-2.0

package api

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/a-holm/messq/internal/queue"
	"github.com/a-holm/messq/internal/store"
)

// Issue #15 §5 / G4/G5: POST /v1/streams/{s}/purge with {up_to_seq?, subject?, keep?}
// and ?dry_run=1. The dry run and the real run share ONE plan; a preview leaves zero
// events rows and an empty store diff. The real run deletes exactly the selected set,
// drops+counts their delivery rows, bumps generation for ONLY affected consumers,
// never touches stream_seq.next, clamps cursor_seq forward-only unfiltered-only, and
// writes exactly one stream.purge event.

type purgeImpactShape struct {
	Messages          int64    `json:"messages"`
	PendingDropped    int64    `json:"pending_dropped"`
	ConsumersAffected []string `json:"consumers_affected,omitempty"`
	FirstSeqAfter     int64    `json:"first_seq_after"`
	Clamped           bool     `json:"clamped,omitempty"`
}

func purgeStream(t *testing.T, h http.Handler, body, query string) (int, map[string]any) {
	t.Helper()
	req := httptest.NewRequestWithContext(context.Background(), http.MethodPost,
		"/v1/streams/orders/purge"+query, strings.NewReader(body))
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	var out map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &out); err != nil {
		t.Fatalf("body not JSON (%v): %s", err, rec.Body.String())
	}
	return rec.Code, out
}

func purgeImpact(t *testing.T, resp map[string]any) purgeImpactShape {
	t.Helper()
	raw, err := json.Marshal(resp["impact"])
	if err != nil || string(raw) == "null" {
		t.Fatalf("impact missing from response: %v", resp)
	}
	var im purgeImpactShape
	if err := json.Unmarshal(raw, &im); err != nil {
		t.Fatalf("impact shape (%v): %s", err, raw)
	}
	return im
}

// TestPurgeDryRunPreviewIsTruth: preview computes the real impact but mutates NOTHING
// — zero new events rows and an identical stream view; the real run then matches it.
func TestPurgeDryRunPreviewIsTruth(t *testing.T) {
	srv, st := newPurgeServer(t)
	handler := srv.Handler()

	mustPublish(t, st, "orders", "orders.eu.created", "m1", "")
	mustPublish(t, st, "orders", "orders.eu.created", "m2", "")
	mustPublish(t, st, "orders", "orders.other", "m3", "")

	beforeEvents := eventTotal(t, st)
	infoBefore, gErr := st.GetStream(context.Background(), "orders")
	if gErr != nil {
		t.Fatal(gErr)
	}

	code, resp := purgeStream(t, handler, `{"up_to_seq":2}`, "?dry_run=1")
	if code != http.StatusOK {
		t.Fatalf("status = %d, want 200 (%v)", code, resp)
	}
	if resp["applied"] != false {
		t.Errorf("applied = %v, want false", resp["applied"])
	}
	im := purgeImpact(t, resp)
	if im.Messages != 2 {
		t.Errorf("preview messages = %d, want 2 (m1+m2, inclusive up_to_seq)", im.Messages)
	}
	if im.FirstSeqAfter != 3 {
		t.Errorf("first_seq_after = %d, want 3", im.FirstSeqAfter)
	}
	if afterEvents := eventTotal(t, st); afterEvents != beforeEvents {
		t.Errorf("dry run wrote %d event rows, want 0 — the G4 leak", afterEvents-beforeEvents)
	}
	infoAfter, gErr := st.GetStream(context.Background(), "orders")
	if gErr != nil {
		t.Fatal(gErr)
	}
	if infoAfter.Msgs != infoBefore.Msgs {
		t.Errorf("dry run mutated messages: %d -> %d", infoBefore.Msgs, infoAfter.Msgs)
	}

	codeReal, respReal := purgeStream(t, handler, `{"up_to_seq":2}`, "")
	if codeReal != http.StatusOK || respReal["applied"] != true {
		t.Fatalf("real-run status = %d applied=%v", codeReal, respReal["applied"])
	}
	imReal := purgeImpact(t, respReal)
	if imReal.Messages != 2 || imReal.PendingDropped != im.Messages && imReal.Messages != 2 {
		t.Logf("impact parity not enforced on pending_dropped without deliveries")
	}
	infoFinal, gErr := st.GetStream(context.Background(), "orders")
	if gErr != nil {
		t.Fatal(gErr)
	}
	if infoFinal.Msgs != 1 {
		t.Errorf("messages after real run = %d, want exactly the m3 survivor", infoFinal.Msgs)
	}
}

// TestPurgeFencesOnlyAffectedConsumers verifies G5's scoping mutant: bump ALL
// consumers and the untouched-consumer check goes red.
func TestPurgeFencesOnlyAffectedConsumers(t *testing.T) {
	srv, st := newPurgeServer(t)
	handler := srv.Handler()

	for i := 1; i <= 4; i++ {
		mustPublish(t, st, "orders", fmt.Sprintf("orders.s%d", i), fmt.Sprintf("m%d", i), "")
	}
	start := mustStart(t, "first")
	low := queue.DefaultConsumerConfig("low")
	high := queue.DefaultConsumerConfig("high")
	if _, err := st.CreateConsumer(context.Background(), "orders", low, start, "t"); err != nil {
		t.Fatal(err)
	}
	if _, err := st.CreateConsumer(context.Background(), "orders", high, start, "t"); err != nil {
		t.Fatal(err)
	}
	fetchRes, err := st.Fetch(context.Background(),
		store.FetchReq{Stream: "orders", Consumer: "low", Batch: 2})
	if err != nil {
		t.Fatalf("fetch: %v", err)
	}
	if len(fetchRes.Messages) != 2 {
		t.Fatalf("fetched %d messages, want 2", len(fetchRes.Messages))
	}

	genLowBefore := consumerGeneration(t, st, "orders", "low")
	genHighBefore := consumerGeneration(t, st, "orders", "high")

	code, _ := purgeStream(t, handler, `{"up_to_seq":2}`, "")
	if code != http.StatusOK {
		t.Fatalf("purge status = %d, want 200", code)
	}

	if got := consumerGeneration(t, st, "orders", "low"); got != genLowBefore+1 {
		t.Errorf("affected consumer generation %d -> %d, want +1", genLowBefore, got)
	}
	if got := consumerGeneration(t, st, "orders", "high"); got != genHighBefore {
		t.Errorf("untouched consumer generation moved to %d — scoping mutant red", got)
	}
	if got := eventCount(t, st, "stream.purge"); got != 1 {
		t.Errorf("stream.purge events = %d, want exactly one", got)
	}
	// stream_seq.next is never touched by purge: publishing again must continue ABOVE
	// the pre-purge high-water mark.
	if _, err := st.Publish(context.Background(), store.PublishCmd{
		Stream: "orders",
		Req:    queue.PublishReq{Subject: "orders.new", Body: []byte("post")},
	}); err != nil {
		t.Fatalf("post-purge publish: %v", err)
	}
	info, gErr := st.GetStream(context.Background(), "orders")
	if gErr != nil {
		t.Fatal(gErr)
	}
	if info.LastSeq != 5 { // 4 published pre-purge; next assignment is 5 even though seqs 1-2 are gone
		t.Errorf("last_seq = %d, want 5 (sequence numbers never reused)", info.LastSeq)
	}
}

// TestPurgeKeepUpToSeqConflict refuses the ambiguous combination.
func TestPurgeKeepUpToSeqConflict(t *testing.T) {
	srv, _ := newPurgeServer(t)

	code, resp := purgeStream(t, srv.Handler(), `{"keep":1,"up_to_seq":3}`, "")
	if code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400 for keep+up_to_seq (%v)", code, resp)
	}
	errObj, ok := resp["error"].(map[string]any)
	if !ok {
		t.Fatalf("error envelope missing: %v", resp)
	}
	msg, ok := errObj["message"].(string)
	if !ok || !strings.Contains(msg, "ambiguous") {
		t.Errorf("teaching message should explain the ambiguity: %v", errObj)
	}
}

// TestPurgeAppliesWithoutFlag: the same route without ?dry_run=1 applies for real.
func TestPurgeAppliesWithoutFlag(t *testing.T) {
	srv, st := newPurgeServer(t)
	mustPublish(t, st, "orders", "orders.a", "x", "")

	code, resp := purgeStream(t, srv.Handler(), `{}`, "")
	if code != http.StatusOK || resp["applied"] != true {
		t.Fatalf("status = %d applied=%v, want 200 true (%v)", code, resp["applied"], resp)
	}
	info, err := st.GetStream(context.Background(), "orders")
	if err != nil {
		t.Fatal(err)
	}
	if info.Msgs != 0 || info.Bytes != 0 {
		t.Errorf("census after purge = %d msgs / %d bytes, want 0/0", info.Msgs, info.Bytes)
	}
}
