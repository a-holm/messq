// SPDX-License-Identifier: Apache-2.0

package api

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/a-holm/messq/internal/queue"
	"github.com/a-holm/messq/internal/store"
)

func defaultConsumerFor(t *testing.T, name string) queue.ConsumerConfig {
	t.Helper()
	return queue.DefaultConsumerConfig(name)
}

func impactOf(t *testing.T, resp map[string]any) map[string]any {
	t.Helper()
	im, ok := resp["impact"].(map[string]any)
	if !ok {
		t.Fatalf("impact missing: %v", resp)
	}
	return im
}

// consumerOutstanding is the deliveries count a seek drops — Pending already means
// ALL outstanding rows in the §9 §8 stats vocabulary (in-flight are a subset).
func consumerOutstanding(t *testing.T, st *store.Store, stream, name string) int64 {
	t.Helper()
	info, err := st.GetConsumer(context.Background(), stream, name)
	if err != nil {
		t.Fatalf("get consumer: %v", err)
	}
	return info.Pending
}

// Issue #15 §7 / G7: POST …/consumers/{c}/seek is T10 with THE creation-time parser.
// Every seek drops delivery rows, bumps the generation (fencing tokens), writes exactly
// one WARN consumer.seek event, and REPORTS clamping instead of applying it silently.
// ?dry_run=1 previews with identical impact.

func postSeek(t *testing.T, h http.Handler, stream, consumer, query, body string) (int, map[string]any) {
	t.Helper()
	req := httptest.NewRequestWithContext(context.Background(), http.MethodPost,
		"/v1/streams/"+stream+"/consumers/"+consumer+"/seek"+query,
		strings.NewReader(body))
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	var out map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &out); err != nil {
		t.Fatalf("body not JSON (%v): %s", err, rec.Body.String())
	}
	return rec.Code, out
}

func TestSeekDryRunMatchesRealRunAndFences(t *testing.T) {
	srv, st := newPurgeServer(t)
	handler := srv.Handler()

	for i := 1; i <= 4; i++ {
		mustPublish(t, st, "orders", "orders.s", "m", "")
	}
	if _, err := st.CreateConsumer(context.Background(), "orders",
		defaultConsumerFor(t, "worker"), mustStart(t, "first"), "t"); err != nil {
		t.Fatal(err)
	}
	fr, err := st.Fetch(context.Background(),
		store.FetchReq{Stream: "orders", Consumer: "worker", Batch: 2})
	if err != nil || len(fr.Messages) != 2 {
		t.Fatalf("fetch: %v (%d)", err, len(fr.Messages))
	}
	genBefore := consumerGeneration(t, st, "orders", "worker")
	pendBefore := consumerOutstanding(t, st, "orders", "worker")

	code, resp := postSeek(t, handler, "orders", "worker", "?dry_run=1", `{"to":"seq:4"}`)
	if code != http.StatusOK || resp["applied"] != false {
		t.Fatalf("preview status=%d applied=%v (%v)", code, resp["applied"], resp)
	}
	im := impactOf(t, resp)
	if im["pending_dropped"] != float64(pendBefore) {
		t.Errorf("preview pending_dropped = %v, want %d", im["pending_dropped"], pendBefore)
	}
	if genAfterPreview := consumerGeneration(t, st, "orders", "worker"); genAfterPreview != genBefore {
		t.Errorf("preview bumped generation: %d -> %d", genBefore, genAfterPreview)
	}

	code, resp = postSeek(t, handler, "orders", "worker", "", `{"to":"seq:4"}`)
	if code != http.StatusOK || resp["applied"] != true {
		t.Fatalf("real status=%d applied=%v (%v)", code, resp["applied"], resp)
	}
	imReal := impactOf(t, resp)
	for _, k := range []string{"pending_dropped", "cursor_after"} {
		if im[k] != imReal[k] {
			t.Errorf("parity broken on %s: preview %v vs real %v", k, im[k], imReal[k])
		}
	}
	if got := consumerGeneration(t, st, "orders", "worker"); got != genBefore+1 {
		t.Errorf("generation %d -> %d, want exactly +1", genBefore, got)
	}
	if got := consumerOutstanding(t, st, "orders", "worker"); got != 0 {
		t.Errorf("outstanding rows after seek = %d, want 0", got)
	}
	if n := eventCount(t, st, "consumer.seek"); n != 1 {
		t.Errorf("consumer.seek events = %d, want exactly one", n)
	}
}

func TestSeekClampsReportedNotSilent(t *testing.T) {
	srv, st := newPurgeServer(t)
	handler := srv.Handler()

	mustPublish(t, st, "orders", "orders.s", "m", "")
	if _, err := st.CreateConsumer(context.Background(), "orders",
		defaultConsumerFor(t, "w2"), mustStart(t, "new"), "t"); err != nil {
		t.Fatal(err)
	}

	code, resp := postSeek(t, handler, "orders", "w2", "?dry_run=1", `{"to":"seq:0"}`)
	// #28 discipline: a raw seq:0 folds below first_seq (1). Creation-time starts
	// clamp with a warning, but the SEEK verb refuses a below-floor target with a
	// 400 bad_request envelope rather than silently skipping live data — the
	// operator asked for a cursor that does not exist, and "silently jump to the
	// floor" would hide that. Assert the refusal's exact wire shape.
	if code == http.StatusOK {
		t.Fatalf("below-floor seek must be refused (400), got 200: %v", resp)
	}
	eb, hasErr := resp["error"].(map[string]any)
	if !hasErr {
		t.Fatalf("error envelope missing on refusal: %v", resp)
	}
	if eb["code"] != "bad_request" {
		t.Errorf("refusal code = %v, want bad_request", eb["code"])
	}
	msg, _ := eb["message"].(string) //nolint:errcheck // absent message fails the Contains assert below
	if !strings.Contains(msg, "below the stream's first_seq") {
		t.Errorf("refusal message should name the first_seq floor, got %q", msg)
	}
}

func TestSeekGarbageTargetIsBadRequest(t *testing.T) {
	srv, _ := newPurgeServer(t)

	code, resp := postSeek(t, srv.Handler(), "orders", "ghost", "", `{"to":"yesterday"}`)
	eb, hasErr := resp["error"].(map[string]any)
	if !hasErr {
		t.Fatalf("error envelope missing: %v", resp)
	}
	gotCode, okCode := eb["code"].(string)
	if !okCode {
		t.Fatalf("error code not a string: %v", eb)
	}
	if code != http.StatusBadRequest || gotCode != string(CodeBadRequest) {
		t.Fatalf("status=%d code=%q — parse errors ride bad_request (T10)", code, gotCode)
	}
}
