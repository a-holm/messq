// SPDX-License-Identifier: Apache-2.0

package api

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/a-holm/messq/internal/queue"
	"github.com/a-holm/messq/internal/store"
)

// Issue #15 §9 / G9: the pending listing is deadline-ascending, bounded with the
// effective limit ECHOED, and mints ack_token ONLY for inflight rows — a scheduled row
// has no live lease, so its token would fence-fail and null is the honest answer.

func fetchPending(t *testing.T, h http.Handler, query string) map[string]any {
	t.Helper()
	req := httptest.NewRequestWithContext(context.Background(), http.MethodGet,
		"/v1/streams/orders/consumers/worker/pending"+query, nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	var out map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &out); err != nil {
		t.Fatalf("body not JSON (%v): %s", err, rec.Body.String())
	}
	return out
}

func TestPendingOrdersByDeadlineAndDerivesTokensOnlyForInflight(t *testing.T) {
	srv, st := newPurgeServer(t)
	handler := srv.Handler()

	for i := 0; i < 3; i++ {
		mustPublish(t, st, "orders", "orders.s", fmt.Sprintf("m%d", i), "")
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

	resp := fetchPending(t, handler, "?limit=10")
	rawItems, ok := resp["items"].([]any)
	if !ok || len(rawItems) != 3 {
		t.Fatalf("items = %v, want 3 outstanding rows", resp["items"])
	}

	deadlines := make([]float64, 0, 3)
	for i, it := range rawItems {
		m, mok := it.(map[string]any)
		if !mok {
			t.Fatalf("item %d is not an object: %v", i, it)
		}
		dlF, okD := m["deadline"].(float64)
		if !okD {
			t.Fatalf("item %d deadline missing: %v", i, m)
		}
		deadlines = append(deadlines, dlF)
		stateS, okS := m["state"].(string)
		if !okS {
			t.Fatalf("item %d state missing: %v", i, m)
		}
		switch stateS {
		case "inflight":
			tk, hasToken := m["ack_token"]
			if !hasToken || tk == nil {
				t.Errorf("inflight row %d lacks its derived ack_token", i)
			} else if tokStr, tokOk := tk.(string); !tokOk {
				t.Errorf("row %d token not a string: %v", i, tk)
			} else if _, pErr := queue.ParseToken(tokStr); pErr != nil {
				t.Errorf("row %d token %q does not parse (D7): %v", i, tokStr, pErr)
			}
		default:
			if tk, present := m["ack_token"]; present && tk != nil {
				t.Errorf("non-inflight row %d carries a live-looking token: %v", i, tk)
			}
		}
	}
	for i := 1; i < len(deadlines); i++ {
		if deadlines[i] < deadlines[i-1] {
			t.Fatalf("deadline order broken at %d: %v", i, deadlines)
		}
	}
}

func TestPendingClampEchoesEffectiveLimit(t *testing.T) {
	srv, st := newPurgeServer(t)
	handler := srv.Handler()

	for i := 0; i < 4; i++ {
		mustPublish(t, st, "orders", "orders.s", "m", "")
	}
	if _, err := st.CreateConsumer(context.Background(), "orders",
		defaultConsumerFor(t, "worker"), mustStart(t, "first"), "t"); err != nil {
		t.Fatal(err)
	}
	// One claim materializes the delivery rows the pending window reports.
	if _, err := st.Fetch(context.Background(),
		store.FetchReq{Stream: "orders", Consumer: "worker", Batch: 4}); err != nil {
		t.Fatal(err)
	}

	resp := fetchPending(t, handler, "?limit=2")
	items, iok := resp["items"].([]any)
	if !iok || len(items) != 2 {
		t.Fatalf("rows = %d ok=%v, want 2 (%v)", len(items), iok, resp)
	}
	next, nok := resp["next_after"].(float64)
	if !nok || next == 0 {
		t.Fatalf("next_after missing from a full page: %v", resp)
	}

	page2 := fetchPending(t, handler, fmt.Sprintf("?limit=2&after=%d", int(next)))
	items2, p2ok := page2["items"].([]any)
	if !p2ok || len(items2) == 0 {
		t.Fatalf("second page empty despite next_after (%v)", page2)
	}
	firstM, f1 := items[0].(map[string]any)
	first2, f2 := items2[0].(map[string]any)
	if !f1 || !f2 {
		t.Fatal("page items are not objects")
	}
	first, second := firstM, first2
	if first["seq"] == second["seq"] {
		t.Fatalf("pagination repeated seq %v across pages", first["seq"])
	}

	// Above the flag cap: the EFFECTIVE value comes back echoed.
	srv.cfg.PendingMaxLimit = 2
	capped := fetchPending(t, handler, "?limit=999")
	limitField, lok := capped["limit"].(float64)
	if !lok || int(limitField) != 2 {
		t.Errorf("effective limit echoed = %v, want clamped 2", capped["limit"])
	}
}
