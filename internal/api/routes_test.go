// SPDX-License-Identifier: Apache-2.0

package api

import (
	"context"
	"encoding/json"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/a-holm/messq/internal/clock"
	"github.com/a-holm/messq/internal/store"
)

// G14: the route registry is the ONE place patterns live, its shape is golden-locked,
// and every registered route provably reaches its own handler rather than the
// catch-all.

func TestRouteRegistryGolden(t *testing.T) {
	t.Parallel()

	var b strings.Builder
	for _, rt := range (*Server)(nil).routes() {
		b.WriteString(rt.Method)
		b.WriteByte(' ')
		b.WriteString(rt.Pattern)
		b.WriteByte(' ')
		b.WriteString(rt.Name)
		b.WriteByte(' ')
		if rt.Mutating {
			b.WriteString("mutating")
		} else {
			b.WriteString("readonly")
		}
		// Destructive rows carry their discipline inline so the golden diff
		// shows a verb going destructive as a reviewed change (#28).
		if rt.Destructive {
			b.WriteString(" destructive confirm=")
			b.WriteString(rt.Confirm)
			b.WriteString(" audit=")
			b.WriteString(rt.Audit.String())
		}
		b.WriteByte('\n')
	}

	golden := filepath.Join("testdata", "routes.golden")
	if os.Getenv("MESSQ_UPDATE_GOLDEN") == "1" {
		if err := os.WriteFile(golden, []byte(b.String()), 0o644); err != nil {
			t.Fatalf("write golden: %v", err)
		}
		return
	}
	want, err := os.ReadFile(golden)
	if err != nil {
		t.Fatalf("read golden (run with MESSQ_UPDATE_GOLDEN=1 to seed): %v", err)
	}
	if string(want) != b.String() {
		t.Fatalf("route registry drifted from the golden:\n--- want\n%s\n+++ got\n%s", want, b.String())
	}
}

// TestEveryMutatingRouteIsRegistered fails when a route is Mutating in the registry but
// its handler never runs through the mux — i.e. someone wired a handler directly,
// bypassing routes(). Each wildcard is substituted with a name that cannot exist, so a
// reaching handler answers with ITS OWN error (store-level not_found names the stream),
// never the catch-all's "no such route".
func TestEveryRegisteredRouteReachesItsHandler(t *testing.T) {
	t.Parallel()

	clk := clock.NewFake(time.UnixMilli(1_700_000_000_000))
	st := openTestStore(t, clk, store.DurabilityFull)
	srv := New(Config{Store: st, Clock: clk, Logger: discardLogger()})
	handler := srv.Handler()

	sub := map[string]string{
		"{stream}":   "__absent_stream",
		"{consumer}": "__absent_consumer",
		"{seq}":      "1",
		"{id}":       "01J8ZQ4K2M9V0X7Y3B5N6C8D1E",
	}
	for _, rt := range srv.routes() {
		if rt.Method == "" {
			continue // the catch-all is exercised everywhere else
		}
		path := rt.Pattern
		for k, v := range sub {
			path = strings.ReplaceAll(path, k, v)
		}
		req := httptest.NewRequestWithContext(context.Background(), rt.Method, path, nil)
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)

		var env Envelope
		if err := json.Unmarshal(rec.Body.Bytes(), &env); err != nil {
			continue // a success shape (or non-JSON body) proves the real handler ran
		}
		if env.Error.Code == CodeNotFound && env.Error.Message == "no such route" {
			t.Errorf("%s %s (%s): answered from the catch-all — the handler is not wired", rt.Method, path, rt.Name)
		}
	}
}
