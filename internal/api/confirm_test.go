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

// mustCreateConsumer inserts one consumer with defaults plus a first-start cursor.
func mustCreateConsumer(t *testing.T, st *store.Store, stream, name string) {
	t.Helper()
	cfg := queue.DefaultConsumerConfig(name)
	start, err := queue.ParseStartPosition("first")
	if err != nil {
		t.Fatalf("parse first: %v", err)
	}
	if _, err := st.CreateConsumer(context.Background(), stream, cfg, start, "test"); err != nil {
		t.Fatalf("create consumer %s/%s: %v", stream, name, err)
	}
}

// Issue #15 §12 edge cases + acceptance: DELETE without ?confirm= is 409
// confirm_required stating the BLAST RADIUS and the exact command; a mismatched
// confirm is 409 confirm_mismatch naming BOTH values; ?dry_run=1 on a route that does
// not declare it is 400 dry_run_unsupported, never silently ignored. The registry now
// declares Roles/DryRun/Confirm and the consistency tests make a bad row unbuildable.

func TestDeleteWithoutConfirmStatesBlastRadius(t *testing.T) {
	t.Parallel()

	clk := clock.NewFake(time.UnixMilli(1_700_000_000_000))
	st := openTestStore(t, clk, store.DurabilityFull)
	mustCreateStream(t, st, queue.DefaultConfig("orders"))
	mustPublish(t, st, "orders", "orders.eu.created", "m1", "")
	mustPublish(t, st, "orders", "orders.eu.created", "m2", "")

	srv := New(Config{Store: st, Clock: clk, Logger: discardLogger()})

	del := httptest.NewRequestWithContext(context.Background(), http.MethodDelete,
		"/v1/streams/orders", nil)
	drec := httptest.NewRecorder()
	srv.Handler().ServeHTTP(drec, del)

	if drec.Code != http.StatusConflict {
		t.Fatalf("status = %d, want 409 confirm_required (body %s)", drec.Code, drec.Body.String())
	}
	var env Envelope
	if err := json.Unmarshal(drec.Body.Bytes(), &env); err != nil {
		t.Fatalf("not an envelope (%v): %s", err, drec.Body.String())
	}
	if env.Error.Code != CodeConfirmRequired {
		t.Errorf("code = %q, want confirm_required", env.Error.Code)
	}
	if !strings.Contains(env.Error.Message, "orders") {
		t.Errorf("blast-radius message does not name the stream: %q", env.Error.Message)
	}
	next := strings.Join(env.Error.Next, "\n")
	for _, want := range []string{"--confirm orders", "--dry-run"} {
		if !strings.Contains(next, want) {
			t.Errorf("next %q does not offer %q", next, want)
		}
	}
}

func TestDeleteWithMismatchedConfirmNamesBoth(t *testing.T) {
	t.Parallel()

	clk := clock.NewFake(time.UnixMilli(1_700_000_000_000))
	st := openTestStore(t, clk, store.DurabilityFull)
	mustCreateStream(t, st, queue.DefaultConfig("orders"))
	srv := New(Config{Store: st, Clock: clk, Logger: discardLogger()})

	req := httptest.NewRequestWithContext(context.Background(), http.MethodDelete,
		"/v1/streams/orders?confirm=other", nil)
	rec := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rec, req)

	if rec.Code != http.StatusConflict {
		t.Fatalf("status = %d, want 409 confirm_mismatch (body %s)", rec.Code, rec.Body.String())
	}
	var env Envelope
	if err := json.Unmarshal(rec.Body.Bytes(), &env); err != nil {
		t.Fatalf("not an envelope (%v): %s", err, rec.Body.String())
	}
	if env.Error.Code != CodeConfirmMismatch {
		t.Errorf("code = %q, want confirm_mismatch", env.Error.Code)
	}
	msg := env.Error.Message
	if !strings.Contains(msg, "other") || !strings.Contains(msg, "orders") {
		t.Errorf("message must name both values: %q", msg)
	}
}

func TestConsumerDeleteConfirmCodes(t *testing.T) {
	t.Parallel()

	clk := clock.NewFake(time.UnixMilli(1_700_000_000_000))
	st := openTestStore(t, clk, store.DurabilityFull)
	mustCreateStream(t, st, queue.DefaultConfig("orders"))
	mustCreateConsumer(t, st, "orders", "worker")
	srv := New(Config{Store: st, Clock: clk, Logger: discardLogger()})

	// Missing confirm: the blast radius is the dropped delivery rows.
	req := httptest.NewRequestWithContext(context.Background(), http.MethodDelete,
		"/v1/streams/orders/consumers/worker", nil)
	rec := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rec, req)
	var env Envelope
	if err := json.Unmarshal(rec.Body.Bytes(), &env); err != nil {
		t.Fatalf("not an envelope (%v): %s", err, rec.Body.String())
	}
	if rec.Code != http.StatusConflict || env.Error.Code != CodeConfirmRequired {
		t.Fatalf("missing confirm: status=%d code=%q, want 409 confirm_required",
			rec.Code, env.Error.Code)
	}

	// Wrong name: mismatch names both values.
	req2 := httptest.NewRequestWithContext(context.Background(), http.MethodDelete,
		"/v1/streams/orders/consumers/worker?confirm=nope", nil)
	rec2 := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rec2, req2)
	var env2 Envelope
	if err := json.Unmarshal(rec2.Body.Bytes(), &env2); err != nil {
		t.Fatalf("not an envelope (%v): %s", err, rec2.Body.String())
	}
	if rec2.Code != http.StatusConflict || env2.Error.Code != CodeConfirmMismatch {
		t.Fatalf("bad confirm: status=%d code=%q, want 409 confirm_mismatch",
			rec2.Code, env2.Error.Code)
	}
	if !strings.Contains(env2.Error.Message, "worker") || !strings.Contains(env2.Error.Message, "nope") {
		t.Errorf("mismatch message names both values: %q", env2.Error.Message)
	}
}

func TestDryRunOnUndeclaredRouteIsUnsupported(t *testing.T) {
	t.Parallel()

	clk := clock.NewFake(time.UnixMilli(1_700_000_000_000))
	st := openTestStore(t, clk, store.DurabilityFull)
	mustCreateStream(t, st, queue.DefaultConfig("orders"))
	srv := New(Config{Store: st, Clock: clk, Logger: discardLogger()})

	req := httptest.NewRequestWithContext(context.Background(), http.MethodDelete,
		"/v1/streams/orders?confirm=orders&dry_run=1", nil)
	rec := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400 (body %s)", rec.Code, rec.Body.String())
	}
	var env Envelope
	if err := json.Unmarshal(rec.Body.Bytes(), &env); err != nil {
		t.Fatalf("not an envelope (%v): %s", err, rec.Body.String())
	}
	if env.Error.Code != CodeDryRunUnsupported {
		t.Errorf("code = %q, want dry_run_unsupported", env.Error.Code)
	}
	// The mutant this kills: delete stream ignored the preview flag and destroyed data.
	if _, err := st.GetStream(context.Background(), "orders"); err != nil {
		t.Fatalf("stream was deleted despite unsupported dry run: %v", err)
	}
}

// TestEveryMutatingRouteDeclaresRole: a Mutates route with an empty role set fails —
// the hook #16 turns into enforcement once bearer tokens land.
func TestEveryMutatingRouteDeclaresRole(t *testing.T) {
	t.Parallel()

	for _, rt := range (*Server)(nil).routes() {
		if rt.Mutating && rt.Roles.Empty() {
			t.Errorf("%s %s mutates but declares no roles", rt.Method, rt.Pattern)
		}
		if rt.Confirm != "" && rt.Confirm != "stream" && rt.Confirm != "consumer" {
			t.Errorf("%s %s declares Confirm=%q, want \"\", \"stream\" or \"consumer\"",
				rt.Method, rt.Pattern, rt.Confirm)
		}
		if rt.Confirm != "" && !rt.Mutating {
			t.Errorf("%s %s declares Confirm but is not mutating", rt.Method, rt.Pattern)
		}
	}
}

// TestUnauthenticatedProbesOnly: exactly healthz and readyz declare the empty role set;
// every other route requires something, so "useless for fingerprinting" stays bounded.
func TestUnauthenticatedProbesOnly(t *testing.T) {
	t.Parallel()

	wantNone := map[string]bool{"/healthz": true, "/readyz": true}
	for _, rt := range (*Server)(nil).routes() {
		if rt.Method == "" {
			continue // the catch-all answers unmatched paths before auth ever runs
		}
		if rt.Roles.Empty() && !wantNone[rt.Pattern] {
			t.Errorf("%s %s is unauthenticated but is not a probe", rt.Method, rt.Pattern)
		}
	}
}
