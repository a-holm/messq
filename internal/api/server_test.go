// SPDX-License-Identifier: Apache-2.0

package api

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"runtime"
	"slices"
	"testing"
	"time"

	"github.com/a-holm/messq/internal/buildinfo"
	"github.com/a-holm/messq/internal/clock"
	"github.com/a-holm/messq/internal/queue"
	"github.com/a-holm/messq/internal/store"
)

// openTestStore opens a fresh store wired to the given clock. Cleanup closes it.
func openTestStore(t *testing.T, clk clock.Clock, dur store.Durability) *store.Store {
	t.Helper()
	dir := filepath.Join(t.TempDir(), "data")
	st, _, err := store.Open(context.Background(), store.Options{
		DataDir:    dir,
		Clock:      clk,
		Durability: dur,
		Logger:     slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() {
		if closeErr := st.Close(context.Background()); closeErr != nil {
			t.Logf("close store: %v", closeErr)
		}
	})
	return st
}

func discardLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// waitFor spins until cond holds or the attempt budget runs out. Tests may not Sleep;
// the engine's synchronisation points are channels, so a bounded Gosched loop is enough
// to let a background goroutine reach one (same idiom as internal/store's waitFor).
func waitFor(cond func() bool) {
	for i := 0; i < 200000 && !cond(); i++ {
		runtime.Gosched()
	}
}

func TestHealthz(t *testing.T) {
	clk := clock.NewFake(time.UnixMilli(1_700_000_000_000))
	srv := New(openTestStore(t, clk, store.DurabilityFull), clk, discardLogger(), time.Minute)

	rec := httptest.NewRecorder()
	req := httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/healthz", nil)
	srv.Handler().ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
}

func TestInfo(t *testing.T) {
	clk := clock.NewFake(time.UnixMilli(1_700_000_000_000))
	st := openTestStore(t, clk, store.DurabilityFull)
	srv := New(st, clk, discardLogger(), time.Minute)

	clk.Advance(1500 * time.Millisecond)

	rec := httptest.NewRecorder()
	req := httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/v1/info", nil)
	srv.Handler().ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200 (body %q)", rec.Code, rec.Body.String())
	}

	var got infoResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &got); err != nil {
		t.Fatalf("body is not JSON (%v): %q", err, rec.Body.String())
	}
	if got.Version != buildinfo.Get().Version {
		t.Errorf("version = %q, want %q", got.Version, buildinfo.Get().Version)
	}
	if got.NodeID != st.NodeID() {
		t.Errorf("node_id = %q, want %q", got.NodeID, st.NodeID())
	}
	if got.Durability != st.Durability().String() {
		t.Errorf("durability = %q, want %q", got.Durability, st.Durability().String())
	}
	if got.Synchronous != st.Durability().Synchronous() {
		t.Errorf("synchronous = %d, want %d", got.Synchronous, st.Durability().Synchronous())
	}
	if got.UptimeMS != 1500 {
		t.Errorf("uptime_ms = %d, want 1500", got.UptimeMS)
	}
	if got.DBBytes <= 0 {
		t.Errorf("db_bytes = %d, want > 0", got.DBBytes)
	}
}

// TestInfoJSONKeys pins the /v1/info wire shape: six fields, nothing more, nothing less.
func TestInfoJSONKeys(t *testing.T) {
	clk := clock.NewFake(time.UnixMilli(1_700_000_000_000))
	srv := New(openTestStore(t, clk, store.DurabilityFull), clk, discardLogger(), time.Minute)

	rec := httptest.NewRecorder()
	req := httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/v1/info", nil)
	srv.Handler().ServeHTTP(rec, req)

	var fields map[string]json.RawMessage
	if err := json.Unmarshal(rec.Body.Bytes(), &fields); err != nil {
		t.Fatalf("body is not JSON: %v", err)
	}
	got := make([]string, 0, len(fields))
	for k := range fields {
		got = append(got, k)
	}
	slices.Sort(got)

	want := []string{"db_bytes", "durability", "node_id", "synchronous", "uptime_ms", "version"}
	if !slices.Equal(got, want) {
		t.Errorf("info JSON keys = %v, want %v", got, want)
	}
}

// TestInfoSynchronousTracksDurability pins the live PRAGMA synchronous value: FULL is 2,
// NORMAL is 1. The store refuses to start when the read-back disagrees (ADR-0002), so this
// number is the verified live value, not the requested one.
func TestInfoSynchronousTracksDurability(t *testing.T) {
	for _, tc := range []struct {
		name string
		dur  store.Durability
		want int
	}{
		{"full", store.DurabilityFull, 2},
		{"relaxed", store.DurabilityRelaxed, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			clk := clock.NewFake(time.UnixMilli(1_700_000_000_000))
			srv := New(openTestStore(t, clk, tc.dur), clk, discardLogger(), time.Minute)

			rec := httptest.NewRecorder()
			req := httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/v1/info", nil)
			srv.Handler().ServeHTTP(rec, req)

			var got infoResponse
			if err := json.Unmarshal(rec.Body.Bytes(), &got); err != nil {
				t.Fatalf("decode: %v", err)
			}
			if got.Synchronous != tc.want {
				t.Errorf("synchronous = %d, want %d", got.Synchronous, tc.want)
			}
		})
	}
}

// publishOne stores a message with an idempotency key and returns the receipt.
func publishOne(t *testing.T, st *store.Store, msgID string) store.Ack {
	t.Helper()
	ack, err := st.Publish(context.Background(), store.PublishCmd{
		Stream: "orders",
		Req:    queue.PublishReq{Subject: "orders.eu.created", Body: []byte("x"), MsgID: msgID},
	})
	if err != nil {
		t.Fatalf("publish %q: %v", msgID, err)
	}
	return ack
}

func makeOrdersStream(t *testing.T, st *store.Store, dedupWindow time.Duration) {
	t.Helper()
	cfg := queue.DefaultConfig("orders")
	cfg.DedupWindow = dedupWindow
	if _, _, err := st.CreateStream(context.Background(), cfg, "slice-a"); err != nil {
		t.Fatalf("create stream: %v", err)
	}
}

// TestSweepOnce proves the sweep clears a key only after its stream's dedup window elapses,
// and that it iterates every stream rather than a hard-coded name.
func TestSweepOnce(t *testing.T) {
	clk := clock.NewFake(time.UnixMilli(1_700_000_000_000))
	st := openTestStore(t, clk, store.DurabilityRelaxed)
	srv := New(st, clk, discardLogger(), time.Minute)

	makeOrdersStream(t, st, 100*time.Millisecond)

	first := publishOne(t, st, "k1")
	if first.Duplicate {
		t.Fatalf("first publish was a duplicate")
	}

	// Before the window elapses the key survives a sweep: the retry stays a duplicate.
	srv.sweepOnce(context.Background())
	if again := publishOne(t, st, "k1"); !again.Duplicate || again.Seq != first.Seq {
		t.Fatalf("pre-expiry republish = seq %d dup %v, want the original receipt", again.Seq, again.Duplicate)
	}

	// Past the window the sweep clears the key, so the same msg_id allocates a new seq.
	clk.Advance(200 * time.Millisecond)
	srv.sweepOnce(context.Background())
	after := publishOne(t, st, "k1")
	if after.Duplicate {
		t.Fatalf("post-expiry republish was a duplicate: sweep did not clear the key")
	}
	if after.Seq != first.Seq+1 {
		t.Errorf("post-expiry seq = %d, want %d", after.Seq, first.Seq+1)
	}
}

// TestSweepOnceEveryStream proves the sweep is per-stream, not a hard-coded name: two streams
// each lose their expired key in one sweep call.
func TestSweepOnceEveryStream(t *testing.T) {
	clk := clock.NewFake(time.UnixMilli(1_700_000_000_000))
	st := openTestStore(t, clk, store.DurabilityRelaxed)
	srv := New(st, clk, discardLogger(), time.Minute)

	for _, name := range []string{"orders", "shipments"} {
		cfg := queue.DefaultConfig(name)
		cfg.DedupWindow = 100 * time.Millisecond
		if _, _, err := st.CreateStream(context.Background(), cfg, "slice-a"); err != nil {
			t.Fatalf("create %s: %v", name, err)
		}
	}
	publish := func(stream, msgID string) store.Ack {
		t.Helper()
		ack, err := st.Publish(context.Background(), store.PublishCmd{
			Stream: stream,
			Req:    queue.PublishReq{Subject: "x.y", Body: []byte("x"), MsgID: msgID},
		})
		if err != nil {
			t.Fatalf("publish %s/%s: %v", stream, msgID, err)
		}
		return ack
	}
	orders := publish("orders", "k1")
	shipments := publish("shipments", "k2")

	clk.Advance(200 * time.Millisecond)
	srv.sweepOnce(context.Background())

	againOrders := publish("orders", "k1")
	againShipments := publish("shipments", "k2")
	if againOrders.Duplicate || againOrders.Seq != orders.Seq+1 {
		t.Errorf("orders key not swept: seq %d dup %v", againOrders.Seq, againOrders.Duplicate)
	}
	if againShipments.Duplicate || againShipments.Seq != shipments.Seq+1 {
		t.Errorf("shipments key not swept: seq %d dup %v", againShipments.Seq, againShipments.Duplicate)
	}
}

// TestServeSweepTickerAndShutdown proves the ticker is armed through the clock seam, a tick
// drives a real sweep, and cancellation shuts the ticker down cleanly.
func TestServeSweepTickerAndShutdown(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	clk := clock.NewFake(time.UnixMilli(1_700_000_000_000))
	st := openTestStore(t, clk, store.DurabilityRelaxed)
	srv := New(st, clk, discardLogger(), time.Minute)

	makeOrdersStream(t, st, 50*time.Millisecond)
	first := publishOne(t, st, "k1")

	ln, err := (&net.ListenConfig{}).Listen(context.Background(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}

	done := make(chan error, 1)
	go func() { done <- srv.Serve(ctx, ln) }()

	// The sweep ticker is armed exactly once, through the clock seam.
	clk.BlockUntil(1)

	// Advance past the dedup window so the first tick sweeps the key.
	clk.Advance(2 * time.Minute)

	// The sweep's effect is observable through the store: a republish of the same key
	// now allocates a new seq instead of returning the original receipt.
	waitFor(func() bool {
		again, perr := st.Publish(context.Background(), store.PublishCmd{
			Stream: "orders",
			Req:    queue.PublishReq{Subject: "orders.eu.created", Body: []byte("x"), MsgID: "k1"},
		})
		return perr == nil && !again.Duplicate && again.Seq == first.Seq+1
	})

	cancel()
	if err := <-done; err != nil {
		t.Fatalf("Serve returned %v, want clean shutdown", err)
	}
	if clk.Armed() != 0 {
		t.Errorf("ticker still armed after shutdown: %d waiters", clk.Armed())
	}
}
