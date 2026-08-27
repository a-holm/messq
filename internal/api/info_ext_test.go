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

// Issue #15 §2: /v1/info is the incident console — live pragma readback for
// durability.synchronous, syscall-backed storage fields cached behind --info-cache,
// row counts, listeners and state. uptime_ms and state are NEVER cached.

// infoCountsShape is the JSON subset the cache tests decode.
type infoCountsShape struct {
	UptimeMS int64 `json:"uptime_ms"`
	Counts   struct {
		Streams    int64 `json:"streams"`
		Consumers  int64 `json:"consumers"`
		EventsRows int64 `json:"events_rows"`
	} `json:"counts"`
}

func decodeInfoCounts(t *testing.T, rec *httptest.ResponseRecorder) infoCountsShape {
	t.Helper()
	var out infoCountsShape
	if err := json.Unmarshal(rec.Body.Bytes(), &out); err != nil {
		t.Fatalf("body not JSON (%v): %s", err, rec.Body.String())
	}
	return out
}

// mustCreateStreamAPI creates one stream through the API surface so the info cache's
// staleness logic has something real to hide and reveal.
func mustCreateStreamAPI(t *testing.T, h http.Handler, name string) {
	t.Helper()
	req := httptest.NewRequestWithContext(context.Background(), http.MethodPost,
		"/v1/streams", strings.NewReader(`{"name":"`+name+`","subjects":["`+name+`.>"]}`))
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusCreated && rec.Code != http.StatusOK {
		t.Fatalf("create stream %s: status %d body %s", name, rec.Code, rec.Body.String())
	}
}

// TestInfoSynchronousIsLiveReadback pins that the wire value comes from a pooled
// connection, not from a config copy: two stores opened with different flags report
// different values through ONE readback path — what SQLite is actually enforcing.
func TestInfoSynchronousIsLiveReadback(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		dur  store.Durability
		want int
	}{{store.DurabilityFull, 2}, {store.DurabilityRelaxed, 1}} {
		clk := clock.NewFake(time.UnixMilli(1_700_000_000_000))
		st := openTestStore(t, clk, tc.dur)

		live, err := st.LiveSynchronous(context.Background())
		if err != nil {
			t.Fatalf("%s: live synchronous: %v", tc.dur, err)
		}
		if live != tc.want {
			t.Errorf("%s: LiveSynchronous = %d, want %d", tc.dur, live, tc.want)
		}

		rec := get(t, New(Config{Store: st, Clock: clk, Logger: discardLogger()}).Handler(), "/v1/info")
		var body struct {
			Synchronous int `json:"synchronous"`
		}
		if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
			t.Fatalf("body not JSON (%v): %s", err, rec.Body.String())
		}
		if body.Synchronous != tc.want {
			t.Errorf("durability %s: /v1/info synchronous = %d, want the live %d", tc.dur, body.Synchronous, tc.want)
		}
	}
}

// TestInfoCacheWindows: counts are cached for --info-cache; a stream created inside
// the window does NOT move counts.streams until the clock passes expiry, then it does.
// uptime_ms advances across the same window because it is never cached.
func TestInfoCacheWindows(t *testing.T) {
	t.Parallel()

	base := time.UnixMilli(1_700_000_000_000)
	clk := clock.NewFake(base)
	st := openTestStore(t, clk, store.DurabilityFull)
	orders := queue.DefaultConfig("orders")
	mustCreateStream(t, st, orders)
	srv2 := New(Config{
		Store: st, Clock: clk, Logger: discardLogger(),
		InfoCacheTTL: 5 * time.Second,
	})
	h2 := srv2.Handler()

	before := decodeInfoCounts(t, get(t, h2, "/v1/info"))
	if before.Counts.Streams != 1 {
		t.Fatalf("pre-create prime saw %d streams, want 1 (orders exists)", before.Counts.Streams)
	}

	mustCreateStreamAPI(t, h2, "archive")
	inside := decodeInfoCounts(t, get(t, h2, "/v1/info"))
	if inside.Counts.Streams != 1 {
		t.Fatalf("streams count moved inside cache window: %+v", inside.Counts)
	}
	if inside.UptimeMS < before.UptimeMS {
		t.Fatalf("uptime_ms non-monotonic inside window: %d -> %d", before.UptimeMS, inside.UptimeMS)
	}

	clk.Advance(10 * time.Second) // past expiry
	after := decodeInfoCounts(t, get(t, h2, "/v1/info"))
	if after.Counts.Streams != 2 {
		t.Fatalf("streams count stayed stale after --info-cache expiry: %+v", after.Counts)
	}
}

// TestInfoNewFieldsPresent: schema_version, started_at_ms, wal_bytes, disk_free_bytes,
// node_id, version, listeners and state all ride the extended shape.
func TestInfoNewFieldsPresent(t *testing.T) {
	t.Parallel()

	base := time.UnixMilli(1_700_000_000_000)
	clk := clock.NewFake(base)
	st := openTestStore(t, clk, store.DurabilityFull)
	srv := New(Config{
		Store: st, Clock: clk, Logger: discardLogger(),
		Listeners: []string{"unix:///run/test/messq.sock"},
	})
	rec := get(t, srv.Handler(), "/v1/info")

	var body struct {
		SchemaVersion int64    `json:"schema_version"`
		StartedAtMS   int64    `json:"started_at_ms"`
		WALBytes      int64    `json:"wal_bytes"`
		DiskFreeBytes int64    `json:"disk_free_bytes"`
		NodeID        string   `json:"node_id"`
		Version       string   `json:"version"`
		Listeners     []string `json:"listeners"`
		State         string   `json:"state"`
		Degraded      []struct {
			Kind string `json:"kind"`
		} `json:"degraded"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("body not JSON (%v): %s", err, rec.Body.String())
	}
	if body.SchemaVersion == 0 {
		t.Error("schema_version missing or zero")
	}
	if body.StartedAtMS != base.UnixMilli() {
		t.Errorf("started_at_ms = %d, want %d", body.StartedAtMS, base.UnixMilli())
	}
	if body.DiskFreeBytes <= 0 {
		t.Errorf("disk_free_bytes = %d, want > 0 on a real filesystem", body.DiskFreeBytes)
	}
	if body.NodeID == "" || body.Version == "" {
		t.Errorf("node_id/version empty: %q/%q", body.NodeID, body.Version)
	}
	if len(body.Listeners) != 1 || body.Listeners[0] != "unix:///run/test/messq.sock" {
		t.Errorf("listeners = %v", body.Listeners)
	}
	if body.State != "ready" {
		t.Errorf("state = %q, want ready", body.State)
	}
	if body.Degraded == nil {
		t.Error("degraded must marshal as an array, never null")
	}
}
