// SPDX-License-Identifier: Apache-2.0

package api

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/a-holm/messq/internal/clock"
	"github.com/a-holm/messq/internal/ops/backup"
	"github.com/a-holm/messq/internal/queue"
	"github.com/a-holm/messq/internal/store"
)

// TestInfoRestoredObject closes #30 §5: after restoring a snapshot into a
// fresh directory, GET /v1/info carries the optional restored object; a fresh
// directory (TestInfoJSONKeys) must omit it entirely.
func TestInfoRestoredObject(t *testing.T) {
	ctx := context.Background()
	clk := clock.NewFake(time.Date(2026, 11, 4, 12, 0, 0, 0, time.UTC))

	src := filepath.Join(t.TempDir(), "data")
	st, _, openErr := store.Open(ctx, store.Options{DataDir: src})
	if openErr != nil {
		t.Fatalf("open: %v", openErr)
	}
	if _, _, crErr := st.CreateStream(ctx,
		queue.DefaultConfig("orders"), "orders"); crErr != nil {
		t.Fatalf("create stream: %v", crErr)
	}
	snapshotPath := filepath.Join(t.TempDir(), "snap.db")
	snapClock := clock.NewFake(time.Date(2026, 11, 4, 12, 5, 0, 0, time.UTC))
	plan, pErr := backup.Plan(ctx, backup.Options{
		DataDir: src, Dest: snapshotPath, Clock: snapClock,
	})
	if pErr != nil {
		t.Fatalf("plan backup: %v", pErr)
	}
	if _, rErr := backup.Run(ctx, plan); rErr != nil {
		t.Fatalf("run backup: %v", rErr)
	}
	if cErr := st.Close(ctx); cErr != nil {
		t.Fatalf("close: %v", cErr)
	}

	// Restore = stop + copy + start: same file lands as another dir's messq.db.
	dst := filepath.Join(t.TempDir(), "restored")
	if mErr := os.MkdirAll(dst, 0o700); mErr != nil {
		t.Fatalf("mkdir restored dir: %v", mErr)
	}
	body, rErr := os.ReadFile(snapshotPath)
	if rErr != nil {
		t.Fatalf("read snapshot: %v", rErr)
	}
	if wErr := os.WriteFile(filepath.Join(dst, "messq.db"), body, 0o600); wErr != nil {
		t.Fatalf("place restored db: %v", wErr)
	}

	rst, report, open2Err := store.Open(ctx, store.Options{DataDir: dst})
	if open2Err != nil {
		t.Fatalf("open restored dir: %v", open2Err)
	}
	defer func() {
		if cErr := rst.Close(ctx); cErr != nil {
			_ = cErr // close of a read-mostly store; the test is done regardless
		}
	}()
	prov := report.Restored
	if prov == nil {
		t.Fatal("opening the restored dir reported no provenance; fixture is wrong")
	}

	srv := New(Config{Store: rst, Clock: clk, Logger: discardLogger()})
	rec := httptest.NewRecorder()
	req := httptest.NewRequestWithContext(ctx, "GET", "/v1/info", nil)
	srv.Handler().ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d", rec.Code)
	}
	var got struct {
		Restored *struct {
			SnapshotAt   string           `json:"snapshot_at"`
			SourceNodeID string           `json:"source_node_id"`
			StreamHeads  map[string]int64 `json:"stream_heads"`
			ToolVersion  string           `json:"tool_version"`
		} `json:"restored"`
	}
	if uErr := json.Unmarshal(rec.Body.Bytes(), &got); uErr != nil {
		t.Fatalf("decode info body: %v\n%s", uErr, rec.Body.String())
	}
	if got.Restored == nil {
		t.Fatalf("restored object missing on a restored directory:\n%s", rec.Body.String())
	}
	if got.Restored.SourceNodeID != prov.SourceNodeID {
		t.Fatalf("source node mismatch: %+v vs %+v", got.Restored, prov)
	}
	if got.Restored.StreamHeads["orders"] != prov.StreamHeads["orders"] {
		t.Fatalf("stream heads mismatch: %+v vs %+v", got.Restored.StreamHeads, prov.StreamHeads)
	}
}
