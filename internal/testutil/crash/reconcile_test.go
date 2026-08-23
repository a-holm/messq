// SPDX-License-Identifier: Apache-2.0

package crash

import (
	"context"
	"crypto/sha256"
	"io"
	"log/slog"
	"path/filepath"
	"testing"
	"time"

	"github.com/a-holm/messq/internal/queue"
	"github.com/a-holm/messq/internal/store"
	"github.com/a-holm/messq/internal/testutil/ledger"
	"github.com/a-holm/messq/internal/testutil/loadgen"
)

// hasRule reports whether vs contains a violation of the named rule for the given key.
func hasRule(vs []Violation, rule, key string) bool {
	for _, v := range vs {
		if v.Rule == rule && v.Key == key {
			return true
		}
	}
	return false
}

// okRec is a shorthand for a reconciled OK record.
func okRec(key string, seq int64, id string, size int, dup bool) ledger.Record {
	return ledger.Record{Key: key, Stream: "crash", Verdict: ledger.OK, Seq: seq, ID: id, Size: size, Duplicate: dup}
}

// TestReconcileOKLost proves OK-LOST fires when an OK record's (stream, seq) is absent, and
// stays silent when the message is present with the same id.
func TestReconcileOKLost(t *testing.T) {
	state := &StateSnapshot{msgAt: map[seqKey]Message{}, dedup: map[string]seqKey{}, next: map[string]int64{"crash": 2}, maxSeq: map[string]int64{}}
	recs := map[string]ledger.Record{"K1": okRec("K1", 1, "id1", 8, false)}
	if vs := Reconcile(state, recs, "crash", 0); !hasRule(vs, "OK-LOST", "K1") {
		t.Fatalf("OK-LOST did not fire for a missing message: %+v", vs)
	}

	state.msgAt[seqKey{"crash", 1}] = Message{ID: "id1", Size: 8, BodySHA: sha256.Sum256(loadgen.Payload("K1", 8))}
	if vs := Reconcile(state, recs, "crash", 0); len(vs) != 0 {
		t.Fatalf("present-and-intact OK record reported violations: %+v", vs)
	}
}

// TestReconcileOKCorrupt proves a flipped body byte is caught: the size matches but the
// recovered hash differs from Payload(key, size).
func TestReconcileOKCorrupt(t *testing.T) {
	state := &StateSnapshot{msgAt: map[seqKey]Message{
		{"crash", 1}: {ID: "id1", Size: 8, BodySHA: sha256.Sum256([]byte("XXXXXXXX"))},
	}, dedup: map[string]seqKey{}, next: map[string]int64{"crash": 2}, maxSeq: map[string]int64{"crash": 1}}
	recs := map[string]ledger.Record{"K1": okRec("K1", 1, "id1", 8, false)}
	if vs := Reconcile(state, recs, "crash", 0); !hasRule(vs, "OK-CORRUPT", "K1") {
		t.Fatalf("OK-CORRUPT did not fire for a flipped body: %+v", vs)
	}
}

// TestReconcileFailedPresent proves a FAILED key that nevertheless appears in state fires
// FAILED-PRESENT.
func TestReconcileFailedPresent(t *testing.T) {
	state := &StateSnapshot{msgAt: map[seqKey]Message{}, dedup: map[string]seqKey{"K1": {"crash", 1}}, next: map[string]int64{"crash": 2}, maxSeq: map[string]int64{"crash": 1}}
	recs := map[string]ledger.Record{"K1": {Key: "K1", Stream: "crash", Verdict: ledger.Failed, Status: 413, Code: "too_large"}}
	if vs := Reconcile(state, recs, "crash", 0); !hasRule(vs, "FAILED-PRESENT", "K1") {
		t.Fatalf("FAILED-PRESENT did not fire: %+v", vs)
	}
}

// TestReconcileGhost proves a message whose dedup key the ledger never recorded fires GHOST.
func TestReconcileGhost(t *testing.T) {
	state := &StateSnapshot{msgAt: map[seqKey]Message{}, dedup: map[string]seqKey{"ghost-key": {"crash", 1}}, next: map[string]int64{"crash": 2}, maxSeq: map[string]int64{"crash": 1}}
	recs := map[string]ledger.Record{}
	if vs := Reconcile(state, recs, "crash", 0); !hasRule(vs, "GHOST", "ghost-key") {
		t.Fatalf("GHOST did not fire: %+v", vs)
	}
}

// TestReconcileSeqCollision proves two ledger keys mapping to the same (stream, seq) fire
// SEQ-COLLISION.
func TestReconcileSeqCollision(t *testing.T) {
	state := &StateSnapshot{msgAt: map[seqKey]Message{
		{"crash", 1}: {ID: "id1", Size: 8, BodySHA: sha256.Sum256(loadgen.Payload("K1", 8))},
	}, dedup: map[string]seqKey{}, next: map[string]int64{"crash": 2}, maxSeq: map[string]int64{"crash": 1}}
	recs := map[string]ledger.Record{
		"K1": okRec("K1", 1, "id1", 8, false),
		"K2": okRec("K2", 1, "id1", 8, false),
	}
	vs := Reconcile(state, recs, "crash", 0)
	if !hasRule(vs, "SEQ-COLLISION", "K1") && !hasRule(vs, "SEQ-COLLISION", "K2") {
		t.Fatalf("SEQ-COLLISION did not fire: %+v", vs)
	}
}

// TestReconcileSeqRegression proves a probe seq that does not exceed the pre-crash maximum
// fires SEQ-REGRESSION.
func TestReconcileSeqRegression(t *testing.T) {
	state := &StateSnapshot{msgAt: map[seqKey]Message{}, dedup: map[string]seqKey{}, next: map[string]int64{"crash": 6}, maxSeq: map[string]int64{"crash": 10}}
	if vs := Reconcile(state, map[string]ledger.Record{}, "crash", 5); !hasRule(vs, "SEQ-REGRESSION", "crash") {
		t.Fatalf("SEQ-REGRESSION did not fire for probe 5 <= max 10: %+v", vs)
	}
	if vs := Reconcile(state, map[string]ledger.Record{}, "crash", 11); len(vs) != 0 {
		t.Fatalf("probe 11 > max 10 reported violations: %+v", vs)
	}
}

// TestReconcileDupInconsistent proves a duplicate:true response pointing at a message with
// a different dedup key fires DUP-INCONSISTENT.
func TestReconcileDupInconsistent(t *testing.T) {
	state := &StateSnapshot{msgAt: map[seqKey]Message{
		{"crash", 1}: {ID: "id1", Size: 8, BodySHA: sha256.Sum256(loadgen.Payload("K1", 8)), DedupKey: "other-key"},
	}, dedup: map[string]seqKey{"other-key": {"crash", 1}}, next: map[string]int64{"crash": 2}, maxSeq: map[string]int64{"crash": 1}}
	recs := map[string]ledger.Record{"K1": okRec("K1", 1, "id1", 8, true)}
	if vs := Reconcile(state, recs, "crash", 0); !hasRule(vs, "DUP-INCONSISTENT", "K1") {
		t.Fatalf("DUP-INCONSISTENT did not fire: %+v", vs)
	}
}

// TestLoadStateRoundTrip proves the snapshot loader reads a real data dir: message id/size/
// body-hash/dedup-key, the seq allocator, and the per-stream maximum.
func TestLoadStateRoundTrip(t *testing.T) {
	ctx := context.Background()
	// The store enforces a 0700 data dir; t.TempDir() itself is not guaranteed 0700, so the
	// data dir is a subdirectory the store creates (0700), matching the store's own tests.
	dir := filepath.Join(t.TempDir(), "data")
	st, _, err := store.Open(ctx, store.Options{
		DataDir:    dir,
		Durability: store.DurabilityFull,
		Logger:     slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	streamCfg := queue.DefaultConfig("crash")
	streamCfg.DedupWindow = 24 * time.Hour
	streamCfg.MaxMsgSize = 1 << 20
	if _, _, createErr := st.CreateStream(ctx, streamCfg, "test"); createErr != nil {
		t.Fatalf("create stream: %v", createErr)
	}
	key := "01J0AAAAAAAAAAAAAAAAAAAAAA"
	body := loadgen.Payload(key, 8)
	ack, err := st.Publish(ctx, store.PublishCmd{Stream: "crash", Req: queue.PublishReq{
		Subject: "crash.a", Body: body, MsgID: key,
	}})
	if err != nil {
		t.Fatalf("publish: %v", err)
	}
	if closeErr := st.Close(ctx); closeErr != nil {
		t.Fatalf("close store: %v", closeErr)
	}

	state, err := LoadState(ctx, dir)
	if err != nil {
		t.Fatalf("LoadState: %v", err)
	}
	m, ok := state.msgAt[seqKey{"crash", ack.Seq}]
	if !ok {
		t.Fatalf("message at (crash, %d) not loaded", ack.Seq)
	}
	if m.ID != ack.ID || m.Size != len(body) {
		t.Errorf("message = id %q size %d, want id %q size %d", m.ID, m.Size, ack.ID, len(body))
	}
	if m.BodySHA != sha256.Sum256(body) {
		t.Errorf("body hash mismatch")
	}
	if m.DedupKey != key {
		t.Errorf("dedup key = %q, want %q", m.DedupKey, key)
	}
	if state.dedup[key] != (seqKey{"crash", ack.Seq}) {
		t.Errorf("dedup index = %+v, want (crash, %d)", state.dedup[key], ack.Seq)
	}
	if state.next["crash"] != ack.Seq+1 {
		t.Errorf("next = %d, want %d", state.next["crash"], ack.Seq+1)
	}
	if state.maxSeq["crash"] != ack.Seq {
		t.Errorf("maxSeq = %d, want %d", state.maxSeq["crash"], ack.Seq)
	}
}
