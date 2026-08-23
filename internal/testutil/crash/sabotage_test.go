// SPDX-License-Identifier: Apache-2.0

package crash

import (
	"context"
	"io"
	"log/slog"
	"path/filepath"
	"testing"

	"github.com/a-holm/messq/internal/queue"
	"github.com/a-holm/messq/internal/store"
	"github.com/a-holm/messq/internal/testutil/ledger"
	"github.com/a-holm/messq/internal/testutil/loadgen"
)

// The self-sabotage battery: the oracle and the reconciler must bite. Each case plants one
// defect — a daemon that acks without persisting, a message that appears with no ledger
// record, a rewound allocator, a forged duplicate, a denied-but-present key — and asserts
// the exact violation rule fires. The reverse direction (a healthy state yields no
// violation) is TestReconcile's clean case, so every rule is proven in both directions.

// sabotageFixture builds a real store with two published messages and returns its state and
// the matching OK ledger records, so each sabotage starts from a clean, green join.
func sabotageFixture(t *testing.T) (*StateSnapshot, map[string]ledger.Record, string) {
	t.Helper()
	ctx := context.Background()
	dir := filepath.Join(t.TempDir(), "data")
	st, _, err := store.Open(ctx, store.Options{
		DataDir:    dir,
		Durability: store.DurabilityFull,
		Logger:     slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	if _, _, createErr := st.CreateStream(ctx, queue.DefaultConfig("orders"), "test"); createErr != nil {
		t.Fatalf("create stream: %v", createErr)
	}
	recs := make(map[string]ledger.Record)
	for _, key := range []string{"K1", "K2"} {
		size := 32
		body := loadgen.Payload(key, size)
		out, pubErr := st.Publish(ctx, store.PublishCmd{
			Stream: "orders",
			Req:    queue.PublishReq{Subject: "orders.a", Body: body, MsgID: key},
		})
		if pubErr != nil {
			t.Fatalf("publish %s: %v", key, pubErr)
		}
		recs[key] = ledger.Record{
			Key: key, Stream: "orders", Seq: out.Seq, ID: out.ID, Size: size,
			Verdict: ledger.OK,
		}
	}
	if closeErr := st.Close(ctx); closeErr != nil {
		t.Fatalf("close store: %v", closeErr)
	}
	state, loadErr := LoadState(ctx, dir)
	if loadErr != nil {
		t.Fatalf("load state: %v", loadErr)
	}
	return state, recs, dir
}

// TestSabotageAckedThenLost plants an acknowledged message that never persisted: the OK
// ledger record survives but the message row is gone. The reconciler must report OK-LOST.
func TestSabotageAckedThenLost(t *testing.T) {
	state, recs, _ := sabotageFixture(t)
	delete(state.msgAt, seqKey{stream: "orders", seq: 1})
	vs := Reconcile(state, recs, "orders", 0)
	if !hasRule(vs, "OK-LOST", "K1") {
		t.Fatalf("acked-then-lost did not fire OK-LOST; got %+v", vs)
	}
}

// TestSabotageGhost plants a message in the state that the ledger never recorded. The
// reconciler must report GHOST.
func TestSabotageGhost(t *testing.T) {
	state, recs, _ := sabotageFixture(t)
	state.dedup["ghost-key"] = seqKey{stream: "orders", seq: 3}
	vs := Reconcile(state, recs, "orders", 0)
	if !hasRule(vs, "GHOST", "ghost-key") {
		t.Fatalf("unrecorded message did not fire GHOST; got %+v", vs)
	}
}

// TestSabotageSeqRegression rewinds the allocator below the pre-crash maximum and probes:
// the durable, gap-free allocator's promise is broken. The reconciler must report
// SEQ-REGRESSION.
func TestSabotageSeqRegression(t *testing.T) {
	state, recs, _ := sabotageFixture(t)
	if vs := Reconcile(state, recs, "orders", 1); !hasRule(vs, "SEQ-REGRESSION", "orders") {
		t.Fatalf("rewound allocator did not fire SEQ-REGRESSION; got %+v", vs)
	}
}

// TestSabotageForgedDuplicate plants a ledger record that claims duplicate but whose
// persisted dedup key points elsewhere. The reconciler must report DUP-INCONSISTENT.
func TestSabotageForgedDuplicate(t *testing.T) {
	state, recs, _ := sabotageFixture(t)
	// The daemon claims duplicate:true, but the persisted message's dedup key points at a
	// different idempotency key — a forged duplicate response.
	sk := seqKey{stream: "orders", seq: 2}
	m := state.msgAt[sk]
	m.DedupKey = "forged-other"
	state.msgAt[sk] = m
	r := recs["K2"]
	r.Duplicate = true
	recs["K2"] = r
	vs := Reconcile(state, recs, "orders", 0)
	if !hasRule(vs, "DUP-INCONSISTENT", "K2") {
		t.Fatalf("forged duplicate did not fire DUP-INCONSISTENT; got %+v", vs)
	}
}

// TestSabotageDeniedButPresent plants a FAILED ledger record whose key is nevertheless in
// the recovered state. The reconciler must report FAILED-PRESENT.
func TestSabotageDeniedButPresent(t *testing.T) {
	state, recs, _ := sabotageFixture(t)
	recs["K1"] = ledger.Record{Key: "K1", Stream: "orders", Verdict: ledger.Failed}
	vs := Reconcile(state, recs, "orders", 0)
	if !hasRule(vs, "FAILED-PRESENT", "K1") {
		t.Fatalf("denied-but-present did not fire FAILED-PRESENT; got %+v", vs)
	}
}

// TestSabotageCleanReverse is the reverse direction: a healthy state and a faithful ledger
// produce no violation at all, so every rule above is a real detection, not a default.
func TestSabotageCleanReverse(t *testing.T) {
	state, recs, _ := sabotageFixture(t)
	if vs := Reconcile(state, recs, "orders", 3); len(vs) != 0 {
		t.Fatalf("healthy join reported violations: %+v", vs)
	}
}
