// SPDX-License-Identifier: Apache-2.0

package store

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/a-holm/messq/internal/clock"
	"github.com/a-holm/messq/internal/queue"
)

// Slice 11: settle crash windows (MESSQ_FAULTS store.settle.after_commit /
// before_reply, #8-harness conventions). The durable truths the ledger oracle scores:
//
//   - after_commit: the settle's commit is durable — a SIGKILL before the reply cannot
//     undo it. On restart the effect is present and a client retry resolves to stale
//     (the row is gone; never re-delivered). Ledger outcome OK.
//   - before_reply / before_commit: the client never saw the reply; both presence and
//     absence of the effect are legal until reconciled, and a retry must not double-apply
//     and must not invent a duplicate. Resolved by idempotent stale on retry.

// openCrashStore opens a single fresh store in its own dir with the default fake clock.
func openCrashStore(t *testing.T) (*Store, *clock.Fake) {
	t.Helper()
	opt := testOptions(filepath.Join(t.TempDir(), "data"), fakeClock(), &logCapture{})
	st, _, err := Open(context.Background(), opt)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	if _, _, err := st.CreateStream(context.Background(), queue.DefaultConfig("orders"), "test"); err != nil {
		t.Fatalf("create stream: %v", err)
	}
	fk, ok := st.clk.(*clock.Fake)
	if !ok {
		t.Fatalf("store clock is not *clock.Fake")
	}
	return st, fk
}

func seedOneClaimed(t *testing.T, st *Store) queue.Token {
	t.Helper()
	if _, err := st.Publish(context.Background(), PublishCmd{Stream: "orders", Req: queue.PublishReq{Subject: "orders.1", Body: []byte("x")}}); err != nil {
		t.Fatalf("publish: %v", err)
	}
	cfg := queue.DefaultConsumerConfig("worker")
	if _, err := st.CreateConsumer(context.Background(), "orders", cfg, queue.StartPosition{Kind: queue.StartFirst}, "test"); err != nil {
		t.Fatalf("create consumer: %v", err)
	}
	res, err := st.Fetch(context.Background(), FetchReq{Stream: "orders", Consumer: "worker", Batch: 1})
	if err != nil {
		t.Fatalf("fetch: %v", err)
	}
	tok, err := queue.ParseToken(res.Messages[0].AckToken)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	return tok
}

func TestSettleCrashAfterCommitIsDurable(t *testing.T) {
	ctx := context.Background()
	st, _ := openCrashStore(t)
	tok := seedOneClaimed(t, st)
	sr, err := st.Settle(ctx, settleCmd(SettleItem{Token: tok, Verb: queue.VerbAck}))
	if err != nil {
		t.Fatalf("ack: %v", err)
	}
	if sr.Results[0].Status != queue.ItemStatusOK {
		t.Fatalf("ack := %s, want ok", sr.Results[0].Status)
	}
	dir := st.dir
	if closeErr := st.Close(ctx); closeErr != nil {
		t.Fatalf("close: %v", closeErr)
	}
	// "SIGKILL after COMMIT, before reply": reopen and score the ledger.
	reopened, _, reopenErr := Open(ctx, testOptions(dir, fakeClock(), &logCapture{}))
	if reopenErr != nil {
		t.Fatalf("reopen: %v", reopenErr)
	}
	defer func() {
		if cErr := reopened.Close(ctx); cErr != nil {
			t.Logf("close reopened: %v", cErr)
		}
	}()
	if n := countRowsOn(reopened); n != 0 {
		t.Fatalf("after_commit crash left %d delivery rows; the delete must be durable", n)
	}
	// a retried ack resolves to stale (ledger OK: no duplicate, no redelivery).
	sr2, err := reopened.Settle(ctx, settleCmd(SettleItem{Token: tok, Verb: queue.VerbAck}))
	if err != nil {
		t.Fatalf("retry: %v", err)
	}
	if sr2.Results[0].Status != queue.ItemStatusStale {
		t.Fatalf("retry after_commit = %s, want stale (ledger OK)", sr2.Results[0].Status)
	}
}

func TestSettleCrashBeforeReplyRetryIsStale(t *testing.T) {
	ctx := context.Background()
	st, _ := openCrashStore(t)
	tok := seedOneClaimed(t, st)
	dir := st.dir
	// The reply is lost: the client settles but never receives the outcome. The batch
	// still commits (after_commit). On restart a retry must resolve idle/idempotent.
	if _, aexErr := st.Settle(ctx, settleCmd(SettleItem{Token: tok, Verb: queue.VerbAck})); aexErr != nil {
		t.Fatalf("ack: %v", aexErr)
	}
	if closeErr := st.Close(ctx); closeErr != nil {
		t.Fatalf("close: %v", closeErr)
	}
	reopened, _, reopenErr := Open(ctx, testOptions(dir, fakeClock(), &logCapture{}))
	if reopenErr != nil {
		t.Fatalf("reopen: %v", reopenErr)
	}
	defer func() {
		if cErr := reopened.Close(ctx); cErr != nil {
			t.Logf("close reopened: %v", cErr)
		}
	}()
	res2, err := reopened.Settle(ctx, settleCmd(SettleItem{Token: tok, Verb: queue.VerbAck}))
	if err != nil {
		t.Fatalf("retry: %v", err)
	}
	if res2.Results[0].Status != queue.ItemStatusStale {
		t.Fatalf("before_reply retry = %s, want stale (reconciliation resolves to OK)", res2.Results[0].Status)
	}
}

// countRowsOn counts delivery rows on a reopened store via its RO pool.
func countRowsOn(st *Store) int64 {
	var n int64
	if err := st.RO().QueryRowContext(context.Background(),
		`SELECT count(*) FROM deliveries`).Scan(&n); err != nil {
		return -1
	}
	return n
}
