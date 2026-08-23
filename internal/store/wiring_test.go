// SPDX-License-Identifier: Apache-2.0

package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/a-holm/messq/internal/errs"
	"github.com/a-holm/messq/internal/queue"
)

// The #7 wiring's own surfaces: the Cmd vocabulary, the engine-less runSolo path,
// and the delete/reap lifecycle that survives a restart. These pin the semantics the
// engine shares with the fallback — same rejection shapes, same marker protocol —
// independently of which door executed a command.

func TestCmdKindVocabulary(t *testing.T) {
	// PLAN §9.2's closed set, as spelled by the queue commands' log labels. A rename
	// here is a breaking change for every log-based consumer; pin it.
	cases := []struct {
		kind CmdKind
		want string
	}{
		{createStreamCmd{}.Kind(), "stream.create"},
		{updateStreamCmd{}.Kind(), "stream.update"},
		{deleteStreamCmd{}.Kind(), "stream.delete"},
		{reapChunkCmd{}.Kind(), "stream.delete.reap"},
		{publishWriteCmd{}.Kind(), "msg.publish"},
		{batchPublishCmd{}.Kind(), "msg.publish.batch"},
		{sweepDedupCmd{}.Kind(), "dedup.sweep"},
	}
	for _, tc := range cases {
		if tc.kind != CmdKind(tc.want) {
			t.Errorf("kind = %q, want %q", tc.kind, tc.want)
		}
	}

	// Bytes passes the wire shape through: metadata-only commands carry 0.
	body := []byte("hello")
	if got := (publishWriteCmd{cmd: pub("orders.a", body)}).Bytes(); got <= len(body) {
		t.Errorf("publishWriteCmd.Bytes() = %d, want more than the bare body", got)
	}
	batch := batchPublishCmd{cmd: BatchCmd{Stream: "orders", Reqs: []queue.PublishReq{
		{Subject: "orders.a", Body: body},
	}}}
	if got := batch.Bytes(); got <= len(body) {
		t.Errorf("batchPublishCmd.Bytes() = %d, want more than the bare body", got)
	}
	for _, c := range []Cmd{
		createStreamCmd{},
		updateStreamCmd{},
		deleteStreamCmd{},
		reapChunkCmd{},
		sweepDedupCmd{},
	} {
		if c.Bytes() != 0 {
			t.Errorf("%T.Bytes() = %d, want 0 for a metadata-only command", c, c.Bytes())
		}
	}
}

// TestEngineLessRejectionLeavesNoGap pins P1 on the fallback path: a domain
// rejection rolls back its savepoint while the transaction commits, so the next
// publish allocates seq 1 — no hole where the refused attempt would have been.
func TestEngineLessRejectionLeavesNoGap(t *testing.T) {
	ctx := context.Background()
	st, _ := openWithStore(t)

	// Unknown stream is a business rejection (ErrNotFound), not engine damage.
	_, err := st.Publish(ctx, PublishCmd{
		Stream: "nope",
		Req:    queue.PublishReq{Subject: "nope.a", Body: []byte("x")},
	})
	if !errors.Is(err, errs.ErrNotFound) {
		t.Fatalf("publish to unknown stream = %v, want ErrNotFound", err)
	}

	mustCreate(t, st, queue.DefaultConfig("orders"))
	ack, err := st.Publish(ctx, pub("orders.a", []byte("x")))
	if err != nil {
		t.Fatalf("publish after rejection: %v", err)
	}
	if ack.Seq != 1 { // the rejected attempt allocated nothing (P1)
		t.Fatalf("seq after rejected publish = %d, want 1", ack.Seq)
	}
}

// TestStoreClosedRefusesWrites covers enqueue's lifecycle refusal: after Close every
// write names the shutdown condition instead of touching a dead handle.
func TestStoreClosedRefusesWrites(t *testing.T) {
	ctx := context.Background()
	dir := testDataDir(t)
	st, _, err := Open(ctx, testOptions(dir, fakeClock(), &logCapture{}))
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	mustCreate(t, st, queue.DefaultConfig("orders"))
	if cerr := st.Close(ctx); cerr != nil {
		t.Fatalf("close: %v", err)
	}

	if _, pErr := st.Publish(ctx, pub("orders.a", []byte("x"))); !errors.Is(pErr, errs.ErrShuttingDown) {
		t.Errorf("closed publish = %v, want ErrShuttingDown", pErr)
	}
	if _, sErr := st.SweepDedup(ctx, "orders"); !errors.Is(sErr, errs.ErrShuttingDown) {
		t.Errorf("closed sweep = %v, want ErrShuttingDown", sErr)
	}
	if _, bErr := st.PublishBatch(ctx, BatchCmd{
		Stream: "orders",
		Reqs:   []queue.PublishReq{{Subject: "orders.a", Body: []byte("x")}},
	}); !errors.Is(bErr, errs.ErrShuttingDown) {
		t.Errorf("closed batch = %v, want ErrShuttingDown", bErr)
	}
}

// TestHandedOffStoreRefusesWrites pins the other enqueue refusal: once the rw handle
// has been handed to an owner, the store no longer writes — the handle's owner is the
// only writer in the process.
func TestHandedOffStoreRefusesWrites(t *testing.T) {
	ctx := context.Background()
	st, _, err := Open(ctx, testOptions(testDataDir(t), fakeClock(), &logCapture{}))
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	mustCreate(t, st, queue.DefaultConfig("orders"))
	if _, tErr := st.TakeWriter(); tErr != nil {
		t.Fatalf("take writer: %v", tErr)
	}
	_, pErr := st.Publish(ctx, pub("orders.a", []byte("x")))
	if !errors.Is(pErr, errs.ErrShuttingDown) || !strings.Contains(pErr.Error(), "read-write handle") {
		t.Errorf("handed-off publish = %v, want ErrShuttingDown naming the handle", pErr)
	}
	if cerr := st.Close(ctx); cerr != nil {
		t.Fatalf("close: %v", cerr)
	}
}

// TestPublishBatchRejectsOverLimit covers the §7 ceiling: more than MaxBatchMessages
// entries is a bad request before any transaction opens.
func TestPublishBatchRejectsOverLimit(t *testing.T) {
	ctx := context.Background()
	st, _ := openWithStore(t)
	mustCreate(t, st, queue.DefaultConfig("orders"))

	reqs := make([]queue.PublishReq, defaultMaxBatch+1)
	for i := range reqs {
		reqs[i] = queue.PublishReq{Subject: "orders.a", Body: []byte("x")}
	}
	_, err := st.PublishBatch(ctx, BatchCmd{Stream: "orders", Reqs: reqs})
	if !errors.Is(err, errs.ErrBadRequest) || !strings.Contains(err.Error(), "at most") {
		t.Fatalf("over-limit batch = %v, want ErrBadRequest naming the cap", err)
	}
}

// TestDeleteToleratesMissingSeqCounter covers deleteStreamCmd's tolerance for a
// stream whose stream_seq row never existed (created before any publish, or lost to
// older damage): the high-water mark degrades to "nothing published" instead of
// failing the deletion.
func TestDeleteToleratesMissingSeqCounter(t *testing.T) {
	ctx := context.Background()
	st, _ := openWithStore(t)
	mustCreate(t, st, queue.DefaultConfig("orders"))
	if _, xErr := st.rw.ExecContext(ctx,
		`DELETE FROM stream_seq WHERE stream = 'orders'`); xErr != nil {
		t.Fatalf("drop seq counter: %v", xErr)
	}

	res, dErr := st.enqueue(ctx, "test.delete", deleteStreamCmd{name: "orders", actor: "t"})
	if dErr != nil {
		t.Fatalf("delete without seq counter: %v", dErr)
	}
	delRes, ok := res.(DeleteResult)
	if !ok {
		t.Fatalf("delete returned %T, want DeleteResult", res)
	}
	if got := delRes.Consumers; got != 0 {
		t.Errorf("consumers = %d, want 0", got)
	}
	var raw string
	if err := st.ro.QueryRowContext(ctx,
		`SELECT v FROM meta WHERE k = ?`, metaSeqHwmPrefix+"orders").Scan(&raw); err != nil || raw != "-1" {
		t.Fatalf("seq hwm = (%q, %v), want (\"-1\", nil)", raw, err)
	}
}

// TestReapMultiChunkLoop drives finishInterruptedReaps past the chunk boundary:
// exactly deleteChunkRows rows means the first chunk comes up FULL, the loop takes
// another bite, and the short second chunk clears the marker. It runs on the live
// handle because Open itself calls finishInterruptedReaps during recovery — a
// reopen would consume the marker before the call under test could see it
// (restart-through-Open is TestDeleteReapSurvivesRestart's job).
func TestReapMultiChunkLoop(t *testing.T) {
	ctx := context.Background()
	st, _ := openWithStore(t)
	mustCreate(t, st, queue.DefaultConfig("orders"))

	// Plant deleteChunkRows messages plus the crash state directly: one transaction,
	// one reap marker whose value mirrors deleteStreamCmd's (the row count the
	// chunks walk down), no seq bookkeeping beyond what create already wrote. The
	// body goes in as the BLOB literal x'78' — messages.body is STRICT BLOB.
	tx, xErr := st.rw.BeginTx(ctx, nil)
	if xErr != nil {
		t.Fatalf("begin plant: %v", xErr)
	}
	const perStmt = 50 // rows per INSERT; keeps statements and bind lists bounded
	for start := 0; start < deleteChunkRows; start += perStmt {
		end := min(start+perStmt, deleteChunkRows)
		var sb strings.Builder
		sb.WriteString(`INSERT INTO messages (stream, seq, id, subject, body, size, published_at, trace_id) VALUES `)
		args := make([]any, 0, (end-start)*7)
		for i := start; i < end; i++ {
			if i > start {
				sb.WriteString(",")
			}
			fmt.Fprintf(&sb, "('orders', ?, 'id-%06d',", i)
			sb.WriteString("'orders.a',x'78',1,1,'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa1')")
			args = append(args, i+1)
		}
		if _, eErr := tx.ExecContext(ctx, sb.String(), args...); eErr != nil {
			_ = tx.Rollback() // release the pooled conn: Close would otherwise block
			t.Fatalf("plant messages: %v", eErr)
		}
	}
	if _, mErr := tx.ExecContext(ctx,
		`INSERT INTO meta (k, v) VALUES (?, ?)`,
		metaReapPrefix+"orders", fmt.Sprintf("%d", deleteChunkRows)); mErr != nil {
		t.Fatalf("plant marker: %v", mErr)
	}
	if cErr := tx.Commit(); cErr != nil {
		t.Fatalf("commit plant: %v", cErr)
	}

	names, rErr := finishInterruptedReaps(ctx, st.rw)
	if rErr != nil {
		t.Fatalf("finishInterruptedReaps: %v", rErr)
	}
	if len(names) != 1 || names[0] != "orders" {
		t.Fatalf("reaped names = %v, want [orders]", names)
	}
	var count int64
	if err := st.ro.QueryRowContext(ctx,
		`SELECT count(*) FROM messages WHERE stream = 'orders'`).Scan(&count); err != nil || count != 0 {
		t.Fatalf("rows left = (%d, %v), want (0, nil)", count, err)
	}
	var marker string
	if err := st.ro.QueryRowContext(ctx,
		`SELECT v FROM meta WHERE k = ?`, metaReapPrefix+"orders").Scan(&marker); !errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("marker after multi-chunk reap = (%q, %v), want cleared", marker, err)
	}
}

// TestDeleteReapSurvivesRestart walks issue §9's crash window end to end on the
// engine-less path: the metadata command lands with its reap.<name> marker, the
// process "dies" before any chunk runs, and the next Open finishes the deletion —
// messages gone, marker cleared, high-water mark kept, name recreatable above it
// (P2). Mid-reap recreation is refused with the documented conflict until the last
// chunk clears the marker.
func TestDeleteReapSurvivesRestart(t *testing.T) {
	ctx := context.Background()
	dir := testDataDir(t)
	st, _, err := Open(ctx, testOptions(dir, fakeClock(), &logCapture{}))
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	mustCreate(t, st, queue.DefaultConfig("orders"))
	for i := 0; i < 5; i++ {
		if _, pErr := st.Publish(ctx, pub("orders.a", []byte("x"))); pErr != nil {
			t.Fatalf("publish %d: %v", i, pErr)
		}
	}

	// Crash simulation: run ONLY the metadata command of DeleteStream, leaving the
	// reap.<name> marker behind and all five message rows in place.
	res, dErr := st.enqueue(ctx, "test.delete", deleteStreamCmd{name: "orders", actor: "t"})
	if dErr != nil {
		t.Fatalf("metadata-only delete: %v", dErr)
	}
	delRes, ok := res.(DeleteResult)
	if !ok {
		t.Fatalf("delete returned %T, want DeleteResult", res)
	}
	if got := delRes.Messages; got != 5 {
		t.Fatalf("delete result reports %d messages, want 5", got)
	}
	var raw string
	if err := st.ro.QueryRowContext(ctx,
		`SELECT v FROM meta WHERE k = ?`, metaReapPrefix+"orders").Scan(&raw); err != nil || raw != "5" {
		t.Fatalf("reap marker after crash = (%q, %v), want (\"5\", nil)", raw, err)
	}

	// While the marker stands, recreation is refused with the §9 conflict.
	_, _, cErr := st.CreateStream(ctx, queue.DefaultConfig("orders"), "t")
	var reap *ReapInProgressError
	if !errors.As(cErr, &reap) || !errors.Is(cErr, errs.ErrConflict) ||
		reap.Remaining != 5 || !strings.Contains(cErr.Error(), "still being deleted") {
		t.Fatalf("mid-reap recreate = %v, want ReapInProgressError{Remaining:5} wrapping ErrConflict", cErr)
	}

	if cerr := st.Close(ctx); cerr != nil {
		t.Fatalf("close: %v", cerr)
	}

	// Restart: recovery must finish the reap before anyone can see half a deletion.
	handler := &logCapture{}
	st2, _, oErr := Open(ctx, testOptions(dir, fakeClock(), handler))
	if oErr != nil {
		t.Fatalf("reopen: %v", oErr)
	}
	defer func() {
		if cerr := st2.Close(ctx); cerr != nil {
			t.Logf("close: %v", cerr)
		}
	}()

	var count int64
	if err := st2.ro.QueryRowContext(ctx,
		`SELECT count(*) FROM messages WHERE stream = 'orders'`).Scan(&count); err != nil || count != 0 {
		t.Fatalf("messages left after restart-reap = (%d, %v), want (0, nil)", count, err)
	}
	if err := st2.ro.QueryRowContext(ctx,
		`SELECT v FROM meta WHERE k = ?`, metaReapPrefix+"orders").Scan(&raw); !errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("reap marker after restart = (%q, %v), want sql.ErrNoRows (marker cleared)", raw, err)
	}
	if _, gErr := st2.GetStream(ctx, "orders"); gErr == nil {
		t.Error("stream still visible after restart-reap")
	}

	// Recreation resumes ABOVE the deleted stream's high-water mark (P2).
	if _, _, cErr := st2.CreateStream(ctx, queue.DefaultConfig("orders"), "t"); cErr != nil {
		t.Fatalf("recreate after finished reap: %v", cErr)
	}
	ack, pErr := st2.Publish(ctx, pub("orders.b", []byte("y")))
	if pErr != nil {
		t.Fatalf("publish into recreated stream: %v", pErr)
	}
	if ack.Seq != 6 {
		t.Errorf("first seq in recreated stream = %d, want 6 (above hwm 5)", ack.Seq)
	}
}
