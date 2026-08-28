// SPDX-License-Identifier: Apache-2.0

package store

import (
	"context"
	"database/sql"
	"errors"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/a-holm/messq/internal/errs"
	"github.com/a-holm/messq/internal/queue"
)

// Coverage top-up for the #15 store additions the inspection endpoints drive: the
// purge/seek/pending commands, the /v1/info readback helpers and the admin.action
// audit writer. Everything runs through public Store methods on real files in
// t.TempDir(); the only white-box seams are the field swaps this package's tests
// already use (installQueryOnlyRW, installClosedRO) and direct command Apply calls
// against hand-held transactions for arms the public API cannot phrase.

// i64p returns a pointer to v (PurgeSpec.UpToSeq is an optional int64).
func i64p(v int64) *int64 { return &v }

// spentTx hands back a committed (spent) transaction on a fresh migrated database:
// every statement on it fails with sql.ErrTxDone, which fires each command's first
// unmarked infrastructure-error arm deterministically.
func spentTx(t *testing.T) *sql.Tx {
	t.Helper()
	path := filepath.Join(t.TempDir(), "spent.db")
	migrateFresh(t, path, fakeClock())
	db := openTestDB(t, path)
	ctx := context.Background()
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit: %v", err)
	}
	return tx
}

// seedFetchedConsumer publishes the given subjects and claims the first two as
// INFLIGHT deliveries for consumer "worker" (StartFirst, default config).
func seedFetchedConsumer(t *testing.T, st *Store, subjects ...string) {
	t.Helper()
	publishSubjs(t, st, subjects...)
	newFetchConsumer(t, st, "worker", queue.DefaultConsumerConfig("worker"))
	if _, err := st.Fetch(context.Background(), FetchReq{Stream: "orders", Consumer: "worker", Batch: 2}); err != nil {
		t.Fatalf("fetch: %v", err)
	}
}

// TestFifteenCommandLabels covers the command-label and byte-budget methods of the
// #15 commands, which only the writer engine exercises.
func TestFifteenCommandLabels(t *testing.T) {
	cmds := []Cmd{purgeCmd{}, seekCmd{}, adminActionCmd{}}
	wantKinds := []CmdKind{kindPurge, kindSeek, kindAdminAction}
	for i, c := range cmds {
		if got := c.Kind(); got != wantKinds[i] {
			t.Fatalf("command %d Kind() = %q, want %q", i, got, wantKinds[i])
		}
		if got := c.Bytes(); got != 0 {
			t.Fatalf("command %d Bytes() = %d, want 0", i, got)
		}
	}
}

// TestPurgeValidateSpecRefusals covers every rejection arm of ValidatePurgeSpec and
// the shapes it must accept.
func TestPurgeValidateSpecRefusals(t *testing.T) {
	cases := []struct {
		name string
		spec PurgeSpec
	}{
		{"up_to_seq below one", PurgeSpec{UpToSeq: i64p(0)}},
		{"keep together with up_to_seq", PurgeSpec{UpToSeq: i64p(3), Keep: 1}},
		{"negative keep", PurgeSpec{Keep: -1}},
		{"unparseable subject", PurgeSpec{Subject: "orders..bad"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if err := ValidatePurgeSpec(tc.spec); !errors.Is(err, errs.ErrBadRequest) {
				t.Fatalf("ValidatePurgeSpec(%+v) = %v, want ErrBadRequest", tc.spec, err)
			}
		})
	}
	for _, ok := range []PurgeSpec{{Subject: "orders.>"}, {Keep: 2}, {}} {
		if err := ValidatePurgeSpec(ok); err != nil {
			t.Fatalf("ValidatePurgeSpec(%+v) = %v, want nil", ok, err)
		}
	}
}

// TestPurgeRangeDryRunThenReal covers the fast-range purge: the dry run previews the
// exact census without writing, the real run deletes the range, drops the deliveries,
// clamps lagging cursors forward only (a fetch watermark already past the range stays
// put), bumps only the affected consumer's generation and co-commits one audit event.
func TestPurgeRangeDryRunThenReal(t *testing.T) {
	st := newConsumerStream(t)
	seedFetchedConsumer(t, st, "orders.a", "orders.b", "orders.c")
	newFetchConsumer(t, st, "idle", queue.DefaultConsumerConfig("idle"))
	ctx := context.Background()

	dry, err := st.Purge(ctx, "orders", PurgeSpec{UpToSeq: i64p(2)}, true, "test")
	if err != nil {
		t.Fatalf("dry purge: %v", err)
	}
	if dry.Impact.Messages != 2 || dry.Impact.Bytes <= 0 {
		t.Fatalf("dry impact = %+v, want 2 messages with bytes", dry.Impact)
	}
	if dry.Impact.FirstSeqAfter != 3 || dry.Impact.PendingDropped != 0 || dry.Impact.Clamped {
		t.Fatalf("dry impact = %+v, want first-after 3, no drops, unclamped", dry.Impact)
	}
	if len(dry.Impact.ConsumersAffected) != 1 || dry.Impact.ConsumersAffected[0] != "worker" {
		t.Fatalf("dry consumers affected = %v, want [worker]", dry.Impact.ConsumersAffected)
	}
	if n := countEvent(t, st, "stream.purge"); n != 0 {
		t.Fatalf("dry run wrote %d purge events, want 0", n)
	}
	if _, perr := st.PeekSeq(ctx, "orders", 1); perr != nil {
		t.Fatalf("dry run must not delete: peek seq 1: %v", perr)
	}

	res, err := st.Purge(ctx, "orders", PurgeSpec{UpToSeq: i64p(2)}, false, "test")
	if err != nil {
		t.Fatalf("real purge: %v", err)
	}
	if res.Impact.PendingDropped != 2 {
		t.Fatalf("real impact = %+v, want 2 drops", res.Impact)
	}
	if res.Impact.CursorBefore != 1 || res.Impact.CursorAfter != 3 || !res.Impact.Clamped {
		t.Fatalf("real impact = %+v, want idle cursor clamped 1 -> 3", res.Impact)
	}
	for _, seq := range []int64{1, 2} {
		if _, perr := st.PeekSeq(ctx, "orders", seq); !errors.Is(perr, errs.ErrNotFound) {
			t.Fatalf("peek seq %d after purge = %v, want ErrNotFound", seq, perr)
		}
	}
	if _, perr := st.PeekSeq(ctx, "orders", 3); perr != nil {
		t.Fatalf("seq 3 must survive the range purge: %v", perr)
	}
	worker, err := st.GetConsumer(ctx, "orders", "worker")
	if err != nil {
		t.Fatalf("get consumer: %v", err)
	}
	if worker.CursorSeq != 4 || worker.Generation != 2 {
		t.Fatalf("worker cursor/gen = %d/%d, want 4/2 (affected, watermark forward of the range)", worker.CursorSeq, worker.Generation)
	}
	idle, err := st.GetConsumer(ctx, "orders", "idle")
	if err != nil {
		t.Fatalf("get idle consumer: %v", err)
	}
	if idle.CursorSeq != 3 || idle.Generation != 1 {
		t.Fatalf("idle cursor/gen = %d/%d, want 3/1 (clamped, unaffected generation)", idle.CursorSeq, idle.Generation)
	}
	si, err := st.GetStream(ctx, "orders")
	if err != nil {
		t.Fatalf("get stream: %v", err)
	}
	if si.FirstSeq != 3 {
		t.Fatalf("stream first_seq = %d, want 3", si.FirstSeq)
	}
	if n := countEvent(t, st, "stream.purge"); n != 1 {
		t.Fatalf("purge events = %d, want 1", n)
	}
}

// TestPurgeSubjectFilterKeepsOthersAndReportsTail covers the scan path's survivor
// arithmetic: FirstSeqAfter names the first survivor above the deleted span when one
// exists, and falls to max(victim)+1 when the purge took the stream's tail.
func TestPurgeSubjectFilterKeepsOthersAndReportsTail(t *testing.T) {
	t.Run("survivor inside the span", func(t *testing.T) {
		st := newConsumerStream(t)
		publishSubjs(t, st, "orders.a", "orders.b", "orders.a", "orders.b", "orders.b")
		ctx := context.Background()

		dry, err := st.Purge(ctx, "orders", PurgeSpec{Subject: "orders.a"}, true, "test")
		if err != nil {
			t.Fatalf("dry subject purge: %v", err)
		}
		if dry.Impact.Messages != 2 || dry.Impact.FirstSeqAfter != 4 {
			t.Fatalf("dry impact = %+v, want 2 victims, first-after 4", dry.Impact)
		}
		if _, err := st.Purge(ctx, "orders", PurgeSpec{Subject: "orders.a"}, false, "test"); err != nil {
			t.Fatalf("real subject purge: %v", err)
		}
		for _, seq := range []int64{1, 3} {
			if _, err := st.PeekSeq(ctx, "orders", seq); !errors.Is(err, errs.ErrNotFound) {
				t.Fatalf("victim seq %d = %v, want ErrNotFound", seq, err)
			}
		}
		for _, seq := range []int64{2, 4, 5} {
			if _, err := st.PeekSeq(ctx, "orders", seq); err != nil {
				t.Fatalf("kept seq %d must survive: %v", seq, err)
			}
		}
	})
	t.Run("tail of the stream purged", func(t *testing.T) {
		st := newConsumerStream(t)
		publishSubjs(t, st, "orders.a", "orders.b", "orders.a", "orders.b", "orders.a")
		ctx := context.Background()

		dry, err := st.Purge(ctx, "orders", PurgeSpec{Subject: "orders.a"}, true, "test")
		if err != nil {
			t.Fatalf("dry subject purge: %v", err)
		}
		if dry.Impact.Messages != 3 || dry.Impact.FirstSeqAfter != 6 {
			t.Fatalf("dry impact = %+v, want 3 victims, first-after 6", dry.Impact)
		}
		if _, err := st.Purge(ctx, "orders", PurgeSpec{Subject: "orders.a"}, false, "test"); err != nil {
			t.Fatalf("real subject purge: %v", err)
		}
		for _, seq := range []int64{1, 3, 5} {
			if _, err := st.PeekSeq(ctx, "orders", seq); !errors.Is(err, errs.ErrNotFound) {
				t.Fatalf("victim seq %d = %v, want ErrNotFound", seq, err)
			}
		}
		for _, seq := range []int64{2, 4} {
			if _, err := st.PeekSeq(ctx, "orders", seq); err != nil {
				t.Fatalf("kept seq %d must survive: %v", seq, err)
			}
		}
	})
}

// TestPurgeRefusalsAndWhiteBoxArms covers the public refusal arms plus the direct-Apply
// arms the public API cannot phrase: the spent-transaction infrastructure failure, the
// subject belt-refusal for direct callers, and the missing-stream numbering fallback.
func TestPurgeRefusalsAndWhiteBoxArms(t *testing.T) {
	st := newConsumerStream(t)
	ctx := context.Background()

	if _, err := st.Purge(ctx, "bad/name", PurgeSpec{}, false, "test"); err == nil {
		t.Fatal("bad stream name must be refused")
	}
	if _, err := st.Purge(ctx, "orders", PurgeSpec{Keep: -1}, false, "test"); !errors.Is(err, errs.ErrBadRequest) {
		t.Fatalf("public invalid spec = %v, want ErrBadRequest", err)
	}
	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	if _, err := st.Purge(cancelled, "orders", PurgeSpec{}, false, "test"); err == nil || IsCmdError(err) {
		t.Fatalf("cancelled purge = %v, want an unmarked infrastructure refusal", err)
	}

	tx := spentTx(t)
	now := time.UnixMilli(fakeStartMillis)
	_, _, err := (purgeCmd{stream: "orders"}).Apply(ctx, tx, now)
	if !errors.Is(err, sql.ErrTxDone) || IsCmdError(err) || !strings.Contains(err.Error(), "read stream_seq") {
		t.Fatalf("spent-tx purge = %v, want ErrTxDone via read stream_seq, unmarked", err)
	}
	_, _, err = (purgeCmd{stream: "orders", spec: PurgeSpec{Subject: "orders..bad"}}).Apply(ctx, tx, now)
	// A direct caller's unparseable subject surfaces the pattern error wrapped as
	// a command error (the underlying parse failure chains to a domain sentinel),
	// before any statement runs.
	if err == nil || !IsCmdError(err) || !strings.Contains(err.Error(), `pattern "orders..bad"`) {
		t.Fatalf("belt subject refusal = %v, want the marked pattern error", err)
	}

	fresh := filepath.Join(t.TempDir(), "fresh.db")
	migrateFresh(t, fresh, fakeClock())
	fdb := openTestDB(t, fresh)
	ftx, err := fdb.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("begin fresh: %v", err)
	}
	res, _, err := (purgeCmd{stream: "ghost"}).Apply(ctx, ftx, now)
	if err != nil {
		t.Fatalf("missing-stream purge: %v", err)
	}
	pr, ok := res.(purgeResult)
	if !ok {
		t.Fatalf("engine returned %T, want purgeResult", res)
	}
	if pr.r.Impact.Messages != 0 || pr.r.Impact.FirstSeqAfter != 1 {
		t.Fatalf("missing-stream impact = %+v, want 0 messages, first-after 1", pr.r.Impact)
	}
	if rbErr := ftx.Rollback(); rbErr != nil {
		t.Errorf("rollback: %v", rbErr)
	}
}

// TestSeekRefusals covers the name-validation arms, the missing-consumer fencing
// refusal, the unknown start-kind wrap and the cancelled-context infrastructure
// refusal of the T10 cursor move.
func TestSeekRefusals(t *testing.T) {
	st := newConsumerStream(t)
	ctx := context.Background()
	to := queue.StartPosition{Kind: queue.StartSeq, Seq: 1}

	if _, err := st.Seek(ctx, "bad/name", "worker", to, false, "test"); err == nil {
		t.Fatal("bad stream name must be refused")
	}
	if _, err := st.Seek(ctx, "orders", "bad/name", to, false, "test"); err == nil {
		t.Fatal("bad consumer name must be refused")
	}
	if _, err := st.Seek(ctx, "orders", "ghost", to, false, "test"); !errors.Is(err, errs.ErrNotFound) {
		t.Fatalf("ghost seek = %v, want ErrNotFound", err)
	}
	newFetchConsumer(t, st, "worker", queue.DefaultConsumerConfig("worker"))
	if _, err := st.Seek(ctx, "orders", "worker", queue.StartPosition{Kind: queue.Start("warp")}, false, "test"); !errors.Is(err, errs.ErrBadRequest) {
		t.Fatalf("unknown start kind = %v, want ErrBadRequest", err)
	}
	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	if _, err := st.Seek(cancelled, "orders", "worker", to, false, "test"); err == nil || IsCmdError(err) {
		t.Fatalf("cancelled seek = %v, want an unmarked infrastructure refusal", err)
	}
}

// TestSeekDryRunThenRealMovesCursor covers the T10 contract end to end: the dry run
// previews cursor move, dropped deliveries and redelivered backlog without writing,
// the real run fences the outstanding tokens by dropping their rows and bumping the
// generation, and both outcomes warn about what they are about to do.
func TestSeekDryRunThenRealMovesCursor(t *testing.T) {
	st := newConsumerStream(t)
	seedFetchedConsumer(t, st, "orders.a", "orders.b", "orders.c")
	ctx := context.Background()
	to := queue.StartPosition{Kind: queue.StartSeq, Seq: 2}

	// The fetch watermark put the cursor where it is; the seek must report that
	// starting point honestly rather than a hardcoded value.
	pre, err := st.GetConsumer(ctx, "orders", "worker")
	if err != nil {
		t.Fatalf("get consumer: %v", err)
	}

	dry, err := st.Seek(ctx, "orders", "worker", to, true, "test")
	if err != nil {
		t.Fatalf("dry seek: %v", err)
	}
	if dry.Impact.CursorBefore != pre.CursorSeq || dry.Impact.CursorAfter != 2 {
		t.Fatalf("dry cursor before/after = %d/%d, want %d/2", dry.Impact.CursorBefore, dry.Impact.CursorAfter, pre.CursorSeq)
	}
	if dry.Impact.PendingDropped != 3 || dry.Impact.Messages != 2 {
		t.Fatalf("dry impact = %+v, want 3 drops (ready + inflight), 2 redelivered", dry.Impact)
	}
	if len(dry.Impact.Warnings) != 2 {
		t.Fatalf("dry warnings = %v, want the fencing and redelivery warning", dry.Impact.Warnings)
	}
	if n := countEvent(t, st, "consumer.seek"); n != 0 {
		t.Fatalf("dry run wrote %d seek events, want 0", n)
	}
	before, err := st.PendingList(ctx, "orders", "worker", PendingQuery{Limit: 10})
	if err != nil {
		t.Fatalf("pending after dry seek: %v", err)
	}
	if len(before.Items) != 3 {
		t.Fatalf("dry run must not drop deliveries: %d items, want 3", len(before.Items))
	}

	res, err := st.Seek(ctx, "orders", "worker", to, false, "test")
	if err != nil {
		t.Fatalf("real seek: %v", err)
	}
	if res.Generation != 2 {
		t.Fatalf("seek generation = %d, want 2", res.Generation)
	}
	info, err := st.GetConsumer(ctx, "orders", "worker")
	if err != nil {
		t.Fatalf("get consumer: %v", err)
	}
	if info.CursorSeq != 2 || info.Generation != 2 {
		t.Fatalf("consumer cursor/gen = %d/%d, want 2/2", info.CursorSeq, info.Generation)
	}
	after, err := st.PendingList(ctx, "orders", "worker", PendingQuery{Limit: 10})
	if err != nil {
		t.Fatalf("pending after real seek: %v", err)
	}
	if len(after.Items) != 0 {
		t.Fatalf("real seek must drop every delivery: %d items, want 0", len(after.Items))
	}
	if n := countEvent(t, st, "consumer.seek"); n != 1 {
		t.Fatalf("seek events = %d, want 1", n)
	}
}

// TestSeekTimeAnchorClamping covers the reported-clamp contract of the time grammar:
// an anchor inside the stream resolves exactly, an anchor past the head folds to
// stream_seq.next and reports clamped.
func TestSeekTimeAnchorClamping(t *testing.T) {
	st := newConsumerStream(t)
	publishSubjs(t, st, "orders.a", "orders.b", "orders.c")
	newFetchConsumer(t, st, "worker", queue.DefaultConsumerConfig("worker"))
	ctx := context.Background()

	mid, err := st.Seek(ctx, "orders", "worker",
		queue.StartPosition{Kind: queue.StartTime, Time: fakeStartMillis + 1500}, false, "test")
	if err != nil {
		t.Fatalf("mid-anchor seek: %v", err)
	}
	if mid.Impact.CursorAfter != 3 || mid.Impact.Clamped {
		t.Fatalf("mid-anchor impact = %+v, want cursor 3, unclamped", mid.Impact)
	}
	if mid.Impact.Messages != 1 {
		t.Fatalf("mid-anchor messages = %d, want 1", mid.Impact.Messages)
	}

	newFetchConsumer(t, st, "late", queue.DefaultConsumerConfig("late"))
	past, err := st.Seek(ctx, "orders", "late",
		queue.StartPosition{Kind: queue.StartTime, Time: fakeStartMillis + 999_999}, false, "test")
	if err != nil {
		t.Fatalf("past-head seek: %v", err)
	}
	if past.Impact.CursorAfter != 4 || !past.Impact.Clamped {
		t.Fatalf("past-head impact = %+v, want cursor 4 (next), clamped", past.Impact)
	}
	if past.Impact.Messages != 0 {
		t.Fatalf("past-head messages = %d, want 0", past.Impact.Messages)
	}
}

// TestSeekWriteRefusalOnReadOnlyPool covers the unmarked drop-deliveries failure arm:
// with the writer handle pointing at a query-only twin, the real run's first write is
// refused by the driver and surfaces as an infrastructure error, never a CmdError.
func TestSeekWriteRefusalOnReadOnlyPool(t *testing.T) {
	st := newConsumerStream(t)
	newFetchConsumer(t, st, "worker", queue.DefaultConsumerConfig("worker"))
	restore := installQueryOnlyRW(t, st)
	defer restore()

	_, err := st.Seek(context.Background(), "orders", "worker",
		queue.StartPosition{Kind: queue.StartSeq, Seq: 2}, false, "test")
	if err == nil || IsCmdError(err) ||
		!strings.Contains(err.Error(), "drop deliveries") || !strings.Contains(err.Error(), "readonly") {
		t.Fatalf("readonly seek = %v, want an unmarked drop-deliveries refusal", err)
	}
}

// TestPendingListQueryArms covers the bounded-inspection query grammar: the negative
// limit refusal, the uncapped echo, both state filters with their derived ack tokens,
// the unknown state refusal, the absolute older-than cutoff on both sides, and the
// cancelled-context failure.
func TestPendingListQueryArms(t *testing.T) {
	st := newConsumerStream(t)
	seedFetchedConsumer(t, st, "orders.a", "orders.b", "orders.c")
	ctx := context.Background()

	if _, err := st.PendingList(ctx, "orders", "worker", PendingQuery{Limit: -1}); err == nil {
		t.Fatal("negative limit must be refused")
	}
	inflight, err := st.PendingList(ctx, "orders", "worker", PendingQuery{State: "inflight"})
	if err != nil {
		t.Fatalf("inflight listing: %v", err)
	}
	if len(inflight.Items) != 2 {
		t.Fatalf("inflight items = %d, want 2", len(inflight.Items))
	}
	for _, it := range inflight.Items {
		if it.State != "inflight" || it.AckToken == nil {
			t.Fatalf("item %+v, want inflight with a derived ack token", it)
		}
	}
	ready, err := st.PendingList(ctx, "orders", "worker", PendingQuery{State: "ready", Limit: 10})
	if err != nil {
		t.Fatalf("ready listing: %v", err)
	}
	// The fetch claimed two and admitted the third as ready backlog: the ready
	// filter must surface exactly that row, without an ack token.
	if len(ready.Items) != 1 {
		t.Fatalf("ready items = %d, want 1 (the unclaimed tail)", len(ready.Items))
	}
	for _, it := range ready.Items {
		if it.State != "ready" || it.AckToken != nil {
			t.Fatalf("item %+v, want ready without an ack token", it)
		}
	}
	if _, serr := st.PendingList(ctx, "orders", "worker", PendingQuery{State: "bogus"}); serr == nil {
		t.Fatal("unknown state filter must be refused")
	}
	// The ready tail's visible_at is 0 (ready now), so it survives even a cutoff
	// below every inflight deadline; the claimed rows have real deadlines.
	old, err := st.PendingList(ctx, "orders", "worker", PendingQuery{OlderThanMS: fakeStartMillis + 1000})
	if err != nil || len(old.Items) != 1 || old.Items[0].State != "ready" {
		t.Fatalf("cutoff below every deadline = %v items %+v, want only the ready-now row", err, old.Items)
	}
	win, err := st.PendingList(ctx, "orders", "worker", PendingQuery{OlderThanMS: fakeStartMillis + 60000})
	if err != nil || len(win.Items) != 3 {
		t.Fatalf("cutoff above every deadline = %v items %d, want 3", err, len(win.Items))
	}
	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	if _, err := st.PendingList(cancelled, "orders", "worker", PendingQuery{Limit: 1}); err == nil {
		t.Fatal("cancelled context must fail the scan")
	}
}

// TestInfoReadbackRefusals covers the /v1/info read helpers' failure arms: a closed
// read pool fails the live synchronous read and the census, and a vanished data
// directory fails the statfs subject.
func TestInfoReadbackRefusals(t *testing.T) {
	st := newConsumerStream(t)
	restore := installClosedRO(t, st)
	ctx := context.Background()

	if _, err := st.LiveSynchronous(ctx); err != nil {
		expectClosedRefusal(t, "live synchronous", err)
	} else {
		t.Fatal("live synchronous on a closed read pool must fail")
	}
	if _, err := st.InfoCounts(ctx); err != nil {
		expectClosedRefusal(t, "info counts", err)
	} else {
		t.Fatal("info counts on a closed read pool must fail")
	}
	restore()

	orig := st.dir
	st.dir = filepath.Join(t.TempDir(), "vanished")
	defer func() { st.dir = orig }()
	if _, err := st.DiskFreeBytes(); err == nil || !strings.Contains(err.Error(), "statfs") {
		t.Fatalf("disk free on a vanished dir = %v, want a statfs failure", err)
	}
}

// TestRecordAdminActionAuditTrail covers the admin.action writer: one event row with
// the setting transition as its detail, the cancelled-context refusal, and the
// spent-transaction infrastructure arm of the command itself.
func TestRecordAdminActionAuditTrail(t *testing.T) {
	st := newConsumerStream(t)
	ctx := context.Background()

	if err := st.RecordAdminAction(ctx, "operator", "log.level", "info", "debug"); err != nil {
		t.Fatalf("record: %v", err)
	}
	if n := countEvent(t, st, "admin.action"); n != 1 {
		t.Fatalf("admin.action events = %d, want 1", n)
	}
	var detail string
	if err := st.RO().QueryRowContext(ctx,
		`SELECT detail FROM events WHERE event = 'admin.action'`).Scan(&detail); err != nil {
		t.Fatalf("read event detail: %v", err)
	}
	if !strings.Contains(detail, `"setting":"log.level"`) ||
		!strings.Contains(detail, `"from":"info"`) || !strings.Contains(detail, `"to":"debug"`) {
		t.Fatalf("event detail = %s, want the setting transition", detail)
	}

	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	if err := st.RecordAdminAction(cancelled, "operator", "log.level", "info", "debug"); err == nil || IsCmdError(err) {
		t.Fatalf("cancelled record = %v, want an unmarked infrastructure refusal", err)
	}

	_, _, err := (adminActionCmd{setting: "log.level"}).Apply(ctx, spentTx(t), time.UnixMilli(fakeStartMillis))
	if !errors.Is(err, sql.ErrTxDone) || IsCmdError(err) {
		t.Fatalf("spent-tx admin apply = %v, want ErrTxDone unmarked", err)
	}
}

// TestStorePureHelpers pins the tiny pure helpers the read paths lean on: maxInt's
// second arm (the listing echo) and sortStrings' swap arm (deterministic small
// outputs), which the DB-backed tests only hit when the rows happen to be unordered.
func TestStorePureHelpers(t *testing.T) {
	if got := maxInt(1, 2); got != 2 {
		t.Fatalf("maxInt(1,2) = %d, want 2", got)
	}
	if got := maxInt(2, 1); got != 2 {
		t.Fatalf("maxInt(2,1) = %d, want 2", got)
	}
	names := []string{"b", "a", "c", "a"}
	sortStrings(names)
	if names[0] != "a" || names[1] != "a" || names[2] != "b" || names[3] != "c" {
		t.Fatalf("sortStrings = %v, want [a a b c]", names)
	}
}
