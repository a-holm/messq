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

	"github.com/a-holm/messq/internal/queue"
)

// This file tops up the #30 coverage floor (test-only; no production change): the
// purge plan helpers (holdsOn / distinctDeliveryConsumers / selectVictims /
// countRange / adjustStats), the purge scan-path write arms, seek's read and write
// refusals, the /v1/info live observations (LiveSynchronous, DiskFreeBytes,
// InfoCounts) and the pending-list pagination edges. Everything is deterministic:
// spent-transaction cascades, planted RAISE triggers (state, never timing), a
// query-only rw twin pool, fixed clocks and real files in t.TempDir().

// cov30SpentTx opens a fresh migrated database and hands back an ALREADY-COMMITTED
// transaction: every statement on it fails with sql.ErrTxDone, which deterministically
// reaches the unmarked infrastructure-error propagations of the purge plan helpers
// and seek's consumer read.
func cov30SpentTx(t *testing.T) *sql.Tx {
	t.Helper()
	path := filepath.Join(t.TempDir(), dbFileName)
	migrateFresh(t, path, newStepClock(time.UnixMilli(cov27BaseMS)))
	db := openTestDB(t, path)
	tx, err := db.BeginTx(context.Background(), nil)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit empty tx: %v", err)
	}
	return tx
}

// cov30SeedHeldStream creates "orders" with three identical 4-byte messages and a
// consumer, then plants one READY delivery row per message by raw SQL — the claim
// path's exact row shape at generation 1. Deliveries carry no foreign key to
// consumers, but the consumer row exists so generation bumps and joins land
// honestly. Returns nothing; the caller owns ctx.
func cov30SeedHeldStream(t *testing.T, st *Store, ctx context.Context) {
	t.Helper()
	if _, _, err := st.CreateStream(ctx, queue.DefaultConfig("orders"), "tester"); err != nil {
		t.Fatalf("create stream: %v", err)
	}
	for _, id := range []string{"m1", "m2", "m3"} {
		if _, err := st.Publish(ctx, PublishCmd{
			Stream: "orders",
			Req:    queue.PublishReq{Subject: "orders.new", Body: []byte("body"), MsgID: id},
		}); err != nil {
			t.Fatalf("publish %s: %v", id, err)
		}
	}
	if _, cErr := st.CreateConsumer(ctx, "orders", queue.DefaultConsumerConfig("worker"),
		queue.StartPosition{Kind: queue.StartFirst}, "tester"); cErr != nil {
		t.Fatalf("create consumer: %v", cErr)
	}
	for seq := int64(1); seq <= 3; seq++ {
		if _, xErr := st.rw.ExecContext(ctx,
			`INSERT INTO deliveries (stream, consumer, seq, subject, state, attempts, visible_at, generation)
			 VALUES ('orders', 'worker', ?, 'orders.new', 0, 0, 0, 1)`, seq); xErr != nil {
			t.Fatalf("plant delivery %d: %v", seq, xErr)
		}
	}
}

// TestPurgePlanHelpersRefuseSpentTransaction drives the five purge plan helpers
// against a committed transaction: each must propagate the driver refusal UNMARKED
// — dead infrastructure may not be repainted as a domain refusal.
func TestPurgePlanHelpersRefuseSpentTransaction(t *testing.T) {
	ctx := context.Background()
	tx := cov30SpentTx(t)

	if _, _, err := countRange(ctx, tx, "orders", true, 5); !errors.Is(err, sql.ErrTxDone) {
		t.Errorf("countRange on spent tx = %v, want ErrTxDone", err)
	}
	if _, err := distinctDeliveryConsumers(ctx, nil, tx, "orders", 5); !errors.Is(err, sql.ErrTxDone) {
		t.Errorf("distinctDeliveryConsumers on spent tx = %v, want ErrTxDone", err)
	}
	if err := adjustStats(ctx, tx, "orders", 3, 12); err == nil ||
		!errors.Is(err, sql.ErrTxDone) || !strings.Contains(err.Error(), "adjust stream_stats") {
		t.Errorf("adjustStats on spent tx = %v, want the wrapped ErrTxDone propagation", err)
	}
	if _, err := selectVictims(ctx, tx, nil, "orders", nil, 5); !errors.Is(err, sql.ErrTxDone) {
		t.Errorf("selectVictims on spent tx = %v, want ErrTxDone", err)
	}
	if _, _, err := holdsOn(ctx, tx, nil, "orders", 5); !errors.Is(err, sql.ErrTxDone) {
		t.Errorf("holdsOn on spent tx = %v, want ErrTxDone", err)
	}
}

// TestHoldsOnWalksDeliveryPairs pins holdsOn's single-walk contract: the sorted
// distinct holder names and the per-seq holder lists both come from one scan of
// the deliveries at-or-below the bound.
func TestHoldsOnWalksDeliveryPairs(t *testing.T) {
	st := openCommandPathStore(t, fakeClock())
	ctx := context.Background()
	cov30SeedHeldStream(t, st, ctx)

	tx, err := st.rw.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	t.Cleanup(func() {
		if rbErr := tx.Rollback(); rbErr != nil && !errors.Is(rbErr, sql.ErrTxDone) {
			t.Errorf("rollback scan tx: %v", rbErr)
		}
	})

	names, bySeq, err := holdsOn(ctx, tx, nil, "orders", 3)
	if err != nil {
		t.Fatalf("holdsOn: %v", err)
	}
	if len(names) != 1 || names[0] != "worker" {
		t.Errorf("names = %v, want [worker]", names)
	}
	if len(bySeq[1]) != 1 || bySeq[1][0] != "worker" ||
		len(bySeq[2]) != 1 || bySeq[2][0] != "worker" ||
		len(bySeq[3]) != 1 || bySeq[3][0] != "worker" {
		t.Errorf("bySeq = %v, want every seq 1..3 held by worker", bySeq)
	}
}

// TestKeepPurgeScanPathDropsHeldDeliveries runs a REAL keep-only purge over held
// deliveries: the scan path must drop exactly the victims' held rows, scope the
// affected consumers, bump their generations, shrink the maintained census, and
// leave cursors untouched (no clamp on a filtered purge — S6.2).
func TestKeepPurgeScanPathDropsHeldDeliveries(t *testing.T) {
	st := openCommandPathStore(t, fakeClock())
	ctx := context.Background()
	cov30SeedHeldStream(t, st, ctx)

	var msgsN, bytesN, oneSize, preCur int64
	if err := st.rw.QueryRowContext(ctx,
		`SELECT count(*), coalesce(sum(size),0), (SELECT size FROM messages WHERE stream='orders' AND seq=1),
		        (SELECT cursor_seq FROM consumers WHERE stream='orders' AND name='worker')
		 FROM messages WHERE stream = 'orders'`).Scan(&msgsN, &bytesN, &oneSize, &preCur); err != nil {
		t.Fatalf("read census: %v", err)
	}
	if msgsN != 3 || bytesN != 3*oneSize || oneSize != int64(len("body")) {
		t.Fatalf("seed census = (%d, %d, %d), want three 4-byte messages", msgsN, bytesN, oneSize)
	}

	res, err := st.Purge(ctx, "orders", PurgeSpec{Keep: 1}, false, "op")
	if err != nil {
		t.Fatalf("purge keep=1: %v", err)
	}
	imp := res.Impact
	if imp.Messages != 2 || imp.PendingDropped != 2 {
		t.Errorf("impact = %+v, want 2 messages and 2 dropped deliveries", imp)
	}
	if imp.Bytes != bytesN-oneSize {
		t.Errorf("impact.Bytes = %d, want the two victims' size sum %d", imp.Bytes, bytesN-oneSize)
	}
	if len(imp.ConsumersAffected) != 1 || imp.ConsumersAffected[0] != "worker" {
		t.Errorf("ConsumersAffected = %v, want [worker]", imp.ConsumersAffected)
	}
	if imp.FirstSeqAfter != 3 {
		t.Errorf("FirstSeqAfter = %d, want 3 (the survivor)", imp.FirstSeqAfter)
	}
	// Filtered purges never clamp and never report cursor telemetry: both fields
	// stay zero on the scan path by construction (S6.2).
	if imp.CursorBefore != 0 || imp.CursorAfter != 0 || imp.Clamped {
		t.Errorf("cursor telemetry = (%d, %d, %t), want zero on the scan path (no clamp)",
			imp.CursorBefore, imp.CursorAfter, imp.Clamped)
	}

	var gen, cur int64
	if err := st.rw.QueryRowContext(ctx,
		`SELECT generation, cursor_seq FROM consumers WHERE stream='orders' AND name='worker'`).
		Scan(&gen, &cur); err != nil {
		t.Fatalf("read consumer: %v", err)
	}
	if gen != 2 {
		t.Errorf("generation = %d, want 2 (bumped once for the affected consumer)", gen)
	}
	if cur != preCur {
		t.Errorf("cursor_seq = %d, want %d (no clamp on a filtered purge)", cur, preCur)
	}

	var leftMsgs, leftDels, statsMsgs, statsBytes int64
	if err := st.rw.QueryRowContext(ctx,
		`SELECT (SELECT count(*) FROM messages WHERE stream='orders'),
		        (SELECT count(*) FROM deliveries WHERE stream='orders'),
		        msgs, bytes FROM stream_stats WHERE stream='orders'`).
		Scan(&leftMsgs, &leftDels, &statsMsgs, &statsBytes); err != nil {
		t.Fatalf("read post-state: %v", err)
	}
	// The survivor (seq 3) keeps its held delivery row; only the victims' rows drop.
	if leftMsgs != 1 || leftDels != 1 {
		t.Errorf("post rows = (%d messages, %d deliveries), want (1, 1)", leftMsgs, leftDels)
	}
	if statsMsgs != 1 || statsBytes != bytesN-imp.Bytes {
		t.Errorf("post census = (%d, %d), want the honest (1, %d)", statsMsgs, statsBytes, bytesN-imp.Bytes)
	}
}

// TestPurgeWritesRefusedUnderReadOnlyPool runs both purge paths against a real
// query-only twin of the writer pool: reads and planning succeed, and the FIRST
// write of each path is refused by the driver with the unmarked readonly class.
func TestPurgeWritesRefusedUnderReadOnlyPool(t *testing.T) {
	st := openCommandPathStore(t, fakeClock())
	ctx := context.Background()
	cov30SeedHeldStream(t, st, ctx)

	restore := installQueryOnlyRW(t, st)
	defer restore()

	if _, err := st.Purge(ctx, "orders", PurgeSpec{}, false, "op"); err == nil ||
		IsCmdError(err) ||
		!strings.Contains(err.Error(), "drop deliveries") ||
		!strings.Contains(err.Error(), "readonly") {
		t.Errorf("fast-range purge under readonly pool = %v, want the unmarked deliveries-delete refusal", err)
	}
	if _, err := st.Purge(ctx, "orders", PurgeSpec{Keep: 1}, false, "op"); err == nil ||
		IsCmdError(err) || !strings.Contains(err.Error(), "drop deliveries on") {
		t.Errorf("scan-path purge under readonly pool = %v, want the per-seq deliveries refusal", err)
	}
}

// TestPurgeWriteArmsUnderPlantedRefusals walks the deeper write arms of both purge
// paths: a planted RAISE trigger fails exactly ONE statement per case (state, not
// timing), so each arm's own wrap phrase fires deterministically. The abort is a
// driver-level error the command wraps raw — never a marked business refusal.
func TestPurgeWriteArmsUnderPlantedRefusals(t *testing.T) {
	cases := []struct {
		name string
		spec PurgeSpec
		trig string
		want string
	}{
		{
			"fast clamp-cursors",
			PurgeSpec{},
			`CREATE TRIGGER cov30_no_clamp BEFORE UPDATE ON consumers
			 BEGIN SELECT RAISE(ABORT, 'clamp refused'); END;`,
			"clamp cursors",
		},
		{
			"fast bump-generation",
			PurgeSpec{},
			`CREATE TRIGGER cov30_no_bump BEFORE UPDATE OF generation ON consumers
			 BEGIN SELECT RAISE(ABORT, 'bump refused'); END;`,
			`bump generation for`,
		},
		{
			"fast plan drift",
			PurgeSpec{},
			`CREATE TRIGGER cov30_ignore_msg_del BEFORE DELETE ON messages
			 BEGIN SELECT RAISE(IGNORE); END;`,
			"plan drift",
		},
		{
			"fast adjust-stats",
			PurgeSpec{},
			`CREATE TRIGGER cov30_no_stats BEFORE UPDATE ON stream_stats
			 BEGIN SELECT RAISE(ABORT, 'stats refused'); END;`,
			"adjust stream_stats",
		},
		{
			"fast event row",
			PurgeSpec{},
			`CREATE TRIGGER cov30_no_event BEFORE INSERT ON events
			 BEGIN SELECT RAISE(ABORT, 'event refused'); END;`,
			"insert stream.purge event row",
		},
		{
			"scan bump-generation",
			PurgeSpec{Keep: 1},
			`CREATE TRIGGER cov30_no_bump2 BEFORE UPDATE OF generation ON consumers
			 BEGIN SELECT RAISE(ABORT, 'bump refused'); END;`,
			`bump generation for`,
		},
		{
			"scan vanished message",
			PurgeSpec{Keep: 1},
			`CREATE TRIGGER cov30_ignore_msg_del2 BEFORE DELETE ON messages
			 BEGIN SELECT RAISE(IGNORE); END;`,
			"vanished under the command transaction",
		},
		{
			"scan adjust-stats",
			PurgeSpec{Keep: 1},
			`CREATE TRIGGER cov30_no_stats2 BEFORE UPDATE ON stream_stats
			 BEGIN SELECT RAISE(ABORT, 'stats refused'); END;`,
			"adjust stream_stats",
		},
		{
			"scan event row",
			PurgeSpec{Keep: 1},
			`CREATE TRIGGER cov30_no_event2 BEFORE INSERT ON events
			 BEGIN SELECT RAISE(ABORT, 'event refused'); END;`,
			"insert stream.purge event row",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			st := openCommandPathStore(t, fakeClock())
			ctx := context.Background()
			cov30SeedHeldStream(t, st, ctx)
			if _, xErr := st.rw.ExecContext(ctx, tc.trig); xErr != nil {
				t.Fatalf("plant trigger: %v", xErr)
			}
			_, err := st.Purge(ctx, "orders", tc.spec, false, "op")
			if err == nil || IsCmdError(err) || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("purge = %v, want the unmarked %q refusal", err, tc.want)
			}
		})
	}
}

// TestSeekReadConsumerFailureOnSpentTx drives seekCmd.Apply against a committed
// transaction: the consumer read fails with ErrTxDone, which is NOT ErrNoRows, so
// the unmarked read-consumer propagation fires instead of the not-found refusal.
func TestSeekReadConsumerFailureOnSpentTx(t *testing.T) {
	tx := cov30SpentTx(t)
	_, _, err := seekCmd{stream: "orders", consumer: "worker"}.
		Apply(context.Background(), tx, time.UnixMilli(cov27BaseMS))
	if err == nil || IsCmdError(err) ||
		!strings.Contains(err.Error(), "read consumer") ||
		!errors.Is(err, sql.ErrTxDone) {
		t.Errorf("seek on spent tx = %v, want the unmarked read-consumer ErrTxDone propagation", err)
	}
}

// TestSeekWriteArmsUnderPlantedRefusals reaches seek's mid-transaction write arms:
// RAISE(IGNORE) on the deliveries delete desynchronizes the plan count (drift),
// an aborted consumers update fails the cursor move, and an aborted events insert
// fails the audit row. Each case proves its own arm's wrap phrase, unmarked.
func TestSeekWriteArmsUnderPlantedRefusals(t *testing.T) {
	cases := []struct {
		name string
		trig string
		want string
	}{
		{
			"drop-plan drift",
			`CREATE TRIGGER cov30_seek_ignore_del BEFORE DELETE ON deliveries
			 BEGIN SELECT RAISE(IGNORE); END;`,
			"plan said",
		},
		{
			"move-cursor refusal",
			`CREATE TRIGGER cov30_seek_no_move BEFORE UPDATE ON consumers
			 BEGIN SELECT RAISE(ABORT, 'move refused'); END;`,
			"move cursor",
		},
		{
			"event-row refusal",
			`CREATE TRIGGER cov30_seek_no_event BEFORE INSERT ON events
			 BEGIN SELECT RAISE(ABORT, 'event refused'); END;`,
			"insert consumer.seek event row",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			st := openCommandPathStore(t, fakeClock())
			ctx := context.Background()
			cov30SeedHeldStream(t, st, ctx)
			if _, xErr := st.rw.ExecContext(ctx, tc.trig); xErr != nil {
				t.Fatalf("plant trigger: %v", xErr)
			}
			_, err := st.Seek(ctx, "orders", "worker", queue.StartPosition{Kind: queue.StartNew}, false, "op")
			if err == nil || IsCmdError(err) || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("seek = %v, want the unmarked %q refusal", err, tc.want)
			}
		})
	}
}

// TestLiveSynchronousAndDiskFreeObservations pins the /v1/info live observations:
// LiveSynchronous re-reads PRAGMA synchronous from a live pooled connection (and
// must agree with a direct read of the same pool), and DiskFreeBytes reports the
// data directory's unprivileged free space.
func TestLiveSynchronousAndDiskFreeObservations(t *testing.T) {
	st := openCommandPathStore(t, fakeClock())
	ctx := context.Background()

	want := -1
	if err := st.ro.QueryRowContext(ctx, `PRAGMA synchronous`).Scan(&want); err != nil {
		t.Fatalf("read synchronous directly: %v", err)
	}
	if want < 0 || want > 3 {
		t.Fatalf("direct synchronous read = %d, want a plausible 0..3", want)
	}
	got, err := st.LiveSynchronous(ctx)
	if err != nil {
		t.Fatalf("LiveSynchronous: %v", err)
	}
	if got != want {
		t.Errorf("LiveSynchronous = %d, want the pool's live value %d", got, want)
	}

	free, err := st.DiskFreeBytes()
	if err != nil {
		t.Fatalf("DiskFreeBytes: %v", err)
	}
	if free <= 0 {
		t.Errorf("DiskFreeBytes = %d, want the data dir's free bytes", free)
	}
}

// TestInfoCountsCensus pins the /v1/info row census after one stream creation:
// exactly one stream, no consumers, and an append-only audit trail with at least
// the create-stream event.
func TestInfoCountsCensus(t *testing.T) {
	st := openCommandPathStore(t, fakeClock())
	ctx := context.Background()
	if _, _, err := st.CreateStream(ctx, queue.DefaultConfig("orders"), "tester"); err != nil {
		t.Fatalf("create stream: %v", err)
	}
	counts, err := st.InfoCounts(ctx)
	if err != nil {
		t.Fatalf("InfoCounts: %v", err)
	}
	if counts.Streams != 1 || counts.Consumers != 0 || counts.EventsRows < 1 {
		t.Errorf("counts = %+v, want 1 stream, 0 consumers, events >= 1", counts)
	}
}

// TestPendingListAfterCursorAndNextAfter pins the pending-list pagination edges:
// a full page echoes the effective limit and carries NextAfter (the last row's
// seq), the After cursor is exclusive, and a non-full page carries no NextAfter.
func TestPendingListAfterCursorAndNextAfter(t *testing.T) {
	st := openCommandPathStore(t, fakeClock())
	ctx := context.Background()
	cov30SeedHeldStream(t, st, ctx)

	page, err := st.PendingList(ctx, "orders", "worker", PendingQuery{Limit: 2})
	if err != nil {
		t.Fatalf("PendingList: %v", err)
	}
	if page.Limit != 2 || len(page.Items) != 2 ||
		page.Items[0].Seq != 1 || page.Items[1].Seq != 2 ||
		page.Items[0].State != "ready" {
		t.Errorf("page = %+v, want the first two planted rows as ready", page)
	}
	if page.NextAfter == nil || *page.NextAfter != 2 {
		t.Errorf("NextAfter = %v, want 2 (a full page continues at the last seq)", page.NextAfter)
	}

	tail, err := st.PendingList(ctx, "orders", "worker", PendingQuery{Limit: 5, After: 1})
	if err != nil {
		t.Fatalf("PendingList after: %v", err)
	}
	if len(tail.Items) != 2 || tail.Items[0].Seq != 2 || tail.Items[1].Seq != 3 {
		t.Errorf("tail = %+v, want the rows strictly after seq 1", tail)
	}
	if tail.NextAfter != nil {
		t.Errorf("tail.NextAfter = %v, want nil (page not full)", tail.NextAfter)
	}
}
