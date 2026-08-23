// SPDX-License-Identifier: Apache-2.0

package store

import (
	"context"
	"database/sql"
	"errors"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/a-holm/messq/internal/clock"
	"github.com/a-holm/messq/internal/errs"
	"github.com/a-holm/messq/internal/queue"
)

// This file tops up the wired command-path coverage for #7: the enqueue door onto a
// live writer, the engine-less runSolo fallback's refusal arms, the reap-marker
// guard, the multi-chunk delete walk, planted invariant violations and the writer's
// submit-side refusals. Everything runs through public Store/Writer methods on real
// files in t.TempDir(); the only white-box seams are field swaps this package's
// tests already use elsewhere (killSimulate et al).

// yield gives the scheduler a bounded number of turns without sleeping: a goroutine
// that has been spawned has overwhelmingly often reached its first blocking point
// after this many Gosched rounds, which is the same convention waitFor uses.
func yield(rounds int) {
	for i := 0; i < rounds; i++ {
		runtime.Gosched()
	}
}

// openCommandPathStore opens an engine-less store: no NewWriter, so every write goes
// through the runSolo fallback and the raw rw handle stays available for planted
// corruption. Cleanup closes the store cleanly.
func openCommandPathStore(t *testing.T, clk clock.Clock) *Store {
	t.Helper()
	st, _, err := Open(context.Background(), testOptions(testDataDir(t), clk, &logCapture{}))
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() {
		if closeErr := st.Close(context.Background()); closeErr != nil {
			t.Errorf("close store: %v", closeErr)
		}
	})
	return st
}

// openWiredCommandPathStore opens a store with its writer attached. Cleanup closes
// the writer first (LIFO), then the store, mirroring the ownership contract.
func openWiredCommandPathStore(t *testing.T, clk clock.Clock, cfg Config, hks hooks) (*Store, *Writer) {
	t.Helper()
	st, _, err := Open(context.Background(), testOptions(testDataDir(t), clk, &logCapture{}))
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() {
		if closeErr := st.Close(context.Background()); closeErr != nil {
			t.Errorf("close store: %v", closeErr)
		}
	})
	w, err := st.NewWriter(cfg, withLogger((&logCapture{}).asLogger()),
		withEventSink(&sinkRecorder{}), withHooks(hks))
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}
	t.Cleanup(func() {
		if closeErr := w.Close(context.Background()); closeErr != nil {
			t.Errorf("close writer: %v", closeErr)
		}
	})
	return st, w
}

// swapRW installs replacement as the store's rw handle and returns the restore
// function. A cleanup fallback restores too, so an interrupted subtest cannot leak
// the swap past the store's Close.
func swapRW(t *testing.T, st *Store, replacement *sql.DB) (restore func()) {
	t.Helper()
	st.mu.Lock()
	orig := st.rw
	st.rw = replacement
	st.mu.Unlock()
	restored := false
	restore = func() {
		st.mu.Lock()
		if st.rw == replacement {
			st.rw = orig
		}
		st.mu.Unlock()
		restored = true
	}
	t.Cleanup(func() {
		if !restored {
			restore()
		}
	})
	return restore
}

// installQueryOnlyRW points the store's rw handle at a real second pool on the same
// database file opened with query_only=1: reads succeed, the first write statement
// is refused by the driver. This is how the runSolo infrastructure-error arms are
// reached deterministically — a genuine read-only file under the store.
func installQueryOnlyRW(t *testing.T, st *Store) (restore func()) {
	t.Helper()
	qo, err := sql.Open(driverName, "file:"+dbPath(st.dir)+"?_query_only=1")
	if err != nil {
		t.Fatalf("open query-only handle: %v", err)
	}
	if pingErr := qo.PingContext(context.Background()); pingErr != nil {
		if cerr := qo.Close(); cerr != nil {
			t.Logf("close query-only handle after failed ping: %v", cerr)
		}
		t.Fatalf("ping query-only handle: %v", pingErr)
	}
	t.Cleanup(func() {
		if cerr := qo.Close(); cerr != nil {
			t.Errorf("close query-only handle: %v", cerr)
		}
	})
	return swapRW(t, st, qo)
}

// installClosedRO swaps the read pool for an already-closed handle: every read-path
// query fails at the database/sql layer with ErrConnDone before any driver runs.
func installClosedRO(t *testing.T, st *Store) (restore func()) {
	t.Helper()
	dead, err := sql.Open(driverName, "file:"+dbPath(st.dir))
	if err != nil {
		t.Fatalf("open dead handle: %v", err)
	}
	if cerr := dead.Close(); cerr != nil {
		t.Errorf("close dead handle: %v", cerr)
	}
	return swapRO(t, st, dead)
}

func swapRO(t *testing.T, st *Store, replacement *sql.DB) (restore func()) {
	t.Helper()
	st.mu.Lock()
	orig := st.ro
	st.ro = replacement
	st.mu.Unlock()
	restored := false
	restore = func() {
		st.mu.Lock()
		if st.ro == replacement {
			st.ro = orig
		}
		st.mu.Unlock()
		restored = true
	}
	t.Cleanup(func() {
		if !restored {
			restore()
		}
	})
	return restore
}

// expectClosedRefusal asserts err is database/sql's closed-handle refusal. A query on a
// closed *sql.DB fails with the unexported "sql: database is closed" sentinel — not
// ErrConnDone, which only an already-acquired *sql.Conn produces — and never with a
// fabricated business answer.
func expectClosedRefusal(t *testing.T, op string, err error) {
	t.Helper()
	if err == nil || !strings.Contains(err.Error(), "database is closed") {
		t.Errorf("%s on a closed read pool = %v, want database/sql's closed-handle refusal", op, err)
	}
}

// submitCmdAsync runs one arbitrary command through Writer.Do on its own goroutine.
func submitCmdAsync(w *Writer, cmd Cmd) <-chan DoResult {
	out := make(chan DoResult, 1)
	go func() {
		res, err := w.Do(context.Background(), cmd)
		out <- DoResult{Res: res, Err: err}
	}()
	return out
}

// parkedBlocker builds a probe whose Apply freezes the writer goroutine between the
// savepoint and its write: parked closes when the freeze begins, release unfreezes.
func parkedBlocker(parked, release chan struct{}) *probeCmd {
	return &probeCmd{
		key: 900, val: "blocker", size: 0,
		beforeApply: func(_ context.Context) {
			close(parked)
			<-release
		},
	}
}

// TestWiredWriterCommandDoors pins the wired door: with a writer attached, every
// Store write method funnels through enqueue's writer arm, and the pre-transaction
// validation refusals of each method fire before anything reaches the engine.
func TestWiredWriterCommandDoors(t *testing.T) {
	st, _ := openWiredCommandPathStore(t, fakeClock(), Config{}, hooks{})
	ctx := context.Background()

	// Pre-transaction refusals, one per method — none of these may enqueue.
	if _, err := st.Publish(ctx, PublishCmd{Stream: "bad name"}); err == nil {
		t.Fatal("Publish accepted an invalid stream name")
	}
	if _, err := st.UpdateStream(ctx, "bad name", StreamPatch{}, false, "t"); err == nil {
		t.Fatal("UpdateStream accepted an invalid stream name")
	}
	if _, err := st.DeleteStream(ctx, "bad name", "bad name", "t"); err == nil {
		t.Fatal("DeleteStream accepted an invalid stream name")
	}
	if _, err := st.PeekSeq(ctx, "bad name", 1); err == nil {
		t.Fatal("PeekSeq accepted an invalid stream name")
	}
	if _, err := st.ListMessages(ctx, ListQuery{Stream: "bad name"}); err == nil {
		t.Fatal("ListMessages accepted an invalid stream name")
	}
	// "[" is a legal literal token in this grammar; a genuinely malformed filter is
	// one with an empty token (rule S2), which subject.ParseSet refuses.
	if _, err := st.ListMessages(ctx, ListQuery{Stream: "orders", Subject: "orders..created"}); !errors.Is(err, errs.ErrBadRequest) {
		t.Fatalf("ListMessages on a malformed subject filter = %v, want ErrBadRequest", err)
	}
	if _, err := st.GetStream(ctx, "bad name"); err == nil {
		t.Fatal("GetStream accepted an invalid stream name")
	}
	if _, err := st.SweepDedup(ctx, "bad name"); err == nil {
		t.Fatal("SweepDedup accepted an invalid stream name")
	}
	if _, err := st.PublishBatch(ctx, BatchCmd{Stream: "bad name", Reqs: []queue.PublishReq{
		{Subject: "a", Body: []byte("b")},
	}}); err == nil {
		t.Fatal("PublishBatch accepted an invalid stream name")
	}

	// Wired happy path: create, publish, and read back through the engine door.
	cfg := queue.DefaultConfig("orders")
	if _, existed, cErr := st.CreateStream(ctx, cfg, "tester"); cErr != nil || existed {
		t.Fatalf("CreateStream: err=%v existed=%v", cErr, existed)
	}
	ack, err := st.Publish(ctx, PublishCmd{
		Stream: "orders",
		Req:    queue.PublishReq{Subject: "orders.new", Body: []byte("hi")},
	})
	if err != nil {
		t.Fatalf("Publish through the wired door: %v", err)
	}
	if ack.Seq != 1 || ack.Duplicate {
		t.Fatalf("ack = %+v, want seq 1 non-duplicate", ack)
	}
	if _, err := st.PublishBatch(ctx, BatchCmd{Stream: "ghost", Reqs: []queue.PublishReq{
		{Subject: "a", Body: []byte("b")},
	}}); !errors.Is(err, errs.ErrNotFound) {
		t.Fatalf("PublishBatch on a missing stream = %v, want ErrNotFound", err)
	}
	if _, err := st.GetStream(ctx, "ghost"); !errors.Is(err, errs.ErrNotFound) {
		t.Fatalf("GetStream on a missing stream = %v, want ErrNotFound", err)
	}
}

// TestRunSoloRefusalArms walks the engine-less fallback's infrastructure refusals:
// a pre-cancelled context never acquires a connection, and against a genuinely
// read-only file every command's first write statement fails with an unmarked
// (non-CmdError) driver error — the fsyncgate class, one arm per command.
func TestRunSoloRefusalArms(t *testing.T) {
	clk := fakeClock()
	st := openCommandPathStore(t, clk)
	ctx := context.Background()

	if _, _, err := st.CreateStream(ctx, queue.DefaultConfig("orders"), "tester"); err != nil {
		t.Fatalf("seed create: %v", err)
	}
	if _, err := st.Publish(ctx, PublishCmd{
		Stream: "orders",
		Req:    queue.PublishReq{Subject: "orders.new", Body: []byte("seed"), MsgID: "k1"},
	}); err != nil {
		t.Fatalf("seed publish: %v", err)
	}

	// Pre-cancelled context: refused at connection acquisition, before any SQL.
	deadCtx, cancel := context.WithCancel(ctx)
	cancel()
	if _, err := st.Publish(deadCtx, PublishCmd{
		Stream: "orders",
		Req:    queue.PublishReq{Subject: "orders.new", Body: []byte("x")},
	}); err == nil ||
		!strings.Contains(err.Error(), "acquire writer connection") {
		t.Fatalf("cancelled ctx = %v, want connection-acquisition refusal", err)
	}

	restore := installQueryOnlyRW(t, st)
	maxMsgs := int64(1000)
	refusals := []struct {
		name string
		op   func() error
	}{
		{"create-fresh-stream", func() error {
			_, _, err := st.CreateStream(ctx, queue.DefaultConfig("fresh"), "tester")
			return err
		}},
		{"update-stream", func() error {
			_, err := st.UpdateStream(ctx, "orders", StreamPatch{MaxMsgs: &maxMsgs}, false, "tester")
			return err
		}},
		{"delete-stream", func() error {
			_, err := st.DeleteStream(ctx, "orders", "orders", "tester")
			return err
		}},
		{"publish", func() error {
			_, err := st.Publish(ctx, PublishCmd{
				Stream: "orders",
				Req:    queue.PublishReq{Subject: "orders.new", Body: []byte("b")},
			})
			return err
		}},
		{"sweep-dedup", func() error {
			clk.Advance(24 * time.Hour) // the seeded dedup key expires
			_, err := st.SweepDedup(ctx, "orders")
			return err
		}},
	}
	for _, tc := range refusals {
		err := tc.op()
		if err == nil {
			t.Errorf("%s: write against a query-only handle succeeded", tc.name)
			continue
		}
		if IsCmdError(err) {
			t.Errorf("%s: driver refusal was marked as a business rejection: %v", tc.name, err)
		}
		if !strings.Contains(err.Error(), "readonly") {
			t.Errorf("%s: error does not name the read-only refusal: %v", tc.name, err)
		}
	}
	restore()

	// The real handle still works after the swap-back.
	if _, err := st.Publish(ctx, PublishCmd{
		Stream: "orders",
		Req:    queue.PublishReq{Subject: "orders.new", Body: []byte("after")},
	}); err != nil {
		t.Fatalf("publish after restore: %v", err)
	}
}

// TestReapMarkerGarbageRefused pins the reap guard's corrupt-marker arm: a marker
// whose value is not an integer refuses the recreate with the meta-corruption
// message instead of a fabricated remaining count.
func TestReapMarkerGarbageRefused(t *testing.T) {
	st := openCommandPathStore(t, fakeClock())
	ctx := context.Background()
	if err := upsertMetaDB(ctx, st.rw, metaReapPrefix+"orders2", "soon"); err != nil {
		t.Fatalf("plant reap marker: %v", err)
	}
	t.Cleanup(func() {
		if _, delErr := st.rw.ExecContext(ctx, `DELETE FROM meta WHERE k = ?`,
			metaReapPrefix+"orders2"); delErr != nil {
			t.Errorf("clear planted reap marker: %v", delErr)
		}
	})
	_, _, err := st.CreateStream(ctx, queue.DefaultConfig("orders2"), "tester")
	if err == nil || !strings.Contains(err.Error(), "is not an integer") {
		t.Fatalf("create over a garbage reap marker = %v, want not-an-integer refusal", err)
	}
}

// TestDeleteStreamMultiChunkReap walks the chunk loop past one full chunk: with more
// than deleteChunkRows messages the first chunk comes back full (marker stays), the
// short final chunk clears it, and the name is immediately recreatable.
func TestDeleteStreamMultiChunkReap(t *testing.T) {
	st, _ := openWiredCommandPathStore(t, fakeClock(), Config{}, hooks{})
	ctx := context.Background()

	const total = deleteChunkRows + 5
	if _, _, err := st.CreateStream(ctx, queue.DefaultConfig("bulk"), "tester"); err != nil {
		t.Fatalf("create bulk stream: %v", err)
	}
	const perBatch = 500
	for off := 0; off < total; off += perBatch {
		n := perBatch
		if off+n > total {
			n = total - off
		}
		reqs := make([]queue.PublishReq, n)
		for i := range reqs {
			reqs[i] = queue.PublishReq{Subject: "bulk.hit", Body: []byte("payload")}
		}
		ack, err := st.PublishBatch(ctx, BatchCmd{Stream: "bulk", Reqs: reqs})
		if err != nil {
			t.Fatalf("seed batch at %d: %v", off, err)
		}
		if len(ack.Results) != n {
			t.Fatalf("seed batch at %d: %d results, want %d", off, len(ack.Results), n)
		}
	}

	deleted, err := st.DeleteStream(ctx, "bulk", "bulk", "tester")
	if err != nil {
		t.Fatalf("multi-chunk delete: %v", err)
	}
	if deleted.Messages != total {
		t.Fatalf("deleted %d messages, want %d", deleted.Messages, total)
	}
	// The final chunk cleared the reap marker: the name is free again.
	if _, existed, cErr := st.CreateStream(ctx, queue.DefaultConfig("bulk"), "tester"); cErr != nil || existed {
		t.Fatalf("recreate after reap: err=%v existed=%v", cErr, existed)
	}
}

// TestCheckPublishInvariantsPlantedViolations reaches the two violation arms the
// existing suite misses by planting the corruption directly: a deleted sequence
// counter and a high-water mark the live counter would reuse.
func TestCheckPublishInvariantsPlantedViolations(t *testing.T) {
	st := openCommandPathStore(t, fakeClock())
	ctx := context.Background()

	if _, _, err := st.CreateStream(ctx, queue.DefaultConfig("p2a"), "tester"); err != nil {
		t.Fatalf("create p2a: %v", err)
	}
	if _, err := st.rw.ExecContext(ctx, `DELETE FROM stream_seq WHERE stream = ?`, "p2a"); err != nil {
		t.Fatalf("delete stream_seq row: %v", err)
	}
	if _, _, err := st.CreateStream(ctx, queue.DefaultConfig("p2b"), "tester"); err != nil {
		t.Fatalf("create p2b: %v", err)
	}
	if err := upsertMetaDB(ctx, st.rw, metaSeqHwmPrefix+"p2b", "999999"); err != nil {
		t.Fatalf("plant seq hwm: %v", err)
	}

	vs, err := st.CheckPublishInvariants(ctx)
	if err != nil {
		t.Fatalf("CheckPublishInvariants: %v", err)
	}
	var missingCounter, reusedHwm bool
	for _, v := range vs {
		if v.Stream == "p2a" && v.ID == "P2" && strings.Contains(v.Detail, "stream_seq row missing") {
			missingCounter = true
		}
		if v.Stream == "p2b" && v.ID == "P2" && strings.Contains(v.Detail, "reused deleted hwm") {
			reusedHwm = true
		}
	}
	if !missingCounter {
		t.Errorf("no P2 violation for the deleted stream_seq row; got %+v", vs)
	}
	if !reusedHwm {
		t.Errorf("no P2 violation for the reused high-water mark; got %+v", vs)
	}

	// With the read pool dead the audit fails instead of silently passing.
	restore := installClosedRO(t, st)
	_, auditErr := st.CheckPublishInvariants(ctx)
	expectClosedRefusal(t, "CheckPublishInvariants", auditErr)
	restore()
}

// TestClosedReadPoolRefusals pins the read methods' first-query failure shape: a closed
// read pool surfaces database/sql's closed-handle refusal, never a fabricated answer.
func TestClosedReadPoolRefusals(t *testing.T) {
	st := openCommandPathStore(t, fakeClock())
	ctx := context.Background()
	if _, _, err := st.CreateStream(ctx, queue.DefaultConfig("orders"), "tester"); err != nil {
		t.Fatalf("create: %v", err)
	}

	restore := installClosedRO(t, st)
	defer restore()
	_, getErr := st.GetStream(ctx, "orders")
	expectClosedRefusal(t, "GetStream", getErr)
	_, listErr := st.ListStreams(ctx)
	expectClosedRefusal(t, "ListStreams", listErr)
	_, peekErr := st.PeekSeq(ctx, "orders", 1)
	expectClosedRefusal(t, "PeekSeq", peekErr)
	_, pageErr := st.ListMessages(ctx, ListQuery{Stream: "orders"})
	expectClosedRefusal(t, "ListMessages", pageErr)
}

// freezeForSubmit parks the writer inside a blocker Apply, then fills the command
// channel to capacity with real requests: any further submit can only wait in Do's
// enqueue select. Returns the release function and the stuffed requests.
func freezeForSubmit(t *testing.T, w *Writer, depth int) (release func()) {
	t.Helper()
	parked := make(chan struct{})
	releaseCh := make(chan struct{})
	blocker := parkedBlocker(parked, releaseCh)
	out := submitCmdAsync(w, blocker)
	<-parked // deterministic: the writer goroutine is inside Apply now
	for i := 0; i < depth; i++ {
		w.ch <- &request{cmd: &probeCmd{key: int64(910 + i), val: "stuffed"}, done: make(chan struct{})}
	}
	return func() { close(releaseCh); <-out }
}

// TestWriterSubmitParkedRefusals covers what happens to a caller parked on Do's enqueue
// select against a FULL channel: a cancelled context must win the parked select, and a
// Close landing under the park must resolve the submit — by drain-applied result or by
// the closing guard — never a deadlock, a panic, or a silent loss.
func TestWriterSubmitParkedRefusals(t *testing.T) {
	t.Run("cancel while parked on a full channel", func(t *testing.T) {
		_, w := openWiredCommandPathStore(t, fakeClock(), Config{QueueDepth: 1}, hooks{})
		release := freezeForSubmit(t, w, 1)

		ctx, cancel := context.WithCancel(context.Background())
		waiter := make(chan DoResult, 1)
		go func() {
			res, err := w.Do(ctx, &probeCmd{key: 920, val: "waiter"})
			waiter <- DoResult{Res: res, Err: err}
		}()
		yield(200000) // the waiter reaches the enqueue select long before this returns
		cancel()
		got := <-waiter
		if !errors.Is(got.Err, context.Canceled) {
			t.Fatalf("parked submit with a cancelled ctx = %v, want context.Canceled", got.Err)
		}
		release()
	})

	t.Run("writer closes under a parked submit", func(t *testing.T) {
		_, w := openWiredCommandPathStore(t, fakeClock(), Config{QueueDepth: 2}, hooks{})
		release := freezeForSubmit(t, w, 2)

		waiter := make(chan DoResult, 1)
		go func() {
			res, werr := w.Do(context.Background(), &probeCmd{key: 921, val: "waiter"})
			waiter <- DoResult{Res: res, Err: werr}
		}()
		yield(200000)

		closed := make(chan error, 1)
		go func() { closed <- w.Close(context.Background()) }()
		release() // the drain frees slots, so the parked send can complete

		// Both resolutions are contractual under drain-before-exit: the drain may
		// complete the parked send and apply the command, or the closing guard may
		// win the race and refuse it. The guarantee Close makes is that the submit
		// RESOLVES — no deadlock, no panic, no silent loss.
		got := <-waiter
		applied := got.Err == nil && got.Res == "waiter"
		if !applied && !errors.Is(got.Err, ErrWriterClosing) {
			t.Fatalf("submit parked across Close resolved as (%v, %v), want the applied result or ErrWriterClosing", got.Res, got.Err)
		}
		if err := <-closed; err != nil {
			t.Fatalf("close writer: %v", err)
		}
	})
}

// TestTakeBatchBudgetEdges pins the batch-closing rules through the public door:
// the byte budget holds an arrival for the next batch with and without a linger
// window, and CommitMaxBatch=1 closes the batch before the drain loop even starts.
func TestTakeBatchBudgetEdges(t *testing.T) {
	t.Run("byte budget holds with window zero", func(t *testing.T) {
		obs := &batchObserver{}
		_, w := openWiredCommandPathStore(t, fakeClock(),
			Config{CommitMaxBytes: 100}, obs.hooks())
		release := freezeForSubmit(t, w, 0)

		rA := submitAsync(w, 1, "A", 60)
		rB := submitAsync(w, 2, "B", 60)
		release()
		if got := (<-rA).Res; got != "A" {
			t.Fatalf("A applied as %v", got)
		}
		if got := (<-rB).Res; got != "B" {
			t.Fatalf("B applied as %v", got)
		}
		if n := obs.n.Load(); n != 3 { // blocker, A, B-held-then-alone
			t.Fatalf("%d transactions, want 3 (the byte budget must split A and B)", n)
		}
	})

	t.Run("max batch one closes before the drain", func(t *testing.T) {
		obs := &batchObserver{}
		_, w := openWiredCommandPathStore(t, fakeClock(),
			Config{CommitMaxBatch: 1}, obs.hooks())
		release := freezeForSubmit(t, w, 0)

		rC := submitAsync(w, 3, "C", 0)
		rD := submitAsync(w, 4, "D", 0)
		release()
		if got := (<-rC).Res; got != "C" {
			t.Fatalf("C applied as %v", got)
		}
		if got := (<-rD).Res; got != "D" {
			t.Fatalf("D applied as %v", got)
		}
		if n := obs.n.Load(); n != 3 {
			t.Fatalf("%d transactions, want 3 (one command per batch)", n)
		}
	})

	t.Run("byte budget holds during a linger window", func(t *testing.T) {
		obs := &batchObserver{}
		_, w := openWiredCommandPathStore(t, clock.System{},
			Config{CommitWindow: 25 * time.Millisecond, CommitMaxBytes: 100}, obs.hooks())
		release := freezeForSubmit(t, w, 0)

		rF := submitAsync(w, 5, "F", 60)
		rG := submitAsync(w, 6, "G", 60)
		release()
		if got := (<-rF).Res; got != "F" {
			t.Fatalf("F applied as %v", got)
		}
		if got := (<-rG).Res; got != "G" {
			t.Fatalf("G applied as %v", got)
		}
		if n := obs.n.Load(); n != 3 {
			t.Fatalf("%d transactions, want 3 (the byte budget must split F and G)", n)
		}
	})
}

// TestNilCommandAndSentinelUnits covers the small public seams beside the submit
// path: the nil-command refusal, the nil-passthrough of CmdErr, FatalError's
// printing contract and the synchronous-word renderer's closed set.
func TestNilCommandAndSentinelUnits(t *testing.T) {
	_, w := openWiredCommandPathStore(t, fakeClock(), Config{}, hooks{})
	if _, err := w.Do(context.Background(), nil); err == nil || !strings.Contains(err.Error(), "nil command") {
		t.Fatalf("Do(nil) = %v, want the nil-command refusal", err)
	}

	if err := CmdErr(nil); err != nil {
		t.Fatalf("CmdErr(nil) = %v, want nil", err)
	}
	if IsCmdError(nil) {
		t.Fatal("IsCmdError(nil) = true")
	}

	cause := errors.New("boom")
	fe := &FatalError{Op: "commit", Err: cause, Class: "unknown"}
	if fe.Error() != "boom" {
		t.Fatalf("FatalError.Error() = %q", fe.Error())
	}
	if !errors.Is(fe.Unwrap(), cause) {
		t.Fatal("FatalError.Unwrap() lost the cause")
	}

	words := map[int]string{
		0: "OFF", 1: "NORMAL", 2: "FULL", 3: "EXTRA", 99: "99",
	}
	for v, want := range words {
		if got := synchronousObservedWord(v); got != want {
			t.Errorf("synchronousObservedWord(%d) = %q, want %q", v, got, want)
		}
	}
}
