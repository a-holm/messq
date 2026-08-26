// SPDX-License-Identifier: Apache-2.0

package store

import (
	"context"
	"database/sql"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/a-holm/messq/internal/obs"
	"github.com/a-holm/messq/internal/queue"
)

// The #27 Solo amendment (#27 §3, G6): PRAGMAs cannot run inside a transaction, and a
// SQLITE_BUSY checkpoint is a starved attempt — never a commit-class failure. A Solo
// command therefore runs ALONE, outside every batch transaction, between commit windows,
// and its errors are logged and replied without touching the fsyncgate latch.

// orderLog records the exact sequence of apply/exec events observed on the writer
// goroutine. Program order on one goroutine makes every assertion below clock-free.
type orderLog struct {
	mu      sync.Mutex
	entries []string
}

func (o *orderLog) add(e string) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.entries = append(o.entries, e)
}

func (o *orderLog) snapshot() []string {
	o.mu.Lock()
	defer o.mu.Unlock()
	return append([]string(nil), o.entries...)
}

// testSoloCmd proves the Solo contract is implementable outside the two shipped PRAGMA
// commands. Its batch-path Apply is a tripwire: a conforming engine NEVER calls it.
type testSoloCmd struct {
	label string
	log   *orderLog
	fail  error // when set, ApplySolo replies with this error
}

func (c *testSoloCmd) Kind() CmdKind { return "test_solo" }
func (c *testSoloCmd) Bytes() int    { return 0 }
func (c *testSoloCmd) Solo()         {}

func (c *testSoloCmd) ApplySolo(ctx context.Context, rw *sql.DB, now time.Time) (Result, []obs.Event, error) {
	if c.log != nil {
		c.log.add("solo " + c.label)
	}
	return nil, nil, c.fail
}

func (c *testSoloCmd) Apply(ctx context.Context, tx *sql.Tx, now time.Time) (Result, []obs.Event, error) {
	c.log.add("SOLO-BATCHED-VIOLATION")
	return nil, nil, nil
}

func soloTestStore(t *testing.T) (*Store, *logCapture) {
	t.Helper()
	handler := &logCapture{}
	st, _, err := Open(context.Background(), testOptions(testDataDir(t), fakeClock(), handler))
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() {
		if cerr := st.Close(context.Background()); cerr != nil {
			t.Logf("close store: %v", cerr)
		}
	})
	ctx := context.Background()
	if _, _, err := st.CreateStream(ctx, queue.DefaultConfig("orders"), "test"); err != nil {
		t.Fatalf("create stream: %v", err)
	}
	return st, handler
}

func TestSoloCommandRunsBetweenCommitWindows(t *testing.T) {
	ctx := context.Background()
	handler := &logCapture{}
	fc := fakeClock()
	st, _, err := Open(ctx, testOptions(testDataDir(t), fc, handler))
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer func() {
		if cerr := st.Close(ctx); cerr != nil {
			t.Errorf("close store: %v", cerr)
		}
	}()

	batches := &batchObserver{}
	order := &orderLog{}
	w, err := st.NewWriter(Config{CommitWindow: 50 * time.Millisecond},
		withLogger(handler.asLogger()), withHooks(batches.hooks()))
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}

	// P1 opens a batch and lingers on the window; its apply logs itself.
	req1 := &request{
		cmd: &probeCmd{
			key: 1, val: "a",
			beforeApply: func(context.Context) { order.add("apply") },
		},
		done: make(chan struct{}),
	}
	w.ch <- req1
	fc.BlockUntil(1)

	// The solo command queues behind the open batch (white-box channel send, the same
	// sanctioned pattern as TestBatchClosesOnCommitWindow).
	order.add("queued-solo")
	reqS := &request{cmd: &testSoloCmd{label: "mid", log: order}, done: make(chan struct{})}
	w.ch <- reqS

	// The window closes batch {P1}; the writer then dequeues the solo command and must
	// execute it OUTSIDE any transaction, before P2 forms its own.
	fc.Advance(50 * time.Millisecond)
	<-req1.done

	req2 := &request{
		cmd: &probeCmd{
			key: 2, val: "b",
			beforeApply: func(context.Context) { order.add("apply") },
		},
		done: make(chan struct{}),
	}
	w.ch <- req2
	fc.BlockUntil(1)
	fc.Advance(50 * time.Millisecond)
	<-req2.done

	want := []string{"queued-solo", "apply", "solo mid", "apply"}
	got := order.snapshot()
	if len(got) != len(want) {
		t.Fatalf("order = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("order = %v, want %v (entry %d: %q != %q)", got, want, i, got[i], want[i])
		}
	}
	if n := batches.n.Load(); n != 2 {
		t.Errorf("%d transactions ran, want 2: the solo command must never open or join one", n)
	}

	if werr := w.Close(ctx); werr != nil {
		t.Errorf("close: %v", werr)
	}
}

func TestBusyCheckpointDoesNotLatchTheWriter(t *testing.T) {
	ctx := context.Background()
	st, handler := soloTestStore(t)

	// A live reader pins the WAL: any checkpoint stays busy for its duration.
	roTx, err := st.RO().BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("begin read tx: %v", err)
	}
	defer func() {
		if rerr := roTx.Rollback(); rerr != nil && !errors.Is(rerr, sql.ErrTxDone) {
			t.Logf("rollback read tx: %v", rerr)
		}
	}()
	var n int
	if snapErr := roTx.QueryRowContext(ctx, `SELECT count(*) FROM meta`).Scan(&n); snapErr != nil {
		t.Fatalf("establish reader snapshot: %v", snapErr)
	}

	// Frames past the reader's snapshot make the WAL unresettable.
	body := make([]byte, 4<<10)
	if _, pubErr := st.Publish(ctx, PublishCmd{
		Stream: "orders",
		Req:    queue.PublishReq{Subject: "orders.x", Body: body},
	}); pubErr != nil {
		t.Fatalf("publish under reader: %v", pubErr)
	}

	w, err := st.NewWriter(Config{}, withLogger(handler.asLogger()))
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}
	// TRUNCATE is the mode that must report busy under a held reader (verified against
	// the pinned driver: PASSIVE checkpoints what it can and replies busy=0 even with
	// readers present — it never needs to reset the WAL to do partial work).
	res, err := w.Do(ctx, CheckpointCmd{Mode: "TRUNCATE"})
	if err != nil {
		t.Fatalf("Do(CheckpointCmd): %v", err)
	}
	cp, cpOK := res.(CheckpointResult)
	if !cpOK {
		t.Fatalf("result type %T, want CheckpointResult", res)
	}
	if !cp.Busy {
		t.Fatalf("TRUNCATE checkpoint reported Busy=false under a held reader; the busy signal is how the janitor counts starved attempts")
	}

	// G6: the busy checkpoint is NOT a commit error. The fsyncgate must be silent and
	// the writer fully usable.
	select {
	case fe := <-w.Fatal():
		t.Fatalf("busy checkpoint latched the writer read-only: %+v", fe)
	default:
	}
	if _, err := w.Do(ctx, &probeCmd{key: 9, val: "still-writing"}); err != nil {
		t.Fatalf("writer refused work after a busy checkpoint: %v", err)
	}

	// The other half of G6: a solo command that genuinely FAILS is replied and logged,
	// never latched either. A busy WAL is a starved attempt, not storage damage.
	boom := errors.New("boom: injected solo failure")
	if res, err := w.Do(ctx, &testSoloCmd{label: "fails", fail: boom}); err == nil {
		t.Fatalf("failing solo command replied (%v, nil error)", res)
	} else if !errors.Is(err, boom) {
		t.Fatalf("failing solo command wrapped its error: %v", err)
	}
	select {
	case fe := <-w.Fatal():
		t.Fatalf("failed solo command latched the writer read-only: %+v", fe)
	default:
	}
	if _, err := w.Do(ctx, &probeCmd{key: 10, val: "still-writing-2"}); err != nil {
		t.Fatalf("writer refused work after a failed solo command: %v", err)
	}

	if cerr := w.Close(ctx); cerr != nil {
		t.Errorf("close: %v", cerr)
	}
}

func TestCheckpointTruncateResetsTheWalFile(t *testing.T) {
	ctx := context.Background()
	dir := testDataDir(t)
	handler := &logCapture{}
	st, _, err := Open(ctx, testOptions(dir, fakeClock(), handler))
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer func() {
		if cerr := st.Close(ctx); cerr != nil {
			t.Errorf("close store: %v", cerr)
		}
	}()
	if _, _, cErr := st.CreateStream(ctx, queue.DefaultConfig("orders"), "test"); cErr != nil {
		t.Fatalf("create stream: %v", cErr)
	}

	// Fill the WAL well past any trickle threshold.
	body := make([]byte, 8<<10)
	for i := range body {
		body[i] = byte(i)
	}
	for i := 0; i < 200; i++ {
		if _, perr := st.Publish(ctx, PublishCmd{
			Stream: "orders",
			Req:    queue.PublishReq{Subject: "orders.bulk", Body: body},
		}); perr != nil {
			t.Fatalf("publish %d: %v", i, perr)
		}
	}
	walPath := filepath.Join(dir, "messq.db-wal")
	if fi, serr := os.Stat(walPath); serr != nil || fi.Size() == 0 {
		t.Fatalf("wal missing or empty before checkpoint (stat err=%v)", serr)
	}

	w, err := st.NewWriter(Config{}, withLogger(handler.asLogger()))
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}
	res, err := w.Do(ctx, CheckpointCmd{Mode: "TRUNCATE"})
	if err != nil {
		t.Fatalf("Do(CheckpointCmd TRUNCATE): %v", err)
	}
	if cp, cpOK := res.(CheckpointResult); cpOK && cp.Busy {
		t.Fatalf("TRUNCATE checkpoint reported busy with no readers")
	}
	fi, serr := os.Stat(walPath)
	if serr != nil {
		t.Fatalf("stat wal after TRUNCATE: %v", serr)
	}
	if fi.Size() != 0 {
		t.Fatalf("wal = %d bytes after TRUNCATE, want 0", fi.Size())
	}
	if cerr := w.Close(ctx); cerr != nil {
		t.Errorf("close: %v", cerr)
	}
}

func TestVacuumCmdExecutesAndReports(t *testing.T) {
	ctx := context.Background()
	fc := fakeClock()
	db, err := sql.Open("sqlite", filepath.Join(t.TempDir(), "vac.db"))
	if err != nil {
		t.Fatalf("open scratch db: %v", err)
	}
	defer func() {
		if cerr := db.Close(); cerr != nil {
			t.Logf("close scratch db: %v", cerr)
		}
	}()
	// auto_vacuum=INCREMENTAL must precede any table creation (the #5 pragma hook sets
	// exactly this on real data dirs).
	if _, xerr := db.ExecContext(ctx, `PRAGMA auto_vacuum=INCREMENTAL`); xerr != nil {
		t.Fatalf("set auto_vacuum: %v", xerr)
	}
	if _, xerr := db.ExecContext(ctx,
		`CREATE TABLE bulk (id INTEGER PRIMARY KEY, payload TEXT NOT NULL)`); xerr != nil {
		t.Fatalf("create table: %v", xerr)
	}
	payload := string(make([]byte, 512))
	for i := 0; i < 600; i++ {
		if _, xerr := db.ExecContext(ctx,
			`INSERT INTO bulk (id, payload) VALUES (?, ?)`, i, payload); xerr != nil {
			t.Fatalf("seed row %d: %v", i, xerr)
		}
	}
	if _, xerr := db.ExecContext(ctx, `DELETE FROM bulk`); xerr != nil {
		t.Fatalf("delete rows: %v", xerr)
	}
	freeBefore := queryScratchInt(t, db, `PRAGMA freelist_count`)
	if freeBefore == 0 {
		t.Fatal("freelist empty after mass delete; the test plants no work for the vacuum")
	}

	// The command completes and reports honestly whatever the driver did. On the pinned
	// modernc build this is a verified no-op (FreedPages 0, freelist unchanged) — see
	// the DRIVER FINDING note on VacuumCmd.ApplySolo. The assertion pins the CONTRACT:
	// no error, a well-formed result bounded by the requested page budget, no rows when
	// nothing moved.
	res, _, verr := VacuumCmd{Pages: 50}.ApplySolo(ctx, db, fc.Now())
	if verr != nil {
		t.Fatalf("ApplySolo(VacuumCmd): %v", verr)
	}
	vac, vacOK := res.(VacuumResult)
	if !vacOK {
		t.Fatalf("result type %T, want VacuumResult", res)
	}
	if vac.FreedPages < 0 || vac.FreedPages > 50 {
		t.Fatalf("FreedPages = %d, want within [0,50]", vac.FreedPages)
	}
	if vac.FreedPages > 0 && queryScratchInt(t, db, `PRAGMA freelist_count`) >= freeBefore {
		t.Fatalf("claimed %d freed pages but freelist_count did not drop", vac.FreedPages)
	}

	// Out-of-range budgets are refused before touching the database.
	bad := VacuumCmd{Pages: 0}
	if _, _, berr := bad.ApplySolo(ctx, db, fc.Now()); berr == nil {
		t.Fatal("VacuumCmd{Pages:0} accepted; want a validation refusal")
	}
}

func queryScratchInt(t *testing.T, db *sql.DB, q string) int64 {
	t.Helper()
	var v int64
	if err := db.QueryRowContext(context.Background(), q).Scan(&v); err != nil {
		t.Fatalf("query %q: %v", q, err)
	}
	return v
}
