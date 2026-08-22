// SPDX-License-Identifier: Apache-2.0

package store

import (
	"context"
	"database/sql"
	"errors"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/a-holm/messq/internal/errs"
	"github.com/a-holm/messq/internal/obs"
)

// probeCmd is the trivial insert-shaped command every engine test drives through the real
// SQLite file: INSERT INTO probe(k, v, ts). The table creates itself inside the transaction,
// so reopened databases keep their rows and every assertion can re-check durability on disk.
// Fields grow one slice at a time alongside the guarantees that need them.
type probeCmd struct {
	kind CmdKind
	key  int64
	val  string
	size int

	// bizErr, when set, is returned as a business rejection BEFORE any row is written:
	// the savepoint must undo nothing because there is nothing to undo, and siblings
	// must survive.
	bizErr error

	// rawErr, when set, is returned UNWRAPPED after the row was written: infrastructure
	// damage in the middle of a batch.
	rawErr error

	// panicVal, when set, is panicked with instead of returning: a bug in a command.
	panicVal any

	// beforeApply runs inside Apply on the writer goroutine with the batch context, so a
	// test can freeze the engine here or forward the context into a nested Do.
	beforeApply func(ctx context.Context)
}

func (c *probeCmd) Kind() CmdKind { return c.kind }

func (c *probeCmd) Bytes() int { return c.size }

func (c *probeCmd) Apply(ctx context.Context, tx *sql.Tx, now time.Time) (Result, []obs.Event, error) {
	if c.beforeApply != nil {
		c.beforeApply(ctx)
	}
	if _, err := tx.ExecContext(ctx,
		`CREATE TABLE IF NOT EXISTS probe (k INTEGER PRIMARY KEY, v TEXT NOT NULL, ts INTEGER NOT NULL)`); err != nil {
		return nil, nil, err
	}
	if _, err := tx.ExecContext(ctx,
		`INSERT INTO probe (k, v, ts) VALUES (?, ?, ?)
		 ON CONFLICT (k) DO UPDATE SET v = excluded.v, ts = excluded.ts`,
		c.key, c.val, now.UnixMilli()); err != nil {
		return nil, nil, err
	}
	// Business rejection AFTER the write: the savepoint has real work to undo, which is what
	// makes the isolation test able to catch a missing ROLLBACK TO.
	if c.bizErr != nil {
		return nil, nil, CmdErr(c.bizErr)
	}
	if c.rawErr != nil {
		return nil, nil, c.rawErr
	}
	if c.panicVal != nil {
		panic(c.panicVal)
	}
	ev := obs.Event{Event: "msg.publish", TS: now.UnixMilli(), Detail: map[string]any{"k": c.key}}
	return c.val, []obs.Event{ev}, nil
}

// sinkRecorder captures every Publish hand-off, copying the slices (the writer may reuse
// nothing today, but the contract does not promise it tomorrow).
type sinkRecorder struct {
	mu  sync.Mutex
	got [][]obs.Event
}

func (s *sinkRecorder) Publish(evs []obs.Event) {
	s.mu.Lock()
	defer s.mu.Unlock()
	cp := make([]obs.Event, len(evs))
	copy(cp, evs)
	s.got = append(s.got, cp)
}

func (s *sinkRecorder) events() []obs.Event {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []obs.Event
	for _, batch := range s.got {
		out = append(out, batch...)
	}
	return out
}

// batches returns a copy of the per-publish event counts: one entry per committed batch.
func (s *sinkRecorder) batches() []int {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]int, len(s.got))
	for i, evs := range s.got {
		out[i] = len(evs)
	}
	return out
}

// asLogger wraps the capture in a slog.Logger for injection through withLogger.
func (h *logCapture) asLogger() *slog.Logger { return slog.New(h) }

// newEngineWriter opens a store plus writer wired to a capturing logger and sink, ready for
// Do-driven tests. Both the writer and its store stay open for the lifetime of the test;
// assertions read through st.RO().
func newEngineWriter(t *testing.T, mutate func(*Config)) (*Writer, *Store, *sinkRecorder) {
	t.Helper()
	cfg := Config{}
	if mutate != nil {
		mutate(&cfg)
	}
	handler := &logCapture{}
	sink := &sinkRecorder{}
	opts := testOptions(testDataDir(t), fakeClock(), handler)
	st, _, err := Open(context.Background(), opts)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() {
		if closeErr := st.Close(context.Background()); closeErr != nil {
			t.Errorf("close store: %v", closeErr)
		}
	})
	w, err := st.NewWriter(cfg, withLogger(handler.asLogger()), withEventSink(sink))
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}
	return w, st, sink
}

// probeRow mirrors one probe table row for reopen assertions.
type probeRow struct {
	K  int64
	V  string
	Ts int64
}

// readProbe reads the probe table through the read pool: the same data a restart would see.
// It takes fatalHelper so both *testing.T and *rapid.T can drive it.
func readProbe(t fatalHelper, ro *sql.DB) []probeRow {
	t.Helper()
	rows, err := ro.QueryContext(context.Background(), `SELECT k, v, ts FROM probe ORDER BY k`)
	if err != nil {
		t.Fatalf("read probe: %v", err)
	}
	return scanProbe(t, rows)
}

// readProbeIfAny is readProbe for batches that may legitimately have left nothing behind,
// table included.
func readProbeIfAny(t fatalHelper, ro *sql.DB) []probeRow {
	t.Helper()
	rows, err := ro.QueryContext(context.Background(), `SELECT k, v, ts FROM probe ORDER BY k`)
	if err != nil {
		if strings.Contains(err.Error(), "no such table") {
			return nil
		}
		t.Fatalf("read probe: %v", err)
	}
	return scanProbe(t, rows)
}

func scanProbe(t fatalHelper, rows *sql.Rows) []probeRow {
	t.Helper()
	defer func() {
		if cerr := rows.Close(); cerr != nil {
			t.Errorf("close probe rows: %v", cerr)
		}
	}()
	var out []probeRow
	for rows.Next() {
		var r probeRow
		if scanErr := rows.Scan(&r.K, &r.V, &r.Ts); scanErr != nil {
			t.Fatalf("scan probe row: %v", scanErr)
		}
		out = append(out, r)
	}
	if iterErr := rows.Err(); iterErr != nil {
		t.Fatalf("iterate probe: %v", iterErr)
	}
	return out
}

// TestDoRoundTripsResultEventsAndDurability is the tracer bullet: one command in, the typed
// result out, one event handed to the sink carrying the batch timestamp, and the row on disk
// through the read pool.
func TestDoRoundTripsResultEventsAndDurability(t *testing.T) {
	w, st, sink := newEngineWriter(t, nil)
	ctx := context.Background()

	res, err := w.Do(ctx, &probeCmd{kind: "probe.insert", key: 1, val: "alpha"})
	if err != nil {
		t.Fatalf("Do: %v", err)
	}
	got, ok := res.(string)
	if !ok || got != "alpha" {
		t.Fatalf("Do result = (%#v, %v), want \"alpha\"", res, err)
	}

	rows := readProbe(t, st.RO())
	if len(rows) != 1 || rows[0].K != 1 || rows[0].V != "alpha" {
		t.Fatalf("probe rows = %+v, want one row {1 alpha}", rows)
	}

	evs := sink.events()
	if len(evs) != 1 {
		t.Fatalf("sink saw %d events, want 1", len(evs))
	}
	if evs[0].Event != "msg.publish" || evs[0].TS != rows[0].Ts {
		t.Errorf("event = %+v, want msg.publish at the row's timestamp %d", evs[0], rows[0].Ts)
	}

	if closeErr := w.Close(ctx); closeErr != nil {
		t.Fatalf("close: %v", closeErr)
	}
}

// TestRepliesFollowCommitInSequence pins reply-after-commit with an ordering hook rather
// than timing. commitBatch runs the after_commit_before_reply hook to completion before it
// closes any waiter (program order on the writer goroutine), so a caller resumed from Do
// deterministically observes every side effect the hook already made — no clock involved.
// Driving Do strictly one at a time makes batch boundaries observable from outside: before
// the i-th submission the hook has fired exactly i-1 times, and after the i-th reply
// exactly i. Fewer after a reply means a reply escaped past its own commit gate (a writer
// that closes waiters before COMMIT or before the hook); more means a batch was committed
// and answered ahead of an earlier caller's reply. Note the invariant is NOT "zero callers
// ever unblocked": in a sequential loop the earlier batches' callers have legitimately
// unblocked by the time a later batch reaches the hook, which is exactly what the previous
// zero-snapshot formulation got wrong — it failed for every conforming writer.
func TestRepliesFollowCommitInSequence(t *testing.T) {
	var (
		mu      sync.Mutex
		firings int
	)
	hookCount := func() int {
		mu.Lock()
		defer mu.Unlock()
		return firings
	}

	handler := &logCapture{}
	sink := &sinkRecorder{}
	opts := testOptions(testDataDir(t), fakeClock(), handler)
	st, _, err := Open(context.Background(), opts)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer func() {
		if closeErr := st.Close(context.Background()); closeErr != nil {
			t.Errorf("close store: %v", closeErr)
		}
	}()
	w, err := st.NewWriter(Config{},
		withLogger(handler.asLogger()),
		withEventSink(sink),
		withHooks(hooks{afterCommitBeforeReply: func() {
			mu.Lock()
			firings++
			mu.Unlock()
		}}))
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}

	for i := 1; i <= 5; i++ {
		if got := hookCount(); got != i-1 {
			t.Fatalf("batch %d: post-commit fault point had fired %d time(s) before submission, want %d", i, got, i-1)
		}
		if _, doErr := w.Do(context.Background(),
			&probeCmd{kind: "probe.insert", key: int64(i), val: "v"}); doErr != nil {
			t.Fatalf("Do(%d): %v", i, doErr)
		}
		if got := hookCount(); got != i {
			t.Fatalf("batch %d: post-commit fault point had fired %d time(s) when the reply returned, want %d — a reply escaped its commit gate", i, got, i)
		}
	}
	if closeErr := w.Close(context.Background()); closeErr != nil {
		t.Fatalf("close: %v", closeErr)
	}
	if got := hookCount(); got != 5 {
		t.Fatalf("hook fired %d times in total, want 5", got)
	}
}

// TestDoAppliesInEnqueueOrder pins FIFO: sequential submissions land in ascending key order
// with non-decreasing timestamps, whatever batching does underneath.
func TestDoAppliesInEnqueueOrder(t *testing.T) {
	w, st, _ := newEngineWriter(t, nil)

	for i := int64(1); i <= 10; i++ {
		if _, err := w.Do(context.Background(),
			&probeCmd{kind: "probe.insert", key: i, val: "x"}); err != nil {
			t.Fatalf("Do(%d): %v", i, err)
		}
	}
	if closeErr := w.Close(context.Background()); closeErr != nil {
		t.Fatalf("close: %v", closeErr)
	}

	rows := readProbe(t, st.RO())
	if len(rows) != 10 {
		t.Fatalf("%d rows persisted, want 10", len(rows))
	}
	lastTs := int64(-1)
	for i, r := range rows {
		if r.K != int64(i+1) {
			t.Errorf("row %d has k=%d, want %d (enqueue order broken)", i, r.K, i+1)
		}
		if r.Ts < lastTs {
			t.Errorf("timestamps decreased mid-stream: %d after %d", r.Ts, lastTs)
		}
		lastTs = r.Ts
	}
}

// TestDoAfterCloseIsRefused pins the structural guard that keeps Close crash-free: a
// submission arriving after shutdown began gets ErrWriterClosing instead of racing a closed
// channel.
func TestDoAfterCloseIsRefused(t *testing.T) {
	w, _, _ := newEngineWriter(t, nil)
	if err := w.Close(context.Background()); err != nil {
		t.Fatalf("close: %v", err)
	}
	_, err := w.Do(context.Background(), &probeCmd{key: 1})
	if !errors.Is(err, errs.ErrShuttingDown) {
		t.Fatalf("Do after Close = %v, want ErrShuttingDown", err)
	}
}
