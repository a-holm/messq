package store

import (
	"context"
	"database/sql"
	"errors"
	"log/slog"
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

	// beforeApply runs inside Apply on the writer goroutine: tests freeze the engine here.
	beforeApply func()
}

func (c *probeCmd) Kind() CmdKind { return c.kind }

func (c *probeCmd) Bytes() int { return c.size }

func (c *probeCmd) Apply(ctx context.Context, tx *sql.Tx, now time.Time) (Result, []obs.Event, error) {
	if c.beforeApply != nil {
		c.beforeApply()
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
func readProbe(t *testing.T, ro *sql.DB) []probeRow {
	t.Helper()
	rows, err := ro.QueryContext(context.Background(), `SELECT k, v, ts FROM probe ORDER BY k`)
	if err != nil {
		t.Fatalf("read probe: %v", err)
	}
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
// than timing: the after_commit_before_reply fault point records "committed" before any
// waiter unblocks, and each caller records "replied" after Do returns. Every reply must come
// after its commit — swapping those steps in commitBatch fails this test.
func TestRepliesFollowCommitInSequence(t *testing.T) {
	var (
		mu    sync.Mutex
		order []string
	)
	record := func(s string) {
		mu.Lock()
		defer mu.Unlock()
		order = append(order, s)
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
		withHooks(hooks{afterCommitBeforeReply: func() { record("committed") }}))
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}

	for i := int64(1); i <= 5; i++ {
		if _, doErr := w.Do(context.Background(),
			&probeCmd{kind: "probe.insert", key: i, val: "v"}); doErr != nil {
			t.Fatalf("Do(%d): %v", i, doErr)
		}
		record("replied")
	}
	if closeErr := w.Close(context.Background()); closeErr != nil {
		t.Fatalf("close: %v", closeErr)
	}

	mu.Lock()
	defer mu.Unlock()
	commits, replies := 0, 0
	for _, s := range order {
		switch s {
		case "committed":
			commits++
			if replies > commits {
				t.Fatalf("sequence %v: a reply preceded a commit", order)
			}
		case "replied":
			replies++
		}
	}
	if commits != 5 || replies != 5 {
		t.Errorf("order = %v, want 5 commits each preceding their reply", order)
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
