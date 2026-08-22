// SPDX-License-Identifier: Apache-2.0

package store

import (
	"context"
	"errors"
	"log/slog"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/a-holm/messq/internal/errs"
)

// observerRecorder implements obs.CommitObserver, capturing what the engine reported.
type observerRecorder struct {
	mu       sync.Mutex
	commits  []obsCommit
	readonly int
}

type obsCommit struct {
	batch int
	err   error
}

func (r *observerRecorder) ObserveCommit(batch int, _ time.Duration, err error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.commits = append(r.commits, obsCommit{batch: batch, err: err})
}

func (r *observerRecorder) ObserveQueueDepth(int) {}

func (r *observerRecorder) SetReadOnly(ro bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if ro {
		r.readonly++
	}
}

func (r *observerRecorder) readOnlyCount() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.readonly
}

// atomicCounter is a tiny contention-free counter for hook invocation counts.
type atomicCounter struct {
	mu sync.Mutex
	n  int
}

func (c *atomicCounter) Add(delta int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.n += delta
}

func (c *atomicCounter) Load() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.n
}

// openFailingWriter builds a writer whose commit step is replaced by an injection: the
// before_commit fault point returns inject instead of letting COMMIT run. attempts counts how
// often the engine reached the commit step — the fsyncgate rule says exactly once, ever.
func openFailingWriter(t *testing.T, inject error, rec *observerRecorder) (*Store, *Writer, *logCapture, *atomicCounter) {
	t.Helper()
	handler := &logCapture{}
	attempts := &atomicCounter{}
	st, _, err := Open(context.Background(), testOptions(testDataDir(t), fakeClock(), handler))
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() {
		if closeErr := st.Close(context.Background()); closeErr != nil {
			t.Errorf("close store: %v", closeErr)
		}
	})
	hks := hooks{beforeCommit: func() error {
		attempts.Add(1)
		return inject
	}}
	w, err := st.NewWriter(Config{},
		withLogger(handler.asLogger()),
		withHooks(hks),
		withObserver(rec))
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}
	return st, w, handler, attempts
}

// TestCommitErrorLatchesReadOnlyOnce pins the fsyncgate rule end to end: one injected EIO at
// the commit step produces exactly one storage.fatal at ERROR with op/class named, flips the
// observer's read-only gauge, closes the rw pool, delivers exactly one FatalError, refuses
// every later write with ErrReadOnly while the read pool keeps answering — and never retries
// the commit: the attempt counter stays at 1 no matter how many commands arrive after.
func TestCommitErrorLatchesReadOnlyOnce(t *testing.T) {
	rec := &observerRecorder{}
	st, w, handler, attempts := openFailingWriter(t, syscall.EIO, rec)
	ctx := context.Background()

	_, err := w.Do(ctx, &probeCmd{key: 1, val: "v"})
	if !errors.Is(err, ErrCommitUnknown) {
		t.Fatalf("doomed caller got %v, want ErrCommitUnknown", err)
	}

	// Exactly one fatal, exactly the right shape.
	select {
	case fe := <-w.Fatal():
		if fe.Op != "commit" {
			t.Errorf("FatalError.Op = %q, want commit", fe.Op)
		}
		if fe.Class != "eio" {
			t.Errorf("FatalError.Class = %q, want eio", fe.Class)
		}
		if fe.Batch != 1 {
			t.Errorf("FatalError.Batch = %d, want 1", fe.Batch)
		}
		if !errors.Is(fe.Err, syscall.EIO) {
			t.Errorf("FatalError.Err = %v, want it wrapping EIO", fe.Err)
		}
		if fe.At.IsZero() {
			t.Error("FatalError.At is zero")
		}
	default:
		t.Fatal("Fatal() delivered nothing")
	}
	select {
	case fe := <-w.Fatal():
		t.Fatalf("Fatal() delivered a SECOND fault: %+v", fe)
	default:
	}

	// One storage.fatal line at ERROR, naming op and class.
	fatals := handler.find(slog.LevelError, "storage.fatal")
	if len(fatals) != 1 {
		t.Fatalf("storage.fatal logged %d times, want exactly 1", len(fatals))
	}
	attrs := map[string]string{}
	fatals[0].Attrs(func(a slog.Attr) bool {
		attrs[a.Key] = a.Value.String()
		return true
	})
	for k, want := range map[string]string{"op": "commit", "class": "eio"} {
		if attrs[k] != want {
			t.Errorf("storage.fatal[%s] = %q, want %q", k, attrs[k], want)
		}
	}

	// The latch refuses everything that comes after — immediately, with ErrReadOnly — and
	// never reaches the commit step again.
	for i := int64(2); i <= 4; i++ {
		_, err := w.Do(ctx, &probeCmd{key: i, val: "v"})
		if !errors.Is(err, errs.ErrReadOnly) {
			t.Fatalf("Do after latch = %v, want ErrReadOnly", err)
		}
		if !errors.Is(err, ErrWriterLatched) {
			t.Errorf("latch error lost its store sentinel wrapping: %v", err)
		}
	}
	if got := attempts.Load(); got != 1 {
		t.Errorf("commit step attempted %d times, want exactly 1 — the fsync was retried", got)
	}

	// The same refusal applies to commands already past Do's front door: one queued directly
	// must be failed by the batch loop's own latch check, again without touching the commit
	// step — this is what "never retry" means for work accepted before the fault.
	req := &request{cmd: &probeCmd{key: 50, val: "queued"}, done: make(chan struct{})}
	w.ch <- req
	<-req.done
	if !errors.Is(req.err, ErrWriterLatched) || !errors.Is(req.err, errs.ErrReadOnly) {
		t.Fatalf("queued-after-latch request got %v, want ErrReadOnly", req.err)
	}
	if got := attempts.Load(); got != 1 {
		t.Errorf("attempts grew to %d after a latched batch ran — the commit step was reached again", got)
	}
	if got := rec.readOnlyCount(); got != 1 {
		t.Errorf("SetReadOnly(true) seen %d times, want exactly 1", got)
	}

	// The rw pool is structurally gone; the ro pool still answers.
	if open := w.rw.Stats().OpenConnections; open != 0 {
		t.Errorf("rw pool still has %d open connections after the latch", open)
	}
	var n int
	if scanErr := st.RO().QueryRowContext(ctx, `SELECT count(*) FROM meta`).Scan(&n); scanErr != nil {
		t.Errorf("read pool unusable after latch: %v", scanErr)
	}

	if err := w.Close(ctx); err != nil {
		t.Errorf("close after latch: %v", err)
	}
}

// TestCommitErrorClassesCoverEnospcAndUnknown pins the classification table at the latch:
// ENOSPC lands as class enospc, an untyped driver error as unknown — each logged once.
func TestCommitErrorClassesCoverEnospcAndUnknown(t *testing.T) {
	cases := []struct {
		name   string
		inject error
		class  string
	}{
		{"enospc", syscall.ENOSPC, "enospc"},
		{"unknown", errors.New("sqlite3_step: confused"), "unknown"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rec := &observerRecorder{}
			_, w, handler, attempts := openFailingWriter(t, tc.inject, rec)

			if _, err := w.Do(context.Background(), &probeCmd{key: 1, val: "v"}); !errors.Is(err, ErrCommitUnknown) {
				t.Fatalf("doomed caller got %v, want ErrCommitUnknown", err)
			}
			fe := <-w.Fatal()
			if fe.Class != tc.class {
				t.Errorf("class = %q, want %q", fe.Class, tc.class)
			}
			fatals := handler.find(slog.LevelError, "storage.fatal")
			if len(fatals) != 1 {
				t.Fatalf("storage.fatal lines = %d, want 1", len(fatals))
			}
			if got := attempts.Load(); got != 1 {
				t.Errorf("attempts = %d, want 1", got)
			}
			if err := w.Close(context.Background()); err != nil {
				t.Errorf("close: %v", err)
			}
		})
	}
}

// TestApplyPanicLatchesAndRollsBack pins §10.4: a command bug must never half-apply a
// transaction. The panic is recovered, the WHOLE batch (including the innocent first
// command's already-written row) is rolled back, the process latches read-only with op=apply,
// and nothing survives the reopen.
func TestApplyPanicLatchesAndRollsBack(t *testing.T) {
	ctx := context.Background()
	dir := testDataDir(t)
	handler := &logCapture{}
	fc := fakeClock()
	opts := testOptions(dir, fc, handler)
	st, _, err := Open(ctx, opts)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer func() {
		if closeErr := st.Close(ctx); closeErr != nil {
			t.Errorf("close store: %v", closeErr)
		}
	}()
	rec := &observerRecorder{}
	w, err := st.NewWriter(Config{CommitWindow: 50 * time.Millisecond},
		withLogger(handler.asLogger()), withObserver(rec))
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}

	r1 := submitAsync(w, 1, "innocent", 0)
	fc.BlockUntil(1)
	req2 := &request{cmd: &probeCmd{key: 2, val: "bomb", panicVal: "boom: index out of range"}, done: make(chan struct{})}
	w.ch <- req2
	fc.Advance(50 * time.Millisecond)

	if got := <-r1; !errors.Is(got.Err, ErrCommitUnknown) {
		t.Fatalf("innocent caller got %v, want ErrCommitUnknown", got.Err)
	}
	<-req2.done

	fe := <-w.Fatal()
	if fe.Op != "apply" {
		t.Errorf("FatalError.Op = %q, want apply", fe.Op)
	}
	if !strings.Contains(fe.Err.Error(), "boom: index out of range") {
		t.Errorf("FatalError.Err = %v, want it carrying the panic value", fe.Err)
	}
	if got := rec.readOnlyCount(); got != 1 {
		t.Errorf("SetReadOnly seen %d times, want 1", got)
	}
	if _, lateErr := w.Do(ctx, &probeCmd{key: 9, val: "late"}); !errors.Is(lateErr, errs.ErrReadOnly) {
		t.Errorf("Do after panic latch = %v, want ErrReadOnly", lateErr)
	}

	if closeErr := w.Close(ctx); closeErr != nil {
		t.Fatalf("close: %v", closeErr)
	}
	if storeCloseErr := st.Close(ctx); storeCloseErr != nil {
		t.Fatalf("close store: %v", storeCloseErr)
	}
	st2, _, err := Open(ctx, testOptions(dir, fakeClock(), &logCapture{}))
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer func() {
		if closeErr := st2.Close(ctx); closeErr != nil {
			t.Errorf("close reopened store: %v", closeErr)
		}
	}()
	if rows := readProbeIfAny(t, st2.RO()); len(rows) != 0 {
		t.Fatalf("rows after panicking batch = %+v, want none — the panic half-applied", rows)
	}
}
