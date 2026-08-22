package store

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/a-holm/messq/internal/errs"
)

// TestEnqueueCancelStopsTheCommand pins the first wait's contract: a caller whose context is
// done while the queue is FULL gets ctx.Err(), and its command never enters the queue —
// nothing runs that was not accepted. The queue is filled by freezing the writer behind the
// store.tx.before_apply fault point, then saturating the bounded channel.
func TestEnqueueCancelStopsTheCommand(t *testing.T) {
	ctx := context.Background()
	handler := &logCapture{}
	st, _, err := Open(ctx, testOptions(testDataDir(t), fakeClock(), handler))
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer func() {
		if closeErr := st.Close(ctx); closeErr != nil {
			t.Errorf("close store: %v", closeErr)
		}
	}()

	gate := make(chan struct{})
	hks := hooks{beforeApply: func() { <-gate }}
	w, err := st.NewWriter(Config{QueueDepth: 2},
		withLogger(handler.asLogger()), withHooks(hks))
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}

	r1 := submitAsync(w, 1, "in-flight", 0) // taken by the writer, which then hits the gate
	waitFor(func() bool { return len(w.ch) == 0 })

	sat1 := &request{cmd: &probeCmd{key: 2, val: "queued-1"}, done: make(chan struct{})}
	sat2 := &request{cmd: &probeCmd{key: 3, val: "queued-2"}, done: make(chan struct{})}
	w.ch <- sat1 // saturate the bounded queue (cap 2)…
	w.ch <- sat2 // …so a new Do can only block on enqueue:
	cancelledCtx, cancel := context.WithCancel(ctx)
	cancel() // already expired

	doneEnq := make(chan error, 1)
	go func() {
		_, doErr := w.Do(cancelledCtx, &probeCmd{key: 4, val: "never"})
		doneEnq <- doErr
	}()

	if enqErr := <-doneEnq; !errors.Is(enqErr, context.Canceled) {
		t.Fatalf("cancelled enqueue = %v, want context.Canceled", enqErr)
	}

	// Let the engine run: P1 plus both queued commands commit; key 4 never existed.
	close(gate)
	if got := <-r1; got.Err != nil {
		t.Fatalf("Do(P1): %v", got.Err)
	}
	<-sat1.done
	<-sat2.done
	if closeErr := w.Close(ctx); closeErr != nil {
		t.Fatalf("close: %v", closeErr)
	}

	for _, row := range readProbe(t, st.RO()) {
		if row.K == 4 {
			t.Fatalf("row k=4 persisted — a cancelled command ran despite never being accepted")
		}
	}
	rows := readProbe(t, st.RO())
	if len(rows) != 3 {
		t.Fatalf("rows = %+v, want exactly keys [1 2 3]", rows)
	}
}

// TestWaitCancelIsUnknownButDurable pins §7's deliberate asymmetry: a caller that stops
// WAITING after acceptance gets ErrCommitUnknown (never a definite failure), while its
// command still runs and lands durably — SEMANTICS S2.3 forbids holes in the total order for
// disconnected clients.
func TestWaitCancelIsUnknownButDurable(t *testing.T) {
	ctx := context.Background()
	handler := &logCapture{}
	fc := fakeClock()
	opts := testOptions(testDataDir(t), fc, handler)
	st, _, err := Open(ctx, opts)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer func() {
		if closeErr := st.Close(ctx); closeErr != nil {
			t.Errorf("close store: %v", closeErr)
		}
	}()
	w, err := st.NewWriter(Config{CommitWindow: 50 * time.Millisecond},
		withLogger(handler.asLogger()))
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}

	waiterCtx, cancel := context.WithCancel(ctx)
	resC := make(chan DoResult, 1)
	go func() {
		res, doErr := w.Do(waiterCtx, &probeCmd{key: 7, val: "durable-anyway"})
		resC <- DoResult{Res: res, Err: doErr}
	}()

	fc.BlockUntil(1) // accepted; the batch is open and lingering
	cancel()         // the client gives up BEFORE the commit

	got := <-resC
	if !errors.Is(got.Err, ErrCommitUnknown) {
		t.Fatalf("stopped waiter got %v, want ErrCommitUnknown", got.Err)
	}
	if !errors.Is(got.Err, context.Canceled) {
		t.Errorf("unknown error lost the underlying cause: %v", got.Err)
	}

	fc.Advance(50 * time.Millisecond) // the engine still owns the command
	if closeErr := w.Close(ctx); closeErr != nil {
		t.Fatalf("close: %v", closeErr)
	}
	rows := readProbe(t, st.RO())
	if len(rows) != 1 || rows[0].K != 7 || rows[0].V != "durable-anyway" {
		t.Fatalf("rows = %+v, want {7 durable-anyway}: an accepted command must always run", rows)
	}
}

// TestInApplyDoDeadlockGuard pins the cheap structural guard against self-deadlock: a
// command body calling Writer.Do panics on the writer goroutine, which recovers it as
// infrastructure damage — the batch aborts and the process latches read-only naming the bug.
func TestInApplyDoDeadlockGuard(t *testing.T) {
	ctx := context.Background()
	handler := &logCapture{}
	fc := fakeClock()
	opts := testOptions(testDataDir(t), fc, handler)
	st, _, err := Open(ctx, opts)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer func() {
		if closeErr := st.Close(ctx); closeErr != nil {
			t.Errorf("close store: %v", closeErr)
		}
	}()
	w, err := st.NewWriter(Config{}, withLogger(handler.asLogger()))
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}

	reentrant := &probeCmd{
		key: 1,
		val: "recursion",
		beforeApply: func(applyCtx context.Context) {
			// Runs INSIDE Apply on the writer goroutine, with the batch context a real
			// command would forward: calling Do here must trip the guard.
			_, _ = w.Do(applyCtx, &probeCmd{key: 99, val: "nested"}) //nolint:errcheck // deliberately unchecked: the panic is the point
		},
	}
	if _, doErr := w.Do(ctx, reentrant); !errors.Is(doErr, ErrCommitUnknown) {
		t.Fatalf("reentrant caller got %v, want ErrCommitUnknown", doErr)
	}

	fe := <-w.Fatal()
	if fe.Op != "apply" {
		t.Errorf("FatalError.Op = %q, want apply", fe.Op)
	}
	if !strings.Contains(fe.Err.Error(), "Do called from inside Apply") {
		t.Errorf("FatalError.Err = %v, want it naming the self-deadlock guard", fe.Err)
	}
	if _, err := w.Do(ctx, &probeCmd{key: 5}); !errors.Is(err, errs.ErrReadOnly) {
		t.Errorf("Do after guard latch = %v, want ErrReadOnly", err)
	}
	if closeErr := w.Close(ctx); closeErr != nil {
		t.Errorf("close: %v", closeErr)
	}
}
