// SPDX-License-Identifier: Apache-2.0

package store

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/a-holm/messq/internal/errs"
)

// TestEnqueueCancelStopsTheCommand pins the first wait's contract: a caller whose context is
// done while the queue is FULL gets ctx.Err(), and its command never enters the queue —
// nothing runs that was not accepted. The writer is frozen behind the store.tx.before_apply
// fault point, and only THEN is the bounded channel saturated: the hook itself signals that
// the writer goroutine is parked, and until the gate opens it cannot receive from w.ch again,
// so both saturation sends land in the buffer as a program-order fact. Freezing on
// len(w.ch)==0 instead left a window where takeBatch's non-blocking drain still absorbed the
// saturation sends into the open batch, leaving the channel with room — and Go's select then
// picked randomly between the ready send and the ready ctx.Done, enqueueing the cancelled
// command about half the time.
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

	// before_apply fires once per transaction; only the first firing parks (a bare close
	// would panic when batch 2's firing arrives).
	gate := make(chan struct{})   // open = release the frozen writer
	atGate := make(chan struct{}) // closed = the writer is parked inside before_apply
	var firstFiring sync.Once
	hks := hooks{beforeApply: func() {
		firstFiring.Do(func() { close(atGate); <-gate })
	}}
	w, err := st.NewWriter(Config{QueueDepth: 2},
		withLogger(handler.asLogger()), withHooks(hks))
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}

	r1 := submitAsync(w, 1, "in-flight", 0)
	<-atGate // P1 taken AND batch assembly finished: w.ch has no active receiver until open

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

	// Bounded wait: an enqueue path that ignores ctx.Done must fail this test in
	// milliseconds, not hang it until the package timeout.
	var enqErr error
	waitFor(func() bool {
		select {
		case enqErr = <-doneEnq:
			return true
		default:
			return false
		}
	})
	if enqErr == nil {
		t.Fatalf("Do with an already-cancelled context never returned — the enqueue select ignored ctx.Done")
	}
	if !errors.Is(enqErr, context.Canceled) {
		t.Fatalf("cancelled enqueue = %v, want context.Canceled", enqErr)
	}
	if got := len(w.ch); got != 2 {
		t.Fatalf("queue holds %d commands after the cancelled Do returned, want 2 — key 4 entered the queue", got)
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

// TestPreCancelledDoRefusesBeforeEnqueue pins the deterministic refusal of a context that is
// already cancelled when Do is called: the command is never enqueued and ctx.Err() comes back
// unwrapped. The census shape is the point — against an IDLE writer with queue room, the
// enqueue select has TWO ready arms (send and ctx.Done), so without a pre-check Go picks
// pseudo-randomly and roughly half the commands slip into the queue and apply anyway. The
// PR #53 review measured exactly that: 203 of 400 pre-cancelled commands enqueued and applied
// while every caller still received an errors.Is(context.Canceled) error — the wait phase
// answers ErrCommitUnknown wrapping ctx.Err(), which made those coin-flip losses
// indistinguishable from genuine unknown fates.
func TestPreCancelledDoRefusesBeforeEnqueue(t *testing.T) {
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
	w, err := st.NewWriter(Config{QueueDepth: 8}, withLogger(handler.asLogger()))
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}

	// Seed one ACCEPTED command first: it creates the probe table (Apply creates it lazily,
	// so a genuinely all-refused engine would leave readProbe with no table at all) and it
	// keeps the zero-rows claim from being vacuous — a mutant that refused EVERY command
	// would otherwise satisfy the census too.
	if _, err := w.Do(ctx, &probeCmd{key: 4242, val: "seed"}); err != nil {
		t.Fatalf("Do(seed): %v", err)
	}

	cancelled, cancel := context.WithCancel(ctx)
	cancel() // dead on arrival: both enqueue arms are ready from the first call
	for i := 0; i < 200; i++ {
		_, doErr := w.Do(cancelled, &probeCmd{key: int64(i), val: "refused"})
		if doErr == nil {
			t.Fatalf("Do #%d with a pre-cancelled context returned a result instead of refusing", i)
		}
		if !errors.Is(doErr, context.Canceled) {
			t.Fatalf("Do #%d refusal = %v, want context.Canceled", i, doErr)
		}
		if errors.Is(doErr, ErrCommitUnknown) {
			t.Fatalf("Do #%d refusal = %v, must NOT satisfy ErrCommitUnknown: a caller could not tell this refusal (safe to retry blind) from an unknown fate", i, doErr)
		}
	}

	if closeErr := w.Close(ctx); closeErr != nil {
		t.Fatalf("close: %v", closeErr)
	}
	rows := readProbe(t, st.RO())
	if len(rows) != 1 || rows[0].K != 4242 || rows[0].V != "seed" {
		t.Fatalf("rows = %+v, want exactly the {4242 seed}: a command from a pre-cancelled context was enqueued and applied", rows)
	}
}

// TestCancelErrorShapesAreDistinguishable pins the caller-side discriminator between the two
// cancellation shapes: errors.Is(err, ErrCommitUnknown). A REFUSAL — context dead on arrival,
// command never enqueued — is bare ctx.Err() and must never satisfy ErrCommitUnknown; a
// MID-WAIT cancellation — context abandoned after acceptance, while the batch lingers toward
// its commit — must always satisfy it, carrying ctx.Err() as the wrapped cause. That pair is
// what tells a caller whether a blind retry is safe (refusal) or needs Messq-Msg-Id
// deduplication (#7, unknown fate).
func TestCancelErrorShapesAreDistinguishable(t *testing.T) {
	ctx := context.Background()
	handler := &logCapture{}
	fc := fakeClock()
	st, _, err := Open(ctx, testOptions(testDataDir(t), fc, handler))
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer func() {
		if closeErr := st.Close(ctx); closeErr != nil {
			t.Errorf("close store: %v", closeErr)
		}
	}()
	w, err := st.NewWriter(Config{CommitWindow: 50 * time.Millisecond}, withLogger(handler.asLogger()))
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}

	refused, cancelRefused := context.WithCancel(ctx)
	cancelRefused()
	if _, refusal := w.Do(refused, &probeCmd{key: 1, val: "refused"}); refusal == nil || !errors.Is(refusal, context.Canceled) || errors.Is(refusal, ErrCommitUnknown) {
		t.Fatalf("pre-cancelled refusal = %v, want bare context.Canceled and NEVER ErrCommitUnknown", refusal)
	}

	waiterCtx, cancelWaiter := context.WithCancel(ctx)
	resC := make(chan error, 1)
	go func() {
		_, doErr := w.Do(waiterCtx, &probeCmd{key: 9, val: "unknown"})
		resC <- doErr
	}()
	fc.BlockUntil(1) // accepted; the batch is open and lingering
	cancelWaiter()   // the caller gives up BEFORE the commit
	if unknown := <-resC; !errors.Is(unknown, ErrCommitUnknown) || !errors.Is(unknown, context.Canceled) {
		t.Fatalf("mid-wait cancellation = %v, want ErrCommitUnknown wrapping context.Canceled", unknown)
	}

	fc.Advance(50 * time.Millisecond) // the engine still owns the abandoned command
	if closeErr := w.Close(ctx); closeErr != nil {
		t.Fatalf("close: %v", closeErr)
	}
	rows := readProbe(t, st.RO())
	if len(rows) != 1 || rows[0].K != 9 || rows[0].V != "unknown" {
		t.Fatalf("rows = %+v, want exactly {9 unknown}: the refused command never ran, the abandoned one did", rows)
	}
}
