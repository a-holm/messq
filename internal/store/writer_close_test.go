package store

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"syscall"
	"testing"

	"github.com/a-holm/messq/internal/errs"
)

// TestCloseDrainsAcceptedCommandsAndRefusesNew pins the graceful-shutdown contract: Close
// lets every already-accepted command commit (even ones still sitting in the queue when
// Close begins), refuses everything offered afterwards with ErrShuttingDown, and is
// idempotent — the second Close is a no-op.
func TestCloseDrainsAcceptedCommandsAndRefusesNew(t *testing.T) {
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
	var enteredOnce sync.Once
	entered := make(chan struct{})
	hks := hooks{beforeApply: func() {
		enteredOnce.Do(func() { close(entered) }) // fires per batch; signal only the first
		<-gate
	}}
	w, err := st.NewWriter(Config{}, withLogger(handler.asLogger()), withHooks(hks))
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}

	r1 := submitAsync(w, 1, "in-flight", 0)
	<-entered // P1 taken; the writer is parked in the fault point, holding the batch open
	sat1 := &request{cmd: &probeCmd{key: 2, val: "queued-1"}, done: make(chan struct{})}
	sat2 := &request{cmd: &probeCmd{key: 3, val: "queued-2"}, done: make(chan struct{})}
	w.ch <- sat1
	w.ch <- sat2

	closeErrC := make(chan error, 1)
	go func() { closeErrC <- w.Close(ctx) }()

	close(gate) // release the engine; Close must wait out the whole drain

	if closeErr := <-closeErrC; closeErr != nil {
		t.Fatalf("Close: %v", closeErr)
	}
	if got := <-r1; got.Err != nil {
		t.Fatalf("Do(P1): %v", got.Err)
	}
	<-sat1.done
	<-sat2.done

	rows := readProbe(t, st.RO())
	if len(rows) != 3 || rows[0].K != 1 || rows[2].K != 3 {
		t.Fatalf("rows after Close = %+v, want all three accepted commands committed", rows)
	}

	if _, err := w.Do(ctx, &probeCmd{key: 9}); !errors.Is(err, errs.ErrShuttingDown) {
		t.Errorf("Do after Close = %v, want ErrShuttingDown", err)
	}
	if err := w.Close(ctx); err != nil {
		t.Errorf("second Close = %v, want nil (idempotent)", err)
	}
}

// TestCloseConcurrentWithSubmissions pins the no-crash property under the natural race: many
// callers submitting while Close runs. Every submission either commits or gets
// ErrShuttIngDown/ctx.Err — never a panic, never a hang.
func TestCloseConcurrentWithSubmissions(t *testing.T) {
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
	w, err := st.NewWriter(Config{}, withLogger(handler.asLogger()))
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}

	var wg sync.WaitGroup
	outcomes := make(chan error, 64)
	for i := int64(0); i < 64; i++ {
		wg.Add(1)
		go func(key int64) {
			defer wg.Done()
			_, doErr := w.Do(ctx, &probeCmd{key: key, val: "x"})
			outcomes <- doErr
		}(i)
	}
	wg.Wait() // all submissions accepted (queue depth 2048 absorbs 64)

	closeDone := make(chan error, 1)
	go func() { closeDone <- w.Close(ctx) }()

	if closeErr := <-closeDone; closeErr != nil {
		t.Fatalf("Close under load: %v", closeErr)
	}
	close(outcomes)
	for doErr := range outcomes {
		switch {
		case doErr == nil,
			errors.Is(doErr, errs.ErrShuttingDown):
			// Both are legal: the command drained before the stop, or arrived after it.
		default:
			t.Fatalf("submission under Close = %v, want nil or ErrShuttingDown", doErr)
		}
	}
}

// TestCloseSafeAfterFatalLatch pins Close's behaviour once the fsyncgate has fired: the rw
// pool is already gone, Close must still terminate cleanly (the double-close of the pool is
// a tolerated no-op), remain idempotent, and the single fatal stays delivered exactly once.
func TestCloseSafeAfterFatalLatch(t *testing.T) {
	ctx := context.Background()
	rec := &observerRecorder{}
	_, w, handler, attempts := openFailingWriter(t, syscall.EIO, rec)

	if _, err := w.Do(ctx, &probeCmd{key: 1}); !errors.Is(err, ErrCommitUnknown) {
		t.Fatalf("doomed caller = %v, want ErrCommitUnknown", err)
	}
	fe := <-w.Fatal()
	if fe == nil || fe.Op != "commit" {
		t.Fatalf("fatal = %+v, want op=commit", fe)
	}

	if err := w.Close(ctx); err != nil {
		t.Fatalf("first Close after latch = %v, want nil", err)
	}
	if err := w.Close(ctx); err != nil {
		t.Fatalf("second Close after latch = %v, want nil (idempotent)", err)
	}
	if fatals := len(handler.find(slog.LevelError, "storage.fatal")); fatals != 1 {
		t.Errorf("storage.fatal lines = %d, want 1", fatals)
	}
	if got := attempts.Load(); got != 1 {
		t.Errorf("attempts = %d, want 1", got)
	}
	if got := rec.readOnlyCount(); got != 1 {
		t.Errorf("SetReadOnly seen %d times, want 1", got)
	}
}
