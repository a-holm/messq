package store

import (
	"context"
	"runtime"
	"sync/atomic"
	"testing"
	"time"
)

// waitFor spins until cond holds or the attempt budget runs out. Tests may not Sleep; the
// engine's synchronisation points are channels, so a bounded Gosched loop is enough to let a
// background goroutine reach one.
func waitFor(cond func() bool) {
	for i := 0; i < 200000 && !cond(); i++ {
		runtime.Gosched()
	}
}

// batchObserver counts commitBatch invocations through the store.tx.before_apply fault point:
// one firing == one transaction attempted.
type batchObserver struct {
	n atomic.Int64
}

func (b *batchObserver) hooks() hooks {
	return hooks{beforeApply: func() { b.n.Add(1) }}
}

// submitAsync submits one probeCmd on its own goroutine, returning the channel the Do result
// arrives on — how tests queue commands without waiting for them.
func submitAsync(w *Writer, key int64, val string, size int) <-chan DoResult {
	out := make(chan DoResult, 1)
	go func() {
		res, err := w.Do(context.Background(), &probeCmd{key: key, val: val, size: size})
		out <- DoResult{Res: res, Err: err}
	}()
	return out
}

// DoResult mirrors Do's return pair so submitAsync channels stay simple.
type DoResult struct {
	Res Result
	Err error
}

// TestBatchClosesOnCommitWindow pins the window rule end to end: with a live linger armed,
// the first command does NOT commit immediately; arrivals during the linger join its batch;
// the Fake clock crossing the deadline closes exactly one transaction carrying both.
func TestBatchClosesOnCommitWindow(t *testing.T) {
	handler := &logCapture{}
	sink := &sinkRecorder{}
	fc := fakeClock()
	opts := testOptions(testDataDir(t), fc, handler)
	st, _, err := Open(context.Background(), opts)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer func() {
		if closeErr := st.Close(context.Background()); closeErr != nil {
			t.Errorf("close store: %v", closeErr)
		}
	}()

	obs := &batchObserver{}
	w, err := st.NewWriter(Config{CommitWindow: 50 * time.Millisecond},
		withLogger(handler.asLogger()), withEventSink(sink), withHooks(obs.hooks()))
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}

	r1 := submitAsync(w, 1, "a", 0)
	fc.BlockUntil(1) // the writer received P1 and armed the linger timer
	if obs.n.Load() != 0 {
		t.Fatalf("batch committed before the window closed (%d transactions)", obs.n.Load())
	}

	// P2 enters the open batch synchronously from this goroutine (white-box: same package).
	// A plain submitAsync would race the deadline below — its goroutine might not have sent
	// when Advance fires, which would legitimately close {P1} alone. Sending here makes
	// "P2 arrived during the linger" a program-order fact: the fill select sees the channel
	// ready while the timer is not, so P2 joins the batch deterministically.
	req2 := &request{cmd: &probeCmd{key: 2, val: "b"}, done: make(chan struct{})}
	w.ch <- req2

	fc.Advance(50 * time.Millisecond) // now the deadline closes the batch

	got := <-r1
	if got.Err != nil {
		t.Fatalf("Do(P1): %v", got.Err)
	}
	<-req2.done
	waitFor(func() bool { return len(sink.batches()) == 1 }) // fan-out is off the reply path
	if n := obs.n.Load(); n != 1 {
		t.Errorf("%d transactions ran, want exactly 1 (both commands share the fsync)", n)
	}
	if sizes := sink.batches(); len(sizes) != 1 || sizes[0] != 2 {
		t.Errorf("per-batch event counts = %v, want [2]", sizes)
	}
	if closeErr := w.Close(context.Background()); closeErr != nil {
		t.Errorf("close: %v", closeErr)
	}
}

// TestBatchClosesOnMaxBatch pins the max-batch rule: five commands against a 3-command cap,
// under a live window, close as 3+2 — the budget cuts the batch even though the window has
// not elapsed. Batch 1 fills to the cap from arrivals; batch 2 forms from what stayed queued
// and closes when its own window does.
func TestBatchClosesOnMaxBatch(t *testing.T) {
	handler := &logCapture{}
	sink := &sinkRecorder{}
	fc := fakeClock()
	opts := testOptions(testDataDir(t), fc, handler)
	st, _, err := Open(context.Background(), opts)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer func() {
		if closeErr := st.Close(context.Background()); closeErr != nil {
			t.Errorf("close store: %v", closeErr)
		}
	}()

	w, err := st.NewWriter(Config{CommitWindow: 50 * time.Millisecond, CommitMaxBatch: 3},
		withLogger(handler.asLogger()), withEventSink(sink))
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}

	results := make([]<-chan DoResult, 0, 5)
	for i := int64(1); i <= 5; i++ {
		results = append(results, submitAsync(w, i, "v", 0))
	}
	// Batch 1 closes at the cap without any clock movement…
	waitFor(func() bool { return len(sink.batches()) == 1 })
	// …batch 2 {P4,P5} is assembled and lingering on its window.
	waitFor(func() bool { return fc.Armed() == 1 })
	fc.Advance(50 * time.Millisecond)

	for i, r := range results {
		if got := <-r; got.Err != nil {
			t.Fatalf("Do(%d): %v", i+1, got.Err)
		}
	}
	waitFor(func() bool { return len(sink.batches()) == 2 })
	sizes := sink.batches()
	if len(sizes) != 2 || sizes[0] != 3 || sizes[1] != 2 {
		t.Errorf("batches = %v, want [3 2]", sizes)
	}
	if closeErr := w.Close(context.Background()); closeErr != nil {
		t.Errorf("close: %v", closeErr)
	}
}

// TestBatchClosesOnMaxBytes pins the byte budget: three 40-byte commands against a 100-byte
// budget close as 2+1 — adding the third would exceed the budget, so it is held for the next
// batch. A lone oversized command still commits (alone): the budget closes batches, it never
// rejects commands (§10.5).
func TestBatchClosesOnMaxBytes(t *testing.T) {
	handler := &logCapture{}
	sink := &sinkRecorder{}
	fc := fakeClock()
	opts := testOptions(testDataDir(t), fc, handler)
	st, _, err := Open(context.Background(), opts)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer func() {
		if closeErr := st.Close(context.Background()); closeErr != nil {
			t.Errorf("close store: %v", closeErr)
		}
	}()

	w, err := st.NewWriter(Config{CommitWindow: 50 * time.Millisecond, CommitMaxBytes: 100},
		withLogger(handler.asLogger()), withEventSink(sink))
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}

	r1 := submitAsync(w, 1, "a", 40)
	r2 := submitAsync(w, 2, "b", 40)
	r3 := submitAsync(w, 3, "c", 40)

	// Batch 1 = {a,b}: adding c (40) to 80 breaks the budget, so the batch closes early.
	waitFor(func() bool { return len(sink.batches()) == 1 })
	// Batch 2 = {c} alone, lingering.
	waitFor(func() bool { return fc.Armed() == 1 })
	fc.Advance(50 * time.Millisecond)

	// Batch 3 = {big}: 500 bytes alone exceeds the whole budget yet still commits.
	rBig := submitAsync(w, 4, "big", 500)
	waitFor(func() bool { return fc.Armed() == 1 })
	fc.Advance(50 * time.Millisecond)

	for i, r := range []<-chan DoResult{r1, r2, r3, rBig} {
		if got := <-r; got.Err != nil {
			t.Fatalf("Do(%d): %v", i+1, got.Err)
		}
	}
	waitFor(func() bool { return len(sink.batches()) == 3 })
	sizes := sink.batches()
	if len(sizes) != 3 || sizes[0] != 2 || sizes[1] != 1 || sizes[2] != 1 {
		t.Errorf("batches = %v, want [2 1 1]: budget split, then the oversized loner", sizes)
	}
	if closeErr := w.Close(context.Background()); closeErr != nil {
		t.Errorf("close: %v", closeErr)
	}
}

// TestWindowZeroCommitsImmediatelyWithoutArmingTimers pins the documented low-latency
// setting: --commit-window=0 commits whatever is queued right now and never arms a timer —
// three sequential submissions are three immediate transactions with the Fake clock untouched.
func TestWindowZeroCommitsImmediatelyWithoutArmingTimers(t *testing.T) {
	handler := &logCapture{}
	sink := &sinkRecorder{}
	fc := fakeClock()
	opts := testOptions(testDataDir(t), fc, handler)
	st, _, err := Open(context.Background(), opts)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer func() {
		if closeErr := st.Close(context.Background()); closeErr != nil {
			t.Errorf("close store: %v", closeErr)
		}
	}()

	obs := &batchObserver{}
	w, err := st.NewWriter(Config{},
		withLogger(handler.asLogger()), withEventSink(sink), withHooks(obs.hooks()))
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}
	if fc.Armed() != 0 {
		t.Fatalf("idle writer armed %d timers, want 0 (no polling)", fc.Armed())
	}

	for i := int64(1); i <= 3; i++ {
		if _, doErr := w.Do(context.Background(), &probeCmd{key: i, val: "v"}); doErr != nil {
			t.Fatalf("Do(%d): %v", i, doErr)
		}
		if fc.Armed() != 0 {
			t.Errorf("commit-window=0 armed a timer (armed=%d)", fc.Armed())
		}
	}
	if n := obs.n.Load(); n != 3 {
		t.Errorf("%d transactions, want 3 immediate single-command batches", n)
	}
	if closeErr := w.Close(context.Background()); closeErr != nil {
		t.Errorf("close: %v", closeErr)
	}
}

// TestIdleWriterHoldsNoTimers pins the no-idle-CPU property from the acceptance list: after
// construction and after the last commit drained, an armed-timer count of zero means nothing
// polls while waiting for work.
func TestIdleWriterHoldsNoTimers(t *testing.T) {
	handler := &logCapture{}
	fc := fakeClock()
	opts := testOptions(testDataDir(t), fc, handler)
	st, _, err := Open(context.Background(), opts)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer func() {
		if closeErr := st.Close(context.Background()); closeErr != nil {
			t.Errorf("close store: %v", closeErr)
		}
	}()
	w, err := st.NewWriter(Config{CommitWindow: 10 * time.Millisecond},
		withLogger(handler.asLogger()), withEventSink(&sinkRecorder{}))
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}

	if got := fc.Armed(); got != 0 {
		t.Errorf("freshly built writer holds %d timers, want 0", got)
	}
	r := submitAsync(w, 1, "x", 0)
	fc.BlockUntil(1) // the linger timer is armed: the batch is open
	fc.Advance(10 * time.Millisecond)
	if got := <-r; got.Err != nil {
		t.Fatalf("Do: %v", got.Err)
	}
	if got := fc.Armed(); got != 0 {
		t.Errorf("post-commit idle writer holds %d timers, want 0", got)
	}
	if closeErr := w.Close(context.Background()); closeErr != nil {
		t.Errorf("close: %v", closeErr)
	}
}

// TestBatchTimestampsSurviveBackwardsClockJump pins §10.6: stored wall-clock timestamps are
// non-decreasing within a process lifetime even when the Fake clock steps backwards between
// batches. Ordering lives in seq, never in timestamps; ties are expected.
func TestBatchTimestampsSurviveBackwardsClockJump(t *testing.T) {
	fc := fakeClock()
	handler := &logCapture{}
	opts := testOptions(testDataDir(t), fc, handler)
	st, _, err := Open(context.Background(), opts)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer func() {
		if closeErr := st.Close(context.Background()); closeErr != nil {
			t.Errorf("close store: %v", closeErr)
		}
	}()
	w, err := st.NewWriter(Config{}, withLogger(handler.asLogger()))
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}

	if _, doErr := w.Do(context.Background(), &probeCmd{key: 1, val: "before"}); doErr != nil {
		t.Fatalf("Do(before): %v", doErr)
	}
	rowsBefore := readProbe(t, st.RO())

	// A backwards NTP step between batches — the fault family PLAN §11 names.
	fc.Set(time.UnixMilli(rowsBefore[len(rowsBefore)-1].Ts - 3_600_000))

	if _, doErr := w.Do(context.Background(), &probeCmd{key: 2, val: "after"}); doErr != nil {
		t.Fatalf("Do(after): %v", doErr)
	}
	rows := readProbe(t, st.RO())
	if len(rows) != 2 {
		t.Fatalf("%d rows, want 2", len(rows))
	}
	lastTs := int64(-1)
	for _, r := range rows {
		if r.Ts < lastTs {
			t.Errorf("timestamp went backwards across the jump: %d after %d (rows %+v)", r.Ts, lastTs, rows)
		}
		lastTs = r.Ts
	}
	if closeErr := w.Close(context.Background()); closeErr != nil {
		t.Errorf("close: %v", closeErr)
	}
}
