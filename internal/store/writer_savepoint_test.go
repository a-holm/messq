// SPDX-License-Identifier: Apache-2.0

package store

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/a-holm/messq/internal/clock"
	"github.com/a-holm/messq/internal/errs"
)

// openWriterOn opens a store and a plain writer on it, returning both plus the fake clock
// that drives them, so tests can Close the writer, reopen the directory fresh, and inspect
// what actually survived.
func openWriterOn(t *testing.T, dir string, cfg Config) (*Store, *Writer, *clock.Fake) {
	t.Helper()
	ctx := context.Background()
	fc := fakeClock()
	st, _, err := Open(ctx, testOptions(dir, fc, &logCapture{}))
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() {
		if closeErr := st.Close(ctx); closeErr != nil {
			t.Errorf("close store: %v", closeErr)
		}
	})
	w, err := st.NewWriter(cfg, withLogger((&logCapture{}).asLogger()), withEventSink(&sinkRecorder{}))
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}
	return st, w, fc
}

// TestCmdErrorRollsBackOnlyItsSavepoint pins per-command failure isolation: in one batch of
// three, the middle command's business rejection undoes exactly that command — its siblings
// commit and their callers see success. Verified against a REOPENED database, not the shared
// page cache: rows 1 and 3 exist, row 2 does not.
func TestCmdErrorRollsBackOnlyItsSavepoint(t *testing.T) {
	ctx := context.Background()
	dir := testDataDir(t)
	st, w, fc := openWriterOn(t, dir, Config{CommitWindow: 50 * time.Millisecond})

	r1 := submitAsync(w, 1, "first", 0)
	fc.BlockUntil(1) // P1 taken; the linger holds the batch open
	req2 := &request{cmd: &probeCmd{key: 2, val: "doomed", bizErr: errs.ErrStaleAck}, done: make(chan struct{})}
	req3 := &request{cmd: &probeCmd{key: 3, val: "third"}, done: make(chan struct{})}
	w.ch <- req2
	w.ch <- req3
	fc.Advance(50 * time.Millisecond)

	if got := <-r1; got.Err != nil {
		t.Fatalf("Do(P1): %v", got.Err)
	}
	<-req2.done
	<-req3.done
	if !errors.Is(req2.err, errs.ErrStaleAck) {
		t.Fatalf("business caller got %v, want ErrStaleAck verbatim", req2.err)
	}
	if !IsCmdError(req2.err) {
		t.Error("rejection lost its CmdError marking")
	}

	if err := w.Close(ctx); err != nil {
		t.Fatalf("close writer: %v", err)
	}
	if err := st.Close(ctx); err != nil {
		t.Fatalf("close store: %v", err)
	}

	// Reopen from disk: what committed must be exactly {1,3}.
	st2, _, err := Open(ctx, testOptions(dir, fakeClock(), &logCapture{}))
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer func() {
		if closeErr := st2.Close(ctx); closeErr != nil {
			t.Errorf("close reopened store: %v", closeErr)
		}
	}()
	rows := readProbe(t, st2.RO())
	if len(rows) != 2 || rows[0].K != 1 || rows[1].K != 3 {
		t.Fatalf("rows after reopen = %+v, want exactly keys [1 3] — sibling survival broken", rows)
	}
}

// TestInfraApplyErrorAbortsWholeBatch pins the other error class: an unmarked error from
// Apply means the batch state is not trustworthy — every command of the batch is rolled back
// and every caller receives ErrCommitUnknown (never a definite failure), and nothing from
// the batch survives the reopen.
func TestInfraApplyErrorAbortsWholeBatch(t *testing.T) {
	ctx := context.Background()
	dir := testDataDir(t)
	st, w, fc := openWriterOn(t, dir, Config{CommitWindow: 50 * time.Millisecond})

	r1 := submitAsync(w, 1, "innocent-1", 0)
	fc.BlockUntil(1)
	req2 := &request{cmd: &probeCmd{key: 2, val: "boom", rawErr: errors.New("driver exploded")}, done: make(chan struct{})}
	req3 := &request{cmd: &probeCmd{key: 3, val: "innocent-3"}, done: make(chan struct{})}
	w.ch <- req2
	w.ch <- req3
	fc.Advance(50 * time.Millisecond)

	if got := <-r1; !errors.Is(got.Err, ErrCommitUnknown) {
		t.Fatalf("Do(P1) = %v, want ErrCommitUnknown", got.Err)
	}
	<-req2.done
	<-req3.done
	for _, r := range []*request{req2, req3} {
		if !errors.Is(r.err, ErrCommitUnknown) {
			t.Fatalf("batch member got %v, want ErrCommitUnknown", r.err)
		}
	}

	if err := w.Close(ctx); err != nil {
		t.Fatalf("close writer: %v", err)
	}
	if err := st.Close(ctx); err != nil {
		t.Fatalf("close store: %v", err)
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
	// The probe table itself is created by the first successful batch; an aborted batch
	// leaves nothing behind — including the table.
	if rows := readProbeIfAny(t, st2.RO()); len(rows) != 0 {
		t.Fatalf("rows after aborted batch = %+v, want none — a half-applied transaction leaked", rows)
	}
}
