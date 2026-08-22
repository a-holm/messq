// SPDX-License-Identifier: Apache-2.0

package store

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/a-holm/messq/internal/errs"
	"pgregory.net/rapid"
)

// propMachine is the model the property machine checks the engine against: which keys must
// exist (accepted commands), which must not (business rejections), regardless of how the
// engine batched them. Infra faults are excluded here on purpose — they latch the process,
// ending the run; their all-or-nothing semantics are covered by dedicated unit tests.
//
// The invariant, checked against disk after every Drain and across restarts: every OK
// caller's row exists carrying its value; every rejected caller's row does not.
type propMachine struct {
	dir        string
	st         *Store
	w          *Writer
	mu         sync.Mutex
	mustExist  map[int64]string
	mustAbsent map[int64]bool
	nextKey    int64
	pending    []*request
}

func newPropMachine(dir string) *propMachine {
	return &propMachine{
		dir:        dir,
		mustExist:  map[int64]string{},
		mustAbsent: map[int64]bool{},
	}
}

// Init-less by design: rapid's StateMachineActions reflects EVERY exported method taking
// *rapid.T into a drawable action, so setup lives in the test closure below and teardown is
// unexported. Only Submit/Drain/SubmitAndDrain/Restart/Check are actions.

// start opens the first store+writer over a fresh directory.
func (p *propMachine) start(t *rapid.T) {
	if err := os.MkdirAll(p.dir, 0o700); err != nil {
		t.Fatalf("make data dir: %v", err)
	}
	p.restart(t)
	t.Cleanup(func() { p.teardown() })
}

// restart closes whatever is open and starts a fresh store+writer from disk.
func (p *propMachine) restart(t *rapid.T) {
	ctx := context.Background()
	if p.st != nil {
		if closeErr := p.w.Close(ctx); closeErr != nil {
			t.Fatalf("writer close: %v", closeErr)
		}
		if closeErr := p.st.Close(ctx); closeErr != nil {
			t.Fatalf("store close: %v", closeErr)
		}
	}
	st, _, openErr := Open(ctx, testOptions(p.dir, fakeClock(), &logCapture{}))
	if openErr != nil {
		t.Fatalf("open: %v", openErr)
	}
	w, newErr := st.NewWriter(Config{}, withLogger((&logCapture{}).asLogger()))
	if newErr != nil {
		t.Fatalf("NewWriter: %v", newErr)
	}
	p.st, p.w = st, w
}

// Submit submits between 1 and 8 commands without waiting. Each is either accepted (its key
// must exist afterwards, carrying its value) or rejected with a business sentinel (its key
// must stay absent) — decided before the engine sees it.
func (p *propMachine) Submit(t *rapid.T) {
	p.mu.Lock()
	defer p.mu.Unlock()
	n := rapid.IntRange(1, 8).Draw(t, "n")
	for i := 0; i < n; i++ {
		key := p.nextKey
		p.nextKey++
		val := "v" + strconv.Itoa(rapid.IntRange(0, 9999).Draw(t, "val"))
		cmd := &probeCmd{key: key, val: val}
		req := &request{cmd: cmd, done: make(chan struct{})}
		if rapid.Bool().Draw(t, "reject") {
			cmd.bizErr = errs.ErrTooLarge
			p.mustAbsent[key] = true
		} else {
			p.mustExist[key] = val
		}
		p.pending = append(p.pending, req)
		w := p.w
		go func() { wSend(w, req) }()
	}
}

// Drain waits for every outstanding reply and verifies both contracts at once: what callers
// saw (nil or their sentinel) and what hit the disk (existence exactly as modelled).
func (p *propMachine) Drain(t *rapid.T) {
	for _, r := range p.pending {
		<-r.done
		switch {
		case r.err == nil:
		case IsCmdError(r.err):
			if !errors.Is(r.err, errs.ErrTooLarge) {
				t.Fatalf("rejected caller got %v, want its sentinel", r.err)
			}
		default:
			t.Fatalf("unexpected error class without injected faults: %v", r.err)
		}
	}
	p.pending = nil

	rows := readProbeIfAny(t, p.st.RO())
	seen := map[int64]string{}
	for _, row := range rows {
		seen[row.K] = row.V
	}
	for key, want := range p.mustExist {
		got, ok := seen[key]
		if !ok || got != want {
			t.Fatalf("accepted command key=%d missing/altered on disk: got (%q,%v), want %q",
				key, got, ok, want)
		}
	}
	for key := range p.mustAbsent {
		if _, ok := seen[key]; ok {
			t.Fatalf("rejected command key=%d found on disk — savepoint isolation broken", key)
		}
	}
}

// SubmitAndDrain composes the two most common actions so schedules mix bursts with settles.
func (p *propMachine) SubmitAndDrain(t *rapid.T) {
	p.Submit(t)
	p.Drain(t)
}

// Restart settles anything outstanding (accepted commands always run) and reopens from disk.
func (p *propMachine) Restart(t *rapid.T) {
	if len(p.pending) > 0 {
		p.Drain(t)
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	p.restart(t)
}

// Check runs after every action: the queue bound must hold.
func (p *propMachine) Check(t *rapid.T) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.w != nil && len(p.w.ch) > cap(p.w.ch) {
		t.Fatalf("queue length %d exceeds bound %d", len(p.w.ch), cap(p.w.ch))
	}
}

// teardown releases the last store if a failure left it open mid-schedule. Deliberately NOT
// named with a *rapid.T parameter: every exported method taking one is reflected into an
// action by StateMachineActions.
func (p *propMachine) teardown() {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.st != nil {
		p.st.Close(context.Background()) //nolint:errcheck // best-effort teardown after a failure
	}
}

// wSend enqueues one request; the writer's queue never closes, so a send only blocks under
// saturation, which this property never reaches (depths stay tiny).
func wSend(w *Writer, r *request) { w.ch <- r }

// TestWriterLedgerProperty drives random submit/drain/restart schedules through the real
// engine against a real SQLite file and holds the ledger invariant after every step:
// every OK caller's row exists; every CmdError caller's row does not.
func TestWriterLedgerProperty(t *testing.T) {
	if testing.Short() {
		t.Skip("property machine skipped in -short")
	}
	// rapid.Check re-runs the closure many times; each run needs a FRESH database, or run
	// N+1 would see run N's rows under recycled keys and fail spuriously.
	var runSeq atomic.Int64
	dir := t.TempDir()
	rapid.Check(t, func(t *rapid.T) {
		run := filepath.Join(dir, "run"+strconv.FormatInt(runSeq.Add(1), 10))
		p := newPropMachine(filepath.Join(run, "data"))
		p.start(t)
		t.Repeat(rapid.StateMachineActions(p))
	})
}
