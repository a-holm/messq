// SPDX-License-Identifier: Apache-2.0

package ledger

import (
	"context"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/a-holm/messq/internal/clock"
)

// gatedSync is the fake syncer of G1: it counts Sync calls and can be made to block until
// the test releases it, so "Attempt returns before the flush" is observable as a red.
type gatedSync struct {
	calls   atomic.Int64
	called  chan struct{}
	release chan struct{}
}

func newGatedSync() *gatedSync {
	return &gatedSync{called: make(chan struct{}, 16), release: make(chan struct{})}
}

func (g *gatedSync) sync() error {
	g.calls.Add(1)
	g.called <- struct{}{}
	<-g.release
	return nil
}

// countingSync records every fsync but never blocks, for the batching assertions.
type countingSync struct{ calls atomic.Int64 }

func (c *countingSync) sync() error {
	c.calls.Add(1)
	return nil
}

// openTestLedger opens a ledger over a real temp file with an injected syncFn.
func openTestLedger(t *testing.T, cfg Config, syncFn func() error) *Ledger {
	t.Helper()
	path := filepath.Join(t.TempDir(), "ledger.jsonl")
	f, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o600)
	if err != nil {
		t.Fatalf("open ledger file: %v", err)
	}
	l := newLedger(f, cfg, clock.System{}, syncFn)
	t.Cleanup(func() {
		if closeErr := l.Close(); closeErr != nil {
			t.Errorf("close ledger: %v", closeErr)
		}
	})
	return l
}

// TestAttemptBlocksUntilSync is G1: Attempt returns only after its record's group flush
// returned. With MaxBatch 1 a single Attempt closes its group immediately; the gated
// syncer proves the return is ordered after the Sync call, so a mutant that returns from
// Attempt before the group-flush wait is red on both legs.
func TestAttemptBlocksUntilSync(t *testing.T) {
	g := newGatedSync()
	l := openTestLedger(t, Config{MaxBatch: 1, Interval: time.Second}, g.sync)
	clk := clock.System{}

	done := make(chan error, 1)
	go func() {
		done <- l.Attempt(Record{Key: "k", Stream: "s", Verdict: Unknown, Size: 4})
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	select {
	case <-g.called:
		// Sync was invoked: correct. Attempt must now be blocked inside it.
	case <-ctx.Done():
		t.Fatal("Attempt returned without invoking Sync — the group flush was skipped")
	}
	// Give a mutant that returns from Attempt before the flush a chance to (wrongly)
	// complete, so the non-blocking check below actually catches it rather than relying
	// on a scheduling race.
	if err := clk.Sleep(ctx, 10*time.Millisecond); err != nil {
		t.Fatalf("grace sleep: %v", err)
	}
	select {
	case err := <-done:
		t.Fatalf("Attempt returned before Sync completed: %v", err)
	default:
		// still blocked on the gated Sync, as required.
	}
	close(g.release)
	if err := <-done; err != nil {
		t.Fatalf("Attempt: %v", err)
	}
	if got := g.calls.Load(); got != 1 {
		t.Errorf("Sync calls = %d, want 1", got)
	}
}

// TestAttemptBatchesShareSync proves the group commit: MaxBatch concurrent attempts share
// one fsync, not one fsync per attempt. This is the one place the harness may look like the
// SUT (it mirrors #6's group commit) and the comment in ledger.go says so.
func TestAttemptBatchesShareSync(t *testing.T) {
	c := &countingSync{}
	l := openTestLedger(t, Config{MaxBatch: 3, Interval: 100 * time.Millisecond}, c.sync)

	var wg sync.WaitGroup
	errs := make(chan error, 3)
	for i := 0; i < 3; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			errs <- l.Attempt(Record{Key: "k" + strconv.Itoa(i), Stream: "s", Verdict: Unknown, Size: 4})
		}(i)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatalf("Attempt: %v", err)
		}
	}
	if got := c.calls.Load(); got != 1 {
		t.Errorf("Sync calls = %d, want 1 for a full batch of 3", got)
	}
}

// TestResolveDurableByNextSync proves Resolve appends an outcome that is durable by the
// next Sync: the driver records OK, then Sync, and Replay folds to the resolved verdict.
func TestResolveDurableByNextSync(t *testing.T) {
	l := openTestLedger(t, Config{MaxBatch: 256, Interval: time.Second}, (&countingSync{}).sync)

	rec := Record{Key: "k", Stream: "s", Subject: "s.a", Size: 4, Cycle: 1, Verdict: Unknown}
	if err := l.Attempt(rec); err != nil {
		t.Fatalf("Attempt: %v", err)
	}
	if err := l.Resolve("k", OK, Outcome{Seq: 7, ID: "mid", Status: 201}); err != nil {
		t.Fatalf("Resolve: %v", err)
	}
	if err := l.Sync(); err != nil {
		t.Fatalf("Sync: %v", err)
	}

	got, _, err := Replay(l.path())
	if err != nil {
		t.Fatalf("Replay: %v", err)
	}
	out := got["k"]
	if out.Verdict != OK || out.Seq != 7 || out.ID != "mid" {
		t.Errorf("folded = %+v, want verdict OK seq 7 id mid", out)
	}
	if out.Stream != "s" || out.Subject != "s.a" || out.Size != 4 || out.Cycle != 1 {
		t.Errorf("resolved record lost its intent context: %+v", out)
	}
}

// TestResolveLastWriterWins proves two Resolves fold to the final verdict, mirroring the
// attempt-then-resolve-then-resolve sequence of a retried UNKNOWN publish.
func TestResolveLastWriterWins(t *testing.T) {
	l := openTestLedger(t, Config{MaxBatch: 256, Interval: time.Second}, (&countingSync{}).sync)

	if err := l.Attempt(Record{Key: "k", Stream: "s", Verdict: Unknown, Size: 4}); err != nil {
		t.Fatalf("Attempt: %v", err)
	}
	if err := l.Resolve("k", OK, Outcome{Seq: 7, Status: 201}); err != nil {
		t.Fatalf("Resolve OK: %v", err)
	}
	if err := l.Resolve("k", Failed, Outcome{Status: 413, Code: "too_large"}); err != nil {
		t.Fatalf("Resolve Failed: %v", err)
	}
	if err := l.Sync(); err != nil {
		t.Fatalf("Sync: %v", err)
	}

	got, _, err := Replay(l.path())
	if err != nil {
		t.Fatalf("Replay: %v", err)
	}
	out := got["k"]
	if out.Verdict != Failed || out.Status != 413 || out.Code != "too_large" {
		t.Errorf("folded = %+v, want verdict Failed 413 too_large", out)
	}
}

// TestWindowAutoFlush proves a partial group is flushed by the commit window when the
// batch never fills: a lone Attempt under a large MaxBatch still returns (durable) once
// the window elapses.
func TestWindowAutoFlush(t *testing.T) {
	c := &countingSync{}
	l := openTestLedger(t, Config{MaxBatch: 256, Interval: 5 * time.Millisecond}, c.sync)

	if err := l.Attempt(Record{Key: "k", Stream: "s", Verdict: Unknown, Size: 4}); err != nil {
		t.Fatalf("Attempt: %v", err)
	}
	if got := c.calls.Load(); got != 1 {
		t.Errorf("Sync calls = %d, want 1 after the commit window elapses", got)
	}
	got, _, err := Replay(l.path())
	if err != nil {
		t.Fatalf("Replay: %v", err)
	}
	if _, ok := got["k"]; !ok {
		t.Errorf("the window-flushed record was not durable")
	}
}
