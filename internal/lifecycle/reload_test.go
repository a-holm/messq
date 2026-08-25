// SPDX-License-Identifier: Apache-2.0

package lifecycle

import (
	"context"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
)

// fakeReloader records what was applied to it and can fail validation on demand.
type fakeReloader struct {
	name        string
	changes     []Change
	validateErr error

	mu      sync.Mutex
	applied []string
}

func (f *fakeReloader) Name() string { return f.name }

func (f *fakeReloader) Validate(_ context.Context) ([]Change, error) {
	if f.validateErr != nil {
		return nil, f.validateErr
	}
	return f.changes, nil
}

func (f *fakeReloader) Apply(_ context.Context, c Change) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.applied = append(f.applied, c.Subject)
	return nil
}

func (f *fakeReloader) appliedSubjects() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.applied...)
}

// TestFailingValidateLeavesAllUnapplied is the slice's named red: when reloader 2 of
// 3 fails Validate, NONE of the three may have applied — auth tokens cannot end up
// half-rotated because one source was unreadable.
func TestFailingValidateLeavesAllUnapplied(t *testing.T) {
	t.Parallel()

	r1 := &fakeReloader{name: "one", changes: []Change{{Subject: "a", From: "1", To: "2"}}}
	r2 := &fakeReloader{name: "two", validateErr: errors.New("authfile: permission denied")}
	r3 := &fakeReloader{name: "three", changes: []Change{{Subject: "c", From: "7", To: "8"}}}

	reg := NewRegistry(discardLogger().asLogger(), r1, r2, r3)
	err := reg.Reload(context.Background())
	if err == nil {
		t.Fatal("a failing Validate did not fail the reload")
	}
	for _, r := range []*fakeReloader{r1, r3} {
		if got := r.appliedSubjects(); len(got) != 0 {
			t.Fatalf("reloader %q applied %v despite reloader two failing validation", r.name, got)
		}
	}
}

func TestSuccessfulReloadAppliesEveryChange(t *testing.T) {
	t.Parallel()

	r1 := &fakeReloader{name: "loglevel", changes: []Change{{Subject: "loglevel", From: "info", To: "debug"}}}
	r2 := &fakeReloader{name: "authfile", changes: []Change{
		{Subject: "tokens", From: "old", To: "new", Secret: true},
	}}

	reg := NewRegistry(discardLogger().asLogger(), r1, r2)
	if err := reg.Reload(context.Background()); err != nil {
		t.Fatalf("valid reload failed: %v", err)
	}
	if got := r1.appliedSubjects(); len(got) != 1 || got[0] != "loglevel" {
		t.Fatalf("loglevel applied %v, want [loglevel]", got)
	}
	if got := r2.appliedSubjects(); len(got) != 1 || got[0] != "tokens" {
		t.Fatalf("authfile applied %v, want [tokens]", got)
	}
}

func TestSecretNeverAppearsInLogValue(t *testing.T) {
	t.Parallel()

	c := Change{Subject: "tokens", From: "hunter2", To: "correct-horse", Secret: true}
	rendered := c.LogValue().String()
	if strings.Contains(rendered, "hunter2") || strings.Contains(rendered, "correct-horse") {
		t.Fatalf("secret leaked into the log value: %s", rendered)
	}

	open := Change{Subject: "loglevel", From: "info", To: "debug"}
	if !strings.Contains(open.LogValue().String(), "debug") {
		t.Fatalf("plain change over-redacted: %s", open.LogValue().String())
	}
}

func TestRenderedDiffIsAGoldenLine(t *testing.T) {
	t.Parallel()

	diff := RenderDiff([]Change{
		{Subject: "loglevel", From: "info", To: "debug"},
		{Subject: "tokens", From: "x", To: "y", Secret: true},
	})
	want := "loglevel info->debug; tokens [redacted]->[redacted]"
	if diff != want {
		t.Fatalf("diff = %q\nwant    %q", diff, want)
	}
}

// TestBurstOfRequestsCollapsesIntoOnePass: 100 queued SIGHUPs ahead of the serve
// loop must produce exactly one reload pass, not a hundred sequential ones.
func TestBurstOfRequestsCollapsesIntoOnePass(t *testing.T) {
	t.Parallel()

	var passes atomic.Int32
	counter := &countingReloader{onApply: func() { passes.Add(1) }}
	reg := NewRegistry(discardLogger().asLogger(), counter)
	requests := make(chan struct{}, 128)
	for i := 0; i < 100; i++ {
		requests <- struct{}{}
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { reg.Serve(ctx, requests); close(done) }()

	waitUntil(t, hungStopDeadline(t), func() bool { return passes.Load() >= 1 },
		"queued requests never produced a reload")
	cancel()
	<-done
	if n := passes.Load(); n != 1 {
		t.Fatalf("a 100-deep request burst produced %d reload passes, want 1", n)
	}
}

// countingReloader counts Apply invocations — the registry's pass granularity.
type countingReloader struct {
	onApply func()
}

func (*countingReloader) Name() string { return "counter" }

func (*countingReloader) Validate(context.Context) ([]Change, error) {
	return []Change{{Subject: "tick"}}, nil
}

func (c *countingReloader) Apply(context.Context, Change) error {
	c.onApply()
	return nil
}
