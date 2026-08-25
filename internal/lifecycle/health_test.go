// SPDX-License-Identifier: Apache-2.0

package lifecycle

import (
	"context"
	"testing"
	"time"
)

func TestHealthStatusTransitions(t *testing.T) {
	t.Parallel()

	h := NewHealth()
	if got := h.Status(); got != "serving" {
		t.Fatalf("fresh health = %q, want serving", got)
	}
	if !h.Ready() {
		t.Fatal("fresh health is not ready")
	}

	h.SetDraining()
	if got := h.Status(); got != "draining" {
		t.Fatalf("after drain flip = %q, want draining", got)
	}
	if h.Ready() {
		t.Fatal("draining health still reports ready")
	}

	h.SetReadOnly()
	if got := h.Status(); got != "read_only" {
		t.Fatalf("after fatal latch = %q, want read_only", got)
	}
	if h.Ready() {
		t.Fatal("read-only health still reports ready")
	}
}

// TestFatalSupervisorNeverTearsDown is the slice's named red: the fatal path must
// NOT walk the component teardown, because teardown ends in the store's final
// commit + clean_shutdown marker — and a disk that EIO'd must not be trusted (G10).
// The next start must see the missing marker and run quick_check.
func TestFatalSupervisorNeverTearsDown(t *testing.T) {
	t.Parallel()

	d := newDrainFakes()
	comps := []*fakeComp{newFakeComp(d.j, "store"), newFakeComp(d.j, "writer")}
	m := NewManager(d.logs.asLogger(), Config{}, toComponents(comps)...)
	m.clock = d.clk
	m.health = d.h
	walk(t, m, Recovering, Ready)

	fatal := make(chan string, 1)
	done := make(chan bool, 1)
	go func() { done <- m.SuperviseFatal(context.Background(), fatal, 2*time.Second) }()

	fatal <- "EIO while fsyncing WAL segment 7"

	// The read-window runs on the Clock seam; advance past it.
	d.clk.BlockUntil(1)
	d.clk.Advance(2 * time.Second)

	deadline := hungStopDeadline(t)
	select {
	case fired := <-done:
		if !fired {
			t.Fatal("supervisor returned fired=false on a real fatal")
		}
	case <-deadline:
		t.Fatal("supervisor never returned after the fatal event")
	}

	if m.State() != Fatal {
		t.Fatalf("state after fatal = %v, want FATAL", m.State())
	}
	if !d.h.readonly {
		t.Fatal("the read-only latch was never flipped")
	}
	for _, entry := range []string{"stop:store", "stop:writer", "api:close"} {
		if d.journalHas(entry) {
			t.Fatalf("fatal path performed %q — that path ends in clean_shutdown", entry)
		}
	}
}

// TestFatalWindowIsABudgetOnTheClock pins the --fatal-drain read-window to the Clock
// seam: the window elapses by fake-clock advance, so a stepped clock neither
// shortens nor hangs it.
func TestFatalWindowIsABudgetOnTheClock(t *testing.T) {
	t.Parallel()

	d := newDrainFakes()
	m := NewManager(d.logs.asLogger(), Config{})
	m.clock = d.clk
	m.health = d.h
	walk(t, m, Recovering, Ready)

	fatal := make(chan string, 1)
	done := make(chan bool, 1)
	go func() { done <- m.SuperviseFatal(context.Background(), fatal, time.Hour) }()

	fatal <- "ENOSPC appending batch"

	d.clk.BlockUntil(1)
	d.clk.Advance(time.Hour)

	select {
	case <-done:
	case <-hungStopDeadline(t):
		t.Fatal("read-window did not end after the clock advanced past it")
	}
	if m.State() != Fatal {
		t.Fatalf("state after window = %v, want FATAL", m.State())
	}
}

// TestSuperviseFatalIgnoresContextButEndsWithIt: ctx cancel ends a supervisor that
// has not seen a fatal yet (daemon exiting for other reasons), returning false.
func TestSuperviseFatalIgnoresContextButEndsWithIt(t *testing.T) {
	t.Parallel()

	d := newDrainFakes()
	m := NewManager(d.logs.asLogger(), Config{})
	m.clock = d.clk
	walk(t, m, Recovering, Ready)

	fatal := make(chan string)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan bool, 1)
	go func() { done <- m.SuperviseFatal(ctx, fatal, time.Second) }()

	cancel()
	select {
	case fired := <-done:
		if fired {
			t.Fatal("cancel was reported as a fatal")
		}
	case <-hungStopDeadline(t):
		t.Fatal("supervisor ignored context cancellation")
	}
	if m.State() != Ready {
		t.Fatalf("a cancelled supervisor changed state to %v", m.State())
	}
}
