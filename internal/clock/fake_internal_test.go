// SPDX-License-Identifier: Apache-2.0

package clock

import (
	"math/rand/v2"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
)

var internalEpoch = time.Date(2026, 8, 21, 12, 0, 0, 0, time.UTC)

// TestDueOrderIsDeterministic pins the guarantee the fake exists for: whatever order timers
// were armed in, the due ones are fired in (deadline, arming sequence) order. The order is
// asserted on the fake's own dispatch list rather than on channel receives, because a receive
// order across separate channels is decided by the goroutine that reads them, not by the fake.
func TestDueOrderIsDeterministic(t *testing.T) {
	t.Parallel()

	const runs = 1000
	deadlines := []time.Duration{
		30 * time.Millisecond,
		10 * time.Millisecond,
		20 * time.Millisecond,
		10 * time.Millisecond, // ties with index 1: the arming sequence breaks it
		20 * time.Millisecond,
	}

	rng := rand.New(rand.NewPCG(1, 2))
	for run := range runs {
		order := rng.Perm(len(deadlines))

		f := NewFake(internalEpoch)
		armedSeq := make(map[int]uint64, len(deadlines))
		for _, i := range order {
			timer, ok := f.NewTimer(deadlines[i]).(*fakeTimer)
			if !ok {
				t.Fatalf("NewTimer did not return a *fakeTimer")
			}
			armedSeq[i] = timer.w.seq
		}

		f.mu.Lock()
		due := f.due(internalEpoch.Add(time.Second))
		f.mu.Unlock()

		if len(due) != len(deadlines) {
			t.Fatalf("run %d: %d due waiters, want %d", run, len(due), len(deadlines))
		}
		for n := 1; n < len(due); n++ {
			prev, cur := due[n-1], due[n]
			if cur.deadline.Before(prev.deadline) {
				t.Fatalf("run %d: due[%d] deadline %v precedes due[%d] deadline %v",
					run, n, cur.deadline, n-1, prev.deadline)
			}
			if cur.deadline.Equal(prev.deadline) && cur.seq < prev.seq {
				t.Fatalf("run %d: tied deadlines fired out of arming order: seq %d before %d",
					run, prev.seq, cur.seq)
			}
		}
	}
}

// TestDueSkipsWaitersThatAreNotYetDue keeps `due` from being a plain sort of everything armed.
func TestDueSkipsWaitersThatAreNotYetDue(t *testing.T) {
	t.Parallel()

	f := NewFake(internalEpoch)
	f.NewTimer(time.Second)
	f.NewTimer(3 * time.Second)
	f.NewTimer(2 * time.Second)

	f.mu.Lock()
	due := f.due(internalEpoch.Add(2 * time.Second))
	f.mu.Unlock()

	got := make([]time.Duration, 0, len(due))
	for _, w := range due {
		got = append(got, w.deadline.Sub(internalEpoch))
	}
	want := []time.Duration{time.Second, 2 * time.Second}
	if diff := cmp.Diff(want, got); diff != "" {
		t.Fatalf("due deadlines (-want +got):\n%s", diff)
	}
}
