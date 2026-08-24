// SPDX-License-Identifier: Apache-2.0

package queue

import (
	"errors"
	"math/rand/v2"
	"testing"
	"time"

	"github.com/a-holm/messq/internal/errs"
)

// BackoffFor / Jitter / delay-clamp acceptance (issue #10 §4, SEMANTICS S8.1–S8.4):
// the retry delay is backoff[attempt-1] with the last entry repeating, jitter sits in
// [0.8d, 1.2d), an explicit nak --delay is NOT jittered (S8.3, Conflict B), and the
// explicit delay is clamped to [0, MaxNakDelay] else ErrBadRequest (C7).

func TestBackoffForSaturatesAtLast(t *testing.T) {
	c := DefaultConsumerConfig("worker") // default [1s, 5s, 30s, 2m, 10m]
	wants := []time.Duration{1 * time.Second, 5 * time.Second, 30 * time.Second, 2 * time.Minute, 10 * time.Minute}
	last := wants[len(wants)-1]
	for attempt := int32(1); attempt <= 12; attempt++ {
		i := int(attempt) - 1
		want := last
		if i < len(wants) {
			want = wants[i]
		}
		if got := c.BackoffFor(attempt); got != want {
			t.Fatalf("BackoffFor(%d) = %v, want %v", attempt, got, want)
		}
	}
}

func TestBackoffForSingleEntryAndZero(t *testing.T) {
	single := DefaultConsumerConfig("solo")
	single.Backoff = []time.Duration{5 * time.Second}
	for attempt := int32(1); attempt <= 5; attempt++ {
		if got := single.BackoffFor(attempt); got != single.Backoff[0] {
			t.Fatalf("single-entry BackoffFor(%d) = %v, want %v", attempt, got, single.Backoff[0])
		}
	}
	zero := DefaultConsumerConfig("zero")
	zero.Backoff = []time.Duration{0}
	if got := zero.BackoffFor(1); got != 0 {
		t.Fatalf("BackoffFor([0], 1) = %v, want 0", got)
	}
	empty := DefaultConsumerConfig("empty")
	empty.Backoff = nil
	if got := empty.BackoffFor(1); got != 0 {
		t.Fatalf("BackoffFor(empty, 1) = %v, want 0", got)
	}
}

func TestJitterDeterministic(t *testing.T) {
	// A fixed 0.9 multiplier through the seam must yield exact delays.
	fixed := func(d time.Duration) time.Duration { return time.Duration(float64(d) * 0.9) }
	c := DefaultConsumerConfig("worker")
	for _, attempt := range []int32{1, 5} {
		var want time.Duration
		switch attempt {
		case 1:
			want = time.Duration(float64(1*time.Second) * 0.9)
		case 5:
			want = time.Duration(float64(10*time.Minute) * 0.9)
		}
		got, err := ReleaseDelay(c, attempt, nil, fixed)
		if err != nil {
			t.Fatalf("ReleaseDelay(%d): %v", attempt, err)
		}
		if got != want {
			t.Fatalf("ReleaseDelay(%d) = %v, want %v", attempt, got, want)
		}
	}
}

func TestJitterDistributionBounds(t *testing.T) {
	rng := rand.New(rand.NewPCG(uint64(12345), uint64(0)))
	j := StandardJitter(rng)
	const d = 10 * time.Second
	lo, hi := time.Duration(1)<<62, time.Duration(0)
	for i := 0; i < 10000; i++ {
		got := j(d)
		if got.Microseconds() < d.Microseconds()*8/10 || got.Microseconds() > d.Microseconds()*12/10 {
			t.Fatalf("jittered delay %v outside [0.8d, 1.2d) for d=%v", got, d)
		}
		if got < lo {
			lo = got
		}
		if got > hi {
			hi = got
		}
	}
	if lo <= 0 || hi <= lo || lo == 0 {
		t.Fatalf("jitter distribution is degenerate: lo=%v hi=%v", lo, hi)
	}
}

func TestJitterZeroIsZero(t *testing.T) {
	rng := rand.New(rand.NewPCG(uint64(7), uint64(0)))
	j := StandardJitter(rng)
	if got := j(0); got != 0 {
		t.Fatalf("jitter(0) = %v, want 0", got)
	}
}

func TestExplicitDelayIsNotJittered(t *testing.T) {
	// S8.3 (Conflict B ruling): a nak --delay is an override and is NOT jittered.
	c := DefaultConsumerConfig("n")
	loud := func(d time.Duration) time.Duration { return d * 10 }
	explicit := 30 * time.Second
	got, err := ReleaseDelay(c, 3, &explicit, loud)
	if err != nil {
		t.Fatalf("ReleaseDelay(explicit 30s): %v", err)
	}
	if got != 30*time.Second {
		t.Fatalf("explicit delay 30s = %v, want 30s (S8.3: not jittered)", got)
	}
}

func TestNakDelayClamp(t *testing.T) {
	c := DefaultConsumerConfig("n")
	idem := func(d time.Duration) time.Duration { return d }
	zero := time.Duration(0)
	if got, err := ReleaseDelay(c, 1, &zero, idem); err != nil || got != 0 {
		t.Fatalf("explicit 0s = %v err=%v, want 0", got, err)
	}
	for _, bad := range []time.Duration{-1 * time.Second, 24*time.Hour + 1*time.Millisecond} {
		_, err := ReleaseDelay(c, 1, &bad, idem)
		if err == nil || !errors.Is(err, errs.ErrBadRequest) {
			t.Fatalf("explicit delay %v must be ErrBadRequest, got %v", bad, err)
		}
	}
	_ = MaxNakDelay
}
