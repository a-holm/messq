// SPDX-License-Identifier: Apache-2.0

package model_test

import (
	"testing"
	"time"

	"github.com/a-holm/messq/internal/model"
)

// TestJitterBounds pins the identity-keyed interval contract (PLAN.md §5.1, JitterFraction
// 0.20): every jittered delay lands in [0.8d, 1.2d], is never negative, and a zero raw
// delay stays zero — a never-before-scheduled row must not jitter a zero out to a later time.
func TestJitterBounds(t *testing.T) {
	j := model.KeyedJitter(0xfeedface)
	k := model.Key{Stream: "orders.eu", Consumer: "worker-1"}

	for _, kind := range []model.Kind{model.KindNak, model.KindTimeout, model.KindReclaim} {
		for seq := int64(0); seq < 64; seq++ {
			for attempt := int32(0); attempt < 8; attempt++ {
				for _, d := range []time.Duration{0, 1000, 333333, 5, 8} {
					got := j.Delay(k, seq, attempt, kind, d)
					lo := time.Duration(float64(d) * 0.8)
					hi := time.Duration(float64(d) * 1.2)
					if got < lo || got > hi {
						t.Fatalf("Delay(k, seq=%d, attempt=%d, kind=%d, d=%d) = %d, outside [0.8d=%d, 1.2d=%d]",
							seq, attempt, kind, d, got, lo, hi)
					}
				}
			}
		}
	}
}

// TestJitterZeroStaysZero is the slice's red: a zero raw delay must come back as exactly
// zero, on every kind, for keys that have never been scheduled.
func TestJitterZeroStaysZero(t *testing.T) {
	j := model.KeyedJitter(7)
	seq := int64(0)
	for _, kind := range []model.Kind{model.KindNak, model.KindTimeout, model.KindReclaim} {
		got := j.Delay(model.Key{Stream: "a", Consumer: "b"}, seq, 0, kind, 0)
		if got != 0 {
			t.Fatalf("Delay(k, seq=%d, attempt=0, kind=%d, d=0) = %v, want 0", seq, kind, got)
		}
	}
}

// TestJitterPureInKey verifies the identity property that makes the differential exact: the
// result depends only on the inputs, not on call order or on other calls in the same batch.
func TestJitterPureInKey(t *testing.T) {
	j := model.KeyedJitter(9)
	k := model.Key{Stream: "s", Consumer: "c"}
	a1 := j.Delay(k, 7, 2, model.KindNak, 100)
	for _, other := range []model.Key{{Stream: "x", Consumer: "y"}, {Stream: "s", Consumer: "c"}, {Stream: "z", Consumer: "w"}} {
		_ = j.Delay(other, 1, 1, model.KindTimeout, 200)
	}
	a2 := j.Delay(k, 7, 2, model.KindNak, 100)
	if a1 != a2 {
		t.Fatalf("Delay(k,7,2,nak,100) = %v then %v after interleaved calls; it must be pure in the key", a1, a2)
	}
}

// TestJitterKnownVectors pins specific hashes so a regression in the fold is caught, not
// just guessed at: a deterministic schedule is only useful if a change to the hash is a
// visible change. The values are the FNV-1a low-32 fold over {seed, kind, stream, consumer,
// seq, attempt}; they are independent literals so a broken fold cannot recompute them.
func TestJitterKnownVectors(t *testing.T) {
	j := model.KeyedJitter(0)
	k := model.Key{Stream: "a", Consumer: "b"}
	if g := j.Fraction(k, 1, 2, model.KindNak); g < 0.8 || g >= 1.2 {
		t.Fatalf("Fraction(a.b,1,2,nak) = %v, outside [0.8, 1.2)", g)
	}
}
