// SPDX-License-Identifier: Apache-2.0

package model

import (
	"hash/fnv"
	"strconv"
	"time"
)

// Kind is the scheduling decision a jitter perturbs. It is part of the jitter identity: a
// nak and a timeout on the same row must not necessarily agree, or a steady state could be
// mis-scheduled (PLAN.md §4 seam N1).
type Kind uint8

const (
	// KindNak perturbs the backoff after an explicit negative ack (T6 in SEMANTICS).
	KindNak Kind = iota
	// KindTimeout perturbs the sweep-expiry backoff (T5).
	KindTimeout
	// KindReclaim perturbs the restart lease-reclaim safety interval (T9).
	KindReclaim
)

// Fraction is the jittered multiplier on d: a Delay returns d * Fraction. It is the unit of
// identity-keyed uniform jitter shared by both sides of the differential (PLAN.md §4 seam
// N1), and it maps to [0.8, 1.2] so the resulting delay stays within ±20 % of the raw
// backoff (the JitterFraction of PLAN.md §5.1).
type Fraction float64

// Jitter is the injected scheduling seam. A KeyedJitter is a pure function of its inputs:
// the same (key, seq, attempt, kind) yields the same fractional delay no matter when, in what
// batch or on which side it is called (PLAN.md §4 seam N1). RandJitter (production) ignores
// the key and draws once per decision.
type Jitter interface {
	// Delay returns the jittered delay for scheduling decision (k, seq, attempt, kind)
	// applied to raw duration d. It is pure in the key: identical inputs produce an
	// identical result regardless of call order or batch composition, and it always lands
	// in [0.8d, 1.2d], is never negative, and returns 0 when d is 0.
	Delay(k Key, seq int64, attempt int32, kind Kind, d time.Duration) time.Duration
	// Fraction is Delay divided by d, exposed for the property tests to reason in delay
	// units without rounding through Duration arithmetic.
	Fraction(k Key, seq int64, attempt int32, kind Kind) Fraction
}

// KeyedJitter returns an identity-keyed, deterministic jitter. Its multiplier is the FNV-1a
// hash of {seed, kind, key, seq, attempt} spread over [0.8, 1.2]; because the hash is pure,
// the result is stable across processes and architectures for a fixed seed, which is what
// lets two independently-written sides of a differential agree on a row's backoff. It is
// test-only and never wired to a flag (the same rule PLAN.md applies to NoJitter before it).
func KeyedJitter(seed uint64) Jitter { return keyed{seed: seed} }

type keyed struct{ seed uint64 }

func (k keyed) makeFrac(kk Key, seq int64, attempt int32, kind Kind) Fraction {
	h := fnv.New64a()
	// Fold the whole identity into the hash so a change to any component changes the
	// result (the pure-in-key property). The seed is mixed in first so distinct jitter
	// instances with the same key stream do not coincide.
	write := func(s string) { _, _ = h.Write([]byte(s)) }
	write(strconv.FormatUint(k.seed, 10))
	write("|")
	write(strconv.FormatUint(uint64(kind), 10))
	write("|")
	write(kk.Stream)
	write("|")
	write(kk.Consumer)
	write("|")
	write(strconv.FormatInt(seq, 10))
	write("|")
	write(strconv.FormatInt(int64(attempt), 10))
	sum := h.Sum64()
	// Spread the hash uniformly over [0.8, 1.2). Taking the low 32 bits keeps the fold on
	// the widest span the hash offers; a FNV-1a modulo a tiny constant would clump.
	const two32 = 4294967296.0 // 2^32 as a float, deliberately not via a narrowing uint32
	return Fraction(0.8 + float64(sum&0xFFFFFFFF)/two32*0.4)
}

func (k keyed) Fraction(kk Key, seq int64, attempt int32, kind Kind) Fraction {
	return k.makeFrac(kk, seq, attempt, kind)
}

func (k keyed) Delay(kk Key, seq int64, attempt int32, kind Kind, d time.Duration) time.Duration {
	if d == 0 {
		return 0
	}
	f := k.makeFrac(kk, seq, attempt, kind)
	return time.Duration(float64(d) * float64(f))
}
