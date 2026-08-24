// SPDX-License-Identifier: Apache-2.0

package queue

import (
	"math/rand/v2"
	"time"

	"github.com/a-holm/messq/internal/errs"
)

// The retry-backoff computation of issue #10 §4 and the Jitter seam shared with #11.
// BackoffFor owns the schedule lookup (S8.1); ReleaseDelay owns the single rule of
// S8.3 — an explicit nak delay is an override and is NOT jittered, the schedule
// backoff always is (S8.2, always-on ±20%).

// JitterFraction is the ±20% of PLAN §5.1 that perturbs the computed schedule backoff.
// It always applies and is not configurable (S8.2); a knob whose only interesting
// setting is forbidden would be set to 0.01 in production.
const JitterFraction = 0.20

// Jitter is the injected scheduling seam: given the un-jittered delay it returns the
// delay to actually schedule. It is a pure function of its input through the seam so
// property tests and synctest runs are reproducible; production uses StandardJitter.
// The contract: the result always lands in [0.8d, 1.2d], is never negative, and is 0
// when d is 0.
type Jitter func(d time.Duration) time.Duration

// StandardJitter returns the production jitter drawn from rng: a uniform multiplier in
// [0.8, 1.2) scaling d. It draws from a math/rand/v2 generator ("PCG"), never
// crypto/rand — this is not a security decision, and the writer goroutine must not
// block on entropy.
func StandardJitter(rng *rand.Rand) Jitter {
	return func(d time.Duration) time.Duration {
		if d == 0 {
			return 0
		}
		frac := float64(d) // [0.8, 1.2) in one multiply
		return time.Duration(frac * (1 - JitterFraction + rng.Float64()*2*JitterFraction))
	}
}

// BackoffFor is the 1-based schedule lookup of S8.1: the wait after the attempt-th
// failure is backoff[attempt-1], the last entry repeating past the end (retry stays at
// the longest delay, C4). An attempt below 1 clamps to the first entry; an empty
// schedule is zero (retry immediately).
func (c ConsumerConfig) BackoffFor(attempt int32) time.Duration {
	if len(c.Backoff) == 0 {
		return 0
	}
	i := int(attempt) - 1
	switch {
	case i < 0:
		i = 0
	case i >= len(c.Backoff):
		i = len(c.Backoff) - 1
	}
	return c.Backoff[i]
}

// MaxNakDelay is the explicit-delay cap (C7): a naked --delay above 24 h is refused.
const MaxNakDelay = 24 * time.Hour

// ReleaseDelay computes the delay a nak schedules. An explicit delay overrides the
// schedule for that attempt and is used verbatim — NOT jittered (S8.3: the handler
// knows something the schedule does not). Without one, the schedule backoff for the
// attempt is jittered ±20% (S8.2). An explicit delay outside [0, MaxNakDelay] is
// errs.ErrBadRequest (C7). A nil jitter (no seam) returns the raw backoff, which keeps
// the pure planner easy to drive.
func ReleaseDelay(c ConsumerConfig, attempt int32, explicit *time.Duration, j Jitter) (time.Duration, error) {
	if explicit != nil {
		d := *explicit
		if d < 0 || d > MaxNakDelay {
			return 0, errs.E(errs.ErrBadRequest, "queue.ReleaseDelay",
				"nak --delay %v is outside 0..%v", d, MaxNakDelay)
		}
		return d, nil
	}
	base := c.BackoffFor(attempt)
	if j == nil {
		return base, nil
	}
	return j(base), nil
}
