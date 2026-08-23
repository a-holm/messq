// SPDX-License-Identifier: Apache-2.0

package crash

import (
	"context"
	"math/rand"
	"time"

	"github.com/a-holm/messq/internal/clock"
	"github.com/a-holm/messq/internal/testutil/loadgen"
)

// KillStrategy decides when a cycle kills the daemon. It is the seam #32 plugs named fault
// points into; this issue ships the three v1 strategies and nothing more. One seed drives
// everything: the cycle loop draws the strategy, its window and the payload schedule from
// the same RNG, so cycle N is replayed by plan with seed+N even though wall-clock
// interleaving is not.
type KillStrategy interface {
	// Name reports the strategy for the cycle report (afterNOK(37) style).
	Name() string
	// Wait blocks until it is time to kill, then returns nil. ctx cancellation aborts with
	// ctx.Err(). obs is read live, so afterNOK can wait for N observed OK responses.
	Wait(ctx context.Context, clk clock.Clock, rng *rand.Rand, obs *loadgen.Observations) error
}

// uniform draws a duration uniformly from [lo, hi] inclusive.
func uniform(rng *rand.Rand, lo, hi time.Duration) time.Duration {
	span := int64(hi - lo + 1)
	if span <= 0 {
		return lo
	}
	return lo + time.Duration(rng.Int63n(span))
}

// RandomDelay kills in a uniform window [50 ms, 2 s] after load starts — the steady-state,
// mid-batch window.
type RandomDelay struct{}

func (RandomDelay) Name() string { return "randomDelay" }

func (RandomDelay) Wait(ctx context.Context, clk clock.Clock, rng *rand.Rand, _ *loadgen.Observations) error {
	return clk.Sleep(ctx, uniform(rng, 50*time.Millisecond, 2*time.Second))
}

// AfterNOK kills once the driver has observed N OK responses — the post-commit,
// pre-response window. N is fixed here (drawn by the cycle loop's RNG in a sweep), so a
// cycle's plan is deterministic.
type AfterNOK struct{ N int64 }

func (a AfterNOK) Name() string { return "afterNOK" }

func (a AfterNOK) Wait(ctx context.Context, clk clock.Clock, _ *rand.Rand, obs *loadgen.Observations) error {
	for obs.OK.Load() < a.N {
		if err := clk.Sleep(ctx, time.Millisecond); err != nil {
			return err
		}
	}
	return nil
}

// Immediate kills in a uniform window [0, 150 ms] after start — migrations, and
// recovery-during-recovery.
type Immediate struct{}

func (Immediate) Name() string { return "immediate" }

func (Immediate) Wait(ctx context.Context, clk clock.Clock, rng *rand.Rand, _ *loadgen.Observations) error {
	return clk.Sleep(ctx, uniform(rng, 0, 150*time.Millisecond))
}

// pickStrategy draws one of the three v1 strategies from the cycle's RNG and, for afterNOK,
// draws its N. A sweep therefore covers all three windows.
func pickStrategy(rng *rand.Rand) KillStrategy {
	switch rng.Intn(3) {
	case 0:
		return RandomDelay{}
	case 1:
		return AfterNOK{N: int64(1 + rng.Intn(200))}
	default:
		return Immediate{}
	}
}
