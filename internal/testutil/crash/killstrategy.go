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

// randomDelay kills in a uniform window [50 ms, 2 s] after load starts — the steady-state,
// mid-batch window.
type randomDelay struct{}

func (randomDelay) Name() string { return "randomDelay" }

func (randomDelay) Wait(ctx context.Context, clk clock.Clock, rng *rand.Rand, _ *loadgen.Observations) error {
	return clk.Sleep(ctx, uniform(rng, 50*time.Millisecond, 2*time.Second))
}

// afterNOK kills once the driver has observed n OK responses — the post-commit,
// pre-response window. n is drawn U(1, 200) by the cycle loop and fixed here, so a cycle's
// plan is deterministic.
type afterNOK struct{ n int64 }

func (a afterNOK) Name() string { return "afterNOK" }

func (a afterNOK) Wait(ctx context.Context, clk clock.Clock, _ *rand.Rand, obs *loadgen.Observations) error {
	for obs.OK.Load() < a.n {
		if err := clk.Sleep(ctx, time.Millisecond); err != nil {
			return err
		}
	}
	return nil
}

// immediate kills in a uniform window [0, 150 ms] after start — migrations, and
// recovery-during-recovery.
type immediate struct{}

func (immediate) Name() string { return "immediate" }

func (immediate) Wait(ctx context.Context, clk clock.Clock, rng *rand.Rand, _ *loadgen.Observations) error {
	return clk.Sleep(ctx, uniform(rng, 0, 150*time.Millisecond))
}

// pickStrategy draws one of the three v1 strategies from the cycle's RNG and, for afterNOK,
// draws its N. A sweep therefore covers all three windows.
func pickStrategy(rng *rand.Rand) KillStrategy {
	switch rng.Intn(3) {
	case 0:
		return randomDelay{}
	case 1:
		return afterNOK{n: int64(1 + rng.Intn(200))}
	default:
		return immediate{}
	}
}
