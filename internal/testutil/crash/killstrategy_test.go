// SPDX-License-Identifier: Apache-2.0

package crash

import (
	"context"
	"math/rand"
	"testing"
	"time"

	"github.com/a-holm/messq/internal/clock"
	"github.com/a-holm/messq/internal/testutil/loadgen"
)

// TestPickStrategyCoversAllThree proves the seeded pick spans the whole v1 strategy set, so
// a sweep cannot silently skip a kill window.
func TestPickStrategyCoversAllThree(t *testing.T) {
	seen := make(map[string]bool)
	for seed := int64(0); seed < 200; seed++ {
		rng := rand.New(rand.NewSource(seed))
		seen[pickStrategy(rng).Name()] = true
	}
	for _, name := range []string{"randomDelay", "afterNOK", "immediate"} {
		if !seen[name] {
			t.Errorf("strategy %q was never picked over 200 seeds", name)
		}
	}
}

// TestAfterNOKImmediateWhenSatisfied proves afterNOK returns at once when the OK count is
// already at or past n, rather than polling out the cycle's whole window. A mutant that
// inverts the comparison polls until the context deadline and fails here.
func TestAfterNOKImmediateWhenSatisfied(t *testing.T) {
	obs := &loadgen.Observations{}
	obs.OK.Store(50)
	s := afterNOK{n: 20}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := s.Wait(ctx, clock.System{}, rand.New(rand.NewSource(0)), obs); err != nil {
		t.Fatalf("afterNOK(20) with 50 OKs already observed: %v", err)
	}
}

// TestAfterNOKWaitsForOKs proves the polling leg: with OK below n, Wait does not return
// until a concurrent publisher crosses the threshold.
func TestAfterNOKWaitsForOKs(t *testing.T) {
	obs := &loadgen.Observations{}
	s := afterNOK{n: 5}
	done := make(chan error, 1)
	go func() {
		done <- s.Wait(context.Background(), clock.System{}, rand.New(rand.NewSource(0)), obs)
	}()
	timeoutCtx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	select {
	case err := <-done:
		t.Fatalf("afterNOK(5) returned before 5 OKs were observed: %v", err)
	case <-timeoutCtx.Done():
	}
	for i := int64(0); i < 5; i++ {
		obs.OK.Add(1)
	}
	if err := <-done; err != nil {
		t.Fatalf("afterNOK(5) after 5 OKs: %v", err)
	}
}
