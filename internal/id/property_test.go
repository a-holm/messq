// SPDX-License-Identifier: Apache-2.0

package id_test

import (
	"testing"
	"time"

	"github.com/a-holm/messq/internal/clock"
	"github.com/a-holm/messq/internal/id"
	"pgregory.net/rapid"
)

// TestIDMonotonicUnderAnyClock drives random interleavings of "mint", "step the clock forward"
// and "step it backwards". Whatever the clock does, the sequence one generator hands out is
// strictly increasing and every id renders and parses back to itself.
func TestIDMonotonicUnderAnyClock(t *testing.T) {
	t.Parallel()

	rapid.Check(t, func(t *rapid.T) {
		f := clock.NewFake(epoch)
		g := id.NewGen(f)

		var prev id.MsgID
		var minted int
		steps := rapid.SliceOfN(rapid.SampledFrom([]string{"mint", "forward", "backward"}), 1, 60).Draw(t, "steps")
		for i, step := range steps {
			switch step {
			case "mint":
				next := g.New()
				if minted > 0 && next.Compare(prev) <= 0 {
					t.Fatalf("step %d: %s does not follow %s", i, next, prev)
				}
				text := next.String()
				parsed, err := id.ParseMsgID(text)
				if err != nil {
					t.Fatalf("step %d: %q does not parse back: %v", i, text, err)
				}
				if parsed != next {
					t.Fatalf("step %d: %q parsed back as %s", i, text, parsed)
				}
				prev, minted = next, minted+1

			case "forward":
				d := rapid.IntRange(0, 100_000).Draw(t, "forwardMillis")
				f.Advance(time.Duration(d) * time.Millisecond)

			case "backward":
				d := rapid.IntRange(0, 100_000).Draw(t, "backwardMillis")
				f.Set(f.Now().Add(-time.Duration(d) * time.Millisecond))
			}
		}
	})
}
