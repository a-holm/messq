// SPDX-License-Identifier: Apache-2.0

package id_test

import (
	"bytes"
	"errors"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/a-holm/messq/internal/clock"
	"github.com/a-holm/messq/internal/errs"
	"github.com/a-holm/messq/internal/id"
	"github.com/google/go-cmp/cmp"
)

var epoch = time.Date(2026, 8, 21, 12, 0, 0, 0, time.UTC)

// TestGenNewIsStrictlyIncreasing mints a million ids from one goroutine. Strictly increasing
// implies unique, and it is the property `messq trace` and every seq-free ordering depends on.
func TestGenNewIsStrictlyIncreasing(t *testing.T) {
	t.Parallel()

	g := id.NewGen(clock.System{})
	prev := g.New()
	prevText := prev.String()

	for i := 1; i < 1_000_000; i++ {
		next := g.New()
		if next.Compare(prev) <= 0 {
			t.Fatalf("id %d (%s) does not follow %s", i, next, prev)
		}
		if text := next.String(); text <= prevText {
			t.Fatalf("id %d renders as %s, which does not sort after %s", i, text, prevText)
		}
		prev, prevText = next, next.String()
	}
}

// TestGenIsConcurrencySafe is the other half: eight goroutines share one generator, every id
// is unique, and each goroutine's own sequence is strictly increasing because the shared lock
// established its order.
func TestGenIsConcurrencySafe(t *testing.T) {
	t.Parallel()

	const (
		goroutines = 8
		perGoro    = 100_000
	)

	g := id.NewGen(clock.System{})
	batches := make([][]id.MsgID, goroutines)

	var wg sync.WaitGroup
	for worker := range goroutines {
		wg.Add(1)
		go func() {
			defer wg.Done()
			batch := make([]id.MsgID, perGoro)
			for i := range batch {
				batch[i] = g.New()
			}
			batches[worker] = batch
		}()
	}
	wg.Wait()

	all := make([]id.MsgID, 0, goroutines*perGoro)
	for worker, batch := range batches {
		for i := 1; i < len(batch); i++ {
			if batch[i].Compare(batch[i-1]) <= 0 {
				t.Fatalf("worker %d: id %d (%s) does not follow %s", worker, i, batch[i], batch[i-1])
			}
		}
		all = append(all, batch...)
	}

	slices.SortFunc(all, func(a, b id.MsgID) int { return a.Compare(b) })
	for i := 1; i < len(all); i++ {
		if all[i] == all[i-1] {
			t.Fatalf("%s was minted twice", all[i])
		}
	}
}

// TestClockRegression is the policy of PLAN.md section 11.4 made observable: an NTP step
// backwards must not take the daemon down and must not produce a regressing id, but it must
// be reported so the operator learns their clock moved.
func TestClockRegression(t *testing.T) {
	t.Parallel()

	var (
		mu     sync.Mutex
		deltas []time.Duration
	)
	f := clock.NewFake(epoch)
	g := id.NewGen(f, id.WithClockRegressionHook(func(back time.Duration) {
		mu.Lock()
		defer mu.Unlock()
		deltas = append(deltas, back)
	}))

	first := g.New()
	f.Set(epoch.Add(-5 * time.Second))
	second := g.New()

	if second.Compare(first) <= 0 {
		t.Fatalf("after winding the clock back, %s does not follow %s", second, first)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(deltas) != 1 {
		t.Fatalf("the regression hook fired %d times, want exactly 1", len(deltas))
	}
	if got := deltas[0]; got < 5*time.Second-time.Millisecond || got > 5*time.Second+time.Millisecond {
		t.Fatalf("the hook reported %v, want 5s within a millisecond", got)
	}
}

func TestForwardClockStepDoesNotFireTheHook(t *testing.T) {
	t.Parallel()

	fired := 0
	f := clock.NewFake(epoch)
	g := id.NewGen(f, id.WithClockRegressionHook(func(time.Duration) { fired++ }))

	first := g.New()
	f.Advance(time.Hour)
	second := g.New()

	if fired != 0 {
		t.Fatalf("the regression hook fired %d times on a forward step", fired)
	}
	if second.Compare(first) <= 0 {
		t.Fatalf("%s does not follow %s", second, first)
	}
	if got, want := second.Timestamp().UTC(), epoch.Add(time.Hour); !got.Equal(want) {
		t.Fatalf("the second id carries %v, want %v", got, want)
	}
}

// maxEntropy is a reader whose first draw is the largest possible entropy and whose later
// draws are the smallest usable increment. It is what forces ulid.ErrMonotonicOverflow on the
// second mint inside one millisecond.
type maxEntropy struct{ drawn int }

func (m *maxEntropy) Read(p []byte) (int, error) {
	for i := range p {
		if m.drawn < 10 {
			p[i] = 0xFF
		} else {
			// A little-endian uint32 of 1, which the monotonic reader turns into an
			// increment of 2. Adding it to the maximal entropy overflows.
			if (m.drawn-10)%4 == 0 {
				p[i] = 0x01
			} else {
				p[i] = 0x00
			}
		}
		m.drawn++
	}
	return len(p), nil
}

// TestEntropyOverflowStepsAMillisecond pins the loop the issue asks for: 2^80 entropy inside
// one millisecond is exhausted, and the generator steps to the next millisecond rather than
// returning an error nobody can act on.
func TestEntropyOverflowStepsAMillisecond(t *testing.T) {
	t.Parallel()

	f := clock.NewFake(epoch)
	regressions := 0
	g := id.NewGen(f,
		id.WithEntropy(&maxEntropy{}),
		id.WithClockRegressionHook(func(time.Duration) { regressions++ }),
	)

	first := g.New()
	second := g.New()

	if second.Compare(first) <= 0 {
		t.Fatalf("%s does not follow %s", second, first)
	}
	if got, want := second.Time(), first.Time()+1; got != want {
		t.Fatalf("the second id carries millisecond %d, want %d", got, want)
	}
	if second.IsZero() {
		t.Fatal("the second id is the zero ULID")
	}

	// The second id's entropy is a fresh draw from the source rather than the first id's
	// wrapped entropy: an overflow must not quietly turn the ids into a counter. These are
	// the ten bytes the stub yields after its opening run of 0xFF.
	wantEntropy := []byte{1, 0, 0, 0, 1, 0, 0, 0, 1, 0}
	if diff := cmp.Diff(wantEntropy, second.Entropy()); diff != "" {
		t.Fatalf("the second id's entropy was not redrawn (-want +got):\n%s", diff)
	}

	third := g.New()
	if third.Compare(second) <= 0 {
		t.Fatalf("%s does not follow %s", third, second)
	}
	if got, want := third.Time(), first.Time()+1; got != want {
		t.Fatalf("the third id carries millisecond %d, want %d: the generator lost its place", got, want)
	}

	// Stepping the millisecond puts the generator ahead of the wall clock. That is the
	// generator's own doing, not a clock regression, and reporting it would make the
	// clock.regression signal fire on a healthy machine under load.
	if regressions != 0 {
		t.Fatalf("the regression hook fired %d times while the clock never moved", regressions)
	}
}

// brokenEntropy always fails. Publish must not be able to fail for id reasons, so the
// generator has to keep handing out unique, ordered ids even here.
type brokenEntropy struct{}

func (brokenEntropy) Read([]byte) (int, error) { return 0, errors.New("entropy source is gone") }

func TestBrokenEntropyStillMintsOrderedIDs(t *testing.T) {
	t.Parallel()

	f := clock.NewFake(epoch)
	g := id.NewGen(f, id.WithEntropy(brokenEntropy{}))

	prev := g.New()
	for i := 1; i < 100; i++ {
		next := g.New()
		if next.Compare(prev) <= 0 {
			t.Fatalf("id %d (%s) does not follow %s", i, next, prev)
		}
		prev = next
	}
}

func TestNewString(t *testing.T) {
	t.Parallel()

	g := id.NewGen(clock.NewFake(epoch))
	text := g.NewString()
	if len(text) != 26 {
		t.Fatalf("NewString() = %q, want 26 characters", text)
	}
	parsed, err := id.ParseMsgID(text)
	if err != nil {
		t.Fatalf("ParseMsgID(%q) = %v", text, err)
	}
	if parsed.String() != text {
		t.Fatalf("round trip turned %q into %q", text, parsed.String())
	}
}

// TestDeterministicEntropyMakesGoldenTestsPossible is why WithEntropy exists: a golden test
// needs the same id every run.
func TestDeterministicEntropyMakesGoldenTestsPossible(t *testing.T) {
	t.Parallel()

	mint := func() string {
		g := id.NewGen(clock.NewFake(epoch), id.WithEntropy(bytes.NewReader(bytes.Repeat([]byte{0x2a}, 64))))
		return g.NewString()
	}
	first, second := mint(), mint()
	if first != second {
		t.Fatalf("two generators with the same clock and entropy minted %q and %q", first, second)
	}
}

func TestParseMsgID(t *testing.T) {
	t.Parallel()

	valid := id.NewGen(clock.NewFake(epoch)).NewString()

	t.Run("accepts the canonical form", func(t *testing.T) {
		t.Parallel()

		got, err := id.ParseMsgID(valid)
		if err != nil {
			t.Fatalf("ParseMsgID(%q) = %v", valid, err)
		}
		if got.String() != valid {
			t.Fatalf("ParseMsgID(%q).String() = %q", valid, got.String())
		}
	})

	t.Run("accepts lower case", func(t *testing.T) {
		t.Parallel()

		lower := strings.ToLower(valid)
		got, err := id.ParseMsgID(lower)
		if err != nil {
			t.Fatalf("ParseMsgID(%q) = %v", lower, err)
		}
		if got.String() != valid {
			t.Fatalf("ParseMsgID(%q) rendered %q, want %q", lower, got.String(), valid)
		}
	})

	t.Run("rejects everything else", func(t *testing.T) {
		t.Parallel()

		bad := map[string]string{
			"empty":              "",
			"one character shy":  valid[:25],
			"one character over": valid + "0",
			"ambiguous letter":   "0" + strings.Repeat("I", 25),
			"not base32":         strings.Repeat("!", 26),
			"overflows 128 bits": "8" + strings.Repeat("0", 25),
		}
		for name, in := range bad {
			t.Run(name, func(t *testing.T) {
				t.Parallel()

				_, err := id.ParseMsgID(in)
				if !errors.Is(err, id.ErrBadMsgID) {
					t.Fatalf("ParseMsgID(%q) = %v, want ErrBadMsgID", in, err)
				}
				if !errors.Is(err, errs.ErrBadRequest) {
					t.Fatalf("ParseMsgID(%q) does not classify as errs.ErrBadRequest", in)
				}
			})
		}
	})
}

// TestGenTakesAnyClock keeps the seam honest: the generator never reads the wall clock itself.
func TestGenTakesAnyClock(t *testing.T) {
	t.Parallel()

	f := clock.NewFake(epoch)
	g := id.NewGen(f)
	if got, want := g.New().Timestamp().UTC(), epoch; !got.Equal(want) {
		t.Fatalf("the id carries %v, want the fake's %v", got, want)
	}
}

func BenchmarkGenNew(b *testing.B) {
	g := id.NewGen(clock.System{})
	b.Run("serial", func(b *testing.B) {
		for b.Loop() {
			_ = g.New()
		}
	})
	b.Run("parallel", func(b *testing.B) {
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				_ = g.New()
			}
		})
	})
}
