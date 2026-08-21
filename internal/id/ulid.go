// SPDX-License-Identifier: Apache-2.0

package id

import (
	crand "crypto/rand"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/a-holm/messq/internal/clock"
	"github.com/a-holm/messq/internal/errs"
	"github.com/oklog/ulid/v2"
)

// ErrBadMsgID is every way a message id can fail to parse.
var ErrBadMsgID = errs.E(errs.ErrBadRequest, "", "a message id is 26 Crockford base32 characters")

// MsgID is a 128-bit ULID, rendered as 26 Crockford base32 characters.
type MsgID = ulid.ULID

// maxMintAttempts bounds the retry loop in [Gen.New]. One overflow per millisecond is the
// realistic worst case; more than a handful means the entropy source itself is broken, and the
// loop must end rather than spin.
const maxMintAttempts = 8

// Gen mints message ids. It is safe for concurrent use: its own mutex is the serialization
// point, which is why the entropy source is not additionally wrapped in
// ulid.LockedMonotonicReader. One lock, not two.
type Gen struct {
	clk       clock.Clock
	entropy   io.Reader
	onRegress func(back time.Duration)

	mu sync.Mutex
	// ms is the last millisecond handed out. It never decreases, and the mint loop may
	// push it past the wall clock when the entropy inside one millisecond runs out.
	ms uint64
	// seen is the highest wall-clock millisecond observed, and at is the reading it came
	// from. They are kept apart from ms so that the generator stepping ahead of the clock
	// on its own is never mistaken for the clock stepping backwards.
	seen uint64
	at   time.Time
	ent  *ulid.MonotonicEntropy
	last MsgID
}

// Option configures a [Gen].
type Option func(*Gen)

// WithEntropy replaces crypto/rand as the entropy source. A deterministic reader is what makes
// a golden test produce the same id every run.
func WithEntropy(r io.Reader) Option {
	return func(g *Gen) { g.entropy = r }
}

// WithClockRegressionHook registers a callback for a backwards wall-clock step, called with
// how far back the clock went. The event pipeline and the metrics registry subscribe to it.
// It runs under the generator's lock, so it must be cheap and must not block.
func WithClockRegressionHook(f func(back time.Duration)) Option {
	return func(g *Gen) { g.onRegress = f }
}

// NewGen returns a generator reading time through clk.
func NewGen(clk clock.Clock, opts ...Option) *Gen {
	g := &Gen{clk: clk, entropy: crand.Reader}
	for _, opt := range opts {
		opt(g)
	}
	// Increment 0 means a random step bounded by math.MaxUint32, which is what makes the
	// ids unguessable as well as monotonic.
	g.ent = ulid.Monotonic(g.entropy, 0)
	return g
}

// New mints the next id. It never fails; see the package documentation for the two policies
// that make that true.
func (g *Gen) New() MsgID {
	g.mu.Lock()
	defer g.mu.Unlock()

	now := g.clk.Now()
	nowMS := ulid.Timestamp(now)
	if nowMS < g.seen && g.onRegress != nil {
		// The wall clock moved backwards, an NTP step or an operator with a date command.
		// Report it once per step; the millisecond handed out below never follows it down.
		g.onRegress(g.at.Sub(now))
	}
	if nowMS != g.seen {
		g.seen, g.at = nowMS, now
	}
	if nowMS > g.ms {
		g.ms = nowMS
	}

	for range maxMintAttempts {
		u, err := ulid.New(g.ms, g.ent)
		if err == nil && u.Compare(g.last) > 0 {
			g.last = u
			return u
		}
		// Either the entropy inside this millisecond is exhausted or the source failed.
		// Stepping the millisecond both re-seeds the monotonic reader and guarantees the
		// next id sorts after this one.
		g.ms++
	}

	// The entropy source is broken. Ids stop being unguessable here, but they must not stop
	// being unique and ordered, so the last one is incremented as a 128-bit big-endian
	// number.
	u := g.last
	for i := len(u) - 1; i >= 0; i-- {
		u[i]++
		if u[i] != 0 {
			break
		}
	}
	g.last = u
	return u
}

// NewString mints the next id and renders it.
func (g *Gen) NewString() string { return g.New().String() }

// ParseMsgID parses a message id. Parsing accepts either case; [MsgID.String] always renders
// upper case.
func ParseMsgID(s string) (MsgID, error) {
	u, err := ulid.ParseStrict(s)
	if err != nil {
		return MsgID{}, fmt.Errorf("message id %q: %w", s, ErrBadMsgID)
	}
	return u, nil
}
