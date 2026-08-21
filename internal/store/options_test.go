// SPDX-License-Identifier: Apache-2.0

package store

import (
	"log/slog"
	"runtime"
	"testing"
	"time"

	"github.com/a-holm/messq/internal/clock"
	"github.com/a-holm/messq/internal/id"
)

// TestApplyDefaultsOnZeroValue checks that a bare Options literal becomes usable: pool,
// timeouts and cache get the documented defaults, the seams (clock, logger, id generator) are
// never nil, and the minted ids really are unique, parseable ULIDs.
func TestApplyDefaultsOnZeroValue(t *testing.T) {
	var o Options
	o.applyDefaults()

	if o.ReadPoolSize != runtime.NumCPU() {
		t.Errorf("ReadPoolSize = %d, want runtime.NumCPU() = %d", o.ReadPoolSize, runtime.NumCPU())
	}
	if o.BusyTimeout != 5*time.Second {
		t.Errorf("BusyTimeout = %s, want 5s", o.BusyTimeout)
	}
	if o.CacheBytes != 64<<20 {
		t.Errorf("CacheBytes = %d, want 64 MiB (%d)", o.CacheBytes, int64(64<<20))
	}
	if o.ReclaimJitter != 0 {
		t.Errorf("ReclaimJitter = %s, want 0 kept (zero means deterministic, for tests)", o.ReclaimJitter)
	}
	if o.Clock == nil {
		t.Error("Clock seam left nil; Open would need a nil check on every read")
	}
	if o.Logger == nil {
		t.Error("Logger left nil; Open would panic on the first slog call")
	}
	if o.NewID == nil {
		t.Fatal("NewID left nil; node_id minting would panic")
	}
	first, second := o.NewID(), o.NewID()
	if first == second {
		t.Errorf("default NewID minted the same id twice: %s", first)
	}
	if _, err := id.ParseMsgID(first.String()); err != nil {
		t.Errorf("default NewID minted %s, which does not parse: %v", first, err)
	}
}

// TestApplyDefaultsKeepsExplicitValues checks that defaults never overwrite what the caller
// chose: every field set before applyDefaults survives it byte for byte.
func TestApplyDefaultsKeepsExplicitValues(t *testing.T) {
	logger := slog.Default()
	customClock := clock.System{}
	customID := func() id.MsgID { return id.MsgID{} }

	o := Options{
		DataDir:       "/var/lib/messq",
		Durability:    DurabilityRelaxed,
		FullCheck:     true,
		ReadPoolSize:  3,
		BusyTimeout:   9 * time.Millisecond,
		CacheBytes:    1 << 20,
		ReclaimJitter: 2 * time.Millisecond,
		ReadOnly:      true,
		Clock:         customClock,
		Logger:        logger,
		NewID:         customID,
	}
	o.applyDefaults()

	if o.DataDir != "/var/lib/messq" {
		t.Errorf("DataDir changed to %q", o.DataDir)
	}
	if o.Durability != DurabilityRelaxed {
		t.Errorf("Durability changed to %s", o.Durability)
	}
	if !o.FullCheck || !o.ReadOnly {
		t.Error("FullCheck or ReadOnly flipped")
	}
	if o.ReadPoolSize != 3 {
		t.Errorf("ReadPoolSize changed to %d", o.ReadPoolSize)
	}
	if o.BusyTimeout != 9*time.Millisecond {
		t.Errorf("BusyTimeout changed to %s", o.BusyTimeout)
	}
	if o.CacheBytes != 1<<20 {
		t.Errorf("CacheBytes changed to %d", o.CacheBytes)
	}
	if o.ReclaimJitter != 2*time.Millisecond {
		t.Errorf("ReclaimJitter changed to %s", o.ReclaimJitter)
	}
	if o.Clock != customClock {
		t.Error("Clock was replaced")
	}
	if o.Logger != logger {
		t.Error("Logger was replaced")
	}
	if got := o.NewID(); got != (id.MsgID{}) {
		t.Errorf("NewID was replaced; stub returned %s", got)
	}
}

// TestApplyDefaultsIdempotent checks that applying defaults twice is stable: the second run
// must recognise every value as already set and change nothing. The seams are checked by
// identity and behavior — Options is not comparable because of the NewID func.
func TestApplyDefaultsIdempotent(t *testing.T) {
	var o Options
	o.applyDefaults()
	clk, lg, newID := o.Clock, o.Logger, o.NewID
	firstMinted := newID()
	o.applyDefaults()

	if o.Clock != clk {
		t.Error("second applyDefaults replaced Clock")
	}
	if o.Logger != lg {
		t.Error("second applyDefaults replaced Logger")
	}
	if o.NewID == nil {
		t.Fatal("second applyDefaults cleared NewID")
	}
	if minted := o.NewID(); minted == firstMinted {
		t.Errorf("second applyDefaults reset the id generator: %s minted twice", firstMinted)
	}
}

// TestApplyDefaultsNormalizesReclaimJitter pins the one normalization rule for ReclaimJitter:
// a negative value is nonsense (jitter cannot subtract wait time) and falls back to the 1s
// default, while 0 stays 0 — the documented "deterministic, for tests" setting.
func TestApplyDefaultsNormalizesReclaimJitter(t *testing.T) {
	negative := Options{ReclaimJitter: -time.Second}
	negative.applyDefaults()
	if negative.ReclaimJitter != time.Second {
		t.Errorf("negative ReclaimJitter = %s, want the 1s default", negative.ReclaimJitter)
	}
}
