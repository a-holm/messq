// SPDX-License-Identifier: Apache-2.0

package store

import (
	"fmt"

	"github.com/a-holm/messq/internal/errs"
)

// Durability is the crash-promise a stream's publishes carry (ADR-0005). The zero value is the
// safe one: full fsync on every commit.
type Durability uint8

const (
	// DurabilityFull maps to synchronous=FULL in WAL mode: every commit fsyncs the WAL before
	// the 2xx goes out. This is the default.
	DurabilityFull Durability = iota
	// DurabilityRelaxed maps to synchronous=NORMAL: commits are durable against process death
	// but may lose the tail of a power cut. Chosen per stream, never silently.
	DurabilityRelaxed
)

// String renders the flag vocabulary for --durability. An out-of-range value renders as its
// number rather than masquerading as a real mode.
func (d Durability) String() string {
	switch d {
	case DurabilityFull:
		return "full"
	case DurabilityRelaxed:
		return "relaxed"
	default:
		return fmt.Sprintf("Durability(%d)", d)
	}
}

// Synchronous returns the PRAGMA synchronous value the mode enforces: FULL is 2, NORMAL is 1.
// Any unknown value fails safe to 2 — corruption of this field must never reduce fsyncing
// below what was asked for.
func (d Durability) Synchronous() int {
	switch d {
	case DurabilityFull:
		return 2
	case DurabilityRelaxed:
		return 1
	default:
		return 2
	}
}

// ParseDurability parses the flag spelling produced by [Durability.String]. It rejects empty
// and unknown input — including wrong case, so exactly what String renders round-trips — with
// an error wrapping [errs.ErrBadRequest] that teaches the accepted spellings.
func ParseDurability(s string) (Durability, error) {
	switch s {
	case DurabilityFull.String():
		return DurabilityFull, nil
	case DurabilityRelaxed.String():
		return DurabilityRelaxed, nil
	default:
		return DurabilityFull, fmt.Errorf("%w: unknown durability %q: want \"full\" or \"relaxed\"",
			errs.ErrBadRequest, s)
	}
}
