// SPDX-License-Identifier: Apache-2.0

package store

import (
	"errors"
	"strings"
	"testing"

	"github.com/a-holm/messq/internal/errs"
)

// TestDurabilityString pins the flag vocabulary: --durability accepts exactly what String
// renders, and an out-of-range value renders as its number instead of lying about a mode.
func TestDurabilityString(t *testing.T) {
	tests := []struct {
		mode Durability
		want string
	}{
		{DurabilityFull, "full"},
		{DurabilityRelaxed, "relaxed"},
		{Durability(200), "Durability(200)"},
	}
	for _, tt := range tests {
		if got := tt.mode.String(); got != tt.want {
			t.Errorf("Durability(%d).String() = %q, want %q", tt.mode, got, tt.want)
		}
	}
}

// TestDurabilitySynchronous pins the pragma value each mode maps to (synchronous=FULL is 2,
// NORMAL is 1) and that an unknown value fails safe to FULL: a corrupted mode must never
// degrade to fewer fsyncs than asked for.
func TestDurabilitySynchronous(t *testing.T) {
	tests := []struct {
		mode Durability
		want int
	}{
		{DurabilityFull, 2},
		{DurabilityRelaxed, 1},
		{Durability(200), 2},
	}
	for _, tt := range tests {
		if got := tt.mode.Synchronous(); got != tt.want {
			t.Errorf("Durability(%d).Synchronous() = %d, want %d", tt.mode, got, tt.want)
		}
	}
}

// TestDurabilityZeroValueIsFull checks that the zero value of Options carries the documented
// default: DurabilityFull, synchronous=FULL. A struct literal that never mentions Durability
// must not open the database in relaxed mode.
func TestDurabilityZeroValueIsFull(t *testing.T) {
	var zero Durability
	if zero != DurabilityFull {
		t.Fatalf("zero Durability = %v, want DurabilityFull", zero)
	}
	if zero.String() != "full" || zero.Synchronous() != 2 {
		t.Errorf("zero Durability renders %q/%d, want full/2", zero.String(), zero.Synchronous())
	}
}

// TestParseDurabilityRoundTrip checks Parse and String agree for every valid mode.
func TestParseDurabilityRoundTrip(t *testing.T) {
	for _, mode := range []Durability{DurabilityFull, DurabilityRelaxed} {
		got, err := ParseDurability(mode.String())
		if err != nil {
			t.Fatalf("ParseDurability(%q) returned error: %v", mode, err)
		}
		if got != mode {
			t.Errorf("ParseDurability(%q) = %s, want %s", mode, got, mode)
		}
	}
}

// TestParseDurabilityRejects checks the refusals: empty input, unknown words, wrong case.
// Every refusal wraps errs.ErrBadRequest so callers match it without string comparisons,
// and the message teaches the accepted spellings.
func TestParseDurabilityRejects(t *testing.T) {
	tests := []struct {
		name      string
		in        string
		inMessage []string // substrings the teaching message must contain
	}{
		{"empty", "", []string{`"full"`, `"relaxed"`}},
		{"unknown word", "turbo", []string{"turbo", `"full"`, `"relaxed"`}},
		{"wrong case", "FULL", []string{"FULL"}},
		{"trailing space", "full ", []string{"full"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ParseDurability(tt.in)
			if err == nil {
				t.Fatalf("ParseDurability(%q) = %s, want error", tt.in, got)
			}
			if got != 0 {
				t.Errorf("ParseDurability(%q) returned non-zero mode %s alongside error", tt.in, got)
			}
			if !errors.Is(err, errs.ErrBadRequest) {
				t.Errorf("ParseDurability(%q) error does not wrap errs.ErrBadRequest: %v", tt.in, err)
			}
			for _, want := range tt.inMessage {
				if !strings.Contains(err.Error(), want) {
					t.Errorf("ParseDurability(%q) message %q lacks %q", tt.in, err, want)
				}
			}
		})
	}
}
