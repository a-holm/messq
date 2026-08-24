// SPDX-License-Identifier: Apache-2.0

package queue

import (
	"errors"
	"strings"
	"testing"

	"github.com/a-holm/messq/internal/errs"
)

// ParseToken acceptance (issue #10 §1): accepts exactly the canonical D7 grammar and
// every minted token, rejects every deviation with errs.ErrUnknownToken, allocates
// nothing on the accepted path, and never forges a different message.

// validTokens are well-formed tokens the parser must accept and round-trip.
var validTokens = []Token{
	{"orders", "worker", 10494, 1, 1},
	{"orders", "worker", 10495, 2, 1},
	{"orders.eu", "refund-worker", 1000000, 5, 3},
	{"stream.with.dots", "worker", 42, 7, 2},
	{"events", "archive-consumer", 9223372036854775807, 2147483647, 2147483647},
}

func TestParseTokenRoundTrip(t *testing.T) {
	for _, want := range validTokens {
		tok, err := ParseToken(want.String())
		if err != nil {
			t.Fatalf("ParseToken(%q): %v", want.String(), err)
		}
		if tok.Stream != want.Stream || tok.Consumer != want.Consumer ||
			tok.Seq != want.Seq || tok.Attempt != want.Attempt || tok.Generation != want.Generation {
			t.Fatalf("ParseToken(%q) = %+v, want %+v", want.String(), tok, want)
		}
	}
}

func TestParseTokenCanonicalReString(t *testing.T) {
	// canonicality: ParseToken(s).String() == s for every accepted s.
	for _, tok := range validTokens {
		s := tok.String()
		parsed, err := ParseToken(s)
		if err != nil {
			t.Fatalf("ParseToken(%q): %v", s, err)
		}
		if got := parsed.String(); got != s {
			t.Fatalf("ParseToken(%q).String() = %q", s, got)
		}
	}
}

func TestParseTokenRejects(t *testing.T) {
	cases := []struct {
		in  string
		why string
	}{
		{"", "empty token"},
		{"a", "no separators"},
		{"a/b", "one separator"},
		{"a/b/1", "two separators"},
		{"a/b/1/2", "three separators"},
		{"a/b/1/2/3/4", "five separators"},
		{"a//1/2/3", "empty consumer"},
		{"a/b//2/3", "empty seq"},
		{"a/b/1//3", "empty attempt"},
		{"a/b/1/2/", "empty generation"},
		{"/b/1/2/3", "empty stream"},
		{"orders.eu/x/y/1/2", "subject-style stream"},
		{"orders/worker/01/2/3", "leading zero seq"},
		{"orders/worker/1/02/3", "leading zero attempt"},
		{"orders/worker/1/2/03", "leading zero generation"},
		{"orders/worker/+1/2/3", "plus sign"},
		{"orders/worker/1/2/3 ", "trailing whitespace"},
		{"orders/worker/ 1/2/3", "leading whitespace"},
		{"orders/worker/١/2/3", "arabic digit"},
		{"orders/worker/0/2/3", "seq zero"},
		{"orders/worker/1/0/3", "attempt zero"},
		{"orders/worker/1/2/0", "generation zero"},
		{"orders/worker/9223372036854775808/2/3", "seq overflows int64"},
		{"orders/worker/1/2147483648/3", "attempt overflows int32"},
		{"orders/worker/1/2/2147483648", "generation overflows int32"},
	}
	for _, tt := range cases {
		tok, err := ParseToken(tt.in)
		if err == nil {
			t.Fatalf("ParseToken(%q) accepted %q (%s)", tt.in, tok.String(), tt.why)
		}
		if !errors.Is(err, errs.ErrUnknownToken) {
			t.Fatalf("ParseToken(%q) (%s) returned %v, want errs.ErrUnknownToken", tt.in, tt.why, err)
		}
	}
}

func TestParseTokenRejectsOverlong(t *testing.T) {
	// 171 bytes is the longest well-formed token: 64+1+64+1+19+1+10+1+10 (S3.3).
	longStream := "s" + strings.Repeat("x", 63)   // 64 bytes
	longConsumer := "c" + strings.Repeat("y", 63) // 64 bytes
	if len(longStream) != 64 || len(longConsumer) != 64 {
		t.Fatalf("bad fixture lengths: %d/%d, want 64/64", len(longStream), len(longConsumer))
	}
	s := longStream + "/" + longConsumer + "/9223372036854775807/2147483647/2147483647"
	if want := 64 + 1 + 64 + 1 + 19 + 1 + 10 + 1 + 10; len(s) != want {
		t.Fatalf("fixture must be exactly 171 bytes, got %d", len(s))
	}
	if _, err := ParseToken(s); err != nil {
		t.Fatalf("ParseToken(171-byte token) failed: %v; want success", err)
	}
	long := s + "x" // 172
	if len(long) != 172 {
		t.Fatalf("long fixture must be 172 bytes, got %d", len(long))
	}
	if tok, err := ParseToken(long); err == nil {
		t.Fatalf("ParseToken(172-byte token) = %q; want rejection", tok.String())
	} else if !errors.Is(err, errs.ErrUnknownToken) {
		t.Fatalf("ParseToken(172-byte token) returned %v; want errs.ErrUnknownToken", err)
	}
}

func TestParseTokenZeroAlloc(t *testing.T) {
	// G1: the accepted path allocates nothing.
	inputs := make([]string, len(validTokens))
	for i, tok := range validTokens {
		inputs[i] = tok.String() // String() allocates; parse must not
	}
	if n := testing.AllocsPerRun(10, func() {
		for _, s := range inputs {
			if _, err := ParseToken(s); err != nil {
				t.Error("ParseToken on a valid token failed")
			}
		}
	}); n != 0 {
		t.Fatalf("ParseToken allocated %g times/op on an accepted token; want 0", n)
	}
}
