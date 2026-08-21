// SPDX-License-Identifier: Apache-2.0

package id_test

import (
	"fmt"
	"strings"
	"testing"

	"github.com/a-holm/messq/internal/clock"
	"github.com/a-holm/messq/internal/id"
)

// FuzzParseMsgID asserts that parsing never panics and that an accepted id round-trips.
func FuzzParseMsgID(f *testing.F) {
	valid := id.NewGen(clock.NewFake(epoch)).NewString()
	for _, seed := range []string{
		"", valid, strings.ToLower(valid), valid[:25], valid + "0",
		strings.Repeat("0", 26), strings.Repeat("Z", 26), "8" + strings.Repeat("0", 25),
		strings.Repeat("I", 26), strings.Repeat("\x00", 26), "01J8Z0000000000000000000QQ",
	} {
		f.Add(seed)
	}

	f.Fuzz(func(t *testing.T, s string) {
		parsed, err := id.ParseMsgID(s)
		if err != nil {
			if id.Classify(s) == id.KindMsgID {
				t.Fatalf("Classify(%q) says message id but ParseMsgID refuses it: %v", s, err)
			}
			return
		}

		text := parsed.String()
		again, err := id.ParseMsgID(text)
		if err != nil {
			t.Fatalf("re-parsing the rendering %q of %q failed: %v", text, s, err)
		}
		if again != parsed {
			t.Fatalf("round trip changed %q into %q", text, again.String())
		}
		if got := id.Classify(s); got != id.KindMsgID {
			t.Fatalf("Classify(%q) = %v, want KindMsgID", s, got)
		}
	})
}

// FuzzParseTraceparent asserts that a header from an arbitrary upstream never panics, never
// takes unbounded time, and that whatever it yields re-renders into a header that parses back
// to the same values.
func FuzzParseTraceparent(f *testing.F) {
	for _, row := range traceparentTable {
		f.Add(row.In)
	}
	for _, seed := range []string{
		"-", "--", strings.Repeat("-", 55), strings.Repeat("-", 8192),
		"00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01" + strings.Repeat("-x", 100),
		"\x00\x00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
	} {
		f.Add(seed)
	}

	f.Fuzz(func(t *testing.T, s string) {
		trace, parent, sampled, ok := id.ParseTraceparent(s)
		if !ok {
			if !trace.IsZero() || !parent.IsZero() || sampled {
				t.Fatalf("a rejected header %q leaked trace=%v parent=%v sampled=%t", s, trace, parent, sampled)
			}
			return
		}

		if _, err := id.ParseTraceID(trace.String()); err != nil {
			t.Fatalf("the trace id parsed out of %q does not parse back: %v", s, err)
		}

		flags := "00"
		if sampled {
			flags = "01"
		}
		canonical := fmt.Sprintf("00-%s-%s-%s", trace, parent, flags)
		trace2, parent2, sampled2, ok2 := id.ParseTraceparent(canonical)
		if !ok2 || trace2 != trace || parent2 != parent || sampled2 != sampled {
			t.Fatalf("re-rendering %q as %q parsed differently: ok=%t trace=%v parent=%v sampled=%t",
				s, canonical, ok2, trace2, parent2, sampled2)
		}
	})
}
