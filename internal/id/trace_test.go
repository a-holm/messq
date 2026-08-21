// SPDX-License-Identifier: Apache-2.0

package id_test

import (
	"bytes"
	"errors"
	"strings"
	"testing"

	"github.com/a-holm/messq/internal/clock"
	"github.com/a-holm/messq/internal/errs"
	"github.com/a-holm/messq/internal/id"
)

// traceparentRow is one W3C Trace Context vector. Every row of the table in issue #3 is here.
type traceparentRow struct {
	Name    string
	In      string
	OK      bool
	Trace   string
	Parent  string
	Sampled bool
}

var traceparentTable = []traceparentRow{
	{
		Name: "version 00, sampled", OK: true, Sampled: true,
		In:     "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
		Trace:  "4bf92f3577b34da6a3ce929d0e0e4736",
		Parent: "00f067aa0ba902b7",
	},
	{
		Name: "version 00, not sampled", OK: true, Sampled: false,
		In:     "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-00",
		Trace:  "4bf92f3577b34da6a3ce929d0e0e4736",
		Parent: "00f067aa0ba902b7",
	},
	{
		Name: "a later version with trailing fields", OK: true, Sampled: true,
		In:     "01-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01-something-else",
		Trace:  "4bf92f3577b34da6a3ce929d0e0e4736",
		Parent: "00f067aa0ba902b7",
	},
	{
		Name: "flags beyond sampled are ignored", OK: true, Sampled: true,
		In:     "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-ff",
		Trace:  "4bf92f3577b34da6a3ce929d0e0e4736",
		Parent: "00f067aa0ba902b7",
	},
	{Name: "version ff is forbidden by the spec", In: "ff-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"},
	{Name: "an all-zero trace id", In: "00-00000000000000000000000000000000-00f067aa0ba902b7-01"},
	{Name: "an all-zero parent id", In: "00-4bf92f3577b34da6a3ce929d0e0e4736-0000000000000000-01"},
	{Name: "uppercase hex", In: "00-4BF92F3577B34DA6A3CE929D0E0E4736-00f067aa0ba902b7-01"},
	{Name: "uppercase flags", In: "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-0A"},
	{Name: "version 00 with a trailing field", In: "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01-extra"},
	{Name: "version 00 with a trailing dash", In: "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01-"},
	{Name: "a later version with a trailing dash and nothing after it", In: "01-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01-"},
	{Name: "a later version with no separator before the trailing field", In: "01-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01x"},
	{Name: "a trace id one character short", In: "00-4bf92f3577b34da6a3ce929d0e0e473-00f067aa0ba902b7-01"},
	{Name: "a missing field", In: "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7"},
	{Name: "empty", In: ""},
	{Name: "the separators alone", In: strings.Repeat("-", 55)},
	{Name: "eight kibibytes of separators", In: strings.Repeat("-", 8192)},
	{Name: "a non-hex version", In: "0g-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"},
	{Name: "a non-hex trace id", In: "00-4bf92f3577b34da6a3ce929d0e0e473g-00f067aa0ba902b7-01"},
	{Name: "a non-hex parent id", In: "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902bg-01"},
	{Name: "a non-hex flag", In: "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-0g"},
}

func TestParseTraceparent(t *testing.T) {
	t.Parallel()

	for _, row := range traceparentTable {
		t.Run(strings.ReplaceAll(row.Name, " ", "_"), func(t *testing.T) {
			t.Parallel()

			trace, parent, sampled, ok := id.ParseTraceparent(row.In)
			if ok != row.OK {
				t.Fatalf("ParseTraceparent(%q) ok = %t, want %t", row.In, ok, row.OK)
			}
			if !ok {
				// A rejected header means "mint a fresh trace id", never an error, so
				// nothing usable may leak out of the failed parse.
				if !trace.IsZero() || !parent.IsZero() || sampled {
					t.Fatalf("a rejected header produced trace=%v parent=%v sampled=%t", trace, parent, sampled)
				}
				return
			}
			if got := trace.String(); got != row.Trace {
				t.Errorf("trace id = %q, want %q", got, row.Trace)
			}
			if got := parent.String(); got != row.Parent {
				t.Errorf("parent id = %q, want %q", got, row.Parent)
			}
			if sampled != row.Sampled {
				t.Errorf("sampled = %t, want %t", sampled, row.Sampled)
			}
		})
	}
}

func TestParseTraceID(t *testing.T) {
	t.Parallel()

	const valid = "4bf92f3577b34da6a3ce929d0e0e4736"

	got, err := id.ParseTraceID(valid)
	if err != nil {
		t.Fatalf("ParseTraceID(%q) = %v", valid, err)
	}
	if got.String() != valid {
		t.Fatalf("round trip turned %q into %q", valid, got.String())
	}
	if got.IsZero() {
		t.Fatal("a parsed trace id reports itself as zero")
	}

	bad := map[string]string{
		"empty":           "",
		"all zero":        strings.Repeat("0", 32),
		"uppercase":       strings.ToUpper(valid),
		"one short":       valid[:31],
		"one long":        valid + "0",
		"not hex":         strings.Repeat("g", 32),
		"a message id":    id.NewGen(clock.NewFake(epoch)).NewString(),
		"with separators": "4bf9-2f3577b34da6a3ce929d0e0e4736",
	}
	for name, in := range bad {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			if _, err := id.ParseTraceID(in); !errors.Is(err, id.ErrBadTraceID) {
				t.Fatalf("ParseTraceID(%q) = %v, want ErrBadTraceID", in, err)
			}
			if _, err := id.ParseTraceID(in); !errors.Is(err, errs.ErrBadRequest) {
				t.Fatalf("ParseTraceID(%q) does not classify as errs.ErrBadRequest", in)
			}
		})
	}
}

func TestNewTraceID(t *testing.T) {
	t.Parallel()

	t.Run("reads sixteen bytes", func(t *testing.T) {
		t.Parallel()

		src := bytes.NewReader([]byte("0123456789abcdef trailing"))
		got := id.NewTraceID(src)
		if want := "30313233343536373839616263646566"; got.String() != want {
			t.Fatalf("NewTraceID = %q, want %q", got.String(), want)
		}
	})

	t.Run("never returns the all-zero id", func(t *testing.T) {
		t.Parallel()

		zeroes := bytes.NewReader(make([]byte, 1024))
		if got := id.NewTraceID(zeroes); got.IsZero() {
			t.Fatal("NewTraceID returned the all-zero trace id, which the spec forbids")
		}
	})

	t.Run("survives a broken entropy source", func(t *testing.T) {
		t.Parallel()

		if got := id.NewTraceID(brokenEntropy{}); got.IsZero() {
			t.Fatal("NewTraceID returned the all-zero trace id")
		}
	})

	t.Run("draws again after a zero draw", func(t *testing.T) {
		t.Parallel()

		// Sixteen zero bytes, then a usable draw: the degenerate value must be discarded
		// rather than handed out.
		src := bytes.NewReader(append(make([]byte, 16), []byte("0123456789abcdef")...))
		got := id.NewTraceID(src)
		if want := "30313233343536373839616263646566"; got.String() != want {
			t.Fatalf("NewTraceID = %q, want the second draw %q", got.String(), want)
		}
	})
}

func TestSpanIDString(t *testing.T) {
	t.Parallel()

	var zero id.SpanID
	if !zero.IsZero() {
		t.Fatal("the zero SpanID does not report itself as zero")
	}
	if got, want := zero.String(), strings.Repeat("0", 16); got != want {
		t.Fatalf("zero SpanID renders as %q, want %q", got, want)
	}
}

func TestClassify(t *testing.T) {
	t.Parallel()

	msgID := id.NewGen(clock.NewFake(epoch)).NewString()

	cases := map[string]id.Kind{
		msgID:                              id.KindMsgID,
		strings.ToLower(msgID):             id.KindMsgID,
		"4bf92f3577b34da6a3ce929d0e0e4736": id.KindTraceID,
		"":                                 id.KindUnknown,
		"orders":                           id.KindUnknown,
		strings.Repeat("0", 32):            id.KindUnknown, // an all-zero trace id is not one
		strings.Repeat("!", 26):            id.KindUnknown,
	}
	for in, want := range cases {
		if got := id.Classify(in); got != want {
			t.Errorf("Classify(%q) = %v, want %v", in, got, want)
		}
	}
}

func TestKindString(t *testing.T) {
	t.Parallel()

	cases := map[id.Kind]string{
		id.KindUnknown:   "unknown",
		id.KindMsgID:     "message id",
		id.KindTraceID:   "trace id",
		id.Kind(1 << 20): "unknown", // a value no constant names still renders
	}
	for kind, want := range cases {
		if got := kind.String(); got != want {
			t.Errorf("Kind(%d).String() = %q, want %q", int(kind), got, want)
		}
	}
}
