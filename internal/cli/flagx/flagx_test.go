// SPDX-License-Identifier: Apache-2.0

package flagx_test

import (
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/a-holm/messq/internal/cli/flagx"
	"github.com/a-holm/messq/internal/errs"
	"github.com/a-holm/messq/internal/queue"
)

// Every flagx rejection wraps errs.ErrBadRequest, which is what lets the exit-code
// classifier turn client-side validation into exit 2 without consulting the daemon.
func requireBad(t *testing.T, err error, input string) {
	t.Helper()
	if err == nil {
		t.Fatalf("Set(%q) = nil error, want a rejection", input)
	}
	if !errors.Is(err, errs.ErrBadRequest) {
		t.Fatalf("Set(%q) = %v, want it to wrap errs.ErrBadRequest", input, err)
	}
}

func TestDurationAcceptsGoUnitsAndDays(t *testing.T) {
	cases := []struct {
		in   string
		want time.Duration
	}{
		{"500ms", 500 * time.Millisecond},
		{"30s", 30 * time.Second},
		{"5m", 5 * time.Minute},
		{"2h", 2 * time.Hour},
		{"7d", 7 * 24 * time.Hour}, // the d unit PLAN section 8 flags use everywhere
		{"1d", 24 * time.Hour},
		{"0", 0}, // zero is legal where documented
		{"90d", 90 * 24 * time.Hour},
	}
	for _, tc := range cases {
		var d flagx.Duration
		if err := d.Set(tc.in); err != nil {
			t.Fatalf("Duration.Set(%q) = %v, want %v", tc.in, err, tc.want)
		}
		if got := time.Duration(d); got != tc.want {
			t.Fatalf("Duration.Set(%q) = %v, want %v", tc.in, got, tc.want)
		}
	}
}

func TestDurationRejectsNegativesAndJunk(t *testing.T) {
	for _, in := range []string{"-5s", "", "7", "d", "7D", "1.5d", "99999999999999999d"} {
		var d flagx.Duration
		requireBad(t, d.Set(in), in)
	}
}

func TestDurationMillisAndRoundTrip(t *testing.T) {
	var d flagx.Duration
	if err := d.Set("30s"); err != nil {
		t.Fatalf("Duration.Set(30s) = %v", err)
	}
	if got := d.Millis(); got != 30000 {
		t.Fatalf("Duration(30s).Millis() = %d, want 30000", got)
	}
	// pflag displays defaults through String, so String must parse back to the same value.
	var back flagx.Duration
	if err := back.Set(d.String()); err != nil || time.Duration(back) != time.Duration(d) {
		t.Fatalf("String %q does not round-trip: Set gave (%v, %v)", d.String(), time.Duration(back), err)
	}
}

func TestBytesAcceptsIECAndSI(t *testing.T) {
	cases := []struct {
		in   string
		want int64
	}{
		{"1024", 1024},
		{"0", 0},
		{"64KiB", 64 * 1024},
		{"10GiB", 10 * 1024 * 1024 * 1024},
		{"1TiB", 1 << 40},
		{"1KiB", 1024},
		{"1MiB", 1 << 20},
		{"1PiB", 1 << 50},
		{"1MB", 1000 * 1000}, // SI: MB is powers of ten, not MiB
		{"1KB", 1000},
		{"2GB", 2 * 1000 * 1000 * 1000},
		{"1TB", 1e12}, // SI: the big powers of ten are claimed too
		{"1PB", 1e15},
		{"1kib", 1024}, // the unit is case-insensitive
		{"4GIB", 4 * 1024 * 1024 * 1024},
	}
	for _, tc := range cases {
		var b flagx.Bytes
		if err := b.Set(tc.in); err != nil {
			t.Fatalf("Bytes.Set(%q) = %v, want %d", tc.in, err, tc.want)
		}
		if got := int64(b); got != tc.want {
			t.Fatalf("Bytes.Set(%q) = %d, want %d", tc.in, got, tc.want)
		}
	}
}

func TestBytesRejectsJunkAndOverflow(t *testing.T) {
	for _, in := range []string{
		"-1", "", "KiB", "12Bi", "1.5MiB", "+3", "18446744073709551616",
		"9000000000000000000KiB", // parses as int64 but overflows once scaled
	} {
		var b flagx.Bytes
		requireBad(t, b.Set(in), in)
	}
}

// TestFlagValueShape pins the pflag.Value shape (Set/String/Type) every shared flag
// type satisfies structurally, so cobra's pflag accepts them without an import here.
func TestFlagValueShape(t *testing.T) {
	cases := []struct {
		name       string
		typeName   string
		stringForm string
	}{
		{"duration", "duration", "1m0s"},
		{"bytes", "bytes", "1048576"},
		{"headers", "headers", "a=b"},
		{"patterns", "patterns", "orders.*"},
		{"backoff", "backoff", "1s"},
		{"position", "position", "first"},
	}
	for _, tc := range cases {
		switch tc.name {
		case "duration":
			var v flagx.Duration
			if err := v.Set("1m"); err != nil || v.Type() != tc.typeName || v.String() != tc.stringForm {
				t.Fatalf("duration value = (%q, %q, %v)", v.Type(), v.String(), err)
			}
		case "bytes":
			var v flagx.Bytes
			if err := v.Set("1MiB"); err != nil || v.Type() != tc.typeName || v.String() != tc.stringForm {
				t.Fatalf("bytes value = (%q, %q, %v)", v.Type(), v.String(), err)
			}
		case "headers":
			var v flagx.Headers
			if err := v.Set("a=b"); err != nil || v.Type() != tc.typeName || v.String() != tc.stringForm {
				t.Fatalf("headers value = (%q, %q, %v)", v.Type(), v.String(), err)
			}
			if err := v.Set("c=d"); err != nil || v.String() != "a=b, c=d" {
				t.Fatalf("Headers.String() multi-pair = %q (%v)", v.String(), err)
			}
		case "patterns":
			var v flagx.Patterns
			if err := v.Set("orders.*"); err != nil || v.Type() != tc.typeName || v.String() != tc.stringForm {
				t.Fatalf("patterns value = (%q, %q, %v)", v.Type(), v.String(), err)
			}
			if err := v.Set("billing.>"); err != nil || v.String() != "orders.*,billing.>" {
				t.Fatalf("Patterns.String() multi = %q (%v)", v.String(), err)
			}
		case "backoff":
			var v flagx.Backoff
			if err := v.Set("1s"); err != nil || v.Type() != tc.typeName || v.String() != tc.stringForm {
				t.Fatalf("backoff value = (%q, %q, %v)", v.Type(), v.String(), err)
			}
		case "position":
			var v flagx.Position
			if err := v.Set("first"); err != nil || v.Type() != tc.typeName || v.String() != tc.stringForm {
				t.Fatalf("position value = (%q, %q, %v)", v.Type(), v.String(), err)
			}
		}
	}
}

func TestHeadersAppendAndValidate(t *testing.T) {
	var h flagx.Headers
	for _, pair := range []string{"tenant=acme", "reply-to=orders@acme.example"} {
		if err := h.Set(pair); err != nil {
			t.Fatalf("Headers.Set(%q) = %v", pair, err)
		}
	}
	if len(h) != 2 {
		t.Fatalf("len(h) = %d, want 2", len(h))
	}
	if h[0] != (flagx.Header{Key: "tenant", Value: "acme"}) ||
		h[1] != (flagx.Header{Key: "reply-to", Value: "orders@acme.example"}) {
		t.Fatalf("h = %v, want tenant/reply-to pairs", h)
	}
	// An empty value is legal; a missing value is not.
	if err := h.Set("empty="); err != nil {
		t.Fatalf("Headers.Set(empty=) = %v, want accepted", err)
	}
	if h[2].Value != "" || h[2].Key != "empty" {
		t.Fatalf("h[2] = %v, want {empty \"\"}", h[2])
	}
}

func TestHeadersRejectReservedPrefixEmptyKeyControlChars(t *testing.T) {
	for _, in := range []string{
		"novalue",
		"=v",
		"Messq-Origin-Stream=x", // S3.4/C5: reserved prefix, exact case
		"messq-origin-stream=x", // ... compared case-insensitively
		"MESSQ-X=y",
		"mEsSq-Loophole=1",
		"k\n=v",  // control char in key
		"k=a\nb", // control char in value
		"k\tx=y", // tab counts as a control character
	} {
		var h flagx.Headers
		requireBad(t, h.Set(in), in)
	}
}

func TestPatternsValidatesAgainstSubjectGrammar(t *testing.T) {
	var p flagx.Patterns
	for _, raw := range []string{"orders.*", ">", "orders.eu.created"} {
		if err := p.Set(raw); err != nil {
			t.Fatalf("Patterns.Set(%q) = %v", raw, err)
		}
	}
	if len(p) != 3 || p[0] != "orders.*" {
		t.Fatalf("p = %v, want the three raw patterns stored verbatim", p)
	}
}

func TestPatternsRejectsGrammarViolations(t *testing.T) {
	for _, in := range []string{"", "a..b", "a.>.b", "a b"} {
		var p flagx.Patterns
		requireBad(t, p.Set(in), in)
	}
}

func TestBackoffParsesCommaList(t *testing.T) {
	var b flagx.Backoff
	if err := b.Set("1s,5s,30s,2m,10m"); err != nil {
		t.Fatalf("Backoff.Set(1s,5s,30s,2m,10m) = %v", err)
	}
	want := []time.Duration{time.Second, 5 * time.Second, 30 * time.Second, 2 * time.Minute, 10 * time.Minute}
	if len(b) != len(want) {
		t.Fatalf("len(b) = %d, want %d", len(b), len(want))
	}
	for i := range want {
		if b[i] != want[i] {
			t.Fatalf("b[%d] = %v, want %v", i, b[i], want[i])
		}
	}
	ms := b.Millis()
	if ms[4] != 600000 {
		t.Fatalf("Millis()[4] = %d, want 600000", ms[4])
	}

	// Ascending order is not required; the last value repeats.
	var unordered flagx.Backoff
	if err := unordered.Set("10m,1s"); err != nil {
		t.Fatalf("Backoff.Set(10m,1s) = %v, want accepted", err)
	}
}

func TestBackoffRejectsEmptyZeroNegative(t *testing.T) {
	for _, in := range []string{
		"",       // C9: an empty schedule is rejected
		"1s,,2s", // an empty element is not a duration
		"1s,",    // trailing comma
		",1s",
		"0s", // every entry must be strictly positive
		"-1s",
		"1s,abc",
	} {
		var b flagx.Backoff
		requireBad(t, b.Set(in), in)
	}
}

func TestBackoffRoundTripsThroughItsOwnParser(t *testing.T) {
	var b flagx.Backoff
	in := "1s,5s,30s"
	if err := b.Set(in); err != nil {
		t.Fatalf("Backoff.Set(%q) = %v", in, err)
	}
	var again flagx.Backoff
	if err := again.Set(b.String()); err != nil {
		t.Fatalf("Backoff.Set(%q) (from String) = %v", b.String(), err)
	}
	if b.String() == "" || !strings.Contains(b.String(), "1s") {
		t.Fatalf("Backoff.String() = %q, want a comma list containing 1s", b.String())
	}
}

func TestPositionReusesQueueParser(t *testing.T) {
	cases := []struct {
		in   string
		kind queue.Start
		seq  int64
		time int64
	}{
		{"first", queue.StartFirst, 0, 0},
		{"new", queue.StartNew, 0, 0},
		{"seq:42", queue.StartSeq, 42, 0},
		{"time:1700000000000", queue.StartTime, 0, 1700000000000},
		{"seq:0", queue.StartSeq, 0, 0},
	}
	for _, tc := range cases {
		var p flagx.Position
		if err := p.Set(tc.in); err != nil {
			t.Fatalf("Position.Set(%q) = %v", tc.in, err)
		}
		if p.Kind != tc.kind || p.Seq != tc.seq || p.Time != tc.time {
			t.Fatalf("Position.Set(%q) = {kind %v seq %d time %d}, want kind %v seq %d time %d",
				tc.in, p.Kind, p.Seq, p.Time, tc.kind, tc.seq, tc.time)
		}
		// The wire form round-trips.
		var back flagx.Position
		if err := back.Set(p.String()); err != nil || back.StartPosition != p.StartPosition {
			t.Fatalf("Position %q does not round-trip: got (%+v, %v)", p.String(), back.StartPosition, err)
		}
	}
}

func TestPositionRejectsJunk(t *testing.T) {
	for _, in := range []string{"", "bogus", "seq:-1", "seq:x", "time:nope", "first:new"} {
		var p flagx.Position
		requireBad(t, p.Set(in), in)
	}
}

// TestRejectionsAreErrBadRequest walks one bad input per type and pins the sentinel:
// exit-2 mapping depends on it.
func TestRejectionsAreErrBadRequest(t *testing.T) {
	var (
		d flagx.Duration
		b flagx.Bytes
		h flagx.Headers
		p flagx.Patterns
		k flagx.Backoff
		s flagx.Position
	)
	for _, tc := range []struct {
		name string
		fn   func() error
	}{
		{"duration", func() error { return d.Set("-1s") }},
		{"bytes", func() error { return b.Set("-1") }},
		{"headers", func() error { return h.Set("Messq-X=1") }},
		{"patterns", func() error { return p.Set("") }},
		{"backoff", func() error { return k.Set("") }},
		{"position", func() error { return s.Set("nope") }},
	} {
		err := tc.fn()
		if !errors.Is(err, errs.ErrBadRequest) {
			t.Fatalf("%s rejection = %v, want errs.ErrBadRequest", tc.name, err)
		}
	}
}
