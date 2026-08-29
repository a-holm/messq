// SPDX-License-Identifier: Apache-2.0

package queue

import (
	"errors"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/a-holm/messq/internal/errs"
)

// Issue #28 slice 1: the position grammar is ONE grammar shared by consumer
// creation, seek --to and (later) replay. This file extends #9's table with the
// forms #28 adds: the "start" alias, absolute RFC3339 times and relative
// time:-<dur> offsets resolved by the daemon clock.

func TestParseStartPositionExtended(t *testing.T) {
	t.Parallel()

	utc := func(y int, mo time.Month, d, h, mi, s, ms int) int64 {
		return time.Date(y, mo, d, h, mi, s, ms*int(time.Millisecond), time.UTC).UnixMilli()
	}

	tests := []struct {
		name string
		in   string
		want StartPosition
	}{
		{"start alias", "start", StartPosition{Kind: StartFirst}},
		{"relative hours", "time:-2h", StartPosition{Kind: StartTime, Rel: -2 * time.Hour}},
		{"relative seconds", "time:-90s", StartPosition{Kind: StartTime, Rel: -90 * time.Second}},
		{"relative compound", "time:-1h30m", StartPosition{Kind: StartTime, Rel: -(time.Hour + 30*time.Minute)}},
		{"relative millis", "time:-250ms", StartPosition{Kind: StartTime, Rel: -250 * time.Millisecond}},
		{
			"rfc3339 whole second",
			"time:2026-11-02T07:12:04Z",
			StartPosition{Kind: StartTime, Time: utc(2026, time.November, 2, 7, 12, 4, 0)},
		},
		{
			"rfc3339 fractional",
			"time:2026-11-02T07:12:04.114Z",
			StartPosition{Kind: StartTime, Time: utc(2026, time.November, 2, 7, 12, 4, 114)},
		},
		{
			"rfc3339 offset",
			"time:2026-11-02T09:12:04+02:00",
			StartPosition{Kind: StartTime, Time: utc(2026, time.November, 2, 7, 12, 4, 0)},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ParseStartPosition(tt.in)
			if err != nil {
				t.Fatalf("ParseStartPosition(%q) = error %v", tt.in, err)
			}
			if got != tt.want {
				t.Fatalf("ParseStartPosition(%q) = %+v, want %+v", tt.in, got, tt.want)
			}
		})
	}
}

func TestParseStartPositionExtendedRejects(t *testing.T) {
	t.Parallel()

	bad := []string{
		"",                          // required-position message is kept from #9
		"START",                     // case-sensitive
		"First",                     // case-sensitive
		"seq:",                      // no number
		"seq:-1",                    // negative
		"seq:1.5",                   // not an integer
		"time:",                     // empty spec
		"time:-",                    // bare minus
		"time:-0s",                  // zero offset: "new" is the spelling for now
		"time:+2h",                  // positive offsets are not in the grammar
		"time:1h",                   // a duration without the '-' prefix
		"time:--2h",                 // double sign
		"time:notatime",             // neither duration nor timestamp nor integer
		"time:2026-13-45T99:99:99Z", // invalid calendar values
		"time:1762067524000ms",      // integer with a unit suffix is not legacy unix ms
		"strings",                   // plausible garbage
	}
	for _, in := range bad {
		t.Run(in, func(t *testing.T) {
			got, err := ParseStartPosition(in)
			if !errors.Is(err, errs.ErrBadRequest) {
				t.Fatalf("ParseStartPosition(%q) = %+v, %v; want ErrBadRequest", in, got, err)
			}
			if err == nil || err.Error() == "" {
				t.Fatalf("ParseStartPosition(%q) must carry a teaching message", in)
			}
		})
	}

	// The two GENERIC rejection paths must teach the whole grammar, so an
	// operator sees the accepted spellings rather than just "no".
	for _, in := range []string{"", "strings"} {
		_, err := ParseStartPosition(in)
		if !errors.Is(err, errs.ErrBadRequest) || !strings.Contains(err.Error(), "seq:N") || !strings.Contains(err.Error(), "time:-<duration>") {
			t.Fatalf("ParseStartPosition(%q) error should list the accepted forms, got %q", in, err)
		}
	}
}

func TestParseStartPositionLegacyFormsStable(t *testing.T) {
	t.Parallel()

	// The pre-#28 spellings must parse exactly as before, and their String()
	// rendering must stay byte-stable: recorded start positions in the meta
	// table are compared byte-exactly across re-POSTs (checkStartImmutable).
	tests := []struct {
		in   string
		want StartPosition
		str  string
	}{
		{"first", StartPosition{Kind: StartFirst}, "first"},
		{"new", StartPosition{Kind: StartNew}, "new"},
		{"seq:42", StartPosition{Kind: StartSeq, Seq: 42}, "seq:42"},
		{"time:1700000000000", StartPosition{Kind: StartTime, Time: 1700000000000}, "time:1700000000000"},
	}
	for _, tt := range tests {
		got, err := ParseStartPosition(tt.in)
		if err != nil {
			t.Fatalf("ParseStartPosition(%q): %v", tt.in, err)
		}
		if got != tt.want {
			t.Fatalf("ParseStartPosition(%q) = %+v, want %+v", tt.in, got, tt.want)
		}
		if s := got.String(); s != tt.str {
			t.Fatalf("String() = %q, want %q (renderings are byte-compared against meta)", s, tt.str)
		}
	}
}

func TestStartPositionRoundTrip(t *testing.T) {
	t.Parallel()

	vals := []StartPosition{
		{Kind: StartFirst},
		{Kind: StartNew},
		{Kind: StartSeq, Seq: 7},
		{Kind: StartTime, Time: 1700000000123},
		{Kind: StartTime, Rel: -time.Hour},
		{Kind: StartTime, Rel: -(90 * time.Second)},
	}
	for _, sp := range vals {
		s := sp.String()
		got, err := ParseStartPosition(s)
		if err != nil {
			t.Fatalf("round trip %q: %v", s, err)
		}
		if got != sp {
			t.Fatalf("round trip of %+v rendered %q and parsed back to %+v", sp, s, got)
		}
	}
}

// referenceParsePosition is the naive straight-line restatement of the grammar
// the differential fuzz compares against. It shares no code with
// ParseStartPosition on purpose.
func referenceParsePosition(s string) (StartPosition, error) {
	badReq := errors.New("bad position")
	switch s {
	case "first", "start":
		return StartPosition{Kind: StartFirst}, nil
	case "new":
		return StartPosition{Kind: StartNew}, nil
	case "":
		return StartPosition{}, badReq
	}
	if head, rest, found := strings.Cut(s, ":"); found && head == "seq" {
		n, err := strconv.ParseInt(rest, 10, 64)
		if err != nil || n < 0 {
			return StartPosition{}, badReq
		}
		return StartPosition{Kind: StartSeq, Seq: n}, nil
	}
	if head, rest, found := strings.Cut(s, ":"); found && head == "time" {
		if len(rest) > 1 && rest[0] == '-' {
			d, derr := time.ParseDuration(rest)
			if derr == nil && d < 0 {
				return StartPosition{Kind: StartTime, Rel: d}, nil
			}
			return StartPosition{}, badReq
		}
		if ts, terr := time.Parse(time.RFC3339, rest); terr == nil {
			ms := ts.UnixMilli()
			if ms < 0 {
				return StartPosition{}, badReq
			}
			return StartPosition{Kind: StartTime, Time: ms}, nil
		}
		n, nerr := strconv.ParseInt(rest, 10, 64)
		if nerr != nil || n < 0 {
			return StartPosition{}, badReq
		}
		return StartPosition{Kind: StartTime, Time: n}, nil
	}
	return StartPosition{}, badReq
}

func FuzzParsePosition(f *testing.F) {
	for _, seed := range []string{
		"first", "start", "new", "", "seq:0", "seq:42", "seq:-1", "seq:",
		"time:1700000000000", "time:-2h", "time:-90s", "time:-0s", "time:+2h",
		"time:1h", "time:", "time:-", "time:2026-11-02T07:12:04Z",
		"time:2026-11-02T07:12:04.114Z", "time:2026-13-45T99:99:99Z",
		"time:99999999999999999999", "orders", "time:-1ns", "TIME:-1H",
	} {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, s string) {
		want, wantErr := referenceParsePosition(s)
		got, gotErr := ParseStartPosition(s)
		if (gotErr == nil) != (wantErr == nil) {
			t.Fatalf("ParseStartPosition(%q) accept/disagree: prod err=%v, reference err=%v", s, gotErr, wantErr)
		}
		if gotErr == nil && got != want {
			t.Fatalf("ParseStartPosition(%q) = %+v, reference says %+v", s, got, want)
		}
	})
}
