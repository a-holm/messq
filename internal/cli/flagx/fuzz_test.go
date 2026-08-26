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
	"github.com/a-holm/messq/internal/subject"
)

// The fuzz invariants are the same five for every parser: an accepted input never
// panics, never produces a negative where negatives are banned, round-trips through its
// own String, and — the contract exit-2 mapping leans on — every rejection wraps
// errs.ErrBadRequest.

func isBadRequest(err error) bool {
	return err != nil && errors.Is(err, errs.ErrBadRequest)
}

func FuzzDuration(f *testing.F) {
	for _, seed := range []string{"7d", "500ms", "0", "-5s", "1h30m", "99999999999999999d", "d"} {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, s string) {
		var d flagx.Duration
		err := d.Set(s)
		switch err {
		case nil:
			if d < 0 {
				t.Fatalf("Duration.Set(%q) accepted a negative duration %v", s, time.Duration(d))
			}
			var back flagx.Duration
			if backErr := back.Set(d.String()); backErr != nil || back != d {
				t.Fatalf("Duration %v (from %q) does not round-trip through %q: (%v, %v)",
					time.Duration(d), s, d.String(), time.Duration(back), backErr)
			}
		default:
			if !isBadRequest(err) {
				t.Fatalf("Duration.Set(%q) = %v, want an errs.ErrBadRequest wrap", s, err)
			}
		}
	})
}

func FuzzBytes(f *testing.F) {
	for _, seed := range []string{"64KiB", "1MB", "0", "-1", "10GiB", "1.5MiB", "KiB", "18446744073709551616"} {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, s string) {
		var b flagx.Bytes
		err := b.Set(s)
		switch err {
		case nil:
			if b < 0 {
				t.Fatalf("Bytes.Set(%q) accepted a negative size %d", s, int64(b))
			}
			var back flagx.Bytes
			if backErr := back.Set(b.String()); backErr != nil || back != b {
				t.Fatalf("Bytes %d (from %q) does not round-trip through %q: (%d, %v)",
					int64(b), s, b.String(), int64(back), backErr)
			}
		default:
			if !isBadRequest(err) {
				t.Fatalf("Bytes.Set(%q) = %v, want an errs.ErrBadRequest wrap", s, err)
			}
		}
	})
}

// hasControlByte reports whether s carries an ASCII control character (bytes below
// 0x20, or DEL). Byte-wise on purpose: header keys and values are opaque bytes.
func hasControlByte(s string) bool {
	for i := 0; i < len(s); i++ {
		if s[i] < 0x20 || s[i] == 0x7f {
			return true
		}
	}
	return false
}

func FuzzHeaders(f *testing.F) {
	for _, seed := range []string{"tenant=acme", "=v", "novalue", "Messq-X=1", "k\n=v", "empty="} {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, s string) {
		var h flagx.Headers
		err := h.Set(s)
		switch err {
		case nil:
			last := h[len(h)-1]
			if last.Key == "" || strings.HasPrefix(strings.ToLower(last.Key), "messq-") {
				t.Fatalf("Headers.Set(%q) stored forbidden key %q", s, last.Key)
			}
			if hasControlByte(last.Key) || hasControlByte(last.Value) {
				t.Fatalf("Headers.Set(%q) stored a control byte: %+v", s, last)
			}
		default:
			if !isBadRequest(err) {
				t.Fatalf("Headers.Set(%q) = %v, want an errs.ErrBadRequest wrap", s, err)
			}
		}
	})
}

func FuzzPatterns(f *testing.F) {
	for _, seed := range []string{"orders.*", ">", "", "a..b", "a b", "a.*.b"} {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, s string) {
		var p flagx.Patterns
		err := p.Set(s)
		switch err {
		case nil:
			if _, perr := subject.ParsePattern(p[len(p)-1]); perr != nil {
				t.Fatalf("Patterns.Set(%q) stored %q but subject.ParsePattern rejects it: %v",
					s, p[len(p)-1], perr)
			}
		default:
			if !isBadRequest(err) {
				t.Fatalf("Patterns.Set(%q) = %v, want an errs.ErrBadRequest wrap", s, err)
			}
		}
	})
}

func FuzzBackoff(f *testing.F) {
	for _, seed := range []string{"1s,5s", "0s", "1s,,2s", "", "-1s", "10m,1s"} {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, s string) {
		var b flagx.Backoff
		err := b.Set(s)
		switch err {
		case nil:
			if len(b) == 0 {
				t.Fatalf("Backoff.Set(%q) stored an empty schedule", s)
			}
			for i, d := range b {
				if d <= 0 {
					t.Fatalf("Backoff.Set(%q) stored non-positive entry %d = %v", s, i, d)
				}
			}
		default:
			if !isBadRequest(err) {
				t.Fatalf("Backoff.Set(%q) = %v, want an errs.ErrBadRequest wrap", s, err)
			}
		}
	})
}

func FuzzPosition(f *testing.F) {
	for _, seed := range []string{"first", "new", "seq:42", "time:1700000000000", "bogus", "seq:-1", ""} {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, s string) {
		var p flagx.Position
		err := p.Set(s)
		switch err {
		case nil:
			// queue.StartPosition's zero value is invalid by construction; any accepted
			// position therefore has a kind.
			if p.Kind == "" {
				t.Fatalf("Position.Set(%q) left the zero value", s)
			}
			var back flagx.Position
			if backErr := back.Set(p.String()); backErr != nil || back.StartPosition != p.StartPosition {
				t.Fatalf("Position from %q does not round-trip through %q: (%+v, %v)",
					s, p.String(), back.StartPosition, backErr)
			}
		default:
			if !isBadRequest(err) {
				t.Fatalf("Position.Set(%q) = %v, want an errs.ErrBadRequest wrap", s, err)
			}
			// Rejections must match the shared grammar exactly: anything queue rejects,
			// flagx rejects, and vice versa.
			if _, qerr := queue.ParseStartPosition(s); qerr == nil {
				t.Fatalf("Position.Set(%q) rejected but queue.ParseStartPosition accepts it", s)
			}
		}
	})
}
