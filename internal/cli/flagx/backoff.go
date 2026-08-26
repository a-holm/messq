// SPDX-License-Identifier: Apache-2.0

package flagx

import (
	"strings"
	"time"

	"github.com/a-holm/messq/internal/errs"
)

// Backoff is the --backoff flag value: a comma-separated duration list such as
// "1s,5s,30s,2m,10m". The list must be non-empty and every entry strictly positive
// (C9 rejects an empty schedule; a zero entry would collapse two attempts into one).
// Ascending order is not required because the schedule's last value repeats (S8.2).
type Backoff []time.Duration

const backoffOp = "flagx.Backoff.Set"

// Set parses s. Each element goes through the shared Duration parser — one parser per
// concept, so --backoff accepts exactly what --ack-wait accepts.
func (b *Backoff) Set(s string) error {
	parts := strings.Split(s, ",")
	out := make(Backoff, 0, len(parts))
	for _, p := range parts {
		var d Duration
		if derr := d.Set(p); derr != nil {
			return errs.E(errs.ErrBadRequest, backoffOp,
				"backoff entry %q: %v", p, derr)
		}
		if d == 0 {
			return errs.E(errs.ErrBadRequest, backoffOp,
				"backoff entry %q is zero; every entry must be strictly positive", p)
		}
		out = append(out, time.Duration(d))
	}
	*b = out
	return nil
}

// String renders the entries comma-joined in Go duration form, which Set parses back
// to the same schedule.
func (b Backoff) String() string {
	parts := make([]string, len(b))
	for i, d := range b {
		parts[i] = d.String()
	}
	return strings.Join(parts, ",")
}

// Millis renders the schedule as wire milliseconds, one per entry.
func (b Backoff) Millis() []int64 {
	out := make([]int64, len(b))
	for i, d := range b {
		out[i] = int64(d / time.Millisecond)
	}
	return out
}

// Type names the value for pflag's default display.
func (b Backoff) Type() string { return "backoff" }
