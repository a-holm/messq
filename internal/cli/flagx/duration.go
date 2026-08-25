// SPDX-License-Identifier: Apache-2.0

package flagx

import (
	"math"
	"strconv"
	"strings"
	"time"

	"github.com/a-holm/messq/internal/errs"
)

// Duration is a shared flag value for durations: Go duration syntax plus a day unit
// ("500ms", "30s", "5m", "2h", "7d"), so --max-age and --older-than cannot disagree
// about what a day is. Zero ("0") is legal where documented; negative values are
// rejected, because every flag this package serves measures a forward-looking amount.
type Duration time.Duration

const (
	day        = 24 * time.Hour
	maxDays    = int64(math.MaxInt64) / int64(day)
	durationOp = "flagx.Duration.Set"
)

// Set parses s. A trailing integer "d" counts days; everything else delegates to
// time.ParseDuration, so Go's own spellings keep working.
func (d *Duration) Set(s string) error {
	if rest, ok := strings.CutSuffix(s, "d"); ok {
		n, err := strconv.ParseInt(rest, 10, 64)
		if err != nil || n < 0 || n > maxDays {
			return errs.E(errs.ErrBadRequest, durationOp,
				"%q is not a duration: days are whole and non-negative (500ms, 30s, 5m, 2h, 7d)", s)
		}
		*d = Duration(time.Duration(n) * day)
		return nil
	}
	v, err := time.ParseDuration(s)
	if err != nil {
		return errs.E(errs.ErrBadRequest, durationOp,
			"%q is not a duration: use Go syntax plus d for days (500ms, 30s, 5m, 2h, 7d)", s)
	}
	if v < 0 {
		return errs.E(errs.ErrBadRequest, durationOp,
			"%q is negative; durations count forward and cannot be negative", s)
	}
	*d = Duration(v)
	return nil
}

// String renders the duration in Go form, which Set parses back to the same value.
func (d Duration) String() string { return time.Duration(d).String() }

// Type names the value for pflag's default display.
func (d Duration) Type() string { return "duration" }

// Millis renders the duration as wire milliseconds; sub-millisecond durations
// truncate.
func (d Duration) Millis() int64 { return int64(time.Duration(d) / time.Millisecond) }
