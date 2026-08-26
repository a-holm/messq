// SPDX-License-Identifier: Apache-2.0

package flagx

import (
	"math"
	"strconv"
	"strings"

	"github.com/a-holm/messq/internal/errs"
)

// Bytes is a shared flag value for byte sizes ("1024", "64KiB", "10GiB", "1MB"):
// plain decimal integers, IEC powers of two (KiB, MiB, GiB, TiB, PiB) and SI powers of
// ten (KB, MB, GB, TB, PB). The unit is case-insensitive. Negative values and
// fractions are rejected; a size that overflows int64 is rejected rather than wrapped.
type Bytes int64

var byteUnits = map[string]int64{
	"kib": 1 << 10, "mib": 1 << 20, "gib": 1 << 30, "tib": 1 << 40, "pib": 1 << 50,
	"kb": 1e3, "mb": 1e6, "gb": 1e9, "tb": 1e12, "pb": 1e15,
}

// Set parses s into bytes.
func (b *Bytes) Set(s string) error {
	i := 0
	for i < len(s) && s[i] >= '0' && s[i] <= '9' {
		i++
	}
	num, unit := s[:i], strings.ToLower(s[i:])
	if num == "" {
		return errs.E(errs.ErrBadRequest, "flagx.Bytes.Set",
			"size %q is not a number followed by an optional unit (1024, 64KiB, 10GiB, 1MB)", s)
	}
	v, err := strconv.ParseInt(num, 10, 64)
	if err != nil {
		return errs.E(errs.ErrBadRequest, "flagx.Bytes.Set",
			"size %q does not fit in 64 bits", s)
	}
	var mult int64 = 1
	if unit != "" {
		m, ok := byteUnits[unit]
		if !ok {
			return errs.E(errs.ErrBadRequest, "flagx.Bytes.Set",
				"size %q has an unknown unit %q: use IEC (KiB MiB GiB TiB PiB), SI (KB MB GB TB PB) or no unit at all", s, s[i:])
		}
		mult = m
	}
	if v > 0 && mult > 0 && v > math.MaxInt64/mult {
		return errs.E(errs.ErrBadRequest, "flagx.Bytes.Set",
			"size %q overflows the maximum representable byte count", s)
	}
	*b = Bytes(v * mult)
	return nil
}

// String renders the plain decimal form, which Set parses back to the same value.
func (b Bytes) String() string { return strconv.FormatInt(int64(b), 10) }

// Type names the value for pflag's default display.
func (b Bytes) Type() string { return "bytes" }
