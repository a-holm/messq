// SPDX-License-Identifier: Apache-2.0

package flagx

import (
	"strings"

	"github.com/a-holm/messq/internal/errs"
)

// Header is one user header supplied as -H KEY=VALUE and sent to the daemon as
// Messq-Header-KEY.
type Header struct {
	Key   string
	Value string
}

// Headers is the repeatable -H flag value (issue #24 slice 1). The Messq- prefix is
// reserved (SEMANTICS.md S3.4/C5): a key beginning with it, compared case-insensitively,
// is rejected client-side with exit 2 rather than silently dropped. Empty keys, keys
// without a "=" value and control characters in either half are rejected too; an empty
// value ("empty=") is legal.
type Headers []Header

const headersOp = "flagx.Headers.Set"

// Set parses one KEY=VALUE pair and appends it. Repeating the flag appends another
// pair; duplicate keys are the caller's business.
func (h *Headers) Set(s string) error {
	key, val, ok := strings.Cut(s, "=")
	if !ok {
		return errs.E(errs.ErrBadRequest, headersOp,
			"header %q is not KEY=VALUE", s)
	}
	if key == "" {
		return errs.E(errs.ErrBadRequest, headersOp,
			"header %q has an empty key", s)
	}
	if strings.HasPrefix(strings.ToLower(key), "messq-") {
		return errs.E(errs.ErrBadRequest, headersOp,
			"header key %q uses the reserved Messq- prefix (SEMANTICS S3.4): user headers are sent as Messq-Header-%s", s, key)
	}
	if hasControlByte(key) {
		return errs.E(errs.ErrBadRequest, headersOp,
			"header key %q contains a control character", key)
	}
	if hasControlByte(val) {
		return errs.E(errs.ErrBadRequest, headersOp,
			"value of header %q contains a control character", key)
	}
	*h = append(*h, Header{Key: key, Value: val})
	return nil
}

// String renders display-grade "k=v" pairs joined by ", ". It is not a canonical
// encoding — values containing commas or "=" do not round-trip through Set — and pflag
// only uses String to show defaults.
func (h Headers) String() string {
	parts := make([]string, len(h))
	for i, hd := range h {
		parts[i] = hd.Key + "=" + hd.Value
	}
	return strings.Join(parts, ", ")
}

// Type names the value for pflag's default display.
func (h Headers) Type() string { return "headers" }

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
