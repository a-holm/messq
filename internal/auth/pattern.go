// SPDX-License-Identifier: Apache-2.0

package auth

import (
	"fmt"
	"strings"

	"github.com/a-holm/messq/internal/errs"
)

// Pattern is a compiled stream pattern. The grammar is exactly three forms (issue #16, §3):
//
//   - "*"        — every stream
//   - "orders*"  — a prefix match (orders, orders.dlq, orders2, …)
//   - "orders"   — exact equality
//
// A pattern containing ">" or an interior "*" is a parse error. This is deliberately not the
// subject matcher from internal/subject: ">" consumes subject tokens, which have no meaning in
// a stream name, and "*" is only allowed whole or trailing. The zero value is invalid and
// matches nothing; build one with [ParsePattern].
type Pattern struct {
	raw    string
	star   bool // "*" — matches every stream
	prefix bool // "<name>*" — prefix match
	lit    string
}

// ParsePattern compiles s. The returned error classifies as [errs.ErrBadRequest] and carries
// the exact message the token-file parser wraps with a line number.
func ParsePattern(s string) (Pattern, error) {
	if s == "" {
		return Pattern{}, fmt.Errorf("%w: stream pattern is empty", errs.ErrBadRequest)
	}
	if strings.Contains(s, ">") {
		return Pattern{}, fmt.Errorf(
			`%w: invalid stream pattern %q: stream patterns are not subject patterns; ">" matches subject tokens, not stream names`,
			errs.ErrBadRequest, s)
	}
	idx := strings.IndexByte(s, '*')
	switch {
	case idx < 0:
		return Pattern{raw: s, lit: s}, nil
	case s == "*":
		return Pattern{raw: s, star: true}, nil
	case idx == len(s)-1:
		// A trailing "*" is a prefix match on the literal before it.
		return Pattern{raw: s, prefix: true, lit: s[:len(s)-1]}, nil
	default:
		return Pattern{}, fmt.Errorf(
			`%w: invalid stream pattern %q: "*" is only allowed as a whole pattern or a trailing character`,
			errs.ErrBadRequest, s)
	}
}

// Match reports whether stream is a stream this pattern covers. It allocates nothing and never
// panics. Matching is byte-for-byte: stream names are case-sensitive, and an exact pattern
// does not cover a derived ".dlq" name.
func (p Pattern) Match(stream string) bool {
	switch {
	case p.star:
		return true
	case p.prefix:
		return strings.HasPrefix(stream, p.lit)
	default:
		return stream == p.lit
	}
}

// String returns exactly the text the pattern was compiled from, so a pattern round-trips
// through the token file unchanged.
func (p Pattern) String() string { return p.raw }
