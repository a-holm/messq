// SPDX-License-Identifier: Apache-2.0

package queue

import (
	"fmt"
	"math"
	"strconv"
	"strings"

	"github.com/a-holm/messq/internal/errs"
	"github.com/a-holm/messq/internal/subject"
)

// Token is the fenced, human-readable ack token of D7 (issue #9 §7). String renders
// the grammar "#4 §S3": "stream/consumer/seq/attempt/generation", where attempt is
// the POST-increment attempt count (during attempt n the token carries n) and
// generation is the consumer's current generation. Parsing and fencing land in #10;
// this issue only mints tokens, so it commits the corpus #10's fuzz target seeds from
// (testdata/tokens/valid.txt).
type Token struct {
	Stream     string
	Consumer   string
	Seq        int64
	Attempt    int32
	Generation int32
}

// String renders the token in its wire form. The stream and consumer names are
// guaranteed slash-free by rule S11 (and the consumer-name grammar's '/' ban), so the
// four slashes are unambiguous field separators.
func (t Token) String() string {
	return fmt.Sprintf("%s/%s/%d/%d/%d", t.Stream, t.Consumer, t.Seq, t.Attempt, t.Generation)
}

// MaxTokenLen is the longest well-formed token in bytes (S3.3):
// 64 + 1 + 64 + 1 + 19 + 1 + 10 + 1 + 10 = 171, for the full-length stream and consumer
// names, a 19-digit seq and two 10-digit attempt/generation counts. ParseToken rejects
// longer input before any scanning (C6).
const MaxTokenLen = 171

// ParseToken accepts only the canonical rendering of the D7 grammar, so
// ParseToken(t.String()) == t for every minted t and ParseToken(s).String() == s for
// every accepted s. It parses with four strings.Cut calls and never splits (zero
// allocations on the accepted path), so "one token, one string" stays greppable across
// a log line, a DLQ header and an event row. Every rejection is errs.ErrUnknownToken
// wrapped with the offending field; no forged token parses into a well-formed Token
// that names a different message (that is the no-forgery property the fuzzer asserts).
func ParseToken(s string) (Token, error) {
	const op = "queue.ParseToken"
	switch {
	case len(s) > MaxTokenLen:
		return Token{}, errs.E(errs.ErrUnknownToken, op,
			"token is %d bytes, want at most %d", len(s), MaxTokenLen)
	case s == "":
		return Token{}, errs.E(errs.ErrUnknownToken, op, "token is empty")
	}

	// Four separator cuts, five fields, never strings.Split (which would allocate).
	stream, rest, ok := strings.Cut(s, "/")
	if !ok {
		return Token{}, errs.E(errs.ErrUnknownToken, op, "token %q names no consumer", s)
	}
	consumer, rest, ok := strings.Cut(rest, "/")
	if !ok {
		return Token{}, errs.E(errs.ErrUnknownToken, op, "token %q names no seq", s)
	}
	seqStr, rest, ok := strings.Cut(rest, "/")
	if !ok {
		return Token{}, errs.E(errs.ErrUnknownToken, op, "token %q names no attempt", s)
	}
	attemptStr, genStr, ok := strings.Cut(rest, "/")
	if !ok {
		return Token{}, errs.E(errs.ErrUnknownToken, op, "token %q names no generation", s)
	}

	// stream may carry '.', so subject.ValidateStreamName (not the creation-time cap,
	// which would reject derived streams); consumer never carries '.' so the consumer
	// rule is the right one here. Both ban '/' and bound 1..64 bytes.
	if err := subject.ValidateStreamName(stream); err != nil {
		return Token{}, errs.E(errs.ErrUnknownToken, op,
			"token's stream field %q is not a valid stream name", stream)
	}
	if err := ValidateConsumerName(consumer); err != nil {
		return Token{}, errs.E(errs.ErrUnknownToken, op,
			"token's consumer field %q is not a valid consumer name", consumer)
	}

	seq, err := parseUintField(seqStr, "seq")
	if err != nil || seq == 0 || seq > math.MaxInt64 {
		if err == nil {
			return Token{}, errs.E(errs.ErrUnknownToken, op, "token's seq %q is out of range 1..%d", seqStr, math.MaxInt64)
		}
		return Token{}, err
	}
	attempt, err := parseUintField(attemptStr, "attempt")
	if err != nil || attempt == 0 || attempt > math.MaxInt32 {
		if err == nil {
			return Token{}, errs.E(errs.ErrUnknownToken, op, "token's attempt %q is out of range 1..%d", attemptStr, math.MaxInt32)
		}
		return Token{}, err
	}
	generation, err := parseUintField(genStr, "generation")
	if err != nil || generation == 0 || generation > math.MaxInt32 {
		if err == nil {
			return Token{}, errs.E(errs.ErrUnknownToken, op, "token's generation %q is out of range 1..%d", genStr, math.MaxInt32)
		}
		return Token{}, err
	}

	return Token{
		Stream:     stream,
		Consumer:   consumer,
		Seq:        int64(seq),
		Attempt:    int32(attempt),
		Generation: int32(generation),
	}, nil
}

// parseUintField parses one decimal field with strict canonicality: no leading zero,
// no sign, no whitespace, no non-ASCII digits. strconv.ParseUint already rejects signs
// and non-ASCII; the leading-zero check is the parser's own, because canonical "one
// token, one string" is what keeps tokens greppable.
func parseUintField(field, name string) (uint64, error) {
	const op = "queue.ParseToken"
	if field == "" {
		return 0, errs.E(errs.ErrUnknownToken, op, "token's %s field is empty", name)
	}
	if field[0] == '0' {
		return 0, errs.E(errs.ErrUnknownToken, op,
			"token's %s field %q has a leading zero (tokens are canonical)", name, field)
	}
	v, err := strconv.ParseUint(field, 10, 64)
	if err != nil {
		return 0, errs.E(errs.ErrUnknownToken, op,
			"token's %s field %q is not a canonical decimal integer", name, field)
	}
	return v, nil
}
