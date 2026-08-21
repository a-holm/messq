// SPDX-License-Identifier: Apache-2.0

package id

import (
	"encoding/hex"
	"fmt"
	"io"

	"github.com/a-holm/messq/internal/errs"
	"github.com/oklog/ulid/v2"
)

// ErrBadTraceID is every way a trace id can fail to parse.
var ErrBadTraceID = errs.E(errs.ErrBadRequest, "", "a trace id is 32 lowercase hex characters and never all zero")

// TraceID is a 16-byte W3C trace id, rendered as 32 lowercase hex characters.
type TraceID [16]byte

// SpanID is an 8-byte W3C span id, rendered as 16 lowercase hex characters. messq mints none
// of its own; it carries the parent id out of an incoming traceparent so a log line can be
// tied back to the caller's span.
type SpanID [8]byte

// String renders the trace id as 32 lowercase hex characters.
func (t TraceID) String() string { return hex.EncodeToString(t[:]) }

// IsZero reports the all-zero value, which the W3C specification declares invalid.
func (t TraceID) IsZero() bool { return t == TraceID{} }

// String renders the span id as 16 lowercase hex characters.
func (s SpanID) String() string { return hex.EncodeToString(s[:]) }

// IsZero reports the all-zero value, which the W3C specification declares invalid.
func (s SpanID) IsZero() bool { return s == SpanID{} }

// traceIDDraws bounds the retry loop in [NewTraceID]. An all-zero draw from a working source
// has probability 2^-128; more than a couple means the source is not working.
const traceIDDraws = 4

// NewTraceID mints a trace id from r. It never returns the all-zero value and it never fails:
// a publish must not be refused because the entropy source hiccuped.
func NewTraceID(r io.Reader) TraceID {
	var t TraceID
	for range traceIDDraws {
		if _, err := io.ReadFull(r, t[:]); err != nil {
			break
		}
		if !t.IsZero() {
			return t
		}
	}
	// The source failed or kept drawing zeros. The result is no longer unguessable, but it
	// is at least well-formed, which is what every consumer of the header requires.
	t[len(t)-1] |= 1
	return t
}

// ParseTraceID parses the canonical rendering: exactly 32 lowercase hex characters, not all
// zero.
func ParseTraceID(s string) (TraceID, error) {
	var t TraceID
	if !decodeLowerHex(s, t[:]) || t.IsZero() {
		return TraceID{}, fmt.Errorf("trace id %q: %w", s, ErrBadTraceID)
	}
	return t, nil
}

// The fixed layout of a version 00 traceparent: two hex characters of version, the trace id,
// the parent id and the trace flags, separated by three dashes.
const (
	traceparentLen = 55
	versionEnd     = 2
	traceStart     = 3
	traceEnd       = 35
	parentStart    = 36
	parentEnd      = 52
	flagsStart     = 53
)

// ParseTraceparent extracts the correlation fields from a W3C traceparent header value.
//
// ok reports whether the header was usable. A false ok means "mint a fresh trace id", never an
// error: a malformed header from an upstream service must not fail a publish. The parse is
// fixed-offset and allocation-free, so a megabyte of separators costs a few comparisons.
func ParseTraceparent(v string) (tid TraceID, parent SpanID, sampled, ok bool) {
	if len(v) < traceparentLen || v[versionEnd] != '-' || v[traceEnd] != '-' || v[parentEnd] != '-' {
		return TraceID{}, SpanID{}, false, false
	}

	var version [1]byte
	if !decodeLowerHex(v[:versionEnd], version[:]) || version[0] == 0xff {
		// Version ff is forbidden outright by the specification.
		return TraceID{}, SpanID{}, false, false
	}
	switch {
	case version[0] == 0:
		// Version 00 is exactly 55 characters. Anything appended is not this version.
		if len(v) != traceparentLen {
			return TraceID{}, SpanID{}, false, false
		}
	case len(v) > traceparentLen:
		// A later version may append fields, each introduced by a separator. Their
		// content is ignored, which is what keeps this parser forward compatible.
		if v[traceparentLen] != '-' || len(v) == traceparentLen+1 {
			return TraceID{}, SpanID{}, false, false
		}
	}

	if !decodeLowerHex(v[traceStart:traceEnd], tid[:]) || tid.IsZero() {
		return TraceID{}, SpanID{}, false, false
	}
	if !decodeLowerHex(v[parentStart:parentEnd], parent[:]) || parent.IsZero() {
		return TraceID{}, SpanID{}, false, false
	}
	var flags [1]byte
	if !decodeLowerHex(v[flagsStart:traceparentLen], flags[:]) {
		return TraceID{}, SpanID{}, false, false
	}
	return tid, parent, flags[0]&0x01 != 0, true
}

// Kind classifies a user-supplied identifier, so `messq trace <arg>` and the message endpoint
// can take either form.
type Kind int

const (
	// KindUnknown is anything the two parsers refuse.
	KindUnknown Kind = iota
	// KindMsgID is a 26-character ULID.
	KindMsgID
	// KindTraceID is a 32-character lowercase hex trace id.
	KindTraceID
)

// String names the kind, for error messages.
func (k Kind) String() string {
	switch k {
	case KindMsgID:
		return "message id"
	case KindTraceID:
		return "trace id"
	case KindUnknown:
		return "unknown"
	}
	return "unknown"
}

// Classify tells a message id and a trace id apart by shape. The two never collide: one is 26
// characters, the other 32.
func Classify(s string) Kind {
	if len(s) == ulid.EncodedSize {
		if _, err := ulid.ParseStrict(s); err == nil {
			return KindMsgID
		}
	}
	if _, err := ParseTraceID(s); err == nil {
		return KindTraceID
	}
	return KindUnknown
}

// decodeLowerHex writes the decoded bytes of s into dst. It rejects upper-case input, which
// the W3C specification requires, and any length other than twice len(dst). It allocates
// nothing.
func decodeLowerHex(s string, dst []byte) bool {
	if len(s) != 2*len(dst) {
		return false
	}
	for i := range dst {
		hi, lo := hexValue(s[2*i]), hexValue(s[2*i+1])
		if hi > 0xf || lo > 0xf {
			return false
		}
		dst[i] = hi<<4 | lo
	}
	return true
}

// hexValue decodes one lowercase hex digit, returning 0xff for anything else.
func hexValue(c byte) byte {
	switch {
	case c >= '0' && c <= '9':
		return c - '0'
	case c >= 'a' && c <= 'f':
		return c - 'a' + 10
	}
	return 0xff
}
