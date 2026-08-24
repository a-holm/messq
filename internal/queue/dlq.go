// SPDX-License-Identifier: Apache-2.0

package queue

import (
	"fmt"
	"net/textproto"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"
)

// The DLQ naming contract (D3/ADR-0004) and the provenance header vocabulary (PLAN
// §5.1, S9.2 step 3, clarification C13). These are pure string functions: they perform
// no I/O and read no wall clock, so the naming round-trip is property-tested over valid
// stream names and the header maps are golden-tested. #29 calls StripProvenance before
// redriving; #21/#29 key off IsDLQ/DLQName.

// DLQSuffix is the dead-letter stream suffix of decision D3. It is NOT configurable:
// it is a naming contract that #29 (redrive), #21 (depth gauge) and #7's
// ValidateStreamName all key off.
const DLQSuffix = ".dlq"

// DLQName derives the dead-letter stream of an origin stream ("orders" -> "orders.dlq").
func DLQName(stream string) string { return stream + DLQSuffix }

// IsDLQ reports whether name is a dead-letter stream. Exact via the suffix (no chain
// exists: #9 refuses a dlq policy on a .dlq consumer, so orders.dlq.dlq is impossible).
func IsDLQ(name string) bool { return strings.HasSuffix(name, DLQSuffix) }

// OriginOf returns the origin stream of a dead-letter stream, plus whether name was
// one ("orders.dlq" -> "orders", true).
func OriginOf(name string) (string, bool) {
	if !IsDLQ(name) {
		return "", false
	}
	return strings.TrimSuffix(name, DLQSuffix), true
}

// ProvenanceHeaders builds the Messq-* header set a DLQ copy must carry (PLAN §5.1 +
// C13 + the ratified proposals): the origin's identity, sequence, consumer, generation
// and published-at, the death's attempts/max-deliver/cause/trigger, the sanitised
// last reason (omitted when empty, truncated on a rune boundary), and the dead-at
// timestamp. Keys are textproto-canonicalised; timestamps are RFC3339 with milliseconds
// in UTC — the one human-facing surface where Unix-ms would be unreadable (SEMANTICS).
//
// maxReason bounds Messq-Last-Reason after sanitisation; a reason that was cut sets
// reasonTruncated. publishedAt is the origin message's published_at (unix ms); nowMS is
// the writer's now — the authority for Messq-Dead-At.
func ProvenanceHeaders(d DeadCtx, publishedAt, nowMS int64, maxReason int) (h map[string]string, reasonTruncated bool) {
	h = make(map[string]string, 13)
	h[canon("Messq-Origin-Id")] = d.MsgID
	h[canon("Messq-Origin-Stream")] = d.Stream
	h[canon("Messq-Origin-Seq")] = fmt.Sprintf("%d", d.Seq)
	h[canon("Messq-Origin-Consumer")] = d.Consumer
	h[canon("Messq-Origin-Generation")] = fmt.Sprintf("%d", d.Generation)
	h[canon("Messq-Origin-Published-At")] = formatRFC3339MS(publishedAt)
	h[canon("Messq-Attempts")] = fmt.Sprintf("%d", d.Attempts)
	h[canon("Messq-Max-Deliver")] = fmt.Sprintf("%d", d.MaxDeliver)
	h[canon("Messq-Cause")] = string(d.Cause)
	if d.Trigger != "" {
		h[canon("Messq-Trigger")] = string(d.Trigger)
	}
	if d.LastReason != "" {
		reason, trunc := SanitizeReason(d.LastReason, maxReason)
		h[canon("Messq-Last-Reason")] = reason
		if trunc {
			h[canon("Messq-Last-Reason-Truncated")] = "true"
			reasonTruncated = true
		}
	}
	h[canon("Messq-Dead-At")] = formatRFC3339MS(nowMS)
	return h, reasonTruncated
}

// canon canonicalises a header key with textproto rules, matching #7's EncodeHeaders.
func canon(k string) string { return textproto.CanonicalMIMEHeaderKey(k) }

// formatRFC3339MS renders a unix-ms timestamp as RFC3339 with milliseconds, UTC — the
// header-timestamp exception (database and events stay unix ms).
func formatRFC3339MS(ms int64) string {
	return time.UnixMilli(ms).UTC().Format("2006-01-02T15:04:05.000Z")
}

// StripProvenance removes every Messq-* header from a copy, preserving the origin's
// user headers. #29 calls it before redriving so a redriven message does not carry the
// previous incarnation's provenance (and so #7's reserved_header rule is never violated
// by our own output). Idempotent: the result never contains a Messq-* key.
func StripProvenance(h map[string]string) map[string]string {
	out := make(map[string]string, len(h))
	for k, v := range h {
		if strings.HasPrefix(k, "Messq-") {
			continue
		}
		out[k] = v
	}
	return out
}

// SanitizeReason prepares an untrusted child-process reason (up to 4 KiB of stderr,
// #25) for a header value: it coerces to valid UTF-8 (invalid sequences -> U+FFFD),
// maps C0/C1 control characters to their escapes, collapses runs of whitespace
// (so no bare newline ever reaches a header value), then truncates to at most maxBytes
// on a rune boundary. It returns whether truncation occurred. The FULL untruncated
// reason lives in the msg.dead event detail, so nothing is lost.
func SanitizeReason(s string, maxBytes int) (out string, truncated bool) {
	var b strings.Builder
	b.Grow(len(s))
	inWS := false
	for _, r := range s {
		if !utf8.ValidRune(r) {
			r = utf8.RuneError // U+FFFD
		}
		if unicode.IsSpace(r) {
			if !inWS {
				b.WriteByte(' ')
				inWS = true
			}
			continue
		}
		inWS = false
		if unicode.IsControl(r) {
			b.WriteString(controlEscape(r))
			continue
		}
		b.WriteRune(r)
	}
	out = b.String()
	if maxBytes > 0 && len(out) > maxBytes {
		out = truncateUTF8(out, maxBytes)
		return out, true
	}
	return out, false
}

// controlEscape renders a control rune as a printable escape. Whitespace controls never
// reach here (they are collapsed to a single space by SanitizeReason).
func controlEscape(r rune) string {
	switch r {
	case '\a':
		return `\a`
	case '\b':
		return `\b`
	case '\x1b':
		return `\e` // ANSI escape — the most common reason a child stderr carries a control
	default:
		return fmt.Sprintf(`\u%04x`, r)
	}
}

// truncateUTF8 shortens s to at most max bytes, never splitting a rune.
func truncateUTF8(s string, max int) string {
	if max <= 0 || len(s) <= max {
		return s
	}
	i := 0
	for i < max {
		_, sz := utf8.DecodeRuneInString(s[i:])
		if i+sz > max {
			break
		}
		i += sz
	}
	return s[:i]
}
