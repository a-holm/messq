// SPDX-License-Identifier: Apache-2.0

package queue

import (
	"encoding/json"
	"fmt"
	"net/textproto"
	"unicode/utf8"

	"github.com/a-holm/messq/internal/errs"
	"github.com/a-holm/messq/internal/subject"
)

// PublishReq is one publish as the validation layer sees it, before any storage
// concern: the target subject, user headers (the Messq-Header-* prefix already
// stripped and keys canonicalised by the caller), the body, an optional idempotency
// key and an optional trace id.
type PublishReq struct {
	Subject string
	Headers map[string]string // keys canonical; "" value allowed; nil = no headers
	Body    []byte            // may be empty: a 0-byte body is a valid signal
	MsgID   string            // "" = no dedup; otherwise an opaque index key ≤ 256 B
	TraceID string            // "" = mint at insert time
}

// TooLargeError reports a payload or header aggregate above its cap, carrying both
// numbers so the API can name them without recomputation.
type TooLargeError struct {
	What  string // "body", "headers"
	Size  int64
	Limit int64
}

func (e *TooLargeError) Error() string {
	return fmt.Sprintf("%s is %d bytes, limit is %d", e.What, e.Size, e.Limit)
}
func (e *TooLargeError) Unwrap() error { return errs.ErrTooLarge }

// MismatchError reports a publish whose subject matches none of the stream's accepted
// patterns. The accepted list travels in the error so the response can name it.
type MismatchError struct {
	Subject  string
	Accepted []string
}

func (e *MismatchError) Error() string {
	return fmt.Sprintf("subject %q matches none of the stream's patterns %v", e.Subject, e.Accepted)
}
func (e *MismatchError) Unwrap() error { return errs.ErrBadSubject }

// ReservedHeaderError reports a user header in the reserved "Messq-" namespace.
type ReservedHeaderError struct{ Key string }

func (e *ReservedHeaderError) Error() string {
	return fmt.Sprintf("header %q is in the reserved \"Messq-\" namespace", e.Key)
}
func (e *ReservedHeaderError) Unwrap() error { return errs.ErrBadRequest }

// maxMsgIDBytes bounds the idempotency key: it is an index key, not a payload.
const maxMsgIDBytes = 256

// EncodeHeaders renders user headers as the canonical hdr JSON: keys canonicalised
// with textproto rules, marshalled with sorted keys (encoding/json sorts map keys), so
// goldens are stable. Caps from l are enforced on count, per-value size and total JSON
// size. An empty map encodes to "", which storage persists as SQL NULL — the common
// case costs zero bytes.
func EncodeHeaders(h map[string]string, l Limits) (string, error) {
	if len(h) == 0 {
		return "", nil
	}
	if len(h) > l.MaxHeaders {
		return "", errs.E(errs.ErrBadRequest, "",
			"%d headers, at most %d are allowed", len(h), l.MaxHeaders)
	}
	canonical := make(map[string]string, len(h))
	for k, v := range h {
		if !utf8.ValidString(v) {
			return "", errs.E(errs.ErrBadRequest, "",
				"header %q holds invalid UTF-8", k)
		}
		if len(v) > 1024 {
			return "", &TooLargeError{What: fmt.Sprintf("header %q", k), Size: int64(len(v)), Limit: 1024}
		}
		ck := textproto.CanonicalMIMEHeaderKey(k)
		if prev, dup := canonical[ck]; dup {
			return "", errs.E(errs.ErrBadRequest, "",
				"headers %q and %q differ only in case (%q vs %q)", prev, k, prev, v)
		}
		if len(ck) >= len("Messq-") && ck[:len("Messq-")] == "Messq-" {
			return "", &ReservedHeaderError{Key: ck}
		}
		canonical[ck] = v
	}
	raw, err := json.Marshal(canonical)
	if err != nil {
		return "", errs.E(errs.ErrBadRequest, "", "headers do not encode as JSON: %v", err)
	}
	if len(raw) > l.MaxHeaderBytes {
		return "", &TooLargeError{What: "headers", Size: int64(len(raw)), Limit: int64(l.MaxHeaderBytes)}
	}
	return string(raw), nil
}

// ValidatePublish runs every publish check that needs no transaction and no clock:
// subject grammar (wildcards never publish), subject match against the stream's
// compiled patterns, body size against the stream's per-message cap, idempotency-key
// shape, trace-id shape and header caps. The transaction re-checks existence, match
// and size against the authoritative row; this fast path keeps rejections out of
// commit batches entirely.
func ValidatePublish(sc StreamConfig, r PublishReq, l Limits) error {
	subj, err := subject.ParseSubject(r.Subject)
	if err != nil {
		return err
	}
	set, err := subject.ParseSet(sc.Subjects)
	if err != nil {
		return err
	}
	if !set.Match(string(subj)) {
		return &MismatchError{Subject: r.Subject, Accepted: set.Strings()}
	}
	if int64(len(r.Body)) > sc.MaxMsgSize {
		return &TooLargeError{What: "body", Size: int64(len(r.Body)), Limit: sc.MaxMsgSize}
	}
	if _, err := EncodeHeaders(r.Headers, l); err != nil {
		return err
	}
	if err := ValidateMsgID(r.MsgID); err != nil {
		return err
	}
	return ValidateTraceIDToken(r.TraceID)
}

// ValidateMsgID checks an optional idempotency key's shape: printable ASCII without
// whitespace, at most maxMsgIDBytes bytes. It is an index key shared with the events
// table, never a payload, so the bound is absolute.
func ValidateMsgID(msgID string) error {
	switch {
	case msgID == "":
		return nil
	case len(msgID) > maxMsgIDBytes:
		return errs.E(errs.ErrBadRequest, "",
			"msg_id is %d bytes, at most %d are allowed", len(msgID), maxMsgIDBytes)
	}
	for i := range len(msgID) {
		c := msgID[i]
		if c <= ' ' || c == 0x7f {
			return errs.E(errs.ErrBadRequest, "",
				"msg_id holds a non-printable character at byte %d", i)
		}
	}
	return nil
}

// ValidateTraceIDToken checks an explicit trace id from Messq-Trace-Id: printable
// ASCII, 8 to 128 bytes, no whitespace, and never the W3C all-zero id. An empty
// string means "mint".
func ValidateTraceIDToken(traceID string) error {
	if traceID == "" {
		return nil
	}
	const minLen, maxLen = 8, 128
	switch {
	case len(traceID) < minLen:
		return errs.E(errs.ErrBadRequest, "", "trace_id is shorter than %d bytes", minLen)
	case len(traceID) > maxLen:
		return errs.E(errs.ErrBadRequest, "", "trace_id is longer than %d bytes", maxLen)
	}
	for i := range len(traceID) {
		c := traceID[i]
		if c <= ' ' || c == 0x7f {
			return errs.E(errs.ErrBadRequest, "",
				"trace_id holds a non-printable character at byte %d", i)
		}
	}
	if traceID == "00000000000000000000000000000000" {
		return errs.E(errs.ErrBadRequest, "", "trace_id is the W3C all-zero id")
	}
	return nil
}
