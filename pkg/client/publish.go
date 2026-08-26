// SPDX-License-Identifier: Apache-2.0

package client

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/textproto"
	"net/url"
	"strings"
)

// The publish half of the data plane (issue §5). A 2xx is a durability promise (D4):
// it is returned only after the daemon's commit fsync came back. There is deliberately
// no async buffering anywhere in this file.

// reservedHeaderPrefix is the daemon's namespace; user headers ride one level below it
// as Messq-Header-*, so a user header NAMED "Messq-*" would be indistinguishable from
// infrastructure and is refused locally.
const reservedHeaderPrefix = "Messq-"

// Publish stores one message. The subject is required; a header in the Messq-
// namespace or an invalid header key is refused before any round trip.
func (c *Client) Publish(ctx context.Context, stream string, m Msg) (PublishAck, error) {
	if err := validateMsg(m); err != nil {
		return PublishAck{}, err
	}
	if err := validStreamName(stream); err != nil {
		return PublishAck{}, err
	}

	body := m.Body
	ack, err := c.publishOnce(ctx, stream, m, bytes.NewReader(body), int64(len(body)))
	if err == nil || !retrySafePublish(m, err) {
		return ack, teachCommitUnknown(m, err)
	}
	// One automatic retry, exactly the pattern §5.1 prescribes: same MsgID, so the
	// dedup pre-check turns "did it land?" into Duplicate:true instead of a second seq.
	return c.publishOnce(ctx, stream, m, bytes.NewReader(body), int64(len(body)))
}

// PublishReader publishes body with a known size without materialising it. The reader
// is consumed once; a retry after an unknown commit is impossible for a plain reader,
// so pass a seeker-backed reader if you need that path to stay retry-safe.
func (c *Client) PublishReader(ctx context.Context, stream string, m Msg, body io.Reader, size int64) (PublishAck, error) {
	if body == nil {
		return PublishAck{}, &Error{Code: "bad_request", Message: "PublishReader body is nil", err: ErrBadRequest}
	}
	if size < 0 {
		return PublishAck{}, &Error{Code: "bad_request", Message: fmt.Sprintf("size %d must be >= 0; use Publish when the length is unknown", size), err: ErrBadRequest}
	}
	if err := validateMsg(m); err != nil {
		return PublishAck{}, err
	}
	if err := validStreamName(stream); err != nil {
		return PublishAck{}, err
	}
	ack, err := c.publishOnce(ctx, stream, m, body, size)
	if err == nil || !retrySafePublish(m, err) {
		return ack, teachCommitUnknown(m, err)
	}
	if s, ok := body.(io.Seeker); ok {
		if _, serr := s.Seek(0, io.SeekStart); serr == nil {
			return c.publishOnce(ctx, stream, m, body, size)
		}
	}
	return ack, teachCommitUnknown(m, err)
}

// batchEntryWire mirrors internal/api's NDJSON line shape.
type batchEntryWire struct {
	Subject string            `json:"subject"`
	BodyB64 string            `json:"body_b64,omitempty"`
	Body    string            `json:"body,omitempty"`
	MsgID   string            `json:"msg_id,omitempty"`
	Headers map[string]string `json:"headers,omitempty"`
	TraceID string            `json:"trace_id,omitempty"`
}

// PublishBatch submits every message as ONE transaction (#14's messages:batch):
// all-or-nothing, one receipt per entry in input order. Over the server's default cap
// it refuses with ErrTooLarge — never a silent split. Chunk explicitly with MaxBatch.
func (c *Client) PublishBatch(ctx context.Context, stream string, msgs []Msg) ([]PublishAck, error) {
	if len(msgs) == 0 {
		return nil, &Error{Code: "bad_request", Message: "batch holds no entries", err: ErrBadRequest}
	}
	if err := validStreamName(stream); err != nil {
		return nil, err
	}
	if len(msgs) > DefaultPublishBatchCap {
		return nil, &Error{
			Code:    "too_large",
			Message: fmt.Sprintf("batch holds %d messages, at most %d (--max-batch-messages); chunk it with client.MaxBatch", len(msgs), DefaultPublishBatchCap),
			err:     ErrTooLarge,
		}
	}
	for i := range msgs {
		if err := validateMsg(msgs[i]); err != nil {
			return nil, err
		}
	}

	var b strings.Builder
	enc := base64.StdEncoding
	for i := range msgs {
		line := batchEntryWire{
			Subject: msgs[i].Subject,
			MsgID:   msgs[i].MsgID,
			Headers: msgs[i].Header,
			TraceID: msgs[i].TraceID,
		}
		if len(msgs[i].Body) > 0 {
			line.BodyB64 = enc.EncodeToString(msgs[i].Body)
		} else {
			line.BodyB64 = ""
			line.Body = "" // zero-length body: neither field set means empty server-side
		}
		if i > 0 {
			b.WriteByte('\n')
		}
		j, jerr := json.Marshal(line)
		if jerr != nil {
			return nil, &Error{Code: "internal", Message: "encode batch line: " + jerr.Error(), err: jerr}
		}
		b.Write(j)
	}

	path := "/v1/streams/" + url.PathEscape(stream) + "/messages:batch"
	ack, err := do[PublishBatchAck](ctx, c, "POST", path, nil, jsonRaw(b.String()))
	if err != nil {
		return nil, err
	}
	return ack.Results, nil
}

// MaxBatch splits msgs into chunks of at most n so chunking is explicit and visible
// in caller code rather than hidden inside the client.
func MaxBatch(msgs []Msg, n int) [][]Msg {
	if n <= 0 {
		n = DefaultPublishBatchCap
	}
	out := make([][]Msg, 0, (len(msgs)+n-1)/max(n, 1))
	for len(msgs) > 0 {
		k := min(n, len(msgs))
		out = append(out, msgs[:k:k])
		msgs = msgs[k:]
	}
	return out
}

func (c *Client) publishOnce(ctx context.Context, stream string, m Msg, body io.Reader, size int64) (PublishAck, error) {
	hdrs := make([][2]string, 0, len(m.Header)+3)
	hdrs = append(hdrs, [2]string{"Messq-Subject", m.Subject})
	if m.MsgID != "" {
		hdrs = append(hdrs, [2]string{"Messq-Msg-Id", m.MsgID})
	}
	if m.TraceID != "" {
		hdrs = append(hdrs, [2]string{"Messq-Trace-Id", m.TraceID})
	}
	for k, v := range m.Header {
		hdrs = append(hdrs, [2]string{"Messq-Header-" + k, v}) // canonicalised by net/http on write
	}
	path := "/v1/streams/" + url.PathEscape(stream) + "/messages"
	return send[PublishAck](ctx, c, request{
		method:         "POST",
		path:           path,
		rawBody:        body,
		rawLen:         size,
		rawContentType: "application/octet-stream",
		extraHeaders:   hdrs,
	})
}

// retrySafePublish reports whether ONE automatic retry is honest: only with a dedup
// key, and only for failures where the commit outcome is genuinely unknown.
func retrySafePublish(m Msg, err error) bool {
	if m.MsgID == "" || err == nil {
		return false
	}
	var e *Error
	if errors.As(err, &e) && e.err == nil {
		switch e.Code {
		case "commit_unknown":
			return true
		default:
			return false // a real answer from the daemon: retrying cannot help
		}
	}
	return Classify(err) == KindUnavailable // transport died mid-flight
}

// teachCommitUnknown appends the §5.1 teaching hint to a commit_unknown failure that
// will not be retried because there is no MsgID to dedup with.
func teachCommitUnknown(m Msg, err error) error {
	if m.MsgID != "" || err == nil {
		return err
	}
	var e *Error
	if errors.As(err, &e) && e.Code == "commit_unknown" {
		e.Next = append(e.Next,
			"set Msg.MsgID (the wire's Messq-Msg-Id) to make this publish retry-safe")
	}
	return err
}

// validateMsg applies the local refusals of issue §10: subject required, no forged
// infrastructure headers, no keys that cannot ride a MIME header.
func validateMsg(m Msg) error {
	if m.Subject == "" {
		return &Error{Code: "bad_request", Message: "publish needs a Subject", err: ErrBadRequest}
	}
	for k := range m.Header {
		ck := textproto.CanonicalMIMEHeaderKey(k)
		if !validHeaderKey(k) {
			return &Error{Code: "bad_request", Message: fmt.Sprintf("header key %q is not a valid MIME header name", k), err: ErrBadRequest}
		}
		if strings.HasPrefix(ck, reservedHeaderPrefix) {
			return &Error{
				Code:    "reserved_header",
				Message: fmt.Sprintf("user header %q starts with the reserved %q namespace; send application data under any other name", k, reservedHeaderPrefix),
				err:     ErrReservedHeader,
			}
		}
	}
	return nil
}

// validHeaderKey mirrors RFC 7230 token rules (what textproto accepts on write).
func validHeaderKey(k string) bool {
	if k == "" {
		return false
	}
	for i := range len(k) {
		c := k[i]
		switch {
		case c >= 'a' && c <= 'z', c >= 'A' && c <= 'Z', c >= '0' && c <= '9':
		case strings.IndexByte("!#$%&'*+-.^_`|~", c) >= 0:
		default:
			return false
		}
	}
	return true
}

// jsonRaw wraps an already-encoded JSON document so send does not re-marshal it.
type jsonRaw string

func isJSONRaw(v any) bool {
	_, ok := v.(jsonRaw)
	return ok
}

// name validation lives in names.go.
