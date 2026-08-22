// SPDX-License-Identifier: Apache-2.0

package store

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/a-holm/messq/internal/errs"
	"github.com/a-holm/messq/internal/id"
	"github.com/a-holm/messq/internal/queue"
)

// The publish path (issue §3). Publish is the thin command wrapper; publishTx is THE
// insert primitive that #12 (dead-letter), #28 (replay) and #29 (redrive) reuse
// verbatim, so provenance overrides and preserved ids stay one code path. The
// authoritative validation happens inside the transaction against the stored stream
// row, so a config narrowed or deleted earlier in the same commit batch can never be
// raced by a stale handler snapshot.

// Ack is the durable-publish receipt (issue §2). Its JSON field names are the HTTP
// response body of POST /v1/streams/{s}/messages and are golden-tested from day one.
type Ack struct {
	Stream      string `json:"stream"`
	Seq         int64  `json:"seq"`
	ID          string `json:"id"`
	TraceID     string `json:"trace_id"`
	Duplicate   bool   `json:"duplicate"`
	PublishedAt int64  `json:"published_at"` // unix ms
}

// PublishCmd names the target stream and carries the validated request shape from
// internal/queue.
type PublishCmd struct {
	Stream string
	Req    queue.PublishReq
}

// Bytes estimates this command's WAL footprint for #6's commit-max-bytes budget:
// body plus header JSON plus a constant for the row's other columns.
func (c PublishCmd) Bytes() int {
	hdrBytes := 0
	for k, v := range c.Req.Headers {
		hdrBytes += len(k) + len(v) + 4 // JSON punctuation overhead per pair
	}
	return len(c.Req.Body) + hdrBytes + 256
}

// publishOpts carries what internal callers (#12/#28/#29) override: a preserved
// original ULID and the audit event name. Callers never invent event names outside
// PLAN §9.2's closed set.
type publishOpts struct {
	IDOverride string
	Event      string // "" = msg.publish
}

// Publish stores one message durably and returns its receipt only after the commit
// returned (D1/D4). A duplicate idempotency key returns the original's receipt with
// Duplicate=true and allocates no sequence number (P1).
func (s *Store) Publish(ctx context.Context, c PublishCmd) (Ack, error) {
	if err := queue.ValidateExistingStreamName(c.Stream); err != nil {
		return Ack{}, err
	}
	var ack Ack
	err := s.runWrite(ctx, "store.Publish", func(tx txLike) error {
		a, pErr := publishTx(ctx, tx, nowMS(s.clk), s.limits, s.newID,
			c.Stream, c.Req, publishOpts{})
		if pErr != nil {
			return pErr
		}
		ack = a
		return nil
	})
	if err != nil {
		return Ack{}, err
	}
	return ack, nil
}

// publishTx inserts one message inside the caller's transaction, in order:
//
//  0. re-read the authoritative stream row (the batch may have narrowed or deleted it);
//  1. full request validation against that fresh configuration;
//  2. dedup pre-check — before sequence allocation, so retries stay gap-free (P1);
//  3. contiguous sequence allocation via stream_seq;
//  4. clamped monotone published_at (P4);
//  5. the messages insert (hdr NULL when empty);
//  6. stream_stats maintenance (P5);
//  7. the audit row, same transaction (D11).
//
// dedupWindowMS is the stream's live window: zero disables dedup entirely and the key
// is stored NULL regardless of the request.
func publishTx(ctx context.Context, tx txLike, now int64, limits queue.Limits,
	newID func() id.MsgID, stream string, r queue.PublishReq, o publishOpts,
) (Ack, error) {
	lc, cfgErr := loadStreamConfig(ctx, tx, stream)
	if cfgErr != nil {
		return Ack{}, cfgErr
	}
	return publishTxWithConfig(ctx, tx, now, limits, newID, lc, r, o)
}

// publishTxWithConfig is publishTx's body against an already-loaded authoritative
// config — the shape PublishBatch uses so one batch costs one streams-row lookup.
func publishTxWithConfig(ctx context.Context, tx txLike, now int64,
	limits queue.Limits, newID func() id.MsgID, lc loadedConfig,
	r queue.PublishReq, o publishOpts,
) (Ack, error) {
	stream := lc.cfg.Name
	if err := queue.ValidatePublish(lc.cfg, r, limits); err != nil {
		return Ack{}, err
	}

	dedupKey := nullStr(r.MsgID)
	if lc.window == 0 { // §9: dedup_window_ms = 0 disables dedup, the header has no effect
		dedupKey = sql.Null[string]{}
	}
	eventName := o.Event
	if eventName == "" {
		eventName = "msg.publish"
	}

	if r.MsgID != "" && lc.window > 0 {
		orig, found, dErr := findDedupHit(ctx, tx, stream, r.MsgID)
		if dErr != nil {
			return Ack{}, dErr
		}
		if found {
			subjectDiffers := orig.subject != r.Subject
			detail := fmt.Sprintf(`{"original_seq":%d,"subject_differs":%t}`,
				orig.seq, subjectDiffers)
			if eErr := insertEvent(ctx, tx, event{
				ts:      now,
				name:    "msg.dup",
				stream:  nullStr(stream),
				subject: nullStr(r.Subject),
				msgID:   nullStr(orig.id),
				seq:     nullI64(orig.seq),
				traceID: nullStr(orig.traceID),
				detail:  nullStr(detail),
			}); eErr != nil {
				return Ack{}, eErr
			}
			return Ack{
				Stream: stream, Seq: orig.seq, ID: orig.id, TraceID: orig.traceID,
				Duplicate: true, PublishedAt: orig.publishedAt,
			}, nil
		}
	}

	seq, seqErr := allocSeq(ctx, tx, stream)
	if seqErr != nil {
		return Ack{}, seqErr
	}
	msgID := newID().String()
	if o.IDOverride != "" {
		msgID = o.IDOverride
	}
	traceID := queue.ResolveTraceID(r.TraceID, "", rand.Reader)
	hdrRaw, hdrErr := queue.EncodeHeaders(r.Headers, limits)
	if hdrErr != nil {
		return Ack{}, hdrErr // typed by EncodeHeaders; unreachable after ValidatePublish
	}
	hdr := nullStr(hdrRaw)

	// published_at is monotone per stream (P4): a backwards wall-clock jump must not
	// corrupt the messages_age B-tree that seek/retention binary-search.
	var last sql.Null[int64]
	if err := tx.QueryRowContext(ctx,
		`SELECT max(published_at) FROM messages WHERE stream = ?`, stream).Scan(&last); err != nil {
		return Ack{}, fmt.Errorf("read last published_at of %q: %w", stream, err)
	}
	publishedAt := now
	skew := now - last.V
	if last.Valid && last.V > now {
		publishedAt = last.V
	}

	body := r.Body
	if body == nil {
		body = []byte{} // a 0-byte body is an empty BLOB, never SQL NULL (§9)
	}
	res, iErr := tx.ExecContext(ctx, `INSERT INTO messages
		(stream, seq, id, subject, hdr, body, size, published_at, trace_id, dedup_key)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT (stream, dedup_key) WHERE dedup_key IS NOT NULL DO NOTHING`,
		stream, seq, msgID, r.Subject, hdr, body, len(body), publishedAt, traceID, dedupKey)
	if iErr != nil {
		return Ack{}, fmt.Errorf("insert message into %q: %w", stream, iErr)
	}
	if affected, aErr := res.RowsAffected(); aErr == nil && affected == 0 {
		// Belt and braces (§3): the pre-check above should have caught this. If it ever
		// fires, the pre-check was wrong — refuse loudly rather than lie about a dup.
		return Ack{}, fmt.Errorf("store.publishTx: dedup pre-check missed an existing key for %q", stream)
	}

	if _, uErr := tx.ExecContext(ctx,
		`UPDATE stream_stats SET msgs = msgs + 1, bytes = bytes + ? WHERE stream = ?`,
		len(r.Body), stream); uErr != nil {
		return Ack{}, fmt.Errorf("bump stats of %q: %w", stream, uErr)
	}

	detail, jErr := json.Marshal(struct {
		Size    int    `json:"size"`
		Headers int    `json:"headers"`
		Dedup   bool   `json:"dedup"`
		SkewMS  *int64 `json:"skew_ms,omitempty"`
	}{Size: len(r.Body), Headers: len(r.Headers), Dedup: r.MsgID != "", SkewMS: skewPtr(skew, last)})
	if jErr != nil { // unreachable struct
		detail = []byte(`{}`)
	}
	if eErr := insertEvent(ctx, tx, event{
		ts:      now,
		name:    eventName,
		stream:  nullStr(stream),
		subject: nullStr(r.Subject),
		msgID:   nullStr(msgID),
		seq:     nullI64(seq),
		traceID: nullStr(traceID),
		detail:  nullStr(string(detail)),
	}); eErr != nil {
		return Ack{}, eErr
	}

	return Ack{
		Stream: stream, Seq: seq, ID: msgID, TraceID: traceID,
		Duplicate: false, PublishedAt: publishedAt,
	}, nil
}

func skewPtr(skew int64, last sql.Null[int64]) *int64 {
	if !last.Valid || skew >= 0 {
		return nil // no backwards jump, nothing to record
	}
	return &skew
}

// loadedConfig is the authoritative slice of the streams row a publish checks against.
type loadedConfig struct {
	cfg    queue.StreamConfig
	window int64 // dedup_window_ms
}

// loadStreamConfig reads the streams row inside the transaction; a missing row means
// the stream never existed or was deleted earlier in the same commit batch.
func loadStreamConfig(ctx context.Context, tx txLike, name string) (loadedConfig, error) {
	row := tx.QueryRowContext(ctx, `
		SELECT subjects, max_msg_size, dedup_window_ms FROM streams WHERE name = ?`, name)
	var subjectsJSON string
	var lc loadedConfig
	err := row.Scan(&subjectsJSON, &lc.cfg.MaxMsgSize, &lc.window)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		return lc, errs.E(errs.ErrNotFound, "store.publishTx",
			"stream %q does not exist", name)
	case err != nil:
		return lc, fmt.Errorf("read config of %q: %w", name, err)
	}
	lc.cfg.Name = name
	lc.cfg.Subjects = unmarshalSubjects(subjectsJSON)
	return lc, nil
}

// dupHit is what the dedup pre-check returns: the original's receipt fields.
type dupHit struct {
	seq         int64
	id          string
	subject     string
	traceID     string
	publishedAt int64
}

// findDedupHit runs the pre-check on the partial index. It sees rows written earlier
// in the same transaction, which covers duplicates inside one batch or commit window.
func findDedupHit(ctx context.Context, tx txLike, stream, key string) (dupHit, bool, error) {
	var h dupHit
	err := tx.QueryRowContext(ctx, `
		SELECT seq, id, subject, trace_id, published_at FROM messages
		 WHERE stream = ? AND dedup_key = ?`, stream, key,
	).Scan(&h.seq, &h.id, &h.subject, &h.traceID, &h.publishedAt)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		return h, false, nil
	case err != nil:
		return h, false, fmt.Errorf("dedup lookup %q/%q: %w", stream, key, err)
	}
	return h, true, nil
}

// allocSeq takes the next sequence number with a RETURNING update: post-update
// values come back, so next - n is the first of the n allocated sequences. It runs
// inside the command's savepoint, so a later rejection rolls the allocation back —
// a rejected publish leaves no gap (P1).
func allocSeq(ctx context.Context, tx txLike, stream string) (int64, error) {
	var first int64
	err := tx.QueryRowContext(ctx,
		`UPDATE stream_seq SET next = next + 1 WHERE stream = ? RETURNING next - 1`,
		stream).Scan(&first)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		return 0, errs.E(errs.ErrNotFound, "store.publishTx",
			"stream %q does not exist", stream)
	case err != nil:
		return 0, fmt.Errorf("allocate seq of %q: %w", stream, err)
	}
	return first, nil
}
