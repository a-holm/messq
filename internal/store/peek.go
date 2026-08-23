// SPDX-License-Identifier: Apache-2.0

package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/a-holm/messq/internal/errs"
	"github.com/a-holm/messq/internal/queue"
	"github.com/a-holm/messq/internal/subject"
)

// Peek reads (issue §6): side-effect free by construction — every query runs on the
// read pool, fenced read-only by query_only(1) (ADR-0002; mode=ro is reserved for
// offline inspection and accepts the -shm constraint), not on discipline. Reads
// join on stream existence, so rows stranded mid-delete are invisible. Peeking keeps
// working while the writer is latched read-only: reading the evidence must survive
// the failure.

// Message is the read shape of one stored message. Body rides only when the caller
// asked for it (single reads always; listings only with IncludeBody) so I11's no
// unbounded collections holds at the API layer too.
type Message struct {
	Stream      string            `json:"stream"`
	Seq         int64             `json:"seq"`
	ID          string            `json:"id"`
	Subject     string            `json:"subject"`
	Headers     map[string]string `json:"headers,omitempty"` // nil when hdr IS NULL
	Body        []byte            `json:"-"`
	Size        int64             `json:"size"`
	PublishedAt int64             `json:"published_at"` // unix ms
	TraceID     string            `json:"trace_id"`
}

// PeekMissError is a missing sequence that explains itself (issue §6): "message 5 is
// gone" and "message 5 does not exist yet" are different incidents. Boundary is
// first_seq for Reason "expired" and last_seq for "never_published".
type PeekMissError struct {
	Stream   string `json:"stream"`
	Reason   string `json:"reason"`
	Boundary int64  `json:"-"`
}

func (e *PeekMissError) Error() string {
	return fmt.Sprintf("stream %q seq %d: %s (boundary %d)", e.Stream, e.boundarySeq(), e.Reason, e.Boundary)
}
func (e *PeekMissError) Unwrap() error { return errs.ErrNotFound }

func (e *PeekMissError) boundarySeq() int64 { return e.Boundary }

const (
	reasonExpired        = "expired"
	reasonNeverPublished = "never_published"
)

// PeekSeq returns one stored message by its per-stream sequence number, body
// included. A seq below the stream's first_seq reports expired; a seq at or above
// stream_seq.next reports never_published; both are typed ErrNotFound.
func (s *Store) PeekSeq(ctx context.Context, stream string, seq int64) (Message, error) {
	if err := queue.ValidateExistingStreamName(stream); err != nil {
		return Message{}, err
	}
	ro := s.readPool()
	var first, next sql.Null[int64]
	if err := ro.QueryRowContext(ctx, `
		SELECT (SELECT min(seq) FROM messages WHERE stream = ?1),
		       coalesce((SELECT next FROM stream_seq WHERE stream = ?1), 0)`,
		stream).Scan(&first, &next); err != nil {
		return Message{}, fmt.Errorf("read bounds of %q: %w", stream, err)
	}
	if !first.Valid && !next.Valid {
		return Message{}, errs.E(errs.ErrNotFound, "store.PeekSeq",
			"stream %q does not exist", stream)
	}
	if first.Valid && seq < first.V {
		return Message{}, &PeekMissError{Stream: stream, Reason: reasonExpired, Boundary: first.V}
	}
	if seq >= next.V {
		return Message{}, &PeekMissError{
			Stream: stream,
			Reason: reasonNeverPublished, Boundary: next.V - 1,
		}
	}
	msg, err := scanMessage(ro.QueryRowContext(ctx, `
		SELECT stream, seq, id, subject, hdr, body, size, published_at, trace_id
		 FROM messages WHERE stream = ? AND seq = ?`, stream, seq))
	if errors.Is(err, sql.ErrNoRows) {
		return Message{}, errs.E(errs.ErrNotFound, "store.PeekSeq",
			"stream %q has no row for seq %d inside [%d, %d)", stream, seq, first.V, next.V)
	}
	return msg, err
}

// PeekID returns one stored message by its ULID through the messages_id index, body
// included. The join on streams makes rows of a deleted (mid-sweep or orphaned)
// stream invisible.
func (s *Store) PeekID(ctx context.Context, id string) (Message, error) {
	ro := s.readPool()
	msg, err := scanMessage(ro.QueryRowContext(ctx, `
		SELECT m.stream, m.seq, m.id, m.subject, m.hdr, m.body, m.size, m.published_at, m.trace_id
		 FROM messages m JOIN streams s ON s.name = m.stream
		 WHERE m.id = ?`, id))
	if errors.Is(err, sql.ErrNoRows) {
		return Message{}, errs.E(errs.ErrNotFound, "store.PeekID",
			"no live message with id %q", id)
	}
	return msg, err
}

// ListQuery selects one bounded page of a stream's messages. FromSeq is an inclusive
// lower bound for asc order and an inclusive upper bound for desc order; zero means
// the natural start (first row / head). Subject accepts literal subjects (served from
// the messages_subj index) and wildcard patterns (bounded primary-key scan filtered
// in Go).
type ListQuery struct {
	Stream      string
	FromSeq     int64
	Subject     string
	Limit       int
	Order       string // "asc" (default) | "desc"
	IncludeBody bool
}

// Page is one listing result. Limit echoes the EFFECTIVE limit after clamping —
// clamping silently is how people build wrong dashboards (issue §6). A wildcard scan
// that exhausted PeekScanLimit with an unfilled page reports Complete=false and
// ScannedToSeq as the resume point (--from-seq N+1).
type Page struct {
	Messages     []Message `json:"messages"`
	Complete     bool      `json:"complete"`
	ScannedToSeq int64     `json:"scanned_to_seq"`
	Limit        int       `json:"limit"`
}

// ListMessages reads one page of a stream's messages in seq order. Bodies ride only
// when IncludeBody is set, which also tightens the effective limit tenfold (§6).
func (s *Store) ListMessages(ctx context.Context, q ListQuery) (Page, error) {
	if err := queue.ValidateExistingStreamName(q.Stream); err != nil {
		return Page{}, err
	}
	effective := s.peekMaxLimit
	if q.IncludeBody {
		effective = s.peekMaxLimit / 10 // §6: 1000 → 100 with bodies
	}
	limit := q.Limit
	if limit <= 0 || limit > effective {
		limit = effective
	}
	desc := q.Order == "desc"

	var matcher subject.Set
	hasFilter := q.Subject != ""
	literalSubject := ""
	if hasFilter {
		set, pErr := subject.ParseSet([]string{q.Subject})
		if pErr != nil {
			return Page{}, errs.E(errs.ErrBadRequest, "store.ListMessages",
				"subject filter %q is not a valid pattern: %v", q.Subject, pErr)
		}
		matcher = set
		if pat, lErr := subject.ParsePattern(q.Subject); lErr == nil && pat.IsLiteral() {
			literalSubject = q.Subject // a literal filter hits the messages_subj index
		}
	}
	page := Page{Messages: make([]Message, 0, min(limit, 64)), Limit: limit}

	ro := s.readPool()
	literalArg := 2
	if q.FromSeq > 0 {
		literalArg = 3
	}
	//nolint:gosec // G202: every fragment is a compile-time constant from the helpers
	// below; caller data reaches SQLite only through bound ?n parameters, never text.
	query := `
		SELECT stream, seq, id, subject, hdr, size, published_at, trace_id` +
		bodyCol(q.IncludeBody) + `
		FROM messages WHERE stream = ?1` + fromClause(q.FromSeq, desc) +
		literalClause(literalSubject, literalArg) + orderBy(desc)
	args := []any{q.Stream}
	if q.FromSeq > 0 {
		args = append(args, q.FromSeq)
	}
	if literalSubject != "" {
		args = append(args, literalSubject)
	}
	rows, err := ro.QueryContext(ctx, query, args...)
	if err != nil {
		return Page{}, fmt.Errorf("list messages of %q: %w", q.Stream, err)
	}
	defer func() {
		if cerr := rows.Close(); cerr != nil {
			s.logger.Warn("store.ListMessages", "error", cerr.Error())
		}
	}()

	scanned := 0
	lastScanned := int64(0)
	boundReached := false
	for rows.Next() {
		if scanned == s.peekScanLimit {
			// The bound is on rows SCANNED, not matches: stop before consuming
			// another row. If the source ends here naturally, the loop below never
			// sets the flag and the page is honestly complete.
			boundReached = true
			break
		}
		scanned++
		var m Message
		dest := []any{
			&m.Stream, &m.Seq, &m.ID, &m.Subject, &hdrDest{m: &m},
			&m.Size, &m.PublishedAt, &m.TraceID,
		}
		if q.IncludeBody {
			dest = append(dest, &m.Body)
		}
		if sErr := rows.Scan(dest...); sErr != nil {
			return Page{}, fmt.Errorf("scan message of %q: %w", q.Stream, sErr)
		}
		lastScanned = m.Seq
		if hasFilter && !matcher.Match(m.Subject) {
			continue // wildcard pattern filtered in Go over the PK-range scan
		}
		page.Messages = append(page.Messages, m)
		if len(page.Messages) == limit {
			break
		}
	}
	if rErr := rows.Err(); rErr != nil {
		return Page{}, fmt.Errorf("iterate messages of %q: %w", q.Stream, rErr)
	}
	switch {
	case len(page.Messages) >= limit || !boundReached:
		page.Complete = true // page filled, or the whole source fit inside the bound
	default:
		// Scan bound hit with an unfilled page: say so and hand back the resume point.
		page.Complete = false
		page.ScannedToSeq = lastScanned
	}
	return page, nil
}

// bodyCol appends the body projection only when the caller asked for bodies.
func bodyCol(include bool) string {
	if include {
		return `, body`
	}
	return ``
}

// fromClause turns FromSeq into SQL: inclusive lower bound ascending, inclusive upper
// bound descending; zero means the natural start either way.
func fromClause(from int64, desc bool) string {
	switch {
	case from <= 0:
		return ``
	case desc:
		return ` AND seq <= ?2`
	default:
		return ` AND seq >= ?2`
	}
}

// literalClause pins an exact subject so SQLite serves the page from messages_subj
// instead of scanning the stream's primary key. The placeholder number depends on
// whether the FromSeq bound claimed ?2.
func literalClause(subject string, n int) string {
	if subject == "" {
		return ``
	}
	return fmt.Sprintf(` AND subject = ?%d`, n)
}

func orderBy(desc bool) string {
	if desc {
		return ` ORDER BY seq DESC`
	}
	return ` ORDER BY seq`
}

// hdrDest scans the nullable hdr column into Message.Headers, treating NULL as the
// empty map and malformed JSON as corruption the read path reports.
type hdrDest struct{ m *Message }

func (h hdrDest) Scan(src any) error {
	raw, ok := src.(string)
	if !ok {
		return nil // SQL NULL: no user headers, the common case
	}
	var out map[string]string
	if err := json.Unmarshal([]byte(raw), &out); err != nil {
		return fmt.Errorf("stored hdr is not header JSON: %w", err)
	}
	h.m.Headers = out
	return nil
}

// scanMessage materialises one full message (body included) from any row source.
func scanMessage(row *sql.Row) (Message, error) {
	var m Message
	var hdr sql.Null[string]
	err := row.Scan(&m.Stream, &m.Seq, &m.ID, &m.Subject, &hdr, &m.Body, &m.Size,
		&m.PublishedAt, &m.TraceID)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		return m, err
	case err != nil:
		return Message{}, fmt.Errorf("scan message row: %w", err)
	}
	if hdr.Valid {
		var out map[string]string
		if jErr := json.Unmarshal([]byte(hdr.V), &out); jErr != nil {
			return Message{}, fmt.Errorf("stored hdr is not header JSON: %w", jErr)
		}
		m.Headers = out
	}
	return m, nil
}
