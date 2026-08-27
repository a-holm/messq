// SPDX-License-Identifier: Apache-2.0

package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/a-holm/messq/internal/errs"
	"github.com/a-holm/messq/internal/obs"
	"github.com/a-holm/messq/internal/queue"
)

// Seek (issue #15 §7, SEMANTICS S5.1 T10): the operator-controlled cursor move with
// T10's full fencing contract. The `to` grammar is queue.ParseStartPosition — THE
// parser creation-time start uses, one grammar, no divergence (G7). Seek ALWAYS drops
// the consumer's delivery rows and bumps generation; keep_pending would be a lie
// because the claim path deletes stale-generation rows anyway.
//
// Clamping is REPORTED, never silent: seq:0→1, sub-floor→first_seq, time-after-head
// and new→stream_seq.next, start→max(1, first_seq); resolveStartPosition owns those
// rules and the impact echoes before/after plus clamped.

const kindSeek CmdKind = "consumer.seek"

// SeekSpec is one seek request: stream, consumer and the parsed `to` position come
// from the API layer; here we validate only against live state.
type SeekSpec struct {
	To queue.StartPosition
}

// SeekImpact is the dry-run/real-run shared wire shape.
type SeekImpact struct {
	CursorBefore   int64    `json:"cursor_before,omitempty"`
	CursorAfter    int64    `json:"cursor_after,omitempty"`
	PendingDropped int64    `json:"pending_dropped,omitempty"`
	Messages       int64    `json:"messages,omitempty"` // backlog redelivered from the new cursor
	Clamped        bool     `json:"clamped,omitempty"`
	Warnings       []string `json:"warnings,omitempty"`
}

// SeekResult wraps the impact for both outcomes.
type SeekResult struct {
	Impact     SeekImpact `json:"impact"`
	Stream     string     `json:"stream"`
	Consumer   string     `json:"consumer"`
	Generation int64      `json:"generation"`
}

type seekCmd struct {
	stream   string
	consumer string
	to       queue.StartPosition
	dryRun   bool
	actor    string
}

func (c seekCmd) Kind() CmdKind { return kindSeek }
func (c seekCmd) Bytes() int    { return 0 }

type seekResult struct{ r SeekResult }

func (c seekCmd) Apply(ctx context.Context, tx *sql.Tx, _ time.Time) (Result, []obs.Event, error) {
	row := tx.QueryRowContext(ctx,
		`SELECT cursor_seq, generation FROM consumers WHERE stream = ? AND name = ?`,
		c.stream, c.consumer)
	var curBefore, genBefore int64
	if err := row.Scan(&curBefore, &genBefore); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, nil, maybeCmdErr(errs.E(errs.ErrNotFound, "store.Seek",
				"consumer %q/%q does not exist", c.stream, c.consumer))
		}
		return nil, nil, fmt.Errorf("seek %s/%s: read consumer: %w", c.stream, c.consumer, err)
	}

	target, rErr := resolveStartPosition(ctx, tx, c.stream, c.to)
	if rErr != nil {
		return nil, nil, maybeCmdErr(rErr)
	}

	var pending, inflight int64
	if err := tx.QueryRowContext(ctx,
		`SELECT count(*) FILTER (WHERE state = 0), count(*) FILTER (WHERE state = 1)
		   FROM deliveries WHERE stream = ? AND consumer = ?`,
		c.stream, c.consumer).Scan(&pending, &inflight); err != nil {
		return nil, nil, fmt.Errorf("seek %s/%s: count deliveries: %w", c.stream, c.consumer, err)
	}
	dropped := pending + inflight

	first, next, bErr := streamBounds(ctx, tx, c.stream)
	if bErr != nil {
		return nil, nil, bErr
	}
	var remaining sql.Null[int64]
	if sErr := tx.QueryRowContext(ctx,
		`SELECT count(*) FROM messages WHERE stream = ? AND seq >= ?`,
		c.stream, target).Scan(&remaining); sErr != nil && !errors.Is(sErr, sql.ErrNoRows) {
		return nil, nil, fmt.Errorf("seek %s/%s: read backlog: %w", c.stream, c.consumer, sErr)
	}

	// Clamp reporting (never silent): the resolver folds targets into
	// [first, next], so clamped == "the raw request maps to a different cursor".
	clamped := false
	switch c.to.Kind {
	case queue.StartSeq:
		clamped = c.to.Seq != target && target == clampCursor(c.to.Seq, first, next)
	case queue.StartTime:
		var seq sql.Null[int64]
		if sErr := tx.QueryRowContext(ctx,
			`SELECT seq FROM messages WHERE stream = ? AND published_at >= ?
			 ORDER BY published_at, seq LIMIT 1`, c.stream, c.to.Time).Scan(&seq); sErr != nil &&
			!errors.Is(sErr, sql.ErrNoRows) {
			return nil, nil, fmt.Errorf("seek %s/%s: read time anchor: %w",
				c.stream, c.consumer, sErr)
		}
		clamped = !seq.Valid // nothing at-or-after T folded to stream_seq.next
	case queue.StartFirst, queue.StartNew:
		// by definition exact resolutions, never clamped
	}

	impact := SeekImpact{
		CursorBefore:   curBefore,
		CursorAfter:    target,
		PendingDropped: dropped,
		Messages:       remaining.V,
		Clamped:        clamped,
	}
	if dropped > 0 {
		impact.Warnings = append(impact.Warnings, fmt.Sprintf(
			"%d outstanding ack token(s) will fence as stale on this consumer", dropped))
	}
	if impact.Messages > 0 {
		impact.Warnings = append(impact.Warnings, fmt.Sprintf(
			"%d message(s) will be redelivered from the moved cursor", impact.Messages))
	}

	if c.dryRun {
		return seekResult{r: SeekResult{
			Impact: impact, Stream: c.stream,
			Consumer: c.consumer,
		}}, nil, nil
	}

	del, dErr := tx.ExecContext(ctx,
		`DELETE FROM deliveries WHERE stream = ? AND consumer = ?`, c.stream, c.consumer)
	if dErr != nil {
		return nil, nil, fmt.Errorf("seek %s/%s: drop deliveries: %w", c.stream, c.consumer, dErr)
	}
	n, raErr := del.RowsAffected()
	if raErr != nil {
		return nil, nil, fmt.Errorf("seek %s/%s: drop receipt: %w", c.stream, c.consumer, raErr)
	}
	if n != dropped {
		return nil, nil, fmt.Errorf("seek %s/%s: dropped %d deliveries, plan said %d",
			c.stream, c.consumer, n, dropped)
	}
	if _, eErr := tx.ExecContext(ctx,
		`UPDATE consumers SET cursor_seq = ?, generation = generation + 1
		  WHERE stream = ? AND name = ?`, target, c.stream, c.consumer); eErr != nil {
		return nil, nil, fmt.Errorf("seek %s/%s: move cursor: %w", c.stream, c.consumer, eErr)
	}

	ev, evErr := commitEvent(ctx, tx, event{
		name:     "consumer.seek",
		stream:   nullStr(c.stream),
		consumer: nullStr(c.consumer),
		actor:    nullStr(c.actor),
		detail: nullStr(fmt.Sprintf(
			`{"from":%d,"to":%d,"dropped":%d,"clamped":%t}`,
			curBefore, target, dropped, impact.Clamped)),
	})
	if evErr != nil {
		return nil, nil, evErr
	}
	return seekResult{r: SeekResult{
		Impact: impact, Stream: c.stream, Consumer: c.consumer,
		Generation: genBefore + 1,
	}}, []obs.Event{ev}, nil
}

// Seek moves one consumer's cursor per T10. DryRun previews identically without writes.
func (s *Store) Seek(ctx context.Context, stream, consumer string, to queue.StartPosition, dryRun bool, actor string) (SeekResult, error) {
	if err := queue.ValidateExistingStreamName(stream); err != nil {
		return SeekResult{}, err
	}
	if err := queue.ValidateConsumerName(consumer); err != nil {
		return SeekResult{}, err
	}
	res, err := s.enqueue(ctx, "store.Seek",
		seekCmd{stream: stream, consumer: consumer, to: to, dryRun: dryRun, actor: actor})
	if err != nil {
		return SeekResult{}, err
	}
	sr, ok := res.(seekResult)
	if !ok {
		return SeekResult{}, fmt.Errorf(
			"store.Seek: engine returned %T, want seekResult", res)
	}
	return sr.r, nil
}
