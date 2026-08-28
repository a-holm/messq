// SPDX-License-Identifier: Apache-2.0

package store

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/a-holm/messq/internal/queue"
)

// The pending listing (issue #15 §9). READ POOL ONLY: inspection is free — it never
// enqueues writer work and never blocks the writer under WAL (§3.2). Ordered by
// visible_at ascending so the first row is the next thing to go wrong; `after` is a
// seq cursor; the limit arrives already clamped by the API layer which echoes the
// effective value.

// PendingItem is one outstanding delivery with its derived ack token for INFLIGHT rows.
type PendingItem struct {
	Seq        int64   `json:"seq"`
	State      string  `json:"state"` // "ready" | "inflight"
	Attempts   int64   `json:"attempt"`
	MaxDeliver int32   `json:"max_deliver"`
	VisibleAt  int64   `json:"deadline"` // claim-time deadline for inflight; ready-at otherwise
	AckToken   *string `json:"ack_token,omitempty"`
}

// PendingList is one bounded page of the outstanding set.
type PendingList struct {
	Stream    string        `json:"stream"`
	Consumer  string        `json:"consumer"`
	Limit     int           `json:"limit"`
	NextAfter *int64        `json:"next_after,omitempty"`
	Items     []PendingItem `json:"items"`
}

// PendingQuery bounds and filters the read.
type PendingQuery struct {
	Limit       int    // >0 only; caller clamps to --pending-max-limit
	After       int64  // exclusive seq cursor
	OlderThanMS int64  // keep rows older than this age relative to... none: absolute visible_at <= now-old (computed by caller clock)
	State       string // "" | "ready" | "inflight"
}

const pendingItemCols = `d.seq, d.state, d.attempts, c.max_deliver, d.visible_at, d.generation`

func (s *Store) PendingList(ctx context.Context, stream, consumer string, q PendingQuery) (PendingList, error) {
	limit := q.Limit
	if limit < 0 {
		return PendingList{}, fmt.Errorf("pending %s/%s: negative limit", stream, consumer)
	}
	if limit == 0 {
		limit = -1 // no LIMIT clause; the clamp echo still names the effective cap
	}

	stateFilter := ""
	switch q.State {
	case "":
	case "ready":
		stateFilter = " AND d.state = 0"
	case "inflight":
		stateFilter = " AND d.state = 1"
	default:
		return PendingList{}, fmt.Errorf("pending %s/%s: unknown state filter %q",
			stream, consumer, q.State)
	}

	timeFilter := ""
	var args []any
	args = append(args, stream, consumer)
	if q.After > 0 {
		timeFilter += " AND d.seq > ?"
		args = append(args, q.After)
	}
	if q.OlderThanMS > 0 {
		timeFilter += " AND d.visible_at <= ?"
		args = append(args, q.OlderThanMS)
	}
	args = append(args, limit)

	ro := s.readPool()
	rows, err := ro.QueryContext(ctx, `
		SELECT `+pendingItemCols+`
		  FROM deliveries d JOIN consumers c ON c.stream = d.stream AND c.name = d.consumer
		 WHERE d.stream = ? AND d.consumer = ?`+stateFilter+timeFilter+`
		 ORDER BY d.visible_at ASC, d.seq ASC LIMIT ?`, args...)
	if err != nil {
		return PendingList{}, fmt.Errorf("pending %s/%s: %w", stream, consumer, err)
	}
	defer func() {
		if cerr := rows.Close(); cerr != nil && s.logger != nil {
			s.logger.Warn("pending: close scan", "err", cerr)
		}
	}()

	out := PendingList{
		Stream: stream, Consumer: consumer,
		Limit: maxInt(q.Limit, limit), Items: []PendingItem{},
	}
	for rows.Next() {
		var it PendingItem
		var state int
		var gen sql.Null[int32]
		if scanErr := rows.Scan(&it.Seq, &state, &it.Attempts, &it.MaxDeliver,
			&it.VisibleAt, &gen); scanErr != nil {
			return PendingList{}, fmt.Errorf("pending %s/%s: scan: %w", stream, consumer, scanErr)
		}
		it.State = "ready"
		if state == 1 {
			it.State = "inflight"
			tok := queueToken(stream, consumer, it.Seq, it.Attempts, gen.V)
			it.AckToken = &tok
		}
		out.Items = append(out.Items, it)
	}
	if rErr := rows.Err(); rErr != nil {
		return PendingList{}, fmt.Errorf("pending %s/%s: iterate: %w", stream, consumer, rErr)
	}
	if n := len(out.Items); n == limit && limit > 0 && n > 0 {
		last := out.Items[n-1].Seq
		out.NextAfter = &last
	}
	return out, nil
}

func queueToken(stream, consumer string, seq, attempts int64, generation int32) string {
	// The derivation rides queue.Token itself: ParseToken(tok.String()) == tok is the
	// no-forgery property the fuzzer owns, and minting on a read path mutates nothing
	// (D7: tokens are pure functions of their five fields).
	return queue.Token{
		Stream: stream, Consumer: consumer, Seq: seq,
		Attempt:    int32(attempts), //nolint:gosec // G115: deliveries.attempts is bounded by max_deliver (int32 domain).
		Generation: generation,
	}.String()
}

func maxInt(a, b int) int {
	if a > b {
		return a
	}
	return b
}
