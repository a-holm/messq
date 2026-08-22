// SPDX-License-Identifier: Apache-2.0

package store

import (
	"context"
	"database/sql"
	"fmt"
)

// The minimal event-row helper of issue #7 (D11): every state change commits its audit
// row in the same transaction, so the events table can never disagree with reality.
// #19 replaces this helper with its single choke point and adds fan-out; the event
// names it writes stay in PLAN §9.2's closed set either way.

// event is one audit row. Null fields store SQL NULL; the events table's columns are
// nullable by design — a stream.create has no msg_id, a publish no actor.
type event struct {
	ts      int64
	name    string // the §9.2 vocabulary entry
	stream  sql.Null[string]
	subject sql.Null[string]
	msgID   sql.Null[string]
	seq     sql.Null[int64]
	attempt sql.Null[int64]
	traceID sql.Null[string]
	actor   sql.Null[string]
	detail  sql.Null[string]
}

func nullStr(s string) sql.Null[string] { return sql.Null[string]{V: s, Valid: s != ""} }

func nullI64(v int64) sql.Null[int64] { return sql.Null[int64]{V: v, Valid: v != 0} }

// insertEvent writes one audit row inside the caller's transaction. It never fires on
// its own: an event without its state change is exactly the disagreement D11 forbids.
func insertEvent(ctx context.Context, tx txLike, e event) error {
	const q = `INSERT INTO events
		(ts, event, stream, consumer, subject, msg_id, seq, attempt, trace_id, actor, detail)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`
	if _, err := tx.ExecContext(ctx, q,
		e.ts, e.name, e.stream, nil, e.subject, e.msgID, e.seq, e.attempt, e.traceID, e.actor, e.detail,
	); err != nil {
		return fmt.Errorf("insert %s event row: %w", e.name, err)
	}
	return nil
}
