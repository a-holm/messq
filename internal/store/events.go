// SPDX-License-Identifier: Apache-2.0

package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/a-holm/messq/internal/errs"
	"github.com/a-holm/messq/internal/obs"
)

// The minimal event-row helper of issue #7 (D11): every state change commits its audit
// row in the same transaction, so the events table can never disagree with reality.
// #19 replaces this helper with its single choke point and adds fan-out; the event
// names it writes stay in PLAN §9.2's closed set either way.

// event is one audit row. Null fields store SQL NULL; the events table's columns are
// nullable by design — a stream.create has no msg_id, a publish no actor.
type event struct {
	ts       int64
	name     string // the §9.2 vocabulary entry
	stream   sql.Null[string]
	consumer sql.Null[string]
	subject  sql.Null[string]
	msgID    sql.Null[string]
	seq      sql.Null[int64]
	attempt  sql.Null[int64]
	traceID  sql.Null[string]
	actor    sql.Null[string]
	detail   sql.Null[string]
}

func nullStr(s string) sql.Null[string] { return sql.Null[string]{V: s, Valid: s != ""} }

func nullI64(v int64) sql.Null[int64] { return sql.Null[int64]{V: v, Valid: v != 0} }

// jsonMarshal serialises an audit-row detail payload. Callers treat failure as
// unreachable for their fixed-shape maps and fall back to a bare `{}`.
func jsonMarshal(v any) ([]byte, error) { return json.Marshal(v) }

// insertEvent writes one audit row inside the caller's transaction. It never fires on
// its own: an event without its state change is exactly the disagreement D11 forbids.
func insertEvent(ctx context.Context, tx *sql.Tx, e event) error {
	const q = `INSERT INTO events
		(ts, event, stream, consumer, subject, msg_id, seq, attempt, trace_id, actor, detail)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`
	if _, err := tx.ExecContext(ctx, q,
		e.ts, e.name, e.stream, e.consumer, e.subject, e.msgID, e.seq, e.attempt, e.traceID, e.actor, e.detail,
	); err != nil {
		return fmt.Errorf("insert %s event row: %w", e.name, err)
	}
	return nil
}

// commitEvent writes one audit row inside the caller's transaction AND returns the
// carrier [obs.Event] for the engine's post-commit fan-out: the row is the source of
// truth (D11), the carrier only projects it after COMMIT. One helper keeps the pair
// inseparable — an Apply that wrote the row but dropped the carrier, or invented a
// carrier without a row, would let the events table and the projections disagree.
func commitEvent(ctx context.Context, tx *sql.Tx, e event) (obs.Event, error) {
	if err := insertEvent(ctx, tx, e); err != nil {
		return obs.Event{}, err
	}
	carrier := obs.Event{
		Event:    e.name,
		TS:       e.ts,
		Stream:   e.stream.V,
		Consumer: e.consumer.V,
		Subject:  e.subject.V,
		MsgID:    e.msgID.V,
		TraceID:  e.traceID.V,
		Actor:    e.actor.V,
		Seq:      e.seq.V,
		Attempt:  e.attempt.V,
	}
	if e.detail.Valid && e.detail.V != "" {
		var d map[string]any
		if err := json.Unmarshal([]byte(e.detail.V), &d); err == nil {
			carrier.Detail = d
		} // unparseable detail stays a table-only event; the row is authoritative
	}
	return carrier, nil
}

// ---- The read layer (issue #20 slice 1). --------------------------------------------
//
// Every query runs on the fenced read pool — reading is free (G2): no query here may
// enqueue writer work or slow a follower. Each query is anchored on one of the three
// §4.2 indexes when an anchor field is given (Decision 1); the residual predicates are
// matched in Go over the bounded scan, exactly like ListMessages' wildcard listings,
// so scanned-row accounting stays honest and the scan budget of slice 2 can bound it.

// Order is the row ordering of an [Store.Events] page. The zero value is [OrderAsc].
type Order uint8

const (
	// OrderAsc walks the journal oldest-first (events.id ascending) — the default.
	OrderAsc Order = iota
	// OrderDesc walks newest-first.
	OrderDesc
)

// EventFilter selects one page of the events journal. The id/time fields are anchors:
// MsgID and TraceID hit their dedicated §4.2 indexes; Since/Until anchor on events_ts
// when no id anchor is given. Stream/Consumer/Events are residual predicates evaluated
// in Go over the bounded scan. AfterID is an EXCLUSIVE cursor in scan direction — the
// resume point handed back as [EventPage.NextAfterID], and later the follow handoff.
type EventFilter struct {
	MsgID      string   // anchor: events_msg(msg_id, id)
	TraceID    string   // anchor: events_trace(trace_id, id)
	Stream     string   // residual: exact stream name
	Consumer   string   // residual: exact consumer name
	Events     []string // residual: exact ("msg.dead") or one-level glob ("msg.*")
	Since      int64    // unix ms, inclusive; 0 = unbounded
	Until      int64    // unix ms, exclusive; 0 = unbounded
	AfterID    int64    // exclusive cursor in scan direction; 0 = start
	Limit      int      // clamped to Options.EventQueryMaxLimit; <= 0 means the ceiling
	ScanBudget int      // max rows examined; clamped to Options.EventScanBudget; <= 0 means it
	Order      Order    // [OrderAsc] (default) | [OrderDesc]
}

// EventPage is one honest answer about the journal. Complete is false only when the
// page stopped before exhausting the filter range — page filled or (from slice 2) the
// scan budget ran out — and NextAfterID then carries the resume cursor; ScannedToID
// always reports how far the scan actually got. HorizonTS is the ts of the oldest
// RETAINED event row (0 when the journal is empty): a caller rendering a story that
// starts at HorizonTS must say "history before T was trimmed" instead of absorbing
// the gap (§4.5).
type EventPage struct {
	Events      []obs.Event `json:"events"`
	Complete    bool        `json:"complete"`
	ScannedToID int64       `json:"scanned_to_id"`
	NextAfterID int64       `json:"next_after_id,omitempty"`
	HorizonTS   int64       `json:"horizon_ts,omitempty"`
}

// Events reads one page of the journal through the read pool. It never blocks on the
// writer and never enqueues work: a stalled reader costs the pool one connection and
// nothing else. The SQL carries LIMIT limit+budget as a hard cap so even the
// time-range class's bounded sorter cannot balloon: rows consumed can never exceed
// what the fill/budget checks stop at.
func (s *Store) Events(ctx context.Context, f EventFilter) (EventPage, error) {
	limit := f.Limit
	if limit <= 0 || limit > s.eventQueryMaxLimit {
		limit = s.eventQueryMaxLimit
	}
	budget := f.ScanBudget
	if budget <= 0 || budget > s.eventScanBudget {
		// A filter may lower the budget below --event-scan-budget, never raise it:
		// the flag is the process-wide I11 ceiling.
		budget = s.eventScanBudget
	}
	horizon, err := s.EventHorizon(ctx)
	if err != nil {
		return EventPage{}, err
	}

	q, args := buildEventQuery(f, limit+budget)
	ro := s.readPool()
	rows, qErr := ro.QueryContext(ctx, q, args...)
	if qErr != nil {
		return EventPage{}, fmt.Errorf("query events: %w", qErr)
	}
	defer func() {
		if cerr := rows.Close(); cerr != nil {
			s.logger.Warn("store.Events", "error", cerr.Error())
		}
	}()

	page := EventPage{
		Events:    make([]obs.Event, 0, min(limit, 64)),
		HorizonTS: horizon,
	}
	filled := false
	boundReached := false
	scanned := 0
	for rows.Next() {
		if scanned == budget {
			// The bound is on rows EXAMINED, not matches: stop before consuming
			// another row (ListMessages' discipline). If the source ends here
			// naturally, rows.Next() ends the loop first and boundReached never
			// fires, so the page stays honestly complete.
			boundReached = true
			break
		}
		scanned++
		e, rowID, scanErr := scanEventRow(rows)
		if scanErr != nil {
			return EventPage{}, fmt.Errorf("scan event row: %w", scanErr)
		}
		page.ScannedToID = rowID
		if !f.matches(e) {
			continue
		}
		page.Events = append(page.Events, e)
		if len(page.Events) == limit {
			filled = true
			break
		}
	}
	if rErr := rows.Err(); rErr != nil {
		return EventPage{}, fmt.Errorf("iterate events: %w", rErr)
	}
	switch {
	case filled || boundReached:
		page.NextAfterID = page.ScannedToID // resume AFTER the last examined row
	default:
		page.Complete = true // the whole filter range fit inside this page
	}
	return page, nil
}

// buildEventQuery renders one Events page query: anchors and cursor in SQL, ORDER BY
// id in scan direction, and LIMIT maxRows as the hard consumption cap (limit+budget).
// Extracted so the EXPLAIN QUERY PLAN audit tests the SHIPPED SQL, not a look-alike.
func buildEventQuery(f EventFilter, maxRows int) (string, []any) {
	var clauses []string
	args := []any{}
	if f.MsgID != "" {
		clauses = append(clauses, "msg_id = ?")
		args = append(args, f.MsgID)
	}
	if f.TraceID != "" {
		clauses = append(clauses, "trace_id = ?")
		args = append(args, f.TraceID)
	}
	if f.Since > 0 {
		clauses = append(clauses, "ts >= ?")
		args = append(args, f.Since)
	}
	if f.Until > 0 {
		clauses = append(clauses, "ts < ?")
		args = append(args, f.Until)
	}
	if f.AfterID > 0 {
		if f.Order == OrderDesc {
			clauses = append(clauses, "id < ?")
		} else {
			clauses = append(clauses, "id > ?")
		}
		args = append(args, f.AfterID)
	}
	where := ""
	if len(clauses) > 0 {
		where = " WHERE " + strings.Join(clauses, " AND ")
	}
	dir := "ASC"
	if f.Order == OrderDesc {
		dir = "DESC"
	}
	q := `SELECT id, ts, event, stream, consumer, subject, msg_id, seq, attempt,
		trace_id, actor, detail FROM events` + where +
		` ORDER BY id ` + dir + ` LIMIT ?`
	args = append(args, maxRows)
	return q, args
}

// matches applies the residual predicates against one carrier. Kept next to the SQL
// so the split between anchors (SQL) and residuals (Go) is visible in one place.
func (f EventFilter) matches(e obs.Event) bool {
	if f.Stream != "" && e.Stream != f.Stream {
		return false
	}
	if f.Consumer != "" && e.Consumer != f.Consumer {
		return false
	}
	for _, pattern := range f.Events {
		if eventPatternMatch(pattern, e.Event) {
			return true
		}
	}
	return len(f.Events) == 0
}

// eventPatternMatch reports whether name satisfies one Events[] term: an exact
// vocabulary entry, or a one-level glob "area.*". The closed §9.2 vocabulary is
// strictly two-segment ("<area>.<name>"), so the glob is a prefix match over "area.".
func eventPatternMatch(pattern, name string) bool {
	if strings.HasSuffix(pattern, ".*") {
		return strings.HasPrefix(name, pattern[:len(pattern)-1])
	}
	return pattern == name
}

// scanEventRow scans one journal row into its carrier plus rowid.
func scanEventRow(rows *sql.Rows) (obs.Event, int64, error) {
	var (
		rowID     int64
		e         obs.Event
		streamN   sql.Null[string]
		consumerN sql.Null[string]
		subjectN  sql.Null[string]
		msgN      sql.Null[string]
		traceN    sql.Null[string]
		actorN    sql.Null[string]
		detailN   sql.Null[string]
		seqN      sql.Null[int64]
		attemptN  sql.Null[int64]
	)
	if err := rows.Scan(&rowID, &e.TS, &e.Event, &streamN, &consumerN, &subjectN,
		&msgN, &seqN, &attemptN, &traceN, &actorN, &detailN); err != nil {
		return obs.Event{}, 0, err
	}
	e.Stream, e.Consumer, e.Subject = streamN.V, consumerN.V, subjectN.V
	e.MsgID, e.TraceID, e.Actor = msgN.V, traceN.V, actorN.V
	e.Seq, e.Attempt = seqN.V, attemptN.V
	if detailN.Valid && detailN.V != "" {
		var d map[string]any
		if err := json.Unmarshal([]byte(detailN.V), &d); err == nil {
			e.Detail = d
		} // unparseable detail stays a table-only event, as in commitEvent
	}
	return e, rowID, nil
}

// MaxEventID returns the highest committed event id (the follow handoff point: a
// subscriber starts from here), or 0 on an empty journal.
func (s *Store) MaxEventID(ctx context.Context) (int64, error) {
	var maxID int64
	if err := s.readPool().QueryRowContext(ctx,
		`SELECT coalesce(max(id), 0) FROM events`).Scan(&maxID); err != nil {
		return 0, fmt.Errorf("max event id: %w", err)
	}
	return maxID, nil
}

// EventHorizon returns the ts of the oldest retained event row — the trim horizon
// --event-retention/--event-max-rows produce (§4.5) — or 0 on an empty journal. It is
// an O(1) descent to the leftmost events_ts entry, safe to call per page.
func (s *Store) EventHorizon(ctx context.Context) (int64, error) {
	var horizon int64
	if err := s.readPool().QueryRowContext(ctx,
		`SELECT coalesce(min(ts), 0) FROM events`).Scan(&horizon); err != nil {
		return 0, fmt.Errorf("event horizon: %w", err)
	}
	return horizon, nil
}

// ParseSince parses a `since` specification into unix milliseconds against an explicit
// now (callers pass Clock-derived time; the parser itself never reads a wall clock).
// Accepted forms: RFC3339 instants, bare unix-ms integers, and signed relative
// durations ("-15m", "+90s"). Empty means unset. Unsigned durations are rejected so
// "15m" cannot silently mean different things across callers.
func ParseSince(spec string, nowMS int64) (int64, error) {
	spec = strings.TrimSpace(spec)
	if spec == "" {
		return 0, nil
	}
	if spec[0] == '-' || spec[0] == '+' {
		d, perr := time.ParseDuration(spec)
		if perr != nil {
			return 0, errs.E(errs.ErrBadRequest, "store.ParseSince",
				"%q is not a relative duration like -15m", spec)
		}
		return nowMS + d.Milliseconds(), nil
	}
	if t, perr := time.Parse(time.RFC3339, spec); perr == nil {
		return t.UnixMilli(), nil
	}
	if ms, perr := strconv.ParseInt(spec, 10, 64); perr == nil {
		return ms, nil
	}
	return 0, errs.E(errs.ErrBadRequest, "store.ParseSince",
		"%q is not RFC3339, unix-ms, or a -duration", spec)
}
