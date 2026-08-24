// SPDX-License-Identifier: Apache-2.0

package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/a-holm/messq/internal/id"
	"github.com/a-holm/messq/internal/obs"
	"github.com/a-holm/messq/internal/queue"
)

// The store half of the dead path (issue #12): the unified DLQ sink that fills the
// TODO(#12) seam #10/#11 left. It runs INSIDE the caller's already-open transaction and
// implements the whole dead-letter in SQLite: auto-create <origin>.dlq from the template,
// copy the payload under the original subject with a NEW id + preserved trace_id, and
// return the single msg.dead event — the caller owns the delivery-row delete, all inside
// the same commit (D1: "partial dead-lettering does not exist"). The body is never
// materialised in Go: the copy is an INSERT…SELECT inside SQLite.

// DeadOutcome is the closed set of msg.dead detail.dlq values (SEMANTICS §9.2).
type DeadOutcome string

// The dead outcomes (not_implemented is removed by this issue).
const (
	DeadOutcomeWritten       DeadOutcome = "written"
	DeadOutcomeDropped       DeadOutcome = "dropped"
	DeadOutcomeOriginMissing DeadOutcome = "origin_missing"
)

// maxDLQHeaderBytes is the copy's hdr cap (2x the user cap): the origin may legitimately
// be at the 4 KiB user budget and provenance must still fit (PLAN §5.1).
func (s DLQSink) maxDLQHeaderBytes() int { return s.limits.MaxHeaderBytes * 2 }

// DLQ metric-name constants (issue #12 §11), handed to #21 which registers the
// collectors. Labelled with the ORIGIN stream (+consumer where defined), never a DLQ
// stream or a message id (cardinality rule, D11). Interim constants land here so early
// counters and #21's golden do not diverge.
const (
	// MessqDLQWrittenTotal counts copies written, labelled {stream=origin}.
	MessqDLQWrittenTotal = "messq_dlq_written_total"
	// MessqDLQBytesTotal counts payload bytes copied, labelled {stream=origin}.
	MessqDLQBytesTotal = "messq_dlq_bytes_total"
	// MessqDLQDeferredTotal counts copies deferred past their transaction by the budget,
	// labelled {stream=origin}.
	MessqDLQDeferredTotal = "messq_dlq_deferred_total"
	// MessqDeadOrphanTotal counts origin_missing deaths, labelled {stream,consumer}; a
	// nonzero value is a bug signal (#21 alerts on it).
	MessqDeadOrphanTotal = "messq_dead_orphan_total"
	// MessqDLQDepth is a gauge #21 computes at scrape time from count(messages) of each
	// <stream>.dlq, labelled {stream=origin}.
	MessqDLQDepth = "messq_dlq_depth"
)

// DLQSink implements DeadSink. It is stateless across calls except for the per-transaction
// copy budget the Store hands a fresh instance (so one transaction's copies stay bounded,
// §7). newID mints the copy's fresh ULID (C1); dlq carries the process template + budget.
type DLQSink struct {
	limits queue.Limits
	dlq    queue.DLQConfig
	newID  func() id.MsgID
	budget *queue.DeadBudget
}

// newDeadSink builds the store's default dead sink — a DLQSink with a FRESH per-command
// budget, so each batch transaction is bounded independently.
func (s *Store) newDeadSink() DeadSink {
	return DLQSink{
		limits: s.limits,
		dlq:    s.dlq,
		newID:  s.newID,
		budget: queue.NewDeadBudget(s.dlq),
	}
}

// Dead runs the dead-letter inside the caller's transaction and returns the msg.dead
// event the caller appends to its reply. It never commits; the delivery-row delete is
// the caller's, in the same transaction (D1).
func (s DLQSink) Dead(ctx context.Context, tx *sql.Tx, d queue.DeadCtx, now time.Time) (obs.Event, error) {
	nowMS := now.UnixMilli()
	policy, err := s.loadDeadPolicy(ctx, tx, d.Stream, d.Consumer)
	if err != nil {
		return obs.Event{}, err
	}
	d.Policy = policy
	plan := queue.PlanDead(d, s.dlq)
	if !plan.Copy {
		return s.deadEvent(ctx, tx, d, nowMS, DeadOutcomeDropped, "", 0, 0, false, false)
	}

	// Read the origin row first: its user headers feed the merge and its size feeds the
	// budget; an absent origin is the origin_missing outcome BEFORE any DLQ is created
	// (nothing to copy — never create an empty .dlq stream).
	var originHdr sql.Null[string]
	var size, publishedAt int64
	err = tx.QueryRowContext(ctx,
		`SELECT hdr, size, published_at FROM messages WHERE stream = ? AND seq = ?`,
		d.Stream, d.Seq).Scan(&originHdr, &size, &publishedAt)
	if errors.Is(err, sql.ErrNoRows) {
		return s.deadEvent(ctx, tx, d, nowMS, DeadOutcomeOriginMissing, "", 0, 0, false, false)
	}
	if err != nil {
		return obs.Event{}, fmt.Errorf("dead-letter read origin %q/%d: %w", d.Stream, d.Seq, err)
	}

	// Budget: decrement-before-write. A crossed budget refuses WITHOUT writing and the
	// caller defers the row to a later transaction (ErrDeadBudget).
	if s.budget != nil && !s.budget.CanCopy(size) {
		return obs.Event{}, queue.ErrDeadBudget
	}

	dlq := plan.DLQStream
	if eErr := s.ensureDLQStream(ctx, tx, dlq, d.Stream, nowMS); eErr != nil {
		return obs.Event{}, eErr
	}
	seq, err := allocSeq(ctx, tx, dlq)
	if err != nil {
		return obs.Event{}, err
	}

	// Provenance headers merged over the origin's user headers.
	prov, reasonTrunc := queue.ProvenanceHeaders(d, publishedAt, nowMS, s.dlq.ReasonHeaderBytes)
	hdrRaw, trimmed, err := s.mergeCopyHeaders(originHdr, prov)
	if err != nil {
		return obs.Event{}, err
	}

	// The copy's published_at is the writer's now, clamped monotone per stream (P4) so a
	// backwards wall-clock jump cannot corrupt the messages_age B-tree of the DLQ stream.
	publishedAtCopy, cErr := clampPublishedAt(ctx, tx, dlq, nowMS)
	if cErr != nil {
		return obs.Event{}, cErr
	}

	newMsgID := s.newID().String()
	res, err := tx.ExecContext(ctx, `
		INSERT INTO messages
			(stream, seq, id, subject, hdr, body, size, published_at, trace_id, dedup_key)
		SELECT ?, ?, ?, m.subject, ?, m.body, m.size, ?, m.trace_id, NULL
		  FROM messages m WHERE m.stream = ? AND m.seq = ?`,
		dlq, seq, newMsgID, hdrRaw, publishedAtCopy, d.Stream, d.Seq)
	if err != nil {
		return obs.Event{}, fmt.Errorf("dead-letter copy into %q: %w", dlq, err)
	}
	n, aErr := res.RowsAffected()
	if aErr != nil {
		return obs.Event{}, fmt.Errorf("dead-letter copy rows-affected %q: %w", dlq, aErr)
	}
	if n != 1 {
		// The origin vanished between our SELECT and the copy — a racing purge behind
		// retention's back. Retire the delivery row anyway (never a stuck consumer),
		// record origin_missing, and do NOT leave a DLQ copy behind.
		return s.deadEvent(ctx, tx, d, nowMS, DeadOutcomeOriginMissing, "", 0, 0, false, false)
	}
	if s.budget != nil {
		s.budget.Take(size)
	}
	return s.deadEvent(ctx, tx, d, nowMS, DeadOutcomeWritten, dlq, seq, size, trimmed, reasonTrunc)
}

// clampPublishedAt clamps a writer-now timestamp monotone per stream (P4): it never goes
// backwards from the stream's last published_at.
func clampPublishedAt(ctx context.Context, tx *sql.Tx, stream string, nowMS int64) (int64, error) {
	var last sql.Null[int64]
	if err := tx.QueryRowContext(ctx,
		`SELECT max(published_at) FROM messages WHERE stream = ?`, stream).Scan(&last); err != nil {
		return 0, fmt.Errorf("read last published_at of %q: %w", stream, err)
	}
	if last.Valid && last.V > nowMS {
		return last.V, nil
	}
	return nowMS, nil
}

// loadDeadPolicy reads the authoritative dead_policy from the consumers row. A missing
// consumer is treated as drop (nothing to copy into); #9 forces drop on .dlq consumers.
func (s DLQSink) loadDeadPolicy(ctx context.Context, tx *sql.Tx, stream, consumer string) (queue.DeadPolicy, error) {
	var raw string
	err := tx.QueryRowContext(ctx,
		`SELECT dead_policy FROM consumers WHERE stream = ? AND name = ?`, stream, consumer).Scan(&raw)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		return queue.DeadPolicyDrop, nil
	case err != nil:
		return "", fmt.Errorf("dead-letter read policy %q/%q: %w", stream, consumer, err)
	}
	return queue.DeadPolicy(raw), nil
}

// dlqTemplateSubjects — a DLQ must accept whatever the origin accepted, forever.
const dlqTemplateSubjects = `[">"]`

// ensureDLQStream lazily auto-creates <origin>.dlq from the template inside the caller's
// transaction. Idempotent (ON CONFLICT DO NOTHING — the second death does not recreate
// it) and race-free under the single-writer group commit. Emits the ordinary
// stream.create event with actor="system" and detail.reason="dead_letter" (no new event
// name, S2.4).
func (s DLQSink) ensureDLQStream(ctx context.Context, tx *sql.Tx, dlq, origin string, nowMS int64) error {
	res, err := tx.ExecContext(ctx, `
		INSERT INTO streams
			(name, subjects, retention, max_msgs, max_bytes, max_age_ms, max_msg_size,
			 discard, dedup_window_ms, created_at)
		VALUES (?, ?, 'limits', ?, ?, ?, ?, 'old', 0, ?)
		ON CONFLICT (name) DO NOTHING`,
		dlq, dlqTemplateSubjects, s.dlq.MaxMsgs, s.dlq.MaxBytes,
		s.dlq.MaxAge.Milliseconds(), s.dlq.MaxMsgSize, nowMS)
	if err != nil {
		return fmt.Errorf("dead-letter auto-create %q: %w", dlq, err)
	}
	created, aErr := res.RowsAffected()
	if aErr != nil {
		return fmt.Errorf("dead-letter auto-create rows-affected %q: %w", dlq, aErr)
	}
	if _, err := tx.ExecContext(ctx,
		`INSERT INTO stream_seq (stream, next) VALUES (?, 1) ON CONFLICT (stream) DO NOTHING`,
		dlq); err != nil {
		return fmt.Errorf("dead-letter seed seq of %q: %w", dlq, err)
	}
	if created == 0 {
		return nil // the DLQ already existed (second death, or operator-created); no create event
	}
	// The stream.create event co-commits ONLY on the first creation (actor=system,
	// reason=dead_letter). Written via insertEvent: the carrier rides the caller's
	// reply only for msg.dead.
	if err := insertEvent(ctx, tx, event{
		ts: nowMS, name: "stream.create", stream: nullStr(dlq), actor: nullStr("system"),
		detail: nullStr(fmt.Sprintf(`{"reason":"dead_letter","origin":%q}`, origin)),
	}); err != nil {
		return err
	}
	return nil
}

// mergeCopyHeaders merges the provenance headers over the origin's user headers and
// renders the copy's hdr JSON. If the merged set exceeds the 2x header budget, it drops
// the proposal headers in the frozen degradation order (last-reason, then
// origin-published-at) and reports trimmed=true — a dead-letter is never failed over a
// header. Returns canonical JSON ("" encodes as SQL NULL).
func (s DLQSink) mergeCopyHeaders(originHdr sql.Null[string], prov map[string]string) (hdrRaw string, trimmed bool, err error) {
	merged := make(map[string]string, len(prov)+8)
	if originHdr.Valid {
		var user map[string]string
		if jErr := json.Unmarshal([]byte(originHdr.V), &user); jErr != nil {
			return "", false, fmt.Errorf("dead-letter origin hdr not JSON: %w", jErr)
		}
		for k, v := range user {
			merged[k] = v
		}
	}
	for k, v := range prov {
		merged[k] = v
	}
	capBytes := s.maxDLQHeaderBytes()
	for {
		raw, jErr := json.Marshal(merged)
		if jErr != nil {
			return "", false, fmt.Errorf("dead-letter marshal headers: %w", jErr)
		}
		if len(raw) <= capBytes {
			return string(raw), trimmed, nil
		}
		// Degradation ladder: drop Messq-Last-Reason, then Messq-Origin-Published-At.
		if _, ok := merged["Messq-Last-Reason"]; ok {
			delete(merged, "Messq-Last-Reason")
			trimmed = true
			continue
		}
		if _, ok := merged["Messq-Origin-Published-At"]; ok {
			delete(merged, "Messq-Origin-Published-At")
			trimmed = true
			continue
		}
		// The mandatory set must fit (it is ~300 B); this is belt-and-braces.
		return "", true, errors.New("dead-letter mandatory header set exceeds the 2x budget")
	}
}

// deadEvent builds the one msg.dead event the caller folds into its reply. The event
// names the ORIGIN (stream/seq/msg_id = the death); the copy's coordinates live in
// detail.dlq_stream / detail.dlq_seq (S9.2).
func (s DLQSink) deadEvent(ctx context.Context, tx *sql.Tx, d queue.DeadCtx, nowMS int64,
	outcome DeadOutcome, dlqStream string, dlqSeq, bytes int64, trimmed, reasonTrunc bool,
) (obs.Event, error) {
	detail := map[string]any{
		"cause":      string(d.Cause),
		"policy":     string(d.Policy),
		"attempts":   d.Attempts,
		"generation": d.Generation,
		"dlq":        string(outcome),
	}
	if d.Trigger != "" {
		detail["trigger"] = string(d.Trigger)
	}
	if d.MaxDeliver > 0 {
		detail["max_deliver"] = d.MaxDeliver
	}
	if d.LastReason != "" {
		detail["last_reason"] = d.LastReason
	}
	if outcome == DeadOutcomeWritten {
		detail["dlq_stream"] = dlqStream
		detail["dlq_seq"] = dlqSeq
		detail["bytes"] = bytes
		if trimmed {
			detail["headers_trimmed"] = true
		}
		if reasonTrunc {
			detail["reason_truncated"] = true
		}
	}
	raw, jErr := json.Marshal(detail)
	if jErr != nil { // unreachable map of scalars
		raw = []byte(`{}`)
	}
	return commitEvent(ctx, tx, event{
		ts: nowMS, name: "msg.dead",
		stream: nullStr(d.Stream), consumer: nullStr(d.Consumer), subject: nullStr(d.Subject),
		//nolint:gosec // G115: d.Seq is bounded by the messages seq space (int64).
		seq:     nullI64(int64(d.Seq)),
		attempt: nullI64(int64(d.Attempts)),
		msgID:   nullStr(d.MsgID), traceID: nullStr(d.TraceID),
		detail: nullStr(string(raw)),
	})
}
