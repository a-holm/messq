// SPDX-License-Identifier: Apache-2.0

package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/a-holm/messq/internal/errs"
	"github.com/a-holm/messq/internal/obs"
	"github.com/a-holm/messq/internal/queue"
	"github.com/a-holm/messq/internal/subject"
)

// Stream purge (issue #15 §5, SEMANTICS S6.2: T10 covers purge as well as seek).
// One plan, one apply — preview and execution share the exact code path (G4): the
// counting queries run inside this command's transaction either way, and ?dry_run=1
// simply stops before the first DELETE. A business refusal rolls back only this
// command's savepoint while batch-mates commit.
//
// Purge fencing semantics locked by T10 + D7:
//   - every delivery row whose message is deleted goes too, counted honestly;
//   - generation++ for ONLY the consumers that held rows in the selected range;
//   - cursor_seq clamps FORWARD ONLY, and only on an unfiltered purge;
//   - stream_seq.next is never touched: sequence numbers are never reused.

const kindPurge CmdKind = "stream.purge"

// PurgeSpec is the validated request shape. UpToSeq is INCLUSIVE.
type PurgeSpec struct {
	UpToSeq *int64
	Subject string
	Keep    int64 // keep the newest K matching messages; 0 = none kept extra
}

// ValidatePurgeSpec rejects ambiguous or malformed combinations.
func ValidatePurgeSpec(spec PurgeSpec) error {
	if spec.UpToSeq != nil {
		if *spec.UpToSeq < 1 {
			return errs.E(errs.ErrBadRequest, "store.ValidatePurgeSpec",
				"up_to_seq %d is below the first valid sequence number", *spec.UpToSeq)
		}
		if spec.Keep > 0 {
			return errs.E(errs.ErrBadRequest, "store.ValidatePurgeSpec",
				"keep %d together with up_to_seq %d is ambiguous: one purges a range, "+
					"the other preserves its newest tail", spec.Keep, *spec.UpToSeq)
		}
	}
	if spec.Keep < 0 {
		return errs.E(errs.ErrBadRequest, "store.ValidatePurgeSpec",
			"keep %d is negative", spec.Keep)
	}
	if spec.Subject != "" {
		if _, err := subject.ParsePattern(spec.Subject); err != nil {
			return errs.E(errs.ErrBadRequest, "store.ValidatePurgeSpec",
				"subject pattern %q does not parse: %v", spec.Subject, err)
		}
	}
	return nil
}

// PurgeImpact is the wire-identical shape both dry-run and real-run return.
type PurgeImpact struct {
	Messages          int64    `json:"messages,omitempty"`
	Bytes             int64    `json:"bytes,omitempty"`
	PendingDropped    int64    `json:"pending_dropped,omitempty"`
	ConsumersAffected []string `json:"consumers_affected,omitempty"`
	FirstSeqAfter     int64    `json:"first_seq_after,omitempty"`
	CursorBefore      int64    `json:"cursor_before,omitempty"`
	CursorAfter       int64    `json:"cursor_after,omitempty"`
	Clamped           bool     `json:"clamped,omitempty"`
	Warnings          []string `json:"warnings,omitempty"`
}

// PurgeResult reports what the command would do (dry run) or did (real run).
type PurgeResult struct {
	Impact PurgeImpact `json:"impact"`
	Stream string      `json:"stream"`
}

type purgeCmd struct {
	stream string
	spec   PurgeSpec
	dryRun bool
	actor  string
	logger *slog.Logger
}

func (c purgeCmd) Kind() CmdKind { return kindPurge }
func (c purgeCmd) Bytes() int    { return 0 }

type purgeResult struct{ r PurgeResult }

func (c purgeCmd) Apply(ctx context.Context, tx *sql.Tx, _ time.Time) (Result, []obs.Event, error) {
	pat := (*subject.Pattern)(nil)
	if c.spec.Subject != "" {
		p, pErr := subject.ParsePattern(c.spec.Subject)
		if pErr != nil {
			return nil, nil, maybeCmdErr(pErr) // already validated; belt for direct callers
		}
		pat = &p
	}

	var next int64
	err := tx.QueryRowContext(ctx,
		`SELECT next FROM stream_seq WHERE stream = ?`, c.stream).Scan(&next)
	if errors.Is(err, sql.ErrNoRows) {
		next = 1
	} else if err != nil {
		return nil, nil, fmt.Errorf("purge %s: read stream_seq: %w", c.stream, err)
	}

	upper := next - 1 // whole live range by default
	if c.spec.UpToSeq != nil {
		upper = min(*c.spec.UpToSeq, upper)
	}

	if pat == nil && c.spec.Keep == 0 {
		return c.applyFastRange(ctx, tx, upper, next)
	}
	return c.applyScanPath(ctx, tx, pat, upper, next)
}

// countRange reads the plan's message census for a seq range.
func countRange(ctx context.Context, tx *sql.Tx, stream string, lowerInclusive bool, bound int64,
) (msgs, bytesSum int64, err error) {
	op := "> "
	if lowerInclusive {
		op = "<="
	}
	var m, b sql.Null[int64]
	q := `SELECT count(*), coalesce(sum(size),0) FROM messages WHERE stream = ? AND seq ` + op + ` ?`
	if err := tx.QueryRowContext(ctx, q, stream, bound).Scan(&m, &b); err != nil {
		return 0, 0, err
	}
	return m.V, b.V, nil
}

// distinctDeliveryConsumers lists consumers holding any delivery row with seq <= bound.
func distinctDeliveryConsumers(ctx context.Context, logger *slog.Logger, q interface {
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
}, stream string, bound int64,
) ([]string, error) {
	rows, err := q.QueryContext(ctx,
		`SELECT DISTINCT consumer FROM deliveries WHERE stream = ? AND seq <= ?`,
		stream, bound)
	if err != nil {
		return nil, err
	}
	defer func() {
		if cerr := rows.Close(); cerr != nil && logger != nil {
			logger.Warn("purge: close consumer scan", "err", cerr)
		}
	}()
	var out []string
	for rows.Next() {
		var name string
		if sErr := rows.Scan(&name); sErr != nil {
			return nil, sErr
		}
		out = append(out, name)
	}
	sortStrings(out)
	return out, rows.Err()
}

// cursorBounds reads min(cursor_seq) for telemetry of the forward-only clamp.
func cursorBounds(ctx context.Context, tx *sql.Tx, stream string) (int64, error) {
	var cur int64
	err := tx.QueryRowContext(ctx,
		`SELECT coalesce(min(cursor_seq),0) FROM consumers WHERE stream = ?`, stream).Scan(&cur)
	return cur, err
}

// adjustStats keeps the maintained census honest with actual deletions (migration
// 0002): a purge that forgot it would lie to every dashboard forever.
func adjustStats(ctx context.Context, tx *sql.Tx, stream string, dMsgs, dBytes int64) error {
	const q = `UPDATE stream_stats SET msgs = msgs - ?, bytes = bytes - ? WHERE stream = ?`
	if _, err := tx.ExecContext(ctx, q, dMsgs, dBytes, stream); err != nil {
		return fmt.Errorf("purge %s: adjust stream_stats: %w", stream, err)
	}
	return nil
}

// applyFastRange handles the unfiltered case with indexed SQL only — the path whose
// preview must stay index-bound. Dry runs stop right after the reads.
func (c purgeCmd) applyFastRange(ctx context.Context, tx *sql.Tx, upper, next int64,
) (Result, []obs.Event, error) {
	impact := PurgeImpact{}
	msgs, bytesSum, cErr := countRange(ctx, tx, c.stream, true, upper)
	if cErr != nil {
		return nil, nil, fmt.Errorf("purge %s: count messages: %w", c.stream, cErr)
	}
	impact.Messages, impact.Bytes = msgs, bytesSum

	if sErr := tx.QueryRowContext(ctx,
		`SELECT coalesce(min(seq), 0) FROM messages WHERE stream = ? AND seq > ?`,
		c.stream, upper).Scan(&impact.FirstSeqAfter); sErr != nil && !errors.Is(sErr, sql.ErrNoRows) {
		return nil, nil, fmt.Errorf("purge %s: first-after: %w", c.stream, sErr)
	}
	if impact.FirstSeqAfter == 0 {
		impact.FirstSeqAfter = next // range empty or fully purged: numbering continues at next
	}

	affected, aErr := distinctDeliveryConsumers(ctx, c.logger, tx, c.stream, upper)
	if aErr != nil {
		return nil, nil, fmt.Errorf("purge %s: affected consumers: %w", c.stream, aErr)
	}
	impact.ConsumersAffected = affected

	curBefore, cbErr := cursorBounds(ctx, tx, c.stream)
	if cbErr != nil {
		return nil, nil, fmt.Errorf("purge %s: read cursors: %w", c.stream, cbErr)
	}
	impact.CursorBefore = curBefore

	if c.dryRun || impact.Messages == 0 {
		return purgeResult{r: PurgeResult{Stream: c.stream, Impact: impact}}, nil, nil
	}

	del, dErr := tx.ExecContext(ctx,
		`DELETE FROM deliveries WHERE stream = ? AND seq <= ?`, c.stream, upper)
	if dErr != nil {
		return nil, nil, fmt.Errorf("purge %s: drop deliveries: %w", c.stream, dErr)
	}
	dropped, raErr := del.RowsAffected()
	if raErr != nil {
		return nil, nil, fmt.Errorf("purge %s: receipt of dropped deliveries: %w", c.stream, raErr)
	}
	impact.PendingDropped = dropped

	newCur := upper + 1
	if _, eErr := tx.ExecContext(ctx,
		`UPDATE consumers SET cursor_seq = ? WHERE stream = ? AND cursor_seq < ?`,
		newCur, c.stream, newCur); eErr != nil {
		return nil, nil, fmt.Errorf("purge %s: clamp cursors: %w", c.stream, eErr)
	}
	curAfter, caErr := cursorBounds(ctx, tx, c.stream)
	if caErr != nil {
		return nil, nil, fmt.Errorf("purge %s: re-read cursors: %w", c.stream, caErr)
	}
	impact.CursorAfter = curAfter
	impact.Clamped = curBefore != curAfter

	for _, name := range affected {
		if _, eErr := tx.ExecContext(ctx,
			`UPDATE consumers SET generation = generation + 1 WHERE stream = ? AND name = ?`,
			c.stream, name); eErr != nil {
			return nil, nil, fmt.Errorf("purge %s: bump generation for %q: %w",
				c.stream, name, eErr)
		}
	}

	dm, dmErr := tx.ExecContext(ctx,
		`DELETE FROM messages WHERE stream = ? AND seq <= ?`, c.stream, upper)
	if dmErr != nil {
		return nil, nil, fmt.Errorf("purge %s: delete messages: %w", c.stream, dmErr)
	}
	n, nErr := dm.RowsAffected()
	if nErr != nil {
		return nil, nil, fmt.Errorf("purge %s: receipt of deleted messages: %w", c.stream, nErr)
	}
	if n != impact.Messages {
		return nil, nil, fmt.Errorf(
			"purge %s: deleted %d rows, plan said %d — plan drift", c.stream, n, impact.Messages)
	}
	if uErr := adjustStats(ctx, tx, c.stream, impact.Messages, impact.Bytes); uErr != nil {
		return nil, nil, uErr
	}

	ev, evErr := commitEvent(ctx, tx, event{
		name:   "stream.purge",
		stream: nullStr(c.stream),
		actor:  nullStr(c.actor),
		detail: nullStr(purgeDetailJSON(impact, upper, "")),
	})
	if evErr != nil {
		return nil, nil, evErr
	}
	return purgeResult{r: PurgeResult{Stream: c.stream, Impact: impact}}, []obs.Event{ev}, nil
}

// victim holds one selected-for-deletion row's identity for the scan path.
type victim struct {
	seq      int64
	bytes    int64
	subjectV string
}

// selectVictims walks candidates newest-descending so keep can cut the oldest tail,
// filtering by pattern when set.
func selectVictims(ctx context.Context, tx *sql.Tx, logger *slog.Logger, stream string,
	pat *subject.Pattern, upper int64,
) ([]victim, error) {
	rows, qErr := tx.QueryContext(ctx,
		`SELECT seq, subject, size FROM messages WHERE stream = ? AND seq <= ?
		 ORDER BY seq DESC`, stream, upper)
	if qErr != nil {
		return nil, qErr
	}
	defer func() {
		if cerr := rows.Close(); cerr != nil && logger != nil {
			logger.Warn("purge: close victim scan", "err", cerr)
		}
	}()
	out := make([]victim, 0, 16)
	for rows.Next() {
		var v victim
		if sErr := rows.Scan(&v.seq, &v.subjectV, &v.bytes); sErr != nil {
			return nil, sErr
		}
		if pat.Match(v.subjectV) {
			out = append(out, v)
		}
	}
	return out, rows.Err()
}

// holdsOn reads every (consumer, seq) delivery pair at-or-below bound once; both the
// affected-consumer scoping and the honest drop counting come from this single walk.
func holdsOn(ctx context.Context, tx *sql.Tx, logger *slog.Logger, stream string, bound int64,
) ([]string, map[int64][]string, error) {
	rows, qErr := tx.QueryContext(ctx,
		`SELECT consumer, seq FROM deliveries WHERE stream = ? AND seq <= ?`,
		stream, bound)
	if qErr != nil {
		return nil, nil, qErr
	}
	defer func() {
		if cerr := rows.Close(); cerr != nil && logger != nil {
			logger.Warn("purge: close delivery walk", "err", cerr)
		}
	}()
	namesSet := map[string]bool{}
	bySeq := map[int64][]string{}
	for rows.Next() {
		var name string
		var seq int64
		if sErr := rows.Scan(&name, &seq); sErr != nil {
			return nil, nil, sErr
		}
		namesSet[name] = true
		bySeq[seq] = append(bySeq[seq], name)
	}
	if err := rows.Err(); err != nil {
		return nil, nil, err
	}
	names := make([]string, 0, len(namesSet))
	for name := range namesSet {
		names = append(names, name)
	}
	sortStrings(names)
	return names, bySeq, nil
}

// applyScanPath covers subject-filtered and keep purges: exact victims are selected,
// then deleted explicitly with the same counts the preview reported.
func (c purgeCmd) applyScanPath(ctx context.Context, tx *sql.Tx, pat *subject.Pattern,
	upper, next int64,
) (Result, []obs.Event, error) {
	victims, vErr := selectVictims(ctx, tx, c.logger, c.stream, pat, upper)
	if vErr != nil {
		return nil, nil, fmt.Errorf("purge %s: select candidates: %w", c.stream, vErr)
	}
	switch {
	case c.spec.Keep >= int64(len(victims)):
		victims = nil // keeping more than exist: nothing to delete
	case c.spec.Keep > 0:
		victims = victims[c.spec.Keep:]
	}

	impact := PurgeImpact{Messages: int64(len(victims))}
	victimSeqs := make([]int64, 0, len(victims))
	inVictims := make(map[int64]bool, len(victims))
	for _, v := range victims {
		impact.Bytes += v.bytes
		victimSeqs = append(victimSeqs, v.seq)
		inVictims[v.seq] = true
	}

	allNames, bySeq, hErr := holdsOn(ctx, tx, c.logger, c.stream, upper)
	if hErr != nil {
		return nil, nil, fmt.Errorf("purge %s: scan deliveries: %w", c.stream, hErr)
	}
	for _, seq := range victimSeqs {
		impact.ConsumersAffected = append(impact.ConsumersAffected, bySeq[seq]...)
	}
	{
		scoped := map[string]bool{}
		for _, n := range impact.ConsumersAffected {
			scoped[n] = true
		}
		impact.ConsumersAffected = impact.ConsumersAffected[:0]
		for n := range scoped {
			impact.ConsumersAffected = append(impact.ConsumersAffected, n)
		}
		sortStrings(impact.ConsumersAffected)
	}
	var heldTotal int64
	for _, seq := range victimSeqs {
		heldTotal += int64(len(bySeq[seq]))
	}
	impact.PendingDropped = heldTotal
	_ = allNames

	if impact.Messages > 0 {
		maxV := victimSeqs[0]
		var survivor sql.Null[int64]
		sSurv := tx.QueryRowContext(ctx,
			`SELECT min(seq) FROM messages WHERE stream = ? AND seq > ? AND seq < ?`,
			c.stream, maxV, next).Scan(&survivor)
		switch {
		case sSurv == nil && survivor.Valid && survivor.V > 0:
			impact.FirstSeqAfter = survivor.V
		case sSurv == nil:
			impact.FirstSeqAfter = maxV + 1 // no gap survivors inside the span
		default:
			impact.FirstSeqAfter = maxV + 1
		}
	}

	if c.dryRun || impact.Messages == 0 {
		return purgeResult{r: PurgeResult{Stream: c.stream, Impact: impact}}, nil, nil
	}

	for _, seq := range victimSeqs {
		r, eErr := tx.ExecContext(ctx,
			`DELETE FROM deliveries WHERE stream = ? AND consumer IS NOT NULL AND seq = ?`,
			c.stream, seq)
		if eErr != nil {
			return nil, nil, fmt.Errorf("purge %s: drop deliveries on %d: %w", c.stream, seq, eErr)
		}
		if _, raE := r.RowsAffected(); raE != nil {
			return nil, nil, fmt.Errorf("purge %s: delivery receipt on %d: %w", c.stream, seq, raE)
		}
	}
	for _, name := range impact.ConsumersAffected {
		if _, eErr := tx.ExecContext(ctx,
			`UPDATE consumers SET generation = generation + 1 WHERE stream = ? AND name = ?`,
			c.stream, name); eErr != nil {
			return nil, nil, fmt.Errorf("purge %s: bump generation for %q: %w",
				c.stream, name, eErr)
		}
	}
	for _, seq := range victimSeqs {
		del, dErr := tx.ExecContext(ctx,
			`DELETE FROM messages WHERE stream = ? AND seq = ?`, c.stream, seq)
		if dErr != nil {
			return nil, nil, fmt.Errorf("purge %s: delete message %d: %w", c.stream, seq, dErr)
		}
		n, nErr := del.RowsAffected()
		if nErr != nil {
			return nil, nil, fmt.Errorf("purge %s: message receipt on %d: %w", c.stream, seq, nErr)
		}
		if n != 1 {
			return nil, nil, fmt.Errorf(
				"purge %s: message %d vanished under the command transaction", c.stream, seq)
		}
	}
	// No cursor clamp on a filtered purge, even though victims lie below cursors.
	if uErr := adjustStats(ctx, tx, c.stream, impact.Messages, impact.Bytes); uErr != nil {
		return nil, nil, uErr
	}

	ev, evErr := commitEvent(ctx, tx, event{
		name:   "stream.purge",
		stream: nullStr(c.stream),
		actor:  nullStr(c.actor),
		detail: nullStr(purgeDetailJSON(impact, upper, c.spec.Subject)),
	})
	if evErr != nil {
		return nil, nil, evErr
	}
	return purgeResult{r: PurgeResult{Stream: c.stream, Impact: impact}}, []obs.Event{ev}, nil
}

func sortStrings(s []string) {
	for i := 1; i < len(s); i++ { // insertion sort over tiny deterministic outputs
		for j := i; j > 0 && s[j] < s[j-1]; j-- {
			s[j], s[j-1] = s[j-1], s[j]
		}
	}
}

func purgeDetailJSON(im PurgeImpact, upper int64, subj string) string {
	b := &strings.Builder{}
	fmt.Fprintf(b, `{"messages":%d,"bytes":%d,"pending_dropped":%d,"first_seq_after":%d`,
		im.Messages, im.Bytes, im.PendingDropped, im.FirstSeqAfter)
	if subj != "" {
		fmt.Fprintf(b, `,"subject":%q`, subj)
	}
	fmt.Fprint(b, "}")
	return b.String()
}

// Purge removes one stream's selected messages. DryRun previews with the identical
// impact and zero writes (G4's one-code-path contract).
func (s *Store) Purge(ctx context.Context, stream string, spec PurgeSpec, dryRun bool, actor string) (PurgeResult, error) {
	if err := queue.ValidateExistingStreamName(stream); err != nil {
		return PurgeResult{}, err
	}
	if err := ValidatePurgeSpec(spec); err != nil {
		return PurgeResult{}, err
	}
	res, err := s.enqueue(ctx, "store.Purge",
		purgeCmd{stream: stream, spec: spec, dryRun: dryRun, actor: actor, logger: s.logger})
	if err != nil {
		return PurgeResult{}, err
	}
	pr, ok := res.(purgeResult)
	if !ok {
		return PurgeResult{}, fmt.Errorf(
			"store.Purge: engine returned %T, want purgeResult", res)
	}
	return pr.r, nil
}
