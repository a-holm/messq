// SPDX-License-Identifier: Apache-2.0

package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/a-holm/messq/internal/errs"
	"github.com/a-holm/messq/internal/id"
	"github.com/a-holm/messq/internal/obs"
	"github.com/a-holm/messq/internal/queue"
)

// The #7 command set, wired onto #6's group-commit engine. Every state-changing
// Store method validates what it can without the transaction, then submits a [Cmd]
// through [Writer.Do]; the command's Apply runs inside the writer's batch
// transaction and its reply comes back only after that batch's COMMIT returned nil
// — the reply-only-after-commit rule is the engine's, not re-implemented here.
//
// Two rules govern every Apply below:
//
//   - A business rejection (validation, missing stream, config conflict, …) MUST be
//     marked with [CmdErr] — [maybeCmdErr] does the marking by sentinel matching — so
//     the engine rolls back exactly this command's savepoint and keeps its
//     batch-mates alive. An unmarked error aborts the whole batch and latches the
//     process read-only (the fsyncgate rule): driver damage, constraint violations
//     messq did not predict, and corrupt bookkeeping are all deliberately left
//     unmarked.
//   - The batch timestamp arrives as now; nothing reads the wall clock. Every row an
//     Apply writes shares one timestamp, including the co-committed audit row (D11)
//     and the carrier event handed to the fan-out sink.

// CmdKind labels for the queue commands. They ride log lines and the observer only;
// the engine never switches on them.
const (
	kindCreateStream CmdKind = "stream.create"
	kindUpdateStream CmdKind = "stream.update"
	kindDeleteStream CmdKind = "stream.delete"
	kindDeleteReap   CmdKind = "stream.delete.reap"
	kindPublish      CmdKind = "msg.publish"
	kindPublishBatch CmdKind = "msg.publish.batch"
	kindSweepDedup   CmdKind = "dedup.sweep"
)

// domainSentinels are the typed refusals whose failure is the caller's doing rather
// than the storage medium's. Only these may roll back a single savepoint while
// batch-mates commit; SQLITE_IOERR*/FULL/CORRUPT-class damage stays unmarked and
// fatal to the whole transaction (D4).
var domainSentinels = []error{
	errs.ErrNotFound,
	errs.ErrConflict,
	errs.ErrBadRequest,
	errs.ErrBadSubject,
	errs.ErrTooLarge,
	errs.ErrStreamFull,
}

// isDomainError reports whether err wraps one of the domainSentinels (directly or
// through a typed wrapper's Unwrap).
func isDomainError(err error) bool {
	for _, sentinel := range domainSentinels {
		if errors.Is(err, sentinel) {
			return true
		}
	}
	return false
}

// maybeCmdErr marks err as a business rejection when it wraps a domain sentinel, so
// the engine rejects exactly this command. A nil passes through unwrapped, and an
// infrastructure error ALSO passes through unmarked: the fsyncgate owns those.
//
// Every Apply's error return funnels through here, which makes the marking decision
// one auditable place instead of a convention each command must remember.
func maybeCmdErr(err error) error {
	if err == nil || !isDomainError(err) {
		return err
	}
	return CmdErr(err)
}

// enqueue is the one door from Store methods onto the engine: it checks the store
// lifecycle, then blocks on [Writer.Do] until the command's batch has committed (or
// the engine refused it). op names the caller in refusal messages.
//
// A store whose engine was never constructed still honours its write contract: while
// the rw handle is still the store's (never handed off, not read-only), the command
// runs synchronously via [runSolo] in a single-command transaction. Production
// attaches the engine with [*Store.NewWriter]; the fallback exists so a plainly
// opened store keeps its documented rejection semantics instead of failing with a
// construction-order error.
func (s *Store) enqueue(ctx context.Context, op string, cmd Cmd) (Result, error) {
	s.mu.Lock()
	w, closed, handedOff, rw, clk := s.writer, s.closed, s.handedOff, s.rw, s.clk
	s.mu.Unlock()
	switch {
	case closed:
		return nil, errs.E(errs.ErrShuttingDown, op, "store is closed")
	case w != nil:
		return w.Do(ctx, cmd)
	case handedOff || rw == nil:
		return nil, errs.E(errs.ErrShuttingDown, op,
			"no read-write handle: store closing or opened read-only")
	}
	return runSolo(ctx, rw, clk.Now(), op, cmd)
}

// runSolo is enqueue's engine-less fallback: one command, one transaction, on the
// store's own rw handle. It mirrors the engine's per-command shape — a cmd_0
// savepoint, domain rejections (marked via [maybeCmdErr]) rolled back to the
// savepoint while the transaction still commits, everything else aborting the whole
// transaction — so a command's rejection semantics do not depend on which path ran
// it. The events an Apply returns are dropped here: without the engine there is no
// fan-out pump, and the events-table row, the source of truth (D11), is already
// committed.
//
// database/sql cannot spell BEGIN IMMEDIATE through BeginTx; the fallback's
// transactions run deferred — the write lock is taken by the first statement, which
// for a one-command transaction is equivalent.
func runSolo(ctx context.Context, rw *sql.DB, now time.Time, op string, cmd Cmd) (Result, error) {
	const sp = `cmd_0` // the savepoint name of a one-element engine batch
	conn, err := rw.Conn(ctx)
	if err != nil {
		return nil, fmt.Errorf("%s: acquire writer connection: %w", op, err)
	}
	defer func() {
		if cerr := conn.Close(); cerr != nil {
			_ = cerr // best-effort pool return; the caller's error carries the story
		}
	}()

	tx, err := conn.BeginTx(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("%s: begin: %w", op, err)
	}
	if _, spErr := tx.ExecContext(ctx, `SAVEPOINT `+sp); spErr != nil {
		_ = tx.Rollback()
		return nil, fmt.Errorf("%s: savepoint: %w", op, spErr)
	}

	res, _, applyErr := cmd.Apply(ctx, tx, now)
	if applyErr == nil {
		if _, relErr := tx.ExecContext(ctx, `RELEASE `+sp); relErr != nil {
			_ = tx.Rollback()
			return nil, fmt.Errorf("%s: release savepoint: %w", op, relErr)
		}
		if cErr := tx.Commit(); cErr != nil {
			return nil, fmt.Errorf("%s: commit: %w", op, cErr)
		}
		return res, nil
	}
	if !IsCmdError(maybeCmdErr(applyErr)) {
		_ = tx.Rollback() // I/O-class damage: the whole transaction is untrustworthy
		return nil, fmt.Errorf("%s: %w", op, applyErr)
	}
	// Domain rejection: undo exactly this command's work, commit the remainder,
	// reply with the marked error — the engine's CmdError contract, verbatim.
	if _, rbErr := tx.ExecContext(ctx, `ROLLBACK TO `+sp); rbErr != nil {
		_ = tx.Rollback()
		return nil, fmt.Errorf("%s: rollback to savepoint: %w", op, errors.Join(applyErr, rbErr))
	}
	if _, relErr := tx.ExecContext(ctx, `RELEASE `+sp); relErr != nil {
		_ = tx.Rollback()
		return nil, fmt.Errorf("%s: release savepoint: %w", op, errors.Join(applyErr, relErr))
	}
	if cErr := tx.Commit(); cErr != nil {
		return nil, fmt.Errorf("%s: commit after domain rejection: %w", op,
			errors.Join(applyErr, cErr))
	}
	return nil, maybeCmdErr(applyErr)
}

// createStreamCmd stores one validated stream configuration. See
// (*Store).CreateStream for the public contract; Apply carries the idempotency
// check, the seq high-water resume and the co-committed stream.create event.
type createStreamCmd struct {
	cfg   queue.StreamConfig
	actor string
}

// createStreamResult is what a committed create hands back: the stored shape plus
// whether the name already existed with an identical configuration.
type createStreamResult struct {
	info    StreamInfo
	existed bool
}

func (c createStreamCmd) Kind() CmdKind { return kindCreateStream }
func (c createStreamCmd) Bytes() int    { return 0 } // metadata-only

func (c createStreamCmd) Apply(ctx context.Context, tx *sql.Tx, now time.Time) (Result, []obs.Event, error) {
	ts := now.UnixMilli()

	// A deletion whose message chunks are still reaping holds the name (issue §9):
	// recreating mid-reap would put fresh rows exactly where the chunk commands are
	// about to delete.
	if err := refuseDuringReap(ctx, tx, c.cfg.Name); err != nil {
		return nil, nil, maybeCmdErr(err)
	}

	row := tx.QueryRowContext(ctx,
		`SELECT `+streamCols+` FROM streams WHERE name = ? COLLATE NOCASE`, c.cfg.Name)
	existing, scanErr := scanStreamInfo(row)
	var res createStreamResult
	switch {
	case errors.Is(scanErr, sql.ErrNoRows):
		// fresh name: fall through to the insert
	case scanErr != nil:
		return nil, nil, fmt.Errorf("read existing stream: %w", scanErr) // infra: fsyncgate
	default:
		if existing.Name == c.cfg.Name {
			if diff := configDiff(existing.Config(), c.cfg); len(diff) > 0 {
				return nil, nil, maybeCmdErr(&StreamExistsError{
					Name: c.cfg.Name, Diff: diff, Existing: existing,
				})
			}
			res.info, res.existed = existing, true
			return res, nil, nil
		}
		return nil, nil, maybeCmdErr(&NameCaseCollisionError{
			Name: c.cfg.Name, Existing: existing.Name,
		})
	}

	next, hwmErr := resumeSeq(ctx, tx, c.cfg.Name)
	if hwmErr != nil {
		return nil, nil, hwmErr // corrupt hwm value: unmarked on purpose (fsyncgate)
	}
	if _, execErr := tx.ExecContext(ctx, `INSERT INTO streams
		(name, subjects, retention, max_msgs, max_bytes, max_age_ms, max_msg_size, discard, dedup_window_ms, created_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		c.cfg.Name, marshalSubjects(c.cfg.Subjects), string(c.cfg.Retention),
		c.cfg.MaxMsgs, c.cfg.MaxBytes, c.cfg.MaxAge.Milliseconds(), c.cfg.MaxMsgSize,
		string(c.cfg.Discard), c.cfg.DedupWindow.Milliseconds(), ts,
	); execErr != nil {
		return nil, nil, fmt.Errorf("insert stream row: %w", execErr)
	}
	if _, execErr := tx.ExecContext(ctx,
		`INSERT INTO stream_seq (stream, next) VALUES (?, ?)`, c.cfg.Name, next); execErr != nil {
		return nil, nil, fmt.Errorf("insert stream_seq row: %w", execErr)
	}
	if _, execErr := tx.ExecContext(ctx,
		`INSERT INTO stream_stats (stream, msgs, bytes) VALUES (?, 0, 0)`, c.cfg.Name); execErr != nil {
		return nil, nil, fmt.Errorf("insert stream_stats row: %w", execErr)
	}
	ev, evErr := commitEvent(ctx, tx, event{
		ts:     ts,
		name:   "stream.create",
		stream: nullStr(c.cfg.Name),
		actor:  nullStr(c.actor),
		detail: nullStr(fmt.Sprintf(`{"next_seq":%d}`, next)),
	})
	if evErr != nil {
		return nil, nil, evErr
	}
	res.info = StreamInfo{
		Name: c.cfg.Name, Subjects: c.cfg.Subjects,
		Retention: string(c.cfg.Retention), MaxMsgs: c.cfg.MaxMsgs, MaxBytes: c.cfg.MaxBytes,
		MaxAgeMS: c.cfg.MaxAge.Milliseconds(), MaxMsgSize: c.cfg.MaxMsgSize,
		Discard: string(c.cfg.Discard), DedupWindowMS: c.cfg.DedupWindow.Milliseconds(),
		CreatedAt: ts, DLQ: queue.IsDLQ(c.cfg.Name),
	}
	if origin, ok := queue.OriginOf(c.cfg.Name); ok {
		res.info.Origin = origin
	}
	return res, []obs.Event{ev}, nil
}

// updateStreamCmd applies a sparse patch inside the transaction, where the data-loss
// decision runs against the authoritative row rather than a handler snapshot.
type updateStreamCmd struct {
	name          string
	p             StreamPatch
	allowDataLoss bool
	actor         string
	limits        queue.Limits
}

func (c updateStreamCmd) Kind() CmdKind { return kindUpdateStream }
func (c updateStreamCmd) Bytes() int    { return 0 } // metadata-only

func (c updateStreamCmd) Apply(ctx context.Context, tx *sql.Tx, now time.Time) (Result, []obs.Event, error) {
	ts := now.UnixMilli()
	row := tx.QueryRowContext(ctx, `SELECT `+streamCols+` FROM streams WHERE name = ?`, c.name)
	old, scanErr := scanStreamInfo(row)
	if errors.Is(scanErr, sql.ErrNoRows) {
		return nil, nil, maybeCmdErr(errs.E(errs.ErrNotFound, "store.UpdateStream",
			"stream %q does not exist", c.name))
	}
	if scanErr != nil {
		return nil, nil, fmt.Errorf("read stream %q: %w", c.name, scanErr)
	}

	next, fields := applyPatch(old.Config(), c.p)
	var res UpdateResult
	if len(fields) == 0 { // empty patch: nothing to decide, nothing to audit
		stats, statsErr := streamUsage(ctx, tx, c.name)
		if statsErr != nil {
			return nil, nil, statsErr
		}
		old.Msgs, old.Bytes = stats.msgs, stats.bytes
		res.Info = old
		return res, nil, nil
	}
	if vErr := queue.ValidateStreamConfig(next, c.limits); vErr != nil {
		return nil, nil, maybeCmdErr(vErr)
	}

	u, mErr := measureUsage(ctx, tx, c.name, next, ts)
	if mErr != nil {
		return nil, nil, mErr
	}
	if uErr := queue.ValidateUpdate(old.Config(), next, u, c.allowDataLoss); uErr != nil {
		return nil, nil, maybeCmdErr(uErr)
	}

	if _, xErr := tx.ExecContext(ctx, `UPDATE streams SET
		subjects = ?, retention = ?, max_msgs = ?, max_bytes = ?, max_age_ms = ?,
		max_msg_size = ?, discard = ?, dedup_window_ms = ?
		WHERE name = ?`,
		marshalSubjects(next.Subjects), string(next.Retention),
		next.MaxMsgs, next.MaxBytes, next.MaxAge.Milliseconds(), next.MaxMsgSize,
		string(next.Discard), next.DedupWindow.Milliseconds(), c.name,
	); xErr != nil {
		return nil, nil, fmt.Errorf("update stream row: %w", xErr)
	}
	raw, jErr := jsonMarshal(map[string]any{"fields": fields})
	if jErr != nil { // unreachable for []string
		raw = []byte(`{}`)
	}
	ev, evErr := commitEvent(ctx, tx, event{
		ts:     ts,
		name:   "stream.update",
		stream: nullStr(c.name),
		actor:  nullStr(c.actor),
		detail: nullStr(string(raw)),
	})
	if evErr != nil {
		return nil, nil, evErr
	}

	res.Fields = fields
	res.Info = old // sequence/stat fields are filled below
	res.Info.Subjects = next.Subjects
	res.Info.Retention = string(next.Retention)
	res.Info.MaxMsgs = next.MaxMsgs
	res.Info.MaxBytes = next.MaxBytes
	res.Info.MaxAgeMS = next.MaxAge.Milliseconds()
	res.Info.MaxMsgSize = next.MaxMsgSize
	res.Info.Discard = string(next.Discard)
	res.Info.DedupWindowMS = next.DedupWindow.Milliseconds()
	stats, statsErr := streamUsage(ctx, tx, c.name)
	if statsErr != nil {
		return nil, nil, statsErr
	}
	res.Info.Msgs, res.Info.Bytes = stats.msgs, stats.bytes
	if subjectsChanged(old.Config().Subjects, next.Subjects) {
		n, nErr := countUnmatched(ctx, tx, c.name, next.Subjects)
		if nErr != nil {
			return nil, nil, nErr
		}
		res.NarrowedMsgs = n
	}
	return res, []obs.Event{ev}, nil
}

// deleteStreamCmd removes a stream's metadata in one small transaction: counters,
// consumers, deliveries, the seq counter, the high-water mark (P2), the audit row —
// and a reap.<name> marker whose chunk commands clear when the last messages go
// (issue §9: the metadata disappears immediately, the body follows in bounded
// chunks; concurrent creates are refused with a conflict until the reap finishes).
type deleteStreamCmd struct {
	name  string
	actor string
}

func (c deleteStreamCmd) Kind() CmdKind { return kindDeleteStream }
func (c deleteStreamCmd) Bytes() int    { return 0 } // metadata-only

func (c deleteStreamCmd) Apply(ctx context.Context, tx *sql.Tx, now time.Time) (Result, []obs.Event, error) {
	ts := now.UnixMilli()
	var res DeleteResult
	row := tx.QueryRowContext(ctx,
		`SELECT msgs, bytes FROM stream_stats WHERE stream = ?`, c.name)
	if err := row.Scan(&res.Messages, &res.Bytes); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, nil, maybeCmdErr(errs.E(errs.ErrNotFound, "store.DeleteStream",
				"stream %q does not exist", c.name))
		}
		return nil, nil, fmt.Errorf("read stats of %q: %w", c.name, err)
	}
	if err := tx.QueryRowContext(ctx,
		`SELECT count(*) FROM consumers WHERE stream = ?`, c.name).Scan(&res.Consumers); err != nil {
		return nil, nil, fmt.Errorf("count consumers of %q: %w", c.name, err)
	}
	var next int64
	if err := tx.QueryRowContext(ctx,
		`SELECT next FROM stream_seq WHERE stream = ?`, c.name).Scan(&next); err != nil &&
		!errors.Is(err, sql.ErrNoRows) {
		return nil, nil, fmt.Errorf("read seq counter of %q: %w", c.name, err)
	}
	hwm := next - 1

	for _, q := range []string{
		`DELETE FROM deliveries WHERE stream = ?`,
		`DELETE FROM consumers   WHERE stream = ?`,
		`DELETE FROM stream_stats WHERE stream = ?`,
		`DELETE FROM stream_seq   WHERE stream = ?`,
	} {
		if _, xErr := tx.ExecContext(ctx, q, c.name); xErr != nil {
			return nil, nil, fmt.Errorf("delete %q rows (%.24s…): %w", c.name, q, xErr)
		}
	}
	for _, m := range [][2]any{
		{metaSeqHwmPrefix + c.name, fmt.Sprintf("%d", hwm)},
		// The reap marker: value is the approximate row count the chunks will walk
		// down to zero. Cleared by the final chunk command, or by recovery if this
		// process dies mid-reap.
		{metaReapPrefix + c.name, fmt.Sprintf("%d", res.Messages)},
	} {
		if _, xErr := tx.ExecContext(ctx,
			`INSERT INTO meta (k, v) VALUES (?, ?)
			 ON CONFLICT (k) DO UPDATE SET v = excluded.v`, m[0], m[1]); xErr != nil {
			return nil, nil, fmt.Errorf("record %s for %q: %w", m[0], c.name, xErr)
		}
	}
	if _, xErr := tx.ExecContext(ctx, `DELETE FROM streams WHERE name = ?`, c.name); xErr != nil {
		return nil, nil, fmt.Errorf("delete stream row %q: %w", c.name, xErr)
	}
	raw, jErr := jsonMarshal(map[string]int64{
		"messages": res.Messages, "bytes": res.Bytes, "consumers": res.Consumers,
	})
	if jErr != nil { // unreachable for fixed keys
		raw = []byte(`{}`)
	}
	ev, evErr := commitEvent(ctx, tx, event{
		ts:     ts,
		name:   "stream.delete",
		stream: nullStr(c.name),
		actor:  nullStr(c.actor),
		detail: nullStr(string(raw)),
	})
	if evErr != nil {
		return nil, nil, evErr
	}
	return res, []obs.Event{ev}, nil
}

// reapChunkCmd deletes up to deleteChunkRows message rows of a stream being
// deleted. When a chunk comes up short, the messages are gone and the same
// transaction clears the reap marker — the last thing standing between the name and
// its recreation.
type reapChunkCmd struct {
	name string
}

// reapChunkResult reports how many rows this chunk removed and whether it cleared
// the reap marker (true exactly when the removal came up short of a full chunk).
type reapChunkResult struct {
	Removed int64
	Cleared bool
}

func (c reapChunkCmd) Kind() CmdKind { return kindDeleteReap }
func (c reapChunkCmd) Bytes() int    { return 0 }

func (c reapChunkCmd) Apply(ctx context.Context, tx *sql.Tx, _ time.Time) (Result, []obs.Event, error) {
	r, xErr := tx.ExecContext(ctx,
		`DELETE FROM messages WHERE rowid IN
		   (SELECT rowid FROM messages WHERE stream = ? LIMIT ?)`,
		c.name, deleteChunkRows)
	if xErr != nil {
		return nil, nil, fmt.Errorf("delete message chunk of %q: %w", c.name, xErr)
	}
	removed, xErr := r.RowsAffected()
	if xErr != nil {
		return nil, nil, fmt.Errorf("count deleted chunk of %q: %w", c.name, xErr)
	}
	out := reapChunkResult{Removed: removed}
	if removed < deleteChunkRows {
		if _, dErr := tx.ExecContext(ctx, `DELETE FROM meta WHERE k = ?`,
			metaReapPrefix+c.name); dErr != nil {
			return nil, nil, fmt.Errorf("clear reap marker of %q: %w", c.name, dErr)
		}
		out.Cleared = true
	}
	return out, nil, nil
}

// sweepDedupCmd NULLs at most sweepBound expired dedup keys of one stream (issue §4).
type sweepDedupCmd struct {
	name string
}

// sweepResult is the number of expired keys the committed sweep cleared.
type sweepResult int64

func (c sweepDedupCmd) Kind() CmdKind { return kindSweepDedup }
func (c sweepDedupCmd) Bytes() int    { return 0 }

func (c sweepDedupCmd) Apply(ctx context.Context, tx *sql.Tx, now time.Time) (Result, []obs.Event, error) {
	var window int64
	err := tx.QueryRowContext(ctx,
		`SELECT dedup_window_ms FROM streams WHERE name = ?`, c.name).Scan(&window)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		return nil, nil, maybeCmdErr(errs.E(errs.ErrNotFound, "store.SweepDedup",
			"stream %q does not exist", c.name))
	case err != nil:
		return nil, nil, fmt.Errorf("read dedup window of %q: %w", c.name, err)
	}

	res, xErr := tx.ExecContext(ctx, `
		UPDATE messages SET dedup_key = NULL
		 WHERE rowid IN (
		   SELECT rowid FROM messages
		    WHERE stream = ?1 AND published_at < ?2 AND dedup_key IS NOT NULL
		    ORDER BY published_at LIMIT ?3)`,
		c.name, now.UnixMilli()-window, sweepBound)
	if xErr != nil {
		return nil, nil, fmt.Errorf("expire dedup keys of %q: %w", c.name, xErr)
	}
	cleared, xErr := res.RowsAffected()
	if xErr != nil {
		return nil, nil, fmt.Errorf("count expired keys of %q: %w", c.name, xErr)
	}
	return sweepResult(cleared), nil, nil
}

// publishWriteCmd is PublishCmd dressed for the engine: the public wire shape plus
// the store-scoped validation ceilings and ULID source the Apply needs.
type publishWriteCmd struct {
	cmd    PublishCmd
	limits queue.Limits
	newID  func() id.MsgID
}

func (c publishWriteCmd) Kind() CmdKind { return kindPublish }
func (c publishWriteCmd) Bytes() int    { return c.cmd.Bytes() }

func (c publishWriteCmd) Apply(ctx context.Context, tx *sql.Tx, now time.Time) (Result, []obs.Event, error) {
	ack, ev, err := publishTx(ctx, tx, now.UnixMilli(), c.limits, c.newID,
		c.cmd.Stream, c.cmd.Req, publishOpts{})
	if err != nil {
		return nil, nil, maybeCmdErr(err)
	}
	return ack, []obs.Event{ev}, nil
}

// batchPublishCmd is BatchCmd dressed for the engine: one command, one savepoint, so
// the engine's per-command rollback IS the all-or-nothing guarantee — a failure at
// line k undoes the earlier lines' inserts and sequence allocations with it (P1).
type batchPublishCmd struct {
	cmd    BatchCmd
	limits queue.Limits
	newID  func() id.MsgID
}

func (c batchPublishCmd) Kind() CmdKind { return kindPublishBatch }
func (c batchPublishCmd) Bytes() int    { return c.cmd.Bytes() }

func (c batchPublishCmd) Apply(ctx context.Context, tx *sql.Tx, now time.Time) (Result, []obs.Event, error) {
	lc, cfgErr := loadStreamConfig(ctx, tx, c.cmd.Stream)
	if cfgErr != nil {
		return nil, nil, maybeCmdErr(cfgErr)
	}
	ms := now.UnixMilli()
	ack := BatchAck{Results: make([]Ack, 0, len(c.cmd.Reqs))}
	var evs []obs.Event
	for i, req := range c.cmd.Reqs {
		a, ev, pErr := publishTxWithConfig(ctx, tx, ms, c.limits, c.newID, lc, req, publishOpts{})
		if pErr != nil {
			// All-or-nothing: the wrapper keeps every sentinel findable via
			// errors.Is/As (and the CmdError marking visible to the engine) while
			// naming the offending entry for the response.
			return nil, nil, maybeCmdErr(fmt.Errorf("line %d: %w", i+1, pErr))
		}
		ack.Results = append(ack.Results, a)
		evs = append(evs, ev)
	}
	return ack, evs, nil
}

// refuseDuringReap refuses a create whose name is mid-deletion, naming the
// approximate number of messages the chunk commands still have to walk through
// (issue §9: "stream orders is still being deleted (1.2 M messages remain)"). The
// marker row is written by deleteStreamCmd and cleared by the final reapChunkCmd.
func refuseDuringReap(ctx context.Context, tx *sql.Tx, name string) error {
	var raw string
	err := tx.QueryRowContext(ctx, `SELECT v FROM meta WHERE k = ?`,
		metaReapPrefix+name).Scan(&raw)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		return nil
	case err != nil:
		return fmt.Errorf("read reap marker of %q: %w", name, err)
	}
	var remaining int64
	if _, pErr := fmt.Sscanf(raw, "%d", &remaining); pErr != nil {
		return fmt.Errorf("meta[%s%s] = %q is not an integer", metaReapPrefix, name, raw)
	}
	return &ReapInProgressError{Name: name, Remaining: remaining}
}

// finishInterruptedReaps completes the message-chunk deletions of streams whose
// deleting process died between the metadata transaction and the last chunk (the
// reap.<name> marker survives in meta). It runs on the raw writer handle during
// startup recovery, before any listener exists, so a recreated name can never see
// orphaned rows. Each chunk is its own IMMEDIATE transaction — bounded work, same
// shape as the live reaper's chunks. It returns the names whose reaps it completed.
func finishInterruptedReaps(ctx context.Context, rw *sql.DB) ([]string, error) {
	rows, err := rw.QueryContext(ctx, `SELECT k FROM meta WHERE k LIKE ?`, metaReapPrefix+"%")
	if err != nil {
		return nil, fmt.Errorf("list reap markers: %w", err)
	}
	defer func() {
		if cerr := rows.Close(); cerr != nil {
			// best-effort: the listing is already materialised below
			_ = cerr
		}
	}()
	var names []string
	for rows.Next() {
		var k string
		if err := rows.Scan(&k); err != nil {
			return nil, fmt.Errorf("scan reap marker: %w", err)
		}
		names = append(names, k[len(metaReapPrefix):])
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate reap markers: %w", err)
	}

	for _, name := range names {
		for {
			tx, beginErr := rw.BeginTx(ctx, nil)
			if beginErr != nil {
				return names, fmt.Errorf("begin reap chunk of %q: %w", name, beginErr)
			}
			var removed int64
			r, xErr := tx.ExecContext(ctx,
				`DELETE FROM messages WHERE rowid IN
				   (SELECT rowid FROM messages WHERE stream = ? LIMIT ?)`,
				name, deleteChunkRows)
			if xErr == nil {
				removed, xErr = r.RowsAffected()
				if xErr == nil && removed < deleteChunkRows {
					_, xErr = tx.ExecContext(ctx, `DELETE FROM meta WHERE k = ?`,
						metaReapPrefix+name)
				}
			}
			if xErr != nil {
				_ = tx.Rollback()
				return names, fmt.Errorf("finish reap of %q: %w", name, xErr)
			}
			if commitErr := tx.Commit(); commitErr != nil {
				return names, fmt.Errorf("commit reap chunk of %q: %w", name, commitErr)
			}
			if removed < deleteChunkRows {
				break
			}
		}
	}
	return names, nil
}
