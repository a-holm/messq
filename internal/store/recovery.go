// SPDX-License-Identifier: Apache-2.0

package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/a-holm/messq/internal/clock"
)

// Startup recovery is §4.4's answer to "the power was cut while messages were in flight".
// SQLite itself owns crash consistency — there is deliberately zero bespoke repair code — so
// everything in this file is bookkeeping around that guarantee:
//
//   - an integrity check on a directory whose last close was not graceful, refusing to start
//     on damage instead of "fixing" it;
//   - lease reclaim, which flips INFLIGHT deliveries back to READY without touching attempts
//     (D6/transition T9: the delivery already counted its attempt);
//   - dedup-key trim, keeping the partial unique index bounded by each stream's window;
//   - both writes co-committed with their audit event inside one transaction, so the events
//     table can never disagree with the state change it describes.
//
// The whole procedure runs before any listener exists; a failure here is a startup refusal.

// The integrity-check vocabulary of [RecoveryReport.CheckKind].
const (
	checkQuickCheck     = "quick_check"
	checkIntegrityCheck = "integrity_check"
	checkSkipped        = "skipped"
)

// maxReportedCheckProblems bounds how many check findings are joined into one error line.
func maxReportedCheckProblems() int { return 8 }

// RecoveryReport is what Open hands back about one startup: what shape the directory was in,
// what recovery did to it, and how long it took. It is the payload #6/#17 turn into the
// server.start log line.
type RecoveryReport struct {
	NodeID          string
	SchemaFrom      int
	SchemaTo        int
	Unclean         bool // clean_shutdown marker absent or "0" on a pre-existing database
	CheckKind       string
	CheckDuration   time.Duration
	Reclaimed       int64
	DedupExpired    int64
	CheckpointPages int64
	DBBytes         int64
	WALBytes        int64
	Duration        time.Duration
}

// reclaimSQL flips every INFLIGHT lease back to READY. attempts is deliberately not in the
// SET list (D6/T9/invariant I4): the restart must void leases, never rewrite history.
// visible_at spreads over [now, now+jitter) via (random() & 0x7fffffff) % jitter — masked,
// never abs(), because abs(random()) overflows on -2^63; and guarded against jitter_ms = 0,
// because x % 0 yields NULL and visible_at is NOT NULL.
const reclaimSQL = `
UPDATE deliveries
   SET state        = 0,
       visible_at   = ?1 + CASE WHEN ?2 > 0 THEN (random() & 0x7fffffff) % ?2 ELSE 0 END,
       delivered_at = NULL,
       last_reason  = 'broker_restart'
 WHERE state = 1`

// reclaimEventSQL records the reclaim in the audit trail inside the same transaction as the
// update above. changes() reads the row count of the statement just executed on this
// connection — the events table cannot disagree with the UPDATE it narrates.
const reclaimEventSQL = `
INSERT INTO events (ts, event, detail)
VALUES (?1, 'recovery.reclaimed',
        json_object('count', changes(), 'jitter_ms', ?2, 'reason', 'broker_restart'))`

// trimDedupSQL NULLs expired dedup keys per stream window so the partial unique index stays
// bounded (§5.1 publish dedup). UPDATE … FROM joins the owning stream's window.
const trimDedupSQL = `
UPDATE messages
   SET dedup_key = NULL
  FROM streams s
 WHERE messages.dedup_key IS NOT NULL
   AND s.name = messages.stream
   AND messages.published_at + s.dedup_window_ms < ?1`

// uncleanEventSQL records the recovery.unclean observation in the audit trail. Every open of
// a dirty directory writes its own row: each restart genuinely observed the uncleanliness,
// and the marker staying "0" until a graceful Close is what makes that honest.
const uncleanEventSQL = `
INSERT INTO events (ts, event, detail)
VALUES (?1, 'recovery.unclean', json_object('reason', ?2))`

// recordUncleanEvent writes the recovery.unclean audit row through the writer pool. A
// failure here refuses startup: an unclean restart that cannot be narrated in the audit
// trail is exactly the events-table-disagrees-with-reality case §8.2 exists to prevent.
func recordUncleanEvent(ctx context.Context, rw *sql.DB, clk clock.Clock, reason string) error {
	if _, err := rw.ExecContext(ctx, uncleanEventSQL, clk.Now().UnixMilli(), reason); err != nil {
		return fmt.Errorf("record recovery.unclean event: %w", err)
	}
	return nil
}

// reclaimLeasesAndTrimDedup runs steps 8–9 of §4.4 in ONE transaction on the writer handle:
// the reclaim UPDATE, its co-committed recovery.reclaimed event, and the dedup trim. It
// returns the exact counts the report carries. The transaction is BEGIN IMMEDIATE by virtue
// of the writer DSN's _txlock=immediate (ADR-0002), so the read-modify-write cannot interleave.
func reclaimLeasesAndTrimDedup(ctx context.Context, rw *sql.DB, clk clock.Clock, jitter time.Duration) (reclaimed, dedupExpired int64, err error) {
	tx, err := rw.BeginTx(ctx, nil)
	if err != nil {
		return 0, 0, fmt.Errorf("begin recovery transaction: %w", err)
	}
	committed := false
	defer func() {
		if committed {
			return
		}
		if rbErr := tx.Rollback(); rbErr != nil && !errors.Is(rbErr, sql.ErrTxDone) {
			err = errors.Join(err, fmt.Errorf("rollback recovery transaction: %w", rbErr))
		}
	}()

	nowMs := clk.Now().UnixMilli()
	jitterMs := jitter.Milliseconds()

	res, err := tx.ExecContext(ctx, reclaimSQL, nowMs, jitterMs)
	if err != nil {
		return 0, 0, fmt.Errorf("reclaim inflight deliveries: %w", err)
	}
	reclaimed, err = res.RowsAffected()
	if err != nil {
		return 0, 0, fmt.Errorf("count reclaimed deliveries: %w", err)
	}
	if _, eventErr := tx.ExecContext(ctx, reclaimEventSQL, nowMs, jitterMs); eventErr != nil {
		return 0, 0, fmt.Errorf("record recovery.reclaimed event: %w", eventErr)
	}
	// Belt and braces: the event's changes()-derived count must equal the UPDATE's own count.
	var detailCount int64
	if scanErr := tx.QueryRowContext(ctx,
		`SELECT json_extract(detail, '$.count') FROM events WHERE event = 'recovery.reclaimed' ORDER BY id DESC LIMIT 1`,
	).Scan(&detailCount); scanErr != nil {
		return 0, 0, fmt.Errorf("read back recovery.reclaimed detail: %w", scanErr)
	}
	if detailCount != reclaimed {
		return 0, 0, fmt.Errorf("recovery.reclaimed detail.count = %d but %d rows changed", detailCount, reclaimed)
	}

	res, err = tx.ExecContext(ctx, trimDedupSQL, nowMs)
	if err != nil {
		return 0, 0, fmt.Errorf("trim expired dedup keys: %w", err)
	}
	dedupExpired, err = res.RowsAffected()
	if err != nil {
		return 0, 0, fmt.Errorf("count trimmed dedup keys: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return 0, 0, fmt.Errorf("commit recovery transaction: %w", err)
	}
	committed = true
	return reclaimed, dedupExpired, nil
}

// runStartupCheck executes PRAGMA quick_check or integrity_check and reports the problems it
// found; a single "ok" row means clean. Any other result is damage — returned verbatim for
// the caller to refuse with.
func runStartupCheck(ctx context.Context, db *sql.DB, kind string, clk clock.Clock) (problems []string, took time.Duration, err error) {
	start := clk.Now()
	rows, err := db.QueryContext(ctx, "PRAGMA "+kind)
	if err != nil {
		return nil, 0, fmt.Errorf("run PRAGMA %s: %w", kind, err)
	}
	defer func() {
		if cerr := rows.Close(); cerr != nil {
			err = errors.Join(err, fmt.Errorf("close PRAGMA %s rows: %w", kind, cerr))
		}
	}()

	for rows.Next() {
		var line string
		if scanErr := rows.Scan(&line); scanErr != nil {
			return nil, 0, fmt.Errorf("read PRAGMA %s result row: %w", kind, scanErr)
		}
		if line == "ok" {
			return nil, clk.Since(start), nil
		}
		problems = append(problems, line)
		if len(problems) >= maxReportedCheckProblems() {
			break
		}
	}
	if iterErr := rows.Err(); iterErr != nil {
		return nil, 0, fmt.Errorf("iterate PRAGMA %s results: %w", kind, iterErr)
	}
	if len(problems) == 0 {
		// No rows at all is not success — a healthy check always says "ok".
		return []string{"check produced no result"}, clk.Since(start), nil
	}
	return problems, clk.Since(start), nil
}

// checkpointTruncate runs PRAGMA wal_checkpoint(TRUNCATE) and reports (busy flag, pages moved
// into the database). TRUNCATE also resets the WAL file to zero bytes, giving every session a
// clean, measurable starting baseline.
func checkpointTruncate(ctx context.Context, db *sql.DB) (busy, pages int64, err error) {
	row := db.QueryRowContext(ctx, `PRAGMA wal_checkpoint(TRUNCATE)`)
	if err := row.Scan(&busy, new(int64), &pages); err != nil {
		return 0, 0, fmt.Errorf("wal_checkpoint(TRUNCATE): %w", err)
	}
	return busy, pages, nil
}

// logRecoveryLines emits the structured startup lines the operator sees in place of the
// transcript in PLAN §4.4.
func logRecoveryLines(logger *slog.Logger, nodeID string, from, to int, jitterMs int64, report *RecoveryReport) {
	logger.Info("recovery.migrate", "node", nodeID, "from", from, "to", to)
	if report.Unclean || report.CheckKind != checkSkipped {
		logger.Info("recovery.check",
			"node", nodeID,
			"kind", report.CheckKind,
			"result", "ok",
			"duration_ms", report.CheckDuration.Milliseconds())
	}
	logger.Info("recovery.reclaimed",
		"node", nodeID,
		"count", report.Reclaimed,
		"jitter_ms", jitterMs,
		"dedup_expired", report.DedupExpired)
	logger.Info("recovery.checkpoint",
		"node", nodeID,
		"pages", report.CheckpointPages,
		"wal_bytes", report.WALBytes)
}
