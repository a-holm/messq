// SPDX-License-Identifier: Apache-2.0

package verify

import (
	"context"
	"database/sql"
	"fmt"
)

// The sweeper invariant checkers (issue #11 §13, registered into the registry):
//
//   - S1 — "no expired INFLIGHT row survives a sweep" (I4 liveness), advisory: it is a
//     liveness property, and a saturated writer is late, not wrong. It needs a wall-clock
//     reference, so it uses the newest event ts as a now-proxy rather than a clock (verify
//     runs clock-free).
//   - S2 — no READY row stranded at attempts > max_deliver that the retire pass has not
//     collected (I4). Pure SQL, exactly the issue's §13 query.
//   - S3 — every delivery row's generation equals its consumer's (restates #9's C3 /
//     I7's second half; re-checked after every sweep because the sweep is the code that
//     could violate it).
const (
	S1 = "S1"
	S2 = "S2"
	S3 = "S3"
)

// checkS1 reports expired-but-unswept INFLIGHT rows as an advisory liveness finding.
// now-proxy = the newest events.ts (the last durable state change); a row INFLIGHT with
// visible_at <= that is past its deadline and has not been swept. Advisory: reported, not
// necessarily a corruption — a saturated writer is late, not wrong.
func checkS1(ctx context.Context, tx *sql.Tx, _ bool) ([]Violation, error) {
	var now int64
	if err := tx.QueryRowContext(ctx,
		`SELECT COALESCE(MAX(ts), 0) FROM events`).Scan(&now); err != nil {
		return nil, fmt.Errorf("read events max ts: %w", err)
	}
	rows, err := tx.QueryContext(ctx, `
SELECT d.stream, d.consumer, d.seq, d.visible_at
  FROM deliveries d
 WHERE d.state = 1 AND d.visible_at <= ?
 LIMIT 100`, now)
	if err != nil {
		return nil, fmt.Errorf("run %s: %w", S1, err)
	}
	defer func() {
		if cerr := rows.Close(); cerr != nil {
			err = fmt.Errorf("close %s rows: %w", S1, cerr)
		}
	}()
	var vs []Violation
	for rows.Next() {
		var stream, consumer string
		var seq, visAt int64
		if sErr := rows.Scan(&stream, &consumer, &seq, &visAt); sErr != nil {
			return nil, sErr
		}
		vs = append(vs, Violation{ID: S1, Detail: fmt.Sprintf(
			"stream=%s consumer=%s seq=%d visible_at=%d expired but unswept (now_proxy=%d)",
			stream, consumer, seq, visAt, now)})
	}
	if rErr := rows.Err(); rErr != nil {
		return nil, rErr
	}
	return vs, nil
}

// s2Query is the issue §13's S2 query verbatim: a READY row stranded above max_deliver
// that the retire pass has not yet collected.
const s2Query = `SELECT d.stream, d.consumer, d.seq, d.attempts, c.max_deliver
  FROM deliveries d JOIN consumers c ON c.stream = d.stream AND c.name = d.consumer
 WHERE c.max_deliver > 0 AND d.attempts > c.max_deliver AND d.state = 0
 LIMIT 100`

// s3Query restates I7's generation half, scoped to every delivery row.
const s3Query = `SELECT d.stream, d.consumer, d.seq, d.generation, c.generation
   FROM deliveries d JOIN consumers c ON c.stream = d.stream AND c.name = d.consumer
  WHERE d.generation <> c.generation LIMIT 100`
