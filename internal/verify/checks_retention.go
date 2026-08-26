// SPDX-License-Identifier: Apache-2.0

package verify

import (
	"context"
	"database/sql"
	"fmt"
)

// V7 (#27 §4 Decision 2, G3): stream_stats equals a full scan per stream. The counters
// are what discard=new's exact O(1) publish-time check reads in-transaction, so drift is
// never merely cosmetic — it silently disables a guarantee. Deep-only: it is O(rows).
//
// Three violation shapes:
//   - counters disagree with the scan (the classic maintenance bug),
//   - a stream with no stats row at all (created outside the seeded paths),
//   - a stats row naming no live stream (orphan; FK cascade should prevent it).
func checkV7(ctx context.Context, tx *sql.Tx, _ bool) ([]Violation, error) {
	const driftQuery = `
	  WITH scan AS (
	    SELECT s.name AS stream,
	           COUNT(m.seq)              AS msgs,
	           COALESCE(SUM(m.size), 0)  AS bytes
	      FROM streams s
	      LEFT JOIN messages m ON m.stream = s.name
	     GROUP BY s.name
	  )
	  SELECT scan.stream, scan.msgs, scan.bytes, ss.msgs, ss.bytes
	    FROM scan
	    LEFT JOIN stream_stats ss ON ss.stream = scan.stream
	   WHERE ss.stream IS NULL
	      OR ss.msgs IS NULL
	      OR ss.msgs <> scan.msgs
	      OR ss.bytes <> scan.bytes`

	rows, err := tx.QueryContext(ctx, driftQuery)
	if err != nil {
		return nil, fmt.Errorf("V7 counter drift query: %w", err)
	}
	defer func() {
		if cerr := rows.Close(); cerr != nil {
			_ = cerr // read-only rows on a fenced connection; nothing to recover
		}
	}()
	var vs []Violation
	for rows.Next() {
		var stream string
		var msgs, bytes sql.Null[int64]
		var sMsgs, sBytes sql.Null[int64]
		if scanErr := rows.Scan(&stream, &msgs, &bytes, &sMsgs, &sBytes); scanErr != nil {
			return nil, fmt.Errorf("V7 scan drift row: %w", scanErr)
		}
		switch {
		case !sMsgs.Valid:
			vs = append(vs, Violation{ID: V7, Detail: fmt.Sprintf(
				"stream %s has no stream_stats row (scan found %d msgs / %d bytes)", stream, msgs.V, bytes.V)})
		default:
			vs = append(vs, Violation{ID: V7, Detail: fmt.Sprintf(
				"stream %s stats report %d msgs / %d bytes, scan found %d / %d",
				stream, sMsgs.V, sBytes.V, msgs.V, bytes.V)})
		}
	}
	if rowErr := rows.Err(); rowErr != nil {
		return nil, fmt.Errorf("V7 counter drift rows: %w", rowErr)
	}

	const orphanQuery = `
	  SELECT ss.stream FROM stream_stats ss
	   WHERE NOT EXISTS (SELECT 1 FROM streams s WHERE s.name = ss.stream)`
	orphanRows, err := tx.QueryContext(ctx, orphanQuery)
	if err != nil {
		return nil, fmt.Errorf("V7 orphan query: %w", err)
	}
	defer func() {
		if cerr := orphanRows.Close(); cerr != nil {
			_ = cerr
		}
	}()
	for orphanRows.Next() {
		var stream string
		if err := orphanRows.Scan(&stream); err != nil {
			return nil, fmt.Errorf("V7 orphan row: %w", err)
		}
		vs = append(vs, Violation{ID: V7, Detail: fmt.Sprintf(
			"stream_stats row names no live stream: %s", stream)})
	}
	if err := orphanRows.Err(); err != nil {
		return nil, fmt.Errorf("V7 orphan rows: %w", err)
	}
	return vs, nil
}
