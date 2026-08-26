// SPDX-License-Identifier: Apache-2.0

package verify

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
)

// checkPTS1 walks every stream's messages in seq order and flags any row whose
// published_at sits before its predecessor's — a published_at inversion. It is
// a Go walker rather than one SQL statement because the property is about
// consecutive retained rows (adjacency by seq order among live rows), which a
// self-join on seq+1 cannot express across retention gaps.
//
// Monotonicity holds per stream, never globally: timestamps of different
// streams are incomparable. Retention deletes the oldest rows first, so a
// trimmed suffix stays monotone; the DLQ copy stamps published_at = now, so
// copies are monotone within their own target stream (#28 extends this to
// replay copies for free).
func checkPTS1(ctx context.Context, tx *sql.Tx, _ bool) (out []Violation, retErr error) {
	rows, err := tx.QueryContext(ctx,
		`SELECT stream, seq, published_at FROM messages ORDER BY stream, seq`)
	if err != nil {
		return nil, fmt.Errorf("P-TS1 scan: %w", err)
	}
	defer func() {
		if cerr := rows.Close(); cerr != nil {
			retErr = errors.Join(retErr, fmt.Errorf("close P-TS1 rows: %w", cerr))
		}
	}()

	var (
		curStream string
		prevPub   int64
	)
	for rows.Next() {
		var (
			stream string
			seq    int64
			pub    int64
		)
		if sErr := rows.Scan(&stream, &seq, &pub); sErr != nil {
			return nil, fmt.Errorf("P-TS1 scan row: %w", sErr)
		}
		if stream != curStream {
			// First row of a stream (stream names are never empty, so the
			// zero curStream never matches a real one).
			curStream, prevPub = stream, pub
			continue
		}
		if pub < prevPub {
			out = append(out, Violation{
				ID: PTS1,
				Detail: fmt.Sprintf(
					"published_at inversion: stream=%s seq=%d published_at=%d follows %d",
					stream, seq, pub, prevPub),
			})
		}
		prevPub = pub
	}
	if rErr := rows.Err(); rErr != nil {
		return nil, fmt.Errorf("P-TS1 iterate: %w", rErr)
	}
	return out, nil
}
