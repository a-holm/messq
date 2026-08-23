// SPDX-License-Identifier: Apache-2.0

package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
)

// Publish-path invariant checkers (issue §10), exported for `messq verify` (#8) and
// the reference model (#13). Every check runs on the read-only pool; a violation is
// reported with its stream and a detail naming the offending boundary, never repaired.

// Violation is one broken invariant.
type Violation struct {
	ID       string `json:"id"`                 // P1..P5, C1..C6: the closed set from issue §10/#9 §12
	Stream   string `json:"stream"`             // "" for store-wide findings
	Consumer string `json:"consumer,omitempty"` // the consumer, for C* findings
	Detail   string `json:"detail"`
	// Advisory marks a finding the operator is expected to see but that is not a bug:
	// the shrink residue after lowering max_ack_pending below the pending set (I5).
	Advisory bool `json:"advisory,omitempty"`
}

// CheckPublishInvariants audits every live stream against P1–P5 and returns all
// violations found (not just the first):
//
//	P1  per-stream sequences are gap-free within [min(seq), max(seq)];
//	P2  every messages.seq < its stream_seq.next, and next stays above any
//	    recorded seq_hwm (delete + recreate never reuses numbers);
//	P3  at most one row per non-NULL dedup key, and no key older than its
//	    window plus one sweep interval;
//	P4  published_at is non-decreasing in seq order within a stream;
//	P5  size = length(body) on every row, and stream_stats equals a full scan.
func (s *Store) CheckPublishInvariants(ctx context.Context) ([]Violation, error) {
	ro := s.readPool()
	rows, err := ro.QueryContext(ctx, `SELECT name FROM streams ORDER BY name`)
	if err != nil {
		return nil, fmt.Errorf("list streams: %w", err)
	}
	defer func() {
		if cerr := rows.Close(); cerr != nil {
			s.logger.Warn("store.CheckPublishInvariants", "error", cerr.Error())
		}
	}()
	var names []string
	for rows.Next() {
		var n string
		if err := rows.Scan(&n); err != nil {
			return nil, fmt.Errorf("scan stream names: %w", err)
		}
		names = append(names, n)
	}
	if rErr := rows.Err(); rErr != nil {
		return nil, fmt.Errorf("iterate stream names: %w", rErr)
	}

	var out []Violation
	for _, name := range names {
		vs, cErr := s.checkStreamInvariants(ctx, ro, name)
		if cErr != nil {
			return nil, cErr
		}
		out = append(out, vs...)
	}
	storeWide, wErr := s.checkStoreWideInvariants(ctx, ro, names)
	if wErr != nil {
		return nil, wErr
	}
	return append(out, storeWide...), nil
}

// checkStreamInvariants runs the per-stream legs (P1, P4 and the per-stream halves of
// P2/P3/P5) in three ordered scans over one stream's rows.
func (s *Store) checkStreamInvariants(ctx context.Context, ro *sql.DB, name string) ([]Violation, error) {
	var window int64
	err := ro.QueryRowContext(ctx,
		`SELECT dedup_window_ms FROM streams WHERE name = ?`, name).Scan(&window)
	if err != nil {
		return nil, fmt.Errorf("read window of %q: %w", name, err)
	}

	rows, qErr := ro.QueryContext(ctx, `
		SELECT m.seq, m.published_at, m.size, length(m.body), m.dedup_key
		 FROM messages m WHERE m.stream = ?1 ORDER BY m.seq`, name)
	if qErr != nil {
		return nil, fmt.Errorf("scan messages of %q: %w", name, qErr)
	}
	defer func() {
		if cerr := rows.Close(); cerr != nil {
			s.logger.Warn("store.CheckPublishInvariants", "error", cerr.Error())
		}
	}()

	var out []Violation
	prevSeq, prevPub := int64(-1), int64(-1)
	var msgs int64
	var bytesSum int64
	for rows.Next() {
		var seq, pub, size, bodyLen int64
		var key sql.Null[string]
		if sErr := rows.Scan(&seq, &pub, &size, &bodyLen, &key); sErr != nil {
			return nil, fmt.Errorf("scan message of %q: %w", name, sErr)
		}
		msgs++
		bytesSum += size

		if prevSeq >= 0 && seq != prevSeq+1 { // P1: strictly increasing, gap-free
			out = append(out, Violation{
				ID: "P1", Stream: name,
				Detail: fmt.Sprintf("gap between seq %d and %d", prevSeq, seq),
			})
		}
		if prevPub >= 0 && pub < prevPub { // P4: monotone published_at in seq order
			out = append(out, Violation{
				ID: "P4", Stream: name,
				Detail: fmt.Sprintf("published_at goes backwards from seq %d to %d",
					prevSeq, seq),
			})
		}
		if size != bodyLen { // P5a: size must equal length(body)
			out = append(out, Violation{
				ID: "P5", Stream: name,
				Detail: fmt.Sprintf("seq %d has size %d but body length %d", seq, size, bodyLen),
			})
		}
		if key.Valid && window > 0 &&
			pub < nowMS(s.clk)-window-s.dedupSweep.Milliseconds() { // P3b: expiry bound
			out = append(out, Violation{
				ID: "P3", Stream: name,
				Detail: fmt.Sprintf("dedup key at seq %d outlived its window by %d ms",
					seq, nowMS(s.clk)-window-s.dedupSweep.Milliseconds()-pub),
			})
		}
		prevSeq, prevPub = seq, pub
	}
	if rErr := rows.Err(); rErr != nil {
		return nil, fmt.Errorf("iterate messages of %q: %w", name, rErr)
	}

	// P2a/P2b: the counter exists, sits above every stored row, and above the
	// high-water mark this name left behind at any past delete.
	var liveNext sql.Null[int64]
	if err := ro.QueryRowContext(ctx,
		`SELECT next FROM stream_seq WHERE stream = ?`, name).Scan(&liveNext); err != nil &&
		!isNoRowsErr(err) {
		return nil, fmt.Errorf("read counter of %q: %w", name, err)
	}
	if !liveNext.Valid {
		out = append(out, Violation{
			ID: "P2", Stream: name,
			Detail: "stream_seq row missing",
		})
	} else {
		if prevSeq >= liveNext.V {
			out = append(out, Violation{
				ID: "P2", Stream: name,
				Detail: fmt.Sprintf("max seq %d >= stream_seq.next %d", prevSeq, liveNext.V),
			})
		}
		var hwm sql.Null[string]
		if err := ro.QueryRowContext(ctx,
			`SELECT v FROM meta WHERE k = ?`, metaSeqHwmPrefix+name).Scan(&hwm); err != nil &&
			!isNoRowsErr(err) {
			return nil, fmt.Errorf("read hwm of %q: %w", name, err)
		}
		if hwm.Valid {
			var recorded int64
			if _, pErr := fmt.Sscanf(hwm.V, "%d", &recorded); pErr == nil && liveNext.V <= recorded {
				out = append(out, Violation{
					ID: "P2", Stream: name,
					Detail: fmt.Sprintf("stream_seq.next %d reused deleted hwm %d",
						liveNext.V, recorded),
				})
			}
		}
	}

	// P5b: the counters equal a full scan of this stream's rows.
	var statsMsgs, statsBytes sql.Null[int64]
	if err := ro.QueryRowContext(ctx,
		`SELECT msgs, bytes FROM stream_stats WHERE stream = ?`, name,
	).Scan(&statsMsgs, &statsBytes); err != nil && !isNoRowsErr(err) {
		return nil, fmt.Errorf("read stats of %q: %w", name, err)
	}
	if statsMsgs.Valid && (statsMsgs.V != msgs || statsBytes.V != bytesSum) {
		out = append(out, Violation{
			ID: "P5", Stream: name,
			Detail: fmt.Sprintf("stream_stats reports %d msgs / %d bytes, scan found %d / %d",
				statsMsgs.V, statsBytes.V, msgs, bytesSum),
		})
	}
	return out, nil
}

// checkStoreWideInvariants covers what no single stream can see: duplicate dedup
// keys across or inside streams (P3a).
func (s *Store) checkStoreWideInvariants(ctx context.Context, ro *sql.DB, _ []string) ([]Violation, error) {
	rows, err := ro.QueryContext(ctx, `
		SELECT stream, dedup_key, count(*) AS n FROM messages
		 WHERE dedup_key IS NOT NULL
		 GROUP BY stream, dedup_key HAVING n > 1`)
	if err != nil {
		return nil, fmt.Errorf("group dedup keys: %w", err)
	}
	defer func() {
		if cerr := rows.Close(); cerr != nil {
			s.logger.Warn("store.CheckPublishInvariants", "error", cerr.Error())
		}
	}()
	var out []Violation
	for rows.Next() {
		var stream, key string
		var n int64
		if sErr := rows.Scan(&stream, &key, &n); sErr != nil {
			return nil, fmt.Errorf("scan duplicate keys: %w", sErr)
		}
		out = append(out, Violation{
			ID: "P3", Stream: stream,
			Detail: fmt.Sprintf("key %q occurs %d times (unique index bypassed?)", key, n),
		})
	}
	return out, rows.Err()
}

func isNoRowsErr(err error) bool { return errors.Is(err, sql.ErrNoRows) }
