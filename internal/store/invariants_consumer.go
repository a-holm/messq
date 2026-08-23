// SPDX-License-Identifier: Apache-2.0

package store

import (
	"context"
	"database/sql"
	"fmt"
)

// Consumer invariant checkers C1–C6 (issue #9 §12). Each sharpens a normative I*
// invariant the moment the deliveries table can be non-empty. They are numbered C* so
// they never collide with the normative I* IDs, and each is a single SQL statement so
// #13 can reuse them verbatim as a rapid invariant hook.
//
// Registration seam (issue #8): these belong in internal/verify's registry alongside
// CheckPublishInvariants, so `messq verify` runs them. When #8 lands, register
// CheckConsumerInvariants there; until then it lives here as a Store method with the
// same shape as CheckPublishInvariants.

// CheckConsumerInvariants audits every consumer against C1–C6 and returns all
// violations found (not just the first):
//
//	C1  no delivery row sits at seq >= its consumer's cursor_seq (sharpens I6);
//	C2  every delivery row references its message, and deliveries.subject equals it (I2);
//	C3  every delivery row's generation equals its consumer's generation (I7 fencing);
//	C4  1 <= cursor_seq <= stream_seq.next for every consumer (I6);
//	C5  pending(c) <= max_ack_pending(c) — advisory when the excess is pure shrink
//	    residue (INFLIGHT/in-backoff rows), a violation when READY rows exceed the
//	    bound (an admission bug);
//	C6  row well-formedness (I4).
func (s *Store) CheckConsumerInvariants(ctx context.Context) ([]Violation, error) {
	var out []Violation

	// C1: the cursor dominates every delivery row.
	vs, err := s.runViolationQuery(ctx, `
		SELECT d.stream, d.consumer, d.seq, c.cursor_seq
		  FROM deliveries d JOIN consumers c ON c.stream = d.stream AND c.name = d.consumer
		 WHERE d.seq >= c.cursor_seq LIMIT 100`, func(scan func(dest ...any) error) (Violation, error) {
		var stream, consumer string
		var seq, cursor int64
		if scanErr := scan(&stream, &consumer, &seq, &cursor); scanErr != nil {
			return Violation{}, scanErr
		}
		return Violation{
			ID: "C1", Stream: stream, Consumer: consumer,
			Detail: fmt.Sprintf("delivery seq %d >= cursor_seq %d", seq, cursor),
		}, nil
	})
	if err != nil {
		return nil, err
	}
	out = append(out, vs...)

	// C2a: a delivery row with no message.
	vs, err = s.runViolationQuery(ctx, `
		SELECT d.stream, d.consumer, d.seq
		  FROM deliveries d LEFT JOIN messages m ON m.stream = d.stream AND m.seq = d.seq
		 WHERE m.seq IS NULL LIMIT 100`, func(scan func(dest ...any) error) (Violation, error) {
		var stream, consumer string
		var seq int64
		if scanErr := scan(&stream, &consumer, &seq); scanErr != nil {
			return Violation{}, scanErr
		}
		return Violation{
			ID: "C2", Stream: stream, Consumer: consumer,
			Detail: fmt.Sprintf("delivery seq %d has no message row", seq),
		}, nil
	})
	if err != nil {
		return nil, err
	}
	out = append(out, vs...)

	// C2b: deliveries.subject disagrees with the message's subject.
	vs, err = s.runViolationQuery(ctx, `
		SELECT d.stream, d.consumer, d.seq, d.subject, m.subject
		  FROM deliveries d JOIN messages m ON m.stream = d.stream AND m.seq = d.seq
		 WHERE d.subject <> m.subject LIMIT 100`, func(scan func(dest ...any) error) (Violation, error) {
		var stream, consumer, dsubj, msubj string
		var seq int64
		if scanErr := scan(&stream, &consumer, &seq, &dsubj, &msubj); scanErr != nil {
			return Violation{}, scanErr
		}
		return Violation{
			ID: "C2", Stream: stream, Consumer: consumer,
			Detail: fmt.Sprintf("delivery seq %d subject %q != message subject %q", seq, dsubj, msubj),
		}, nil
	})
	if err != nil {
		return nil, err
	}
	out = append(out, vs...)

	// C3: generation mismatch.
	vs, err = s.runViolationQuery(ctx, `
		SELECT d.stream, d.consumer, d.seq, d.generation, c.generation
		  FROM deliveries d JOIN consumers c ON c.stream = d.stream AND c.name = d.consumer
		 WHERE d.generation <> c.generation LIMIT 100`, func(scan func(dest ...any) error) (Violation, error) {
		var stream, consumer string
		var seq, dgen, cgen int64
		if scanErr := scan(&stream, &consumer, &seq, &dgen, &cgen); scanErr != nil {
			return Violation{}, scanErr
		}
		return Violation{
			ID: "C3", Stream: stream, Consumer: consumer,
			Detail: fmt.Sprintf("delivery seq %d generation %d != consumer generation %d", seq, dgen, cgen),
		}, nil
	})
	if err != nil {
		return nil, err
	}
	out = append(out, vs...)

	// C4: cursor within [1, stream_seq.next].
	vs, err = s.runViolationQuery(ctx, `
		SELECT c.stream, c.name, c.cursor_seq, coalesce((SELECT next FROM stream_seq s WHERE s.stream = c.stream), 0)
		  FROM consumers c
		 WHERE c.cursor_seq < 1 OR c.cursor_seq > (SELECT next FROM stream_seq s WHERE s.stream = c.stream)
		 LIMIT 100`, func(scan func(dest ...any) error) (Violation, error) {
		var stream, name string
		var cursor, next int64
		if scanErr := scan(&stream, &name, &cursor, &next); scanErr != nil {
			return Violation{}, scanErr
		}
		return Violation{
			ID: "C4", Stream: stream, Consumer: name,
			Detail: fmt.Sprintf("cursor_seq %d outside [1, %d]", cursor, next),
		}, nil
	})
	if err != nil {
		return nil, err
	}
	out = append(out, vs...)

	// C5: pending vs bound.
	vs, err = s.runViolationQuery(ctx, `
		SELECT c.stream, c.name, count(d.seq) AS pending,
		       coalesce(sum(d.state = 0), 0) AS ready, c.max_ack_pending
		  FROM consumers c LEFT JOIN deliveries d ON d.stream = c.stream AND d.consumer = c.name
		 GROUP BY c.stream, c.name
		HAVING pending > c.max_ack_pending LIMIT 100`, func(scan func(dest ...any) error) (Violation, error) {
		var stream, name string
		var pending, ready, bound int64
		if scanErr := scan(&stream, &name, &pending, &ready, &bound); scanErr != nil {
			return Violation{}, scanErr
		}
		// READY rows above the bound is an admission bug; only-INFLIGHT excess is the
		// deliberate shrink residue (advisory).
		return Violation{
			ID: "C5", Stream: stream, Consumer: name, Advisory: ready <= bound,
			Detail: fmt.Sprintf("pending %d exceeds max_ack_pending %d", pending, bound),
		}, nil
	})
	if err != nil {
		return nil, err
	}
	out = append(out, vs...)

	// C6: row well-formedness.
	vs, err = s.runViolationQuery(ctx, `
		SELECT d.stream, d.consumer, d.seq, d.state, d.attempts, d.delivered_at, d.visible_at,
		       c.max_deliver
		  FROM deliveries d JOIN consumers c ON c.stream = d.stream AND c.name = d.consumer
		 WHERE d.state NOT IN (0, 1)
		    OR d.attempts < 0
		    OR (c.max_deliver > 0 AND d.attempts > c.max_deliver)
		    OR (d.state = 1 AND (d.delivered_at IS NULL OR d.visible_at <= d.delivered_at))
		    OR (d.state = 0 AND d.delivered_at IS NOT NULL AND d.attempts = 0)
		 LIMIT 100`, func(scan func(dest ...any) error) (Violation, error) {
		var stream, consumer string
		var seq, state, attempts, maxDeliver int64
		var deliveredAt, visibleAt sql.Null[int64]
		if scanErr := scan(&stream, &consumer, &seq, &state, &attempts, &deliveredAt, &visibleAt, &maxDeliver); scanErr != nil {
			return Violation{}, scanErr
		}
		return Violation{
			ID: "C6", Stream: stream, Consumer: consumer,
			Detail: fmt.Sprintf("delivery seq %d: state %d attempts %d delivered_at %v visible_at %v max_deliver %d",
				seq, state, attempts, deliveredAt.V, visibleAt.V, maxDeliver),
		}, nil
	})
	if err != nil {
		return nil, err
	}
	out = append(out, vs...)

	return out, nil
}

// runViolationQuery runs one checker query on the read pool, calling f to turn each row
// into a Violation. The rows handle is closed via defer, mirroring
// CheckPublishInvariants' read-path discipline.
func (s *Store) runViolationQuery(ctx context.Context, query string, f func(scan func(dest ...any) error) (Violation, error)) ([]Violation, error) {
	rows, err := s.readPool().QueryContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("query violations: %w", err)
	}
	defer func() {
		if cerr := rows.Close(); cerr != nil {
			s.logger.Warn("store.CheckConsumerInvariants", "error", cerr.Error())
		}
	}()
	var out []Violation
	for rows.Next() {
		v, scanErr := f(rows.Scan)
		if scanErr != nil {
			return nil, fmt.Errorf("scan violation row: %w", scanErr)
		}
		out = append(out, v)
	}
	if rErr := rows.Err(); rErr != nil {
		return nil, fmt.Errorf("iterate violation rows: %w", rErr)
	}
	return out, nil
}
