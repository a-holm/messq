// SPDX-License-Identifier: Apache-2.0

package verify

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
)

// The DLQ invariant checkers (issue #12 §15, registered into the registry). I8 is refined
// per (stream, consumer, seq, generation); P-DLQ1–P-DLQ5 and P-ID1 close the DLQ's
// conservation, provenance, cardinality and identity contracts. Every checker runs inside
// the same read transaction as the rest of the report.

// The DLQ checker IDs (issue #12 §15).
const (
	PDLQ1 = "P-DLQ1"
	PDLQ2 = "P-DLQ2"
	PDLQ3 = "P-DLQ3"
	PDLQ4 = "P-DLQ4"
	PDLQ5 = "P-DLQ5"
	PID1  = "P-ID1"
)

// deadDetail is the slice of a msg.dead event's detail this issue's checkers read.
type deadDetail struct {
	dlq        string // "written" | "dropped" | "origin_missing"
	dlqStream  string
	dlqSeq     int64
	generation int64
}

// parseDeadDetail decodes a msg.dead detail JSON into the fields the checkers need. A
// malformed detail is itself a violation (carried via ok=false).
func parseDeadDetail(raw sql.Null[string]) (d deadDetail, ok bool) {
	if !raw.Valid || raw.V == "" {
		return d, false
	}
	var m map[string]any
	if err := json.Unmarshal([]byte(raw.V), &m); err != nil {
		return d, false
	}
	d.dlq = asString(m["dlq"])
	d.dlqStream = asString(m["dlq_stream"])
	if v, ok := m["dlq_seq"].(float64); ok {
		d.dlqSeq = int64(v)
	}
	if v, ok := m["generation"].(float64); ok {
		d.generation = int64(v)
	}
	return d, true
}

// asString reads a detail value as a string, empty when absent or the wrong type.
func asString(v any) string {
	if s, ok := v.(string); ok {
		return s
	}
	return ""
}

// deadDupeCount reports any (stream, consumer, seq, generation) key that entered DEAD
// more than once, under the given check ID. The I8 per-generation refinement lets a
// legitimate re-death after a seek (which bumps generation) pass while a same-key double
// death without a seek — impossible by construction — is caught.
func deadDupeCount(ctx context.Context, tx *sql.Tx, id string) ([]Violation, error) {
	rows, err := tx.QueryContext(ctx, `
		SELECT stream, consumer, seq, detail FROM events WHERE event = 'msg.dead' ORDER BY id`)
	if err != nil {
		return nil, fmt.Errorf("run %s: %w", id, err)
	}
	defer func() {
		if cerr := rows.Close(); cerr != nil {
			err = fmt.Errorf("close %s rows: %w", id, cerr)
		}
	}()
	type key struct {
		stream, consumer string
		seq, gen         int64
	}
	seen := make(map[key]bool)
	var vs []Violation
	for rows.Next() {
		var stream, consumer string
		var seq sql.Null[int64]
		var detail sql.Null[string]
		if sErr := rows.Scan(&stream, &consumer, &seq, &detail); sErr != nil {
			return nil, sErr
		}
		d, ok := parseDeadDetail(detail)
		if !ok {
			vs = append(vs, Violation{ID: id, Detail: fmt.Sprintf(
				"msg.dead stream=%s consumer=%s seq=%d has an unparseable detail (no generation)", stream, consumer, seq.V)})
			continue
		}
		k := key{stream, consumer, seq.V, d.generation}
		if seen[k] {
			vs = append(vs, Violation{ID: id, Detail: fmt.Sprintf(
				"two msg.dead share (stream=%s consumer=%s seq=%d generation=%d)", stream, consumer, seq.V, d.generation)})
		}
		seen[k] = true
	}
	if rErr := rows.Err(); rErr != nil {
		return nil, rErr
	}
	return vs, nil
}

// checkI8 reports DEAD-entered-more-than-once as the I8 conservation invariant.
func checkI8(ctx context.Context, tx *sql.Tx, _ bool) ([]Violation, error) {
	return deadDupeCount(ctx, tx, I8)
}

// checkP_DLQ1: every msg.dead{detail.dlq="written"} in the event window has its DLQ row,
// or the DLQ stream's first_seq has advanced past it (retention/purge) — in which case
// the copy is legitimately gone.
func checkP_DLQ1(ctx context.Context, tx *sql.Tx, _ bool) ([]Violation, error) {
	rows, err := tx.QueryContext(ctx, `
		SELECT stream, consumer, seq, detail FROM events WHERE event = 'msg.dead' ORDER BY id`)
	if err != nil {
		return nil, fmt.Errorf("run %s: %w", PDLQ1, err)
	}
	defer func() {
		if cerr := rows.Close(); cerr != nil {
			err = fmt.Errorf("close %s rows: %w", PDLQ1, cerr)
		}
	}()
	var vs []Violation
	for rows.Next() {
		var stream, consumer string
		var seq sql.Null[int64]
		var detail sql.Null[string]
		if sErr := rows.Scan(&stream, &consumer, &seq, &detail); sErr != nil {
			return nil, sErr
		}
		d, ok := parseDeadDetail(detail)
		if !ok || d.dlq != "written" {
			continue
		}
		// The row exists, or first_seq advanced past it.
		var exists int
		if eErr := tx.QueryRowContext(ctx,
			`SELECT count(*) FROM messages WHERE stream = ? AND seq = ?`, d.dlqStream, d.dlqSeq).Scan(&exists); eErr != nil {
			return nil, eErr
		}
		if exists == 1 {
			continue
		}
		var first sql.Null[int64]
		if fErr := tx.QueryRowContext(ctx,
			`SELECT min(seq) FROM messages WHERE stream = ?`, d.dlqStream).Scan(&first); fErr != nil {
			return nil, fErr
		}
		if first.Valid && first.V > d.dlqSeq {
			continue // retention/purge advanced the DLQ's first_seq past the copy
		}
		vs = append(vs, Violation{ID: PDLQ1, Detail: fmt.Sprintf(
			"msg.dead stream=%s consumer=%s seq=%d claims written copy at %s/%d, but no row and first_seq has not passed it",
			stream, consumer, seq.V, d.dlqStream, d.dlqSeq)})
	}
	if rErr := rows.Err(); rErr != nil {
		return nil, rErr
	}
	return vs, nil
}

// checkP_DLQ2: every row in a .dlq stream carries a complete, parseable provenance set.
func checkP_DLQ2(ctx context.Context, tx *sql.Tx, _ bool) ([]Violation, error) {
	rows, err := tx.QueryContext(ctx, `
		SELECT stream, seq, coalesce(hdr,'') FROM messages WHERE stream LIKE '%.dlq' ORDER BY stream, seq`)
	if err != nil {
		return nil, fmt.Errorf("run %s: %w", PDLQ2, err)
	}
	defer func() {
		if cerr := rows.Close(); cerr != nil {
			err = fmt.Errorf("close %s rows: %w", PDLQ2, cerr)
		}
	}()
	required := []string{
		"Messq-Origin-Id", "Messq-Origin-Stream", "Messq-Origin-Seq",
		"Messq-Origin-Consumer", "Messq-Attempts", "Messq-Cause", "Messq-Dead-At",
	}
	var vs []Violation
	for rows.Next() {
		var stream string
		var seq int64
		var hdrRaw string
		if sErr := rows.Scan(&stream, &seq, &hdrRaw); sErr != nil {
			return nil, sErr
		}
		var hdr map[string]string
		if hdrRaw == "" || json.Unmarshal([]byte(hdrRaw), &hdr) != nil {
			vs = append(vs, Violation{ID: PDLQ2, Detail: fmt.Sprintf(
				"stream=%s seq=%d hdr is empty or not a JSON object (must carry provenance)", stream, seq)})
			continue
		}
		for _, k := range required {
			if hdr[k] == "" {
				vs = append(vs, Violation{ID: PDLQ2, Detail: fmt.Sprintf(
					"stream=%s seq=%d missing provenance header %q", stream, seq, k)})
			}
		}
	}
	if rErr := rows.Err(); rErr != nil {
		return nil, rErr
	}
	return vs, nil
}

// pdlq3Query: no consumer on a .dlq stream carries dead_policy=dlq (#9 forces drop there;
// a dlq-of-a-dlq would be a chain, which must not exist).
const pdlq3Query = `SELECT c.stream, c.name FROM consumers c
 WHERE c.stream LIKE '%.dlq' AND c.dead_policy = 'dlq' LIMIT 100`

// checkP_DLQ4: zero msg.dead{detail.dlq="origin_missing"} events — any occurrence is a
// bug signal (a death raced a purge).
func checkP_DLQ4(ctx context.Context, tx *sql.Tx, _ bool) ([]Violation, error) {
	rows, err := tx.QueryContext(ctx, `
		SELECT stream, consumer, seq, detail FROM events WHERE event = 'msg.dead' ORDER BY id`)
	if err != nil {
		return nil, fmt.Errorf("run %s: %w", PDLQ4, err)
	}
	defer func() {
		if cerr := rows.Close(); cerr != nil {
			err = fmt.Errorf("close %s rows: %w", PDLQ4, cerr)
		}
	}()
	var vs []Violation
	for rows.Next() {
		var stream, consumer string
		var seq sql.Null[int64]
		var detail sql.Null[string]
		if sErr := rows.Scan(&stream, &consumer, &seq, &detail); sErr != nil {
			return nil, sErr
		}
		d, ok := parseDeadDetail(detail)
		if ok && d.dlq == "origin_missing" {
			vs = append(vs, Violation{ID: PDLQ4, Detail: fmt.Sprintf(
				"msg.dead stream=%s consumer=%s seq=%d is origin_missing (a death raced a purge)", stream, consumer, seq.V)})
		}
	}
	if rErr := rows.Err(); rErr != nil {
		return nil, rErr
	}
	return vs, nil
}

// checkP_DLQ5 restates the I8 double-death invariant under its own P-DLQ5 ID.
func checkP_DLQ5(ctx context.Context, tx *sql.Tx, _ bool) ([]Violation, error) {
	return deadDupeCount(ctx, tx, PDLQ5)
}

// pid1Query: no non-.dlq stream holds two messages rows with the same id. Duplicate ids
// are legal only inside a DLQ (two consumers / generations dead-lettering one origin);
// anywhere else a shared id is corruption (C1: ids are globally unique across streams).
const pid1Query = `SELECT id, COUNT(*) AS n, GROUP_CONCAT(stream) AS streams FROM messages
 WHERE substr(stream, -4) != '.dlq'
 GROUP BY id HAVING n > 1 LIMIT 100`
