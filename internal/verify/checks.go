// SPDX-License-Identifier: Apache-2.0

package verify

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/a-holm/messq/internal/subject"
)

// The check IDs (S15-stable) and the SQL/Run bodies behind each one. The delivery checks
// (I2/I4/I5/I6/I8) are written now against schema v2 and stay vacuously true until #9–#12
// create the first delivery row — deliberate, so they are green the day that row lands.

const (
	V1  = "V1"
	V2  = "V2"
	V3  = "V3"
	V4  = "V4"
	V5  = "V5"
	V6  = "V6"
	V7  = "V7"
	I2  = "I2"
	I4  = "I4"
	I5  = "I5"
	I6  = "I6"
	I7  = "I7"
	I8  = "I8"
	I10 = "I10"
)

// currentSchemaVersion is the schema version this binary ships (the migration ladder's
// length). It must be bumped alongside every new migrations/*.sql file; V1 refuses anything
// newer so a downgraded binary never misinterprets a future schema.
const currentSchemaVersion = 4

// checkV1 verifies the schema version matches the binary: a newer schema is refused, never
// interpreted (S12 step 2), and an absent or unreadable version is a violation.
func checkV1(ctx context.Context, tx *sql.Tx, _ bool) ([]Violation, error) {
	var raw string
	err := tx.QueryRowContext(ctx, `SELECT v FROM meta WHERE k = 'schema_version'`).Scan(&raw)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		return []Violation{{ID: V1, Detail: "meta.schema_version is absent"}}, nil
	case err != nil:
		return nil, fmt.Errorf("read schema_version: %w", err)
	}
	v, ok := parseVersion(raw)
	if !ok {
		return []Violation{{ID: V1, Detail: fmt.Sprintf("meta.schema_version=%q is not an integer", raw)}}, nil
	}
	switch {
	case v > currentSchemaVersion:
		return []Violation{{ID: V1, Detail: fmt.Sprintf("schema v%d is newer than this binary's v%d; refuse, never interpret", v, currentSchemaVersion)}}, nil
	case v < 1:
		return []Violation{{ID: V1, Detail: fmt.Sprintf("schema_version=%d is not a known messq schema", v)}}, nil
	}
	return nil, nil
}

// parseVersion parses a meta.schema_version value, reporting whether it was an integer.
func parseVersion(raw string) (int, bool) {
	v, err := strconv.Atoi(raw)
	return v, err == nil
}

// checkV2 runs quick_check, upgrading to integrity_check under --deep. Either reports every
// problem row as a violation; the result "ok" means clean.
func checkV2(ctx context.Context, tx *sql.Tx, deep bool) ([]Violation, error) {
	pragma := "quick_check"
	if deep {
		pragma = "integrity_check"
	}
	rows, err := tx.QueryContext(ctx, "PRAGMA "+pragma)
	if err != nil {
		return nil, fmt.Errorf("PRAGMA %s: %w", pragma, err)
	}
	defer func() {
		if cerr := rows.Close(); cerr != nil {
			err = cerr
		}
	}()
	var vs []Violation
	for rows.Next() {
		var r string
		if scanErr := rows.Scan(&r); scanErr != nil {
			return nil, scanErr
		}
		if r != "ok" {
			vs = append(vs, Violation{ID: V2, Detail: r})
		}
	}
	if iterErr := rows.Err(); iterErr != nil {
		return nil, iterErr
	}
	return vs, nil
}

// checkV3 runs foreign_key_check and reports every orphaned reference.
func checkV3(ctx context.Context, tx *sql.Tx, _ bool) ([]Violation, error) {
	rows, err := tx.QueryContext(ctx, "PRAGMA foreign_key_check")
	if err != nil {
		return nil, fmt.Errorf("PRAGMA foreign_key_check: %w", err)
	}
	defer func() {
		if cerr := rows.Close(); cerr != nil {
			err = cerr
		}
	}()
	var vs []Violation
	for rows.Next() {
		// foreign_key_check returns (table, rowid, parent, fkid). Read them dynamically.
		cols, cerr := rows.Columns()
		if cerr != nil {
			return nil, cerr
		}
		vals := make([]any, len(cols))
		ptrs := make([]any, len(cols))
		for i := range vals {
			ptrs[i] = &vals[i]
		}
		if scanErr := rows.Scan(ptrs...); scanErr != nil {
			return nil, scanErr
		}
		vs = append(vs, Violation{ID: V3, Detail: renderRow(cols, vals)})
	}
	if iterErr := rows.Err(); iterErr != nil {
		return nil, iterErr
	}
	return vs, nil
}

// checkV4 reads back the persisted file properties and the durability pragma: journal_mode
// must be WAL, auto_vacuum must be incremental (2), and synchronous must be a valid mode
// (1=NORMAL or 2=FULL). The first two are file-header properties stamped at creation; the
// third is reported so a corrupted connection is not silently accepted.
func checkV4(ctx context.Context, tx *sql.Tx, _ bool) ([]Violation, error) {
	var vs []Violation
	read := func(pragma string) (string, error) {
		var v string
		err := tx.QueryRowContext(ctx, "PRAGMA "+pragma).Scan(&v)
		return v, err
	}
	journal, err := read("journal_mode")
	if err != nil {
		return nil, err
	}
	if !strings.EqualFold(journal, "wal") {
		vs = append(vs, Violation{ID: V4, Detail: fmt.Sprintf("journal_mode=%s, want wal", journal)})
	}
	sync, err := read("synchronous")
	if err != nil {
		return nil, err
	}
	if sync != "1" && sync != "2" {
		vs = append(vs, Violation{ID: V4, Detail: fmt.Sprintf("synchronous=%s, want 1 (NORMAL) or 2 (FULL)", sync)})
	}
	vacuum, err := read("auto_vacuum")
	if err != nil {
		return nil, err
	}
	if vacuum != "2" {
		vs = append(vs, Violation{ID: V4, Detail: fmt.Sprintf("auto_vacuum=%s, want 2 (incremental)", vacuum)})
	}
	return vs, nil
}

// checkV5 verifies message integrity: size equals the body length, the header is valid JSON
// within 4 KiB, and the subject matches one of the stream's accepted patterns. The header
// and subject predicates need Go (json.Valid and the #3 matcher), so this is one message
// scan over the streams' compiled pattern sets.
func checkV5(ctx context.Context, tx *sql.Tx, _ bool) ([]Violation, error) {
	sets := make(map[string]subject.Set)
	srows, err := tx.QueryContext(ctx, `SELECT name, subjects FROM streams`)
	if err != nil {
		return nil, err
	}
	defer func() {
		if cerr := srows.Close(); cerr != nil {
			err = cerr
		}
	}()
	for srows.Next() {
		var name, raw string
		if scanErr := srows.Scan(&name, &raw); scanErr != nil {
			return nil, scanErr
		}
		var pats []string
		if jsonErr := json.Unmarshal([]byte(raw), &pats); jsonErr != nil {
			return []Violation{{ID: V5, Detail: fmt.Sprintf("stream %s subjects are not valid JSON: %v", name, jsonErr)}}, nil
		}
		set, parseErr := subject.ParseSet(pats)
		if parseErr != nil {
			return []Violation{{ID: V5, Detail: fmt.Sprintf("stream %s subjects do not parse: %v", name, parseErr)}}, nil
		}
		sets[name] = set
	}
	if iterErr := srows.Err(); iterErr != nil {
		return nil, iterErr
	}

	var vs []Violation
	mrows, err := tx.QueryContext(ctx, `SELECT stream, seq, subject, size, length(body), hdr FROM messages`)
	if err != nil {
		return nil, err
	}
	defer func() {
		if cerr := mrows.Close(); cerr != nil {
			err = cerr
		}
	}()
	for mrows.Next() {
		var stream, subj string
		var seq, size, actual int64
		var hdr sql.Null[string]
		if scanErr := mrows.Scan(&stream, &seq, &subj, &size, &actual, &hdr); scanErr != nil {
			return nil, scanErr
		}
		if size != actual {
			vs = append(vs, Violation{ID: V5, Detail: fmt.Sprintf("stream=%s seq=%d size=%d != length(body)=%d", stream, seq, size, actual)})
		}
		if hdr.Valid && (len(hdr.V) > 4096 || !json.Valid([]byte(hdr.V))) {
			vs = append(vs, Violation{ID: V5, Detail: fmt.Sprintf("stream=%s seq=%d hdr is %d bytes or not valid JSON", stream, seq, len(hdr.V))})
		}
		if set, ok := sets[stream]; ok && !set.Match(subj) {
			vs = append(vs, Violation{ID: V5, Detail: fmt.Sprintf("stream=%s seq=%d subject=%q matches no accepted pattern", stream, seq, subj)})
		}
	}
	if iterErr := mrows.Err(); iterErr != nil {
		return nil, iterErr
	}
	return vs, nil
}

const v6Query = `SELECT s.stream, s.next, IFNULL(MAX(m.seq), 0) AS max_seq
FROM stream_seq s LEFT JOIN messages m ON m.stream = s.stream
GROUP BY s.stream HAVING s.next <= IFNULL(MAX(m.seq), 0) LIMIT 100`

const i2Query = `SELECT d.stream, d.consumer, d.seq, d.state FROM deliveries d
LEFT JOIN messages m ON m.stream = d.stream AND m.seq = d.seq
WHERE m.seq IS NULL OR d.state NOT IN (0, 1) LIMIT 100`

const i4Query = `SELECT d.stream, d.consumer, d.seq, d.attempts, c.max_deliver
FROM deliveries d JOIN consumers c ON c.stream = d.stream AND c.name = d.consumer
WHERE c.max_deliver > 0 AND d.attempts > c.max_deliver LIMIT 100`

const i5Query = `SELECT d.stream, d.consumer, COUNT(*) AS pending, c.max_ack_pending
FROM deliveries d JOIN consumers c ON c.stream = d.stream AND c.name = d.consumer
GROUP BY d.stream, d.consumer HAVING pending > c.max_ack_pending LIMIT 100`

// checkI6 verifies the cursor bounds: cursor_seq never passes the stream's allocator, and no
// delivery row sits at or beyond the consumer's cursor (resolved rows are deleted, so the
// cursor must be strictly ahead of every pending row).
func checkI6(ctx context.Context, tx *sql.Tx, _ bool) ([]Violation, error) {
	v1, err := runQueryCheck(ctx, tx, I6, `SELECT c.stream, c.name, c.cursor_seq, s.next
FROM consumers c JOIN stream_seq s ON s.stream = c.stream
WHERE c.cursor_seq > s.next`, 100)
	if err != nil {
		return nil, err
	}
	v2, err := runQueryCheck(ctx, tx, I6, `SELECT d.stream, d.consumer, d.seq, c.cursor_seq
FROM deliveries d JOIN consumers c ON c.stream = d.stream AND c.name = d.consumer
WHERE d.seq >= c.cursor_seq`, 100)
	if err != nil {
		return nil, err
	}
	return append(v1, v2...), nil
}

// checkI7 is the settle-fence invariant (issue #10 §5.2, S15): no stale-fenced
// ack/nak/term/extend ever mutates a live row. Every settle write repeats the
// generation/attempts fence, so a violation cannot be produced by the settle path —
// it is reported when an out-of-band write (or a planner bug that got past the second
// line of defence) leaves a delivery row that contradicts the fence. The observable
// fingerprints checked here:
//
//   - an INFLIGHT row with attempts = 0 — a claim or settle wrote the row without the
//     post-increment (D6: claims always number the row);
//   - a delivery row whose generation differs from its consumer's current generation —
//     #28 deletes rows when it bumps, so a survivor with a stale generation is an I7 anomaly.
func checkI7(ctx context.Context, tx *sql.Tx, _ bool) ([]Violation, error) {
	v1, err := runQueryCheck(ctx, tx, I7,
		`SELECT stream, consumer, seq, state, attempts FROM deliveries WHERE state = 1 AND attempts < 1 LIMIT 100`, 100)
	if err != nil {
		return nil, err
	}
	v2, err := runQueryCheck(ctx, tx, I7,
		`SELECT d.stream, d.consumer, d.seq, d.generation, c.generation
		   FROM deliveries d JOIN consumers c ON c.stream = d.stream AND c.name = d.consumer
		  WHERE d.generation <> c.generation LIMIT 100`, 100)
	if err != nil {
		return nil, err
	}
	return append(v1, v2...), nil
}
