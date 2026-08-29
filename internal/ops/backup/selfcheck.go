// SPDX-License-Identifier: Apache-2.0

package backup

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"

	// The driver registration is borrowed from internal/store (see
	// internal/verify/open.go for the same pattern): the engine import stays
	// inside the store package.
	_ "github.com/a-holm/messq/internal/store"
)

// SelfExpectations carries what the snapshot must agree with: the source's
// identity and pragmas as read at Plan time, plus the per-stream heads recorded
// on the snapshot connection just before the VACUUM INTO.
type SelfExpectations struct {
	Verify       VerifyMode
	SchemaVer    int              // source meta.schema_version
	UserVersion  int64            // source PRAGMA user_version (the mirror)
	PageSize     int64            // source PRAGMA page_size
	AutoVacuum   int64            // source PRAGMA auto_vacuum
	RecordedHead map[string]int64 // stream → last seq at snapshot start
}

// SelfCheckError is the aggregated verdict of a failed self-check. A backup
// that cannot be opened is not a backup — the failure names every broken
// assertion so the operator (and the log) sees why the run refused to publish.
type SelfCheckError struct {
	Failures []string
}

func (e *SelfCheckError) Error() string {
	return "snapshot self-check failed: " + strings.Join(e.Failures, "; ")
}

// selfCheck reopens the finished snapshot READ-ONLY and asserts it is a
// restorable database: quick_check/integrity_check clean, schema version and
// its user_version mirror equal to the source's, page_size/auto_vacuum
// preserved, and every recorded stream head present in the copy. Reading the
// values back rather than assuming them is the point (issue #30 §11).
func selfCheck(ctx context.Context, snapPath string, e SelfExpectations) error {
	var failures []string

	if e.Verify != VerifyNone {
		check := "quick_check"
		if e.Verify == VerifyFull {
			check = "integrity_check"
		}
		db, openErr := sql.Open("sqlite", "file:"+snapPath+"?_pragma=query_only(1)")
		if openErr != nil {
			return &SelfCheckError{Failures: []string{fmt.Sprintf("reopen snapshot read-only: %v", openErr)}}
		}

		rows, queryErr := db.QueryContext(ctx, `PRAGMA `+check)
		if queryErr != nil {
			failures = append(failures, fmt.Sprintf("%s: %v", check, queryErr))
		} else {
			defer func() {
				if closeErr := rows.Close(); closeErr != nil {
					failures = append(failures, fmt.Sprintf("%s rows: %v", check, closeErr))
				}
			}()
			for rows.Next() {
				var line string
				if scanErr := rows.Scan(&line); scanErr != nil {
					failures = append(failures, fmt.Sprintf("%s row: %v", check, scanErr))
					continue
				}
				if line != "ok" {
					failures = append(failures, fmt.Sprintf("%s: %s", check, line))
				}
			}
			if errErr := rows.Err(); errErr != nil {
				failures = append(failures, fmt.Sprintf("%s iteration: %v", check, errErr))
			}
		}

		var (
			schemaVer  int64
			userVer    int64
			pageSize   int64
			autoVacuum int64
		)
		scanErr := db.QueryRowContext(ctx,
			`SELECT (SELECT CAST(v AS INTEGER) FROM meta WHERE k = 'schema_version'),
			        (SELECT user_version FROM pragma_user_version),
			        (SELECT page_size FROM pragma_page_size),
			        (SELECT auto_vacuum FROM pragma_auto_vacuum)`).
			Scan(&schemaVer, &userVer, &pageSize, &autoVacuum)
		switch {
		case errors.Is(scanErr, sql.ErrNoRows):
			failures = append(failures, "meta.schema_version missing from the snapshot")
		case scanErr != nil:
			failures = append(failures, fmt.Sprintf("read snapshot identity pragmas: %v", scanErr))
		default:
			if schemaVer != int64(e.SchemaVer) {
				failures = append(failures, fmt.Sprintf("schema_version %d, want the source's %d",
					schemaVer, e.SchemaVer))
			}
			if userVer != e.UserVersion {
				failures = append(failures, fmt.Sprintf("user_version %d, want the source's %d",
					userVer, e.UserVersion))
			}
			if e.PageSize != 0 && pageSize != e.PageSize {
				failures = append(failures, fmt.Sprintf("page_size %d, want the preserved %d",
					pageSize, e.PageSize))
			}
			if e.AutoVacuum != 0 && autoVacuum != e.AutoVacuum {
				failures = append(failures, fmt.Sprintf("auto_vacuum did not survive (%d, want %d); "+
					"it cannot be turned on later without a full VACUUM", autoVacuum, e.AutoVacuum))
			}
		}

		for stream, head := range e.RecordedHead {
			var maxSeq int64
			headErr := db.QueryRowContext(ctx,
				`SELECT COALESCE(MAX(seq), 0) FROM messages WHERE stream = ?`, stream).Scan(&maxSeq)
			if headErr != nil {
				failures = append(failures, fmt.Sprintf("head spot-check %s: %v", stream, headErr))
				continue
			}
			if maxSeq < head {
				failures = append(failures, fmt.Sprintf(
					"stream %s head %d below the recorded %d — the copy lost messages", stream, maxSeq, head))
			}
		}

		if closeErr := db.Close(); closeErr != nil {
			failures = append(failures, fmt.Sprintf("close reopened snapshot: %v", closeErr))
		}
	}

	if len(failures) > 0 {
		return &SelfCheckError{Failures: failures}
	}
	return nil
}
