// SPDX-License-Identifier: Apache-2.0

package verify

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sort"
)

// The checker registry. Each check has a stable ID the CLI, the crash harness, #13's rapid
// hook, the soak (#33) and the upgrade fixtures (#34) all cite, so renaming one is a
// cross-repository rename (S15). A check is either a single SQL statement whose returned
// rows are the violations, or a Run function for the few that need Go (the pragma readbacks
// and the I10 event fold).

// Violation is one broken invariant: the check's ID and a human-readable detail naming the
// offending rows.
type Violation struct {
	ID     string `json:"id"`
	Detail string `json:"detail"`
}

// Check is one invariant checker. Exactly one of Query or Run is set.
type Check struct {
	ID    string // "V2", or "I4" for the §5.2 predicates
	Name  string
	Deep  bool   // excluded unless Options.Deep: O(rows) or O(events)
	Query string // one SQL statement; every returned row is a violation
	Run   func(ctx context.Context, tx *sql.Tx, deep bool) ([]Violation, error)
}

// Options tunes a Run.
type Options struct {
	Deep     bool // add integrity_check, I8 and the I10 event fold
	FailFast bool // stop at the first violating check
	Limit    int  // violating rows reported per check; <= 0 means 100
}

func (o *Options) limit() int {
	if o.Limit <= 0 {
		return 100
	}
	return o.Limit
}

// CheckResult is one check's outcome within a report.
type CheckResult struct {
	ID         string      `json:"id"`
	Name       string      `json:"name"`
	OK         bool        `json:"ok"`
	Skipped    bool        `json:"skipped,omitempty"`
	Violations []Violation `json:"violations"`
}

// Report is the outcome of one Run. The JSON field names are the frozen `messq verify
// --output json` shape, schema-tested alongside #18.
type Report struct {
	Checks     []CheckResult `json:"checks"`
	Violations []Violation   `json:"violations"`
}

// Failed reports whether any check produced a violation.
func (r Report) Failed() bool { return len(r.Violations) > 0 }

// checkedElsewhere lists every S15 invariant that verify does NOT implement, with the issue
// whose mechanism owns it. The registry meta-test asserts the S15 register cannot drift:
// every ID is implemented here or named here.
var checkedElsewhere = map[string]string{
	"I1":  "the crash harness's three-valued ledger reconciliation (issue #8)",
	"I3":  "the rapid reference model and golden log tests (issue #13)",
	"I9":  "the rapid reference model and golden log tests (issue #13)",
	"I11": "appendix A1 completeness tests (issue #6)",
}

// Registry returns the full check list in report order. The S15 predicates are written now,
// against schema v2, and stay vacuously true until #9–#12 create delivery rows — deliberate,
// so they are green the day the first delivery row lands.
func Registry() []Check {
	return []Check{
		{V1, "schema version", false, "", checkV1},
		{V2, "quick_check", false, "", checkV2},
		{V3, "foreign_key_check", false, "", checkV3},
		{V4, "pragma readback", false, "", checkV4},
		{V5, "message integrity", false, "", checkV5},
		{V6, "seq allocator", false, v6Query, nil},
		{I2, "delivery partition", false, i2Query, nil},
		{I4, "attempts <= max_deliver", false, i4Query, nil},
		{I5, "flow control", false, i5Query, nil},
		{I6, "cursor bounds", false, "", checkI6},
		{I7, "settle fence", false, "", checkI7},
		{S1, "no expired inflight survives a sweep", false, "", checkS1},
		{S2, "no stranded row above max_deliver", false, s2Query, nil},
		{S3, "delivery generation matches consumer", false, s3Query, nil},
		{I8, "DLQ conservation (per generation)", true, "", checkI8},
		{PDLQ1, "P-DLQ1 written-copy conservation", true, "", checkP_DLQ1},
		{PDLQ2, "P-DLQ2 DLQ rows carry provenance", true, "", checkP_DLQ2},
		{PDLQ3, "P-DLQ3 no dlq policy on a .dlq consumer", false, pdlq3Query, nil},
		{PDLQ4, "P-DLQ4 no origin_missing deaths", true, "", checkP_DLQ4},
		{PDLQ5, "P-DLQ5 no double-death", true, "", checkP_DLQ5},
		{PID1, "P-ID1 unique id outside a DLQ", true, pid1Query, nil},
		{I10, "log = state", true, "", checkI10},
	}
}

// Run executes every check inside one BEGIN DEFERRED read transaction on a single
// connection, so a live daemon committing concurrently cannot make per-check results
// mutually inconsistent. The connection is fenced write-off by query_only (see Open), so a
// check that attempts a write fails loudly rather than mutating the data dir.
func Run(ctx context.Context, db *sql.DB, opt Options) (Report, error) {
	conn, err := db.Conn(ctx)
	if err != nil {
		return Report{}, fmt.Errorf("acquire connection: %w", err)
	}
	defer func() {
		if cerr := conn.Close(); cerr != nil {
			err = fmt.Errorf("release connection: %w", cerr)
		}
	}()

	tx, err := conn.BeginTx(ctx, nil)
	if err != nil {
		return Report{}, fmt.Errorf("begin read transaction: %w", err)
	}
	defer func() {
		if rbErr := tx.Rollback(); rbErr != nil && !errors.Is(rbErr, sql.ErrTxDone) {
			err = fmt.Errorf("rollback read transaction: %w", rbErr)
		}
	}()

	rep := Report{}
	for _, c := range Registry() {
		res := CheckResult{ID: c.ID, Name: c.Name}
		if c.Deep && !opt.Deep {
			res.Skipped = true
			rep.Checks = append(rep.Checks, res)
			continue
		}
		var vs []Violation
		if c.Query != "" {
			vs, err = runQueryCheck(ctx, tx, c.ID, c.Query, opt.limit())
		} else {
			vs, err = c.Run(ctx, tx, opt.Deep)
		}
		if err != nil {
			return rep, fmt.Errorf("%s %s: %w", c.ID, c.Name, err)
		}
		res.Violations = vs
		res.OK = len(vs) == 0
		rep.Checks = append(rep.Checks, res)
		rep.Violations = append(rep.Violations, vs...)
		if opt.FailFast && len(vs) > 0 {
			break
		}
	}
	if err := tx.Commit(); err != nil {
		return rep, fmt.Errorf("commit read transaction: %w", err)
	}
	return rep, nil
}

// runQueryCheck runs one SQL check and renders each returned row as a violation detail,
// using the result's column names so a violation names its offending columns.
func runQueryCheck(ctx context.Context, tx *sql.Tx, id, query string, limit int) (vs []Violation, retErr error) {
	rows, err := tx.QueryContext(ctx, query)
	if err != nil {
		return nil, err
	}
	defer func() {
		if cerr := rows.Close(); cerr != nil {
			retErr = errors.Join(retErr, fmt.Errorf("close rows: %w", cerr))
		}
	}()
	cols, err := rows.Columns()
	if err != nil {
		return nil, err
	}
	vals := make([]any, len(cols))
	ptrs := make([]any, len(cols))
	for i := range vals {
		ptrs[i] = &vals[i]
	}
	for rows.Next() {
		if scanErr := rows.Scan(ptrs...); scanErr != nil {
			return nil, scanErr
		}
		vs = append(vs, Violation{ID: id, Detail: renderRow(cols, vals)})
		if len(vs) >= limit {
			break
		}
	}
	if iterErr := rows.Err(); iterErr != nil {
		return nil, iterErr
	}
	return vs, nil
}

// renderRow joins a row's columns into "col=value" pairs.
func renderRow(cols []string, vals []any) string {
	out := ""
	for i, c := range cols {
		if i > 0 {
			out += " "
		}
		out += fmt.Sprintf("%s=%v", c, vals[i])
	}
	return out
}

// RegistryCoverage reports every S15 invariant in the I1–I11 register that is neither
// implemented by [Registry] nor named in checkedElsewhere. The registry meta-test asserts
// this is always empty, so the S15 register cannot drift: an invariant is either a check
// here or explicitly owned elsewhere.
func RegistryCoverage() []string {
	ids := make(map[string]bool)
	for _, c := range Registry() {
		ids[c.ID] = true
	}
	var missing []string
	for n := 1; n <= 11; n++ {
		id := fmt.Sprintf("I%d", n)
		if ids[id] {
			continue
		}
		if _, ok := checkedElsewhere[id]; !ok {
			missing = append(missing, id)
		}
	}
	// Any ID in checkedElsewhere must NOT also be a check (a drift in the other direction).
	for id := range checkedElsewhere {
		if ids[id] {
			missing = append(missing, id+" is both implemented and listed elsewhere")
		}
	}
	sort.Strings(missing)
	return missing
}
