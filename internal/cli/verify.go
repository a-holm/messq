// SPDX-License-Identifier: Apache-2.0

package cli

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/a-holm/messq/internal/verify"
)

// The `messq verify` command: the invariant checker the crash harness needs, pointed at an
// operator. It runs against a stopped data dir, a live one, or a backup copy, and answers
// "is my broker's state sound?" in seconds. Until #23 re-homes it under the cobra chassis,
// this is a minimal command with exactly these flags and the §8 exit-code contract.

// verifyFlagNames is the closed §8 flag set for `messq verify`.
var verifyFlagNames = map[string]struct{}{
	"--data-dir":  {},
	"--deep":      {},
	"--output":    {},
	"--fail-fast": {},
	"--limit":     {},
}

// verifyConfig is the resolved verify command configuration.
type verifyConfig struct {
	dataDir  string
	deep     bool
	output   string // "table", "json", or "" for auto (TTY -> table, else json)
	failFast bool
	limit    int
}

// parseVerifyFlags parses the verify command line by hand, like runVersion and runServe.
// --data-dir resolves flag -> MESSQ_DATA_DIR -> /var/lib/messq.
func parseVerifyFlags(args []string, getenv func(string) string) (verifyConfig, error) {
	cfg := verifyConfig{output: "", limit: 100}
	for len(args) > 0 {
		arg := args[0]
		args = args[1:]
		name, val, hasEq := arg, "", false
		if eq := strings.Index(arg, "="); eq >= 0 {
			name, val, hasEq = arg[:eq], arg[eq+1:], true
		}
		if !strings.HasPrefix(name, "--") {
			return verifyConfig{}, fmt.Errorf("unexpected argument %q", arg)
		}
		if _, ok := verifyFlagNames[name]; !ok {
			return verifyConfig{}, fmt.Errorf("unknown flag %q", name)
		}
		if name == "--deep" || name == "--fail-fast" {
			if hasEq {
				return verifyConfig{}, fmt.Errorf("%s takes no value", name)
			}
			if name == "--deep" {
				cfg.deep = true
			} else {
				cfg.failFast = true
			}
			continue
		}
		if !hasEq {
			if len(args) == 0 {
				return verifyConfig{}, fmt.Errorf("%s needs a value", name)
			}
			val = args[0]
			args = args[1:]
		}
		switch name {
		case "--data-dir":
			cfg.dataDir = val
		case "--output":
			if val != "table" && val != "json" {
				return verifyConfig{}, fmt.Errorf("--output must be table or json, got %q", val)
			}
			cfg.output = val
		case "--limit":
			n, err := strconv.Atoi(val)
			if err != nil || n <= 0 {
				return verifyConfig{}, fmt.Errorf("--limit must be a positive integer, got %q", val)
			}
			cfg.limit = n
		}
	}
	if cfg.dataDir == "" {
		cfg.dataDir = getenv("MESSQ_DATA_DIR")
	}
	if cfg.dataDir == "" {
		cfg.dataDir = "/var/lib/messq"
	}
	return cfg, nil
}

// runVerify is the `messq verify` command: open the data dir read-only, diagnose an
// incomplete copy, run the registry, and print the report with the §8 exit code.
func runVerify(args []string, getenv func(string) string, stdout, stderr io.Writer) int {
	cfg, err := parseVerifyFlags(args, getenv)
	if err != nil {
		return usageError(stderr, err.Error())
	}

	ctx := context.Background()
	switch code := verifyOpenExit(ctx, cfg.dataDir, stderr); code {
	case exitOK:
	case exitNotFound, exitPermission:
		return code
	default:
		return code
	}

	db, err := verify.Open(cfg.dataDir)
	if err != nil {
		fmt.Fprintf(stderr, "messq verify: %v\n", err)
		if errors.Is(err, os.ErrPermission) {
			return exitPermission
		}
		return exitError
	}
	defer func() {
		if cerr := db.Close(); cerr != nil {
			fmt.Fprintf(stderr, "messq verify: close: %v\n", cerr)
		}
	}()

	// Edge case 15: a copy of an unclean dir that lost its -wal would report phantom loss.
	// Diagnose it explicitly before running any check.
	if incomplete, reason := incompleteCopy(ctx, db, cfg.dataDir); incomplete {
		fmt.Fprintf(stderr, "messq verify: %s\n", reason)
		return exitError
	}

	rep, err := verify.Run(ctx, db, verify.Options{Deep: cfg.deep, FailFast: cfg.failFast, Limit: cfg.limit})
	if err != nil {
		fmt.Fprintf(stderr, "messq verify: %v\n", err)
		return exitError
	}

	output := cfg.output
	if output == "" {
		output = "json"
		if isTTY(stdout) {
			output = "table"
		}
	}
	if err := writeVerifyReport(stdout, output, rep); err != nil {
		fmt.Fprintf(stderr, "messq verify: %v\n", err)
		return exitError
	}
	if rep.Failed() {
		return exitError
	}
	return exitOK
}

// verifyOpenExit resolves the data dir existence and permission questions to their exit
// codes before Open touches anything: a missing directory is exit 3, an unreadable one is
// exit 7.
func verifyOpenExit(ctx context.Context, dataDir string, stderr io.Writer) int {
	st, err := os.Stat(dataDir)
	switch {
	case errors.Is(err, os.ErrNotExist):
		fmt.Fprintf(stderr, "messq verify: data directory %q does not exist\n", dataDir)
		return exitNotFound
	case err != nil:
		fmt.Fprintf(stderr, "messq verify: %v\n", err)
		return exitError
	case !st.IsDir():
		fmt.Fprintf(stderr, "messq verify: %q is not a directory\n", dataDir)
		return exitError
	}
	if _, err := os.Stat(filepath.Join(dataDir, "messq.db")); errors.Is(err, os.ErrNotExist) {
		fmt.Fprintf(stderr, "messq verify: %q holds no messq.db; is this a messq data directory?\n", dataDir)
		return exitNotFound
	} else if err != nil {
		fmt.Fprintf(stderr, "messq verify: %v\n", err)
		if errors.Is(err, os.ErrPermission) {
			return exitPermission
		}
		return exitError
	}
	return exitOK
}

// incompleteCopy reports whether the data dir looks like a copy that lost its -wal: an
// unclean shutdown marker says a -wal should exist, but no -wal file is present. The
// operator must re-copy with the -wal/-shm siblings, not interpret this as data loss.
func incompleteCopy(ctx context.Context, db *sql.DB, dataDir string) (bool, string) {
	// Check the -wal BEFORE touching the database: opening a connection (even read-only)
	// creates a fresh -wal/-shm, which would mask the missing WAL tail of a copied .db.
	if _, statErr := os.Stat(filepath.Join(dataDir, "messq.db-wal")); statErr == nil {
		return false, "" // the -wal is present; the WAL tail will be recovered
	}
	var clean string
	err := db.QueryRowContext(ctx, `SELECT v FROM meta WHERE k = 'clean_shutdown'`).Scan(&clean)
	if errors.Is(err, sql.ErrNoRows) || err != nil {
		return false, "" // a fresh/empty dir has no marker; not a copy-diagnosis case
	}
	if clean == "1" {
		return false, "" // a clean shutdown legitimately leaves no -wal
	}
	return true, fmt.Sprintf("%s is an incomplete copy: meta.clean_shutdown says a -wal should exist but messq.db-wal is missing; re-copy the .db with its -wal and -shm siblings (or restore a messq backup), never interpret this as data loss", dataDir)
}

// verifyJSON is the frozen `--output json` shape: a flat ok flag plus the registry's
// checks and flattened violations.
type verifyJSON struct {
	OK         bool                 `json:"ok"`
	Checks     []verify.CheckResult `json:"checks"`
	Violations []verify.Violation   `json:"violations"`
}

// writeVerifyReport renders the report in the requested format.
func writeVerifyReport(w io.Writer, format string, rep verify.Report) error {
	if format == "json" {
		enc := json.NewEncoder(w)
		enc.SetIndent("", "  ")
		return enc.Encode(verifyJSON{OK: !rep.Failed(), Checks: rep.Checks, Violations: rep.Violations})
	}
	return writeVerifyTable(w, rep)
}

// writeVerifyTable renders the human table: one line per check, then the summary.
func writeVerifyTable(w io.Writer, rep verify.Report) error {
	var b strings.Builder
	fmt.Fprintf(&b, "messq verify\n\n")
	for _, c := range rep.Checks {
		state := "ok"
		switch {
		case c.Skipped:
			state = "skipped  (--deep)"
		case !c.OK:
			state = fmt.Sprintf("FAIL   %d row(s)", len(c.Violations))
		}
		fmt.Fprintf(&b, "  %-4s %-22s %s\n", c.ID, c.Name, state)
		for _, v := range c.Violations {
			fmt.Fprintf(&b, "      %s\n", v.Detail)
		}
	}
	if rep.Failed() {
		fmt.Fprintf(&b, "\nverify: %d violation(s) across %d checks\n", len(rep.Violations), len(rep.Checks))
		fmt.Fprintf(&b, "this is a bug in messq, not a misconfiguration — please file it with:\n")
		fmt.Fprintf(&b, "  messq verify --deep --output json > verify.json   and   messq backup /tmp/messq-repro.db\n")
	} else {
		fmt.Fprintf(&b, "\nverify: OK — %d checks, 0 violations\n", len(rep.Checks))
	}
	_, err := io.WriteString(w, b.String())
	return err
}

// isTTY reports whether w is a character device (a terminal). The §8 contract defaults to
// table output on a TTY and json otherwise, never a third mode.
func isTTY(w io.Writer) bool {
	f, ok := w.(*os.File)
	if !ok {
		return false
	}
	st, err := f.Stat()
	if err != nil {
		return false
	}
	return st.Mode()&os.ModeCharDevice != 0
}
