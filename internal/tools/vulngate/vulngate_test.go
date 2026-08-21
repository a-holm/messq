// SPDX-License-Identifier: Apache-2.0

package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func date(t *testing.T, s string) time.Time {
	t.Helper()
	parsed, err := time.Parse(dateLayout, s)
	if err != nil {
		t.Fatal(err)
	}
	return parsed
}

func TestParseAllow_Table(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		want    []suppression
		wantErr string
	}{
		{
			name: "comments, blanks and a multi-word reason",
			input: "# osv-id       expires      reason\n" +
				"\n" +
				"GO-2026-1234   2026-09-30   only reachable via the cgosqlite build tag; tracked in #5\n",
			want: []suppression{{
				OSV:     "GO-2026-1234",
				Expires: time.Date(2026, 9, 30, 0, 0, 0, 0, time.UTC),
				Reason:  "only reachable via the cgosqlite build tag; tracked in #5",
			}},
		},
		{
			name:  "an empty file is valid and is the steady state",
			input: "# nothing suppressed\n",
		},
		{
			name:    "a reason is mandatory",
			input:   "GO-2026-1234   2026-09-30\n",
			wantErr: "want '<osv-id> <expires> <reason>'",
		},
		{
			name:    "a CVE alias is refused",
			input:   "CVE-2026-1234   2026-09-30   reason\n",
			wantErr: "is not a GO-YYYY-NNNN identifier",
		},
		{
			name:    "a non-ISO date is refused",
			input:   "GO-2026-1234   30/09/2026   reason\n",
			wantErr: "is not a 2006-01-02 date",
		},
		{
			name:    "a duplicate id is refused",
			input:   "GO-2026-1234 2026-09-30 a\nGO-2026-1234 2026-10-30 b\n",
			wantErr: "already suppressed on line 1",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := parseAllow(strings.NewReader(tt.input))

			if tt.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
					t.Fatalf("error = %v, want it to contain %q", err, tt.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("parseAllow() error = %v", err)
			}
			if len(got) != len(tt.want) {
				t.Fatalf("parseAllow() = %v, want %v", got, tt.want)
			}
			for i := range got {
				if got[i] != tt.want[i] {
					t.Errorf("suppression %d = %+v, want %+v", i, got[i], tt.want[i])
				}
			}
		})
	}
}

func TestSuppression_ExpiresAtTheEndOfItsDay(t *testing.T) {
	s := suppression{OSV: "GO-2026-1234", Expires: date(t, "2026-09-30")}

	tests := []struct {
		now  string
		want bool
	}{
		{now: "2026-09-29", want: false},
		{now: "2026-09-30", want: false},
		{now: "2026-10-01", want: true},
	}
	for _, tt := range tests {
		if got := s.Expired(date(t, tt.now)); got != tt.want {
			t.Errorf("Expired(%s) = %v, want %v", tt.now, got, tt.want)
		}
	}
}

const sarifTemplate = `{"version":"2.1.0","runs":[{"tool":{"driver":{"name":"govulncheck"}},"results":[%s]}]}`

func sarifResult(osv, level, message string) string {
	return `{"ruleId":"` + osv + `","level":"` + level + `","message":{"text":"` + message + `"}}`
}

func TestParseSARIF_Table(t *testing.T) {
	t.Run("flattens results across runs", func(t *testing.T) {
		doc := strings.Replace(sarifTemplate, "%s",
			sarifResult("GO-2026-2", levelRequired, "required only")+","+
				sarifResult("GO-2026-1", levelCalled, "called"), 1)

		got, err := parseSARIF(strings.NewReader(doc))
		if err != nil {
			t.Fatalf("parseSARIF() error = %v", err)
		}
		if len(got) != 2 || got[0].OSV != "GO-2026-1" {
			t.Fatalf("parseSARIF() = %+v, want two findings sorted by id", got)
		}
	})

	t.Run("an empty stdin is an error, not a clean scan", func(t *testing.T) {
		if _, err := parseSARIF(strings.NewReader("")); err == nil {
			t.Fatal("parseSARIF() error = nil, want an error")
		}
	})

	t.Run("non-SARIF stdin is an error", func(t *testing.T) {
		if _, err := parseSARIF(strings.NewReader("govulncheck: cannot connect\n")); err == nil {
			t.Fatal("parseSARIF() error = nil, want an error")
		}
	})
}

func TestJudge_Table(t *testing.T) {
	now := date(t, "2026-08-21")
	live := suppression{OSV: "GO-2026-1", Expires: date(t, "2026-12-31"), Reason: "tracked in #5"}
	dead := suppression{OSV: "GO-2026-1", Expires: date(t, "2026-01-01"), Reason: "stale"}

	tests := []struct {
		name           string
		findings       []finding
		allow          []suppression
		wantBlocking   int
		wantSuppressed int
		wantUnused     int
	}{
		{
			name:     "no findings",
			findings: nil,
		},
		{
			name:         "a reachable finding blocks",
			findings:     []finding{{OSV: "GO-2026-1", Level: levelCalled}},
			wantBlocking: 1,
		},
		{
			name:     "an imported but uncalled finding does not block",
			findings: []finding{{OSV: "GO-2026-1", Level: levelImported}},
		},
		{
			name:     "a required-module finding does not block",
			findings: []finding{{OSV: "GO-2026-1", Level: levelRequired}},
		},
		{
			name:           "a live suppression clears a reachable finding",
			findings:       []finding{{OSV: "GO-2026-1", Level: levelCalled}},
			allow:          []suppression{live},
			wantSuppressed: 1,
		},
		{
			name:         "an expired suppression does not clear anything",
			findings:     []finding{{OSV: "GO-2026-1", Level: levelCalled}},
			allow:        []suppression{dead},
			wantBlocking: 1,
		},
		{
			name:       "a suppression that matches nothing is reported",
			findings:   nil,
			allow:      []suppression{live},
			wantUnused: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := judge(tt.findings, tt.allow, now)

			if len(got.Blocking) != tt.wantBlocking {
				t.Errorf("blocking = %v, want %d", got.Blocking, tt.wantBlocking)
			}
			if len(got.Suppressed) != tt.wantSuppressed {
				t.Errorf("suppressed = %v, want %d", got.Suppressed, tt.wantSuppressed)
			}
			if len(got.Unused) != tt.wantUnused {
				t.Errorf("unused = %v, want %d", got.Unused, tt.wantUnused)
			}
		})
	}
}

func TestRun_CheckExpiryFailsOnAStaleEntry(t *testing.T) {
	allow := writeAllow(t, "GO-2026-1234   2026-01-31   only reachable via the cgosqlite build tag\n")

	var stdout, stderr strings.Builder
	code := run([]string{"-allow", allow, "-check-expiry", "-now", "2026-08-21"}, strings.NewReader(""), &stdout, &stderr)

	if code != exitBlocked {
		t.Fatalf("run() = %d, want %d (stdout: %s)", code, exitBlocked, stdout.String())
	}
	if !strings.Contains(stdout.String(), "suppression expired on 2026-01-31") {
		t.Errorf("stdout = %q, want the expiry message", stdout.String())
	}
}

func TestRun_CheckExpiryPassesOnAnEmptyAllowFile(t *testing.T) {
	allow := writeAllow(t, "# nothing suppressed\n")

	var stdout, stderr strings.Builder
	code := run([]string{"-allow", allow, "-check-expiry", "-now", "2026-08-21"}, strings.NewReader(""), &stdout, &stderr)

	if code != exitOK {
		t.Fatalf("run() = %d, want %d (stderr: %s)", code, exitOK, stderr.String())
	}
	if !strings.Contains(stdout.String(), "expected steady state") {
		t.Errorf("stdout = %q, want the steady-state note", stdout.String())
	}
}

func TestRun_ScanTable(t *testing.T) {
	tests := []struct {
		name     string
		allow    string
		results  string
		strict   bool
		wantExit int
		wantText string
	}{
		{
			name:     "a clean scan passes",
			allow:    "# nothing suppressed\n",
			results:  "",
			wantExit: exitOK,
			wantText: "0 reachable, 0 imported but not called, 0 in required modules only",
		},
		{
			name:     "stdlib module advisories do not block",
			allow:    "# nothing suppressed\n",
			results:  sarifResult("GO-2026-5026", levelRequired, "depends on a vulnerable module"),
			wantExit: exitOK,
			wantText: "1 in required modules only",
		},
		{
			name:     "a reachable vulnerability blocks",
			allow:    "# nothing suppressed\n",
			results:  sarifResult("GO-2026-1234", levelCalled, "your code calls a vulnerable symbol"),
			wantExit: exitBlocked,
			wantText: "FAIL GO-2026-1234 is reachable",
		},
		{
			name:     "a live suppression clears it",
			allow:    "GO-2026-1234 2026-12-31 tracked in #5\n",
			results:  sarifResult("GO-2026-1234", levelCalled, "your code calls a vulnerable symbol"),
			wantExit: exitOK,
			wantText: "suppressed GO-2026-1234",
		},
		{
			name:     "an unused suppression only warns by default",
			allow:    "GO-2026-1234 2026-12-31 tracked in #5\n",
			results:  "",
			wantExit: exitOK,
			wantText: "warning: GO-2026-1234 suppresses nothing",
		},
		{
			name:     "an unused suppression fails under -strict",
			allow:    "GO-2026-1234 2026-12-31 tracked in #5\n",
			results:  "",
			strict:   true,
			wantExit: exitBlocked,
			wantText: "FAIL GO-2026-1234 suppresses nothing",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			allow := writeAllow(t, tt.allow)
			doc := strings.Replace(sarifTemplate, "%s", tt.results, 1)
			args := []string{"-allow", allow, "-now", "2026-08-21"}
			if tt.strict {
				args = append(args, "-strict")
			}

			var stdout, stderr strings.Builder
			code := run(args, strings.NewReader(doc), &stdout, &stderr)

			if code != tt.wantExit {
				t.Fatalf("run() = %d, want %d (stdout: %s stderr: %s)", code, tt.wantExit, stdout.String(), stderr.String())
			}
			if !strings.Contains(stdout.String(), tt.wantText) {
				t.Errorf("stdout = %q, want it to contain %q", stdout.String(), tt.wantText)
			}
		})
	}
}

func TestRun_MissingAllowFileExitsTwo(t *testing.T) {
	var stdout, stderr strings.Builder
	code := run([]string{"-allow", filepath.Join(t.TempDir(), "absent")}, strings.NewReader(""), &stdout, &stderr)

	if code != exitUsage {
		t.Fatalf("run() = %d, want %d", code, exitUsage)
	}
}

func writeAllow(t *testing.T, content string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), ".govulncheck-allow")
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
	return path
}
