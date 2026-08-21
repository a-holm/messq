// SPDX-License-Identifier: Apache-2.0

package docsguard_test

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/a-holm/messq/internal/docsguard"
)

// transitionTable renders a well-formed S6.1 table with the given rows appended, so a test row
// only has to say what it breaks.
func transitionTable(rows string) []byte {
	return []byte("### S6.1 The table\n\n" +
		"| ID | From | Trigger | Guard | Effect | Event | On guard failure |\n" +
		"|---|---|---|---|---|---|---|\n" + rows)
}

// TestParse_RejectsMalformedMarkdown covers the failure half of every parser. A parser that
// silently returns an empty slice would make every checker vacuously happy.
func TestParse_RejectsMalformedMarkdown(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name  string
		parse func([]byte) error
		md    string
	}{
		{"transitions: no heading", parseTransitions, "# nothing here\n"},
		{
			"transitions: no separator row",
			parseTransitions,
			"### S6.1 The table\n\n| ID | From |\n| T1 | UNSEEN |\n",
		},
		{
			"transitions: no body rows",
			parseTransitions,
			"### S6.1 The table\n\n| ID | From |\n|---|---|\n\nprose\n",
		},
		{
			"transitions: ragged row",
			parseTransitions,
			"### S6.1 The table\n\n| ID | From | Trigger | Guard | Effect | Event | On guard failure |\n" +
				"|---|---|---|---|---|---|---|\n| T1 | UNSEEN |\n",
		},
		{
			"transitions: wrong column count",
			parseTransitions,
			"### S6.1 The table\n\n| ID | From |\n|---|---|\n| T1 | UNSEEN |\n",
		},
		{
			"transitions: table belongs to the next section",
			parseTransitions,
			"### S6.1 The table\n\nprose only\n\n### S6.2 Notes\n\n| ID | From |\n|---|---|\n| T1 | UNSEEN |\n",
		},
		{
			"invariants: wrong column count",
			parseInvariants,
			"## S15. Invariant register\n\n| ID | Statement |\n|---|---|\n| I1 | something |\n",
		},
		{
			"error outcomes: wrong column count",
			parseErrorOutcomes,
			"## S13. Error outcomes\n\n| Sentinel | Message |\n|---|---|\n| `errs.ErrNotFound` | not found |\n",
		},
		{
			"bounds: wrong column count",
			parseBounds,
			"## A1. Bounds register\n\n| Bound | Default |\n|---|---|\n| a queue | 1024 |\n",
		},
		{"vocabulary: no heading", parseSpecVocabulary, "## S3. Names\n\n```\nserver.start\n```\n"},
		{"vocabulary: unclosed fence", parseSpecVocabulary, "### S2.4 Event vocabulary\n\n```\nserver.start\n"},
		{"vocabulary: no fence at all", parseSpecVocabulary, "### S2.4 Event vocabulary\n\nprose only\n"},
		{"plan vocabulary: no heading", parsePlanVocabulary, "### 9.3 Volume control\n\n```\nx\n```\n"},
		{"plan transitions: no heading", parsePlanTransitionIDs, "### 5.2 Invariants\n\n| ID |\n|---|\n| I1 |\n"},
		{"plan decisions: none declared", parsePlanDecisions, "## 2. Adjudicated decisions\n\nprose only\n"},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()

			if err := c.parse([]byte(c.md)); err == nil {
				t.Fatal("the malformed document was accepted")
			}
		})
	}
}

func parseTransitions(md []byte) error {
	_, err := docsguard.ParseTransitions(md)
	return err
}

func parseInvariants(md []byte) error {
	_, err := docsguard.ParseInvariants(md)
	return err
}

func parseErrorOutcomes(md []byte) error {
	_, err := docsguard.ParseErrorOutcomes(md)
	return err
}

func parseBounds(md []byte) error {
	_, err := docsguard.ParseBounds(md)
	return err
}

func parseSpecVocabulary(md []byte) error {
	_, err := docsguard.ParseSpecVocabulary(md)
	return err
}

func parsePlanVocabulary(md []byte) error {
	_, err := docsguard.ParsePlanVocabulary(md)
	return err
}

func parsePlanTransitionIDs(md []byte) error {
	_, err := docsguard.ParsePlanTransitionIDs(md)
	return err
}

func parsePlanDecisions(md []byte) error {
	_, err := docsguard.ParsePlanDecisions(md)
	return err
}

// TestCheck_RejectsBrokenTables covers the checkers directly, one row per rule. The sabotage
// fixtures prove the same rules end to end; these rows are what keep every branch honest.
func TestCheck_RejectsBrokenTables(t *testing.T) {
	t.Parallel()

	full := docsguard.Transition{
		ID: "T1", From: "UNSEEN", Trigger: "top-up", Guard: "matches",
		Effect: "insert", Event: "none", OnFailure: "held",
	}
	empty := full
	empty.Guard = ""
	suffixOnly := full
	suffixOnly.ID = "T4a"
	badID := full
	badID.ID = "X1"

	cases := []struct {
		name  string
		check func() error
	}{
		{"transitions: empty table", func() error { return docsguard.CheckTransitions(nil) }},
		{"transitions: unparsable ID", func() error {
			return docsguard.CheckTransitions([]docsguard.Transition{badID})
		}},
		{"transitions: empty cell", func() error {
			return docsguard.CheckTransitions([]docsguard.Transition{empty})
		}},
		{"transitions: suffix without a base row", func() error {
			return docsguard.CheckTransitions([]docsguard.Transition{full, suffixOnly})
		}},
		{"transition events: unknown name", func() error {
			t2 := full
			t2.Event = "`msg.acked`"
			return docsguard.CheckTransitionEvents([]docsguard.Transition{t2}, []string{"msg.ack"})
		}},
		{"invariants: empty register", func() error { return docsguard.CheckInvariants(nil) }},
		{"invariants: unparsable ID", func() error {
			return docsguard.CheckInvariants([]docsguard.Invariant{{
				ID: "J1", Statement: "s", Predicate: "p", CheckedBy: "c", FirstRequiredAt: "#1",
			}})
		}},
		{"invariants: duplicate ID", func() error {
			row := docsguard.Invariant{ID: "I1", Statement: "s", Predicate: "p", CheckedBy: "c", FirstRequiredAt: "#1"}
			return docsguard.CheckInvariants([]docsguard.Invariant{row, row})
		}},
		{"invariants: empty cell", func() error {
			return docsguard.CheckInvariants([]docsguard.Invariant{{
				ID: "I1", Statement: "s", Predicate: "", CheckedBy: "c", FirstRequiredAt: "#1",
			}})
		}},
		{"outcomes: sentinel not named as errs.ErrX", func() error {
			return docsguard.CheckErrorOutcomes(
				[]docsguard.ErrorOutcome{{Sentinel: "NotFound", Message: "not found", RaisedBy: "x"}}, nil)
		}},
		{"outcomes: no raised-by cell", func() error {
			return docsguard.CheckErrorOutcomes(
				[]docsguard.ErrorOutcome{{Sentinel: "errs.ErrNotFound", Message: "not found"}}, nil)
		}},
		{"outcomes: one message documented twice", func() error {
			row := docsguard.ErrorOutcome{Sentinel: "errs.ErrNotFound", Message: "not found", RaisedBy: "x"}
			return docsguard.CheckErrorOutcomes([]docsguard.ErrorOutcome{row, row}, nil)
		}},
		{"outcomes: message no sentinel carries", func() error {
			return docsguard.CheckErrorOutcomes(
				[]docsguard.ErrorOutcome{{Sentinel: "errs.ErrTeapot", Message: "i am a teapot", RaisedBy: "x"}},
				[]error{os.ErrNotExist})
		}},
		{"vocabulary: empty", func() error { return docsguard.CheckVocabulary(nil, []string{"a"}) }},
		{"vocabulary: length differs", func() error {
			return docsguard.CheckVocabulary([]string{"a"}, []string{"a", "b"})
		}},
		{"vocabulary: order differs", func() error {
			return docsguard.CheckVocabulary([]string{"b", "a"}, []string{"a", "b"})
		}},
		{"bounds: empty register", func() error { return docsguard.CheckBounds(nil) }},
		{"bounds: empty cell", func() error {
			return docsguard.CheckBounds([]docsguard.Bound{{Name: "queue", SetBy: "--x", Default: "", Owner: "#6"}})
		}},
		{"bounds: no owning issue", func() error {
			return docsguard.CheckBounds([]docsguard.Bound{{Name: "queue", SetBy: "--x", Default: "1", Owner: "later"}})
		}},
		{"mirror: count differs", func() error {
			return docsguard.CheckMirrorsPlan([]docsguard.Transition{full}, []string{"T1", "T2"})
		}},
		{"mirror: order differs", func() error {
			return docsguard.CheckMirrorsPlan([]docsguard.Transition{full}, []string{"T2"})
		}},
		{"claims: unparsable D-number", func() error {
			return docsguard.CheckDecisionsClaimed([]docsguard.ADR{{Adjudicates: "D one", Path: "p"}}, nil)
		}},
		{"claims: decision the plan does not declare", func() error {
			return docsguard.CheckDecisionsClaimed([]docsguard.ADR{{Adjudicates: "D9", Path: "p"}}, nil)
		}},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()

			if err := c.check(); err == nil {
				t.Fatal("the broken input was accepted")
			}
		})
	}
}

// TestParseADRs_RejectsMalformedRecords writes one bad record into a scratch directory per row.
func TestParseADRs_RejectsMalformedRecords(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name string
		file string
		body string
	}{
		{"filename is not NNNN-kebab", "1-first.md", validADR},
		{"filename is not lower case", "0001-First.md", validADR},
		{"no title heading", "0001-first.md", "- Status: Accepted\n"},
		{"title number differs from the filename", "0001-first.md", "# 0002. First\n"},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()

			dir := t.TempDir()
			if err := os.WriteFile(filepath.Join(dir, c.file), []byte(c.body), 0o600); err != nil {
				t.Fatal(err)
			}
			if _, err := docsguard.ParseADRs(dir); err == nil {
				t.Fatalf("%s was accepted", c.file)
			}
		})
	}
}

func TestParseADRs_MissingDirectory(t *testing.T) {
	t.Parallel()

	if _, err := docsguard.ParseADRs(filepath.Join(t.TempDir(), "absent")); err == nil {
		t.Fatal("a missing directory was accepted")
	}
}

// TestParseADRs_SkipsTheTemplateAndTheIndex keeps the two non-records out of the register, which
// is what lets the contiguity check start at 0001.
func TestParseADRs_SkipsTheTemplateAndTheIndex(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	for name, body := range map[string]string{
		"0000-template.md": "# NNNN. Title\n",
		"README.md":        "# index\n",
		"0001-first.md":    validADR,
		"notes.txt":        "ignored",
	} {
		if err := os.WriteFile(filepath.Join(dir, name), []byte(body), 0o600); err != nil {
			t.Fatal(err)
		}
	}

	adrs, err := docsguard.ParseADRs(dir)
	if err != nil {
		t.Fatal(err)
	}
	if len(adrs) != 1 || adrs[0].Number != 1 {
		t.Fatalf("ParseADRs returned %d records, want only 0001", len(adrs))
	}
}

func TestCheckADRs_RejectsBrokenRecords(t *testing.T) {
	t.Parallel()

	ok := docsguard.ADR{
		Number: 1, Title: "First", Status: "Accepted", Date: "2026-08-21",
		Adjudicates: "none", RelatesTo: "issue #4", Path: "0001-first.md",
		Headings: []string{"Context", "Decision", "Alternatives", "Consequences", "Revisit trigger"},
	}
	with := func(mutate func(*docsguard.ADR)) []docsguard.ADR {
		a := ok
		a.Headings = append([]string(nil), ok.Headings...)
		mutate(&a)
		return []docsguard.ADR{a}
	}

	cases := []struct {
		name string
		in   []docsguard.ADR
	}{
		{"no records", nil},
		{"duplicate number", []docsguard.ADR{ok, ok}},
		{"unknown status", with(func(a *docsguard.ADR) { a.Status = "Maybe" })},
		{"date is not ISO", with(func(a *docsguard.ADR) { a.Date = "21 August 2026" })},
		{"no Adjudicates field", with(func(a *docsguard.ADR) { a.Adjudicates = "" })},
		{"Adjudicates is not a D-number", with(func(a *docsguard.ADR) { a.Adjudicates = "everything" })},
		{"no Relates to field", with(func(a *docsguard.ADR) { a.RelatesTo = "" })},
		{"headings out of order", with(func(a *docsguard.ADR) {
			a.Headings = []string{"Decision", "Context", "Alternatives", "Consequences", "Revisit trigger"}
		})},
		{"superseded by a record that does not exist", with(func(a *docsguard.ADR) { a.Status = "Superseded by 0099" })},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()

			if err := docsguard.CheckADRs(c.in); err == nil {
				t.Fatal("the broken record was accepted")
			}
		})
	}
}

func TestCheckADRs_AcceptsASupersededChain(t *testing.T) {
	t.Parallel()

	base := docsguard.ADR{
		Status: "Accepted", Date: "2026-08-21", Adjudicates: "none", RelatesTo: "issue #4",
		Headings: []string{"Context", "Decision", "Alternatives", "Consequences", "Revisit trigger"},
	}
	first, second := base, base
	first.Number, first.Path, first.Status = 1, "0001-first.md", "Superseded by 0002"
	second.Number, second.Path = 2, "0002-second.md"

	if err := docsguard.CheckADRs([]docsguard.ADR{first, second}); err != nil {
		t.Fatal(err)
	}
}

func TestBrokenLinksIn_MissingFile(t *testing.T) {
	t.Parallel()

	if _, err := docsguard.BrokenLinksIn(filepath.Join(t.TempDir(), "absent.md")); err == nil {
		t.Fatal("a missing file was accepted")
	}
}

// TestBrokenLinksIn_IgnoresWhatIsNotARelativePath keeps the check to the one thing it can
// verify offline.
func TestBrokenLinksIn_IgnoresWhatIsNotARelativePath(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	path := filepath.Join(dir, "doc.md")
	body := "[web](https://example.invalid/x) [mail](mailto:a@example.invalid) " +
		"[anchor](#section) [empty]() [fragment only](#) [self](doc.md#top)\n"
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}

	broken, err := docsguard.BrokenLinksIn(path)
	if err != nil {
		t.Fatal(err)
	}
	if len(broken) != 0 {
		t.Fatalf("BrokenLinksIn reported %v, want nothing", broken)
	}
}

func TestMarkdownFiles_MissingRoot(t *testing.T) {
	t.Parallel()

	if _, err := docsguard.MarkdownFiles(filepath.Join(t.TempDir(), "absent")); err == nil {
		t.Fatal("a missing root was accepted")
	}
}

// TestTransitionEvents reads the names out of a cell that also carries a logging level.
func TestTransitionEvents(t *testing.T) {
	t.Parallel()

	ts, err := docsguard.ParseTransitions(transitionTable(
		"| T1 | INFLIGHT | term | fenced | dead-letter | `msg.term`, `msg.dead` (WARN) | stale |\n"))
	if err != nil {
		t.Fatal(err)
	}
	got := ts[0].Events()
	if len(got) != 2 || got[0] != "msg.term" || got[1] != "msg.dead" {
		t.Fatalf("Events() = %v, want [msg.term msg.dead]", got)
	}
}

const validADR = `# 0001. First

- Status: Accepted
- Date: 2026-08-21
- Adjudicates: none
- Relates to: issue #4

## Context

## Decision

## Alternatives

## Consequences

## Revisit trigger
`
