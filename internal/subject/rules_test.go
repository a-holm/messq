// SPDX-License-Identifier: Apache-2.0

package subject_test

import (
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/a-holm/messq/internal/subject"
)

// update rewrites the generated rules document instead of comparing against it.
var update = flag.Bool("update", false, "rewrite docs/generated/subject-rules.md")

// generatedDoc is the file docs/SEMANTICS.md includes. It is written from the same tables the
// tests below iterate, so the documented grammar cannot drift from the tested one.
const generatedDoc = "../../docs/generated/subject-rules.md"

// rule is one row of the normative grammar table.
type rule struct {
	ID   string
	Text string
}

// rules is the grammar of PLAN.md section 1.3 written out. Every row has a test.
var rules = []rule{
	{"S1", "A subject is 1 to 32 tokens joined by `.`; total length at most 512 bytes."},
	{"S2", "Tokens are non-empty. `\"\"`, `\".\"`, `\"a..b\"`, `\".a\"`, `\"a.\"` are invalid."},
	{"S3", "A literal token may contain any valid UTF-8 except space, tab, any control character, `.`, `*` and `>`. There is no escape mechanism, by design."},
	{"S4", "Subjects are case-sensitive: `A.b` is not `a.b`."},
	{"S5", "In a pattern, `*` is valid only as a complete token and matches exactly one token, never a substring. `foo*` and `f*o` are invalid."},
	{"S6", "In a pattern, `>` is valid only as a complete token and only as the last token. `>.a`, `a.>.b` and `foo>` are invalid."},
	{"S7", "`>` matches one or more trailing tokens: `a.>` matches `a.b` and `a.b.c`, but not `a`."},
	{"S8", "The bare pattern `>` matches every valid subject. It is the default consumer filter."},
	{"S9", "A pattern containing no `>` matches only subjects with exactly the same token count."},
	{"S10", "A literal subject, meaning a publish target, must contain no `*` and no `>`."},
	{"S11", "Stream and consumer names are 1 to 64 bytes of `[A-Za-z0-9_-]`, plus `.` for streams only so that `<stream>.dlq` is expressible. A name must not start or end with `.`, must not contain `..` or `/`, and must not be `.` or `..`. A stream name is capped at 60 bytes so its derived `<stream>.dlq` name is itself a valid stream name."},
}

// matchRow is one row of the truth table: does pattern match subject?
type matchRow struct {
	Pattern string
	Subject string
	Want    bool
	Note    string
}

// truthTable is the normative answer for every case worth writing down. It drives
// TestMatchTruthTable, the naive differential reference, and the generated document.
var truthTable = []matchRow{
	{">", "a", true, "S8: the default filter matches a one-token subject"},
	{">", "a.b.c", true, "S8: and a deep one"},
	{">", "", false, "the empty subject is not a subject"},
	{">", "*", false, "a pattern handed in as a subject never matches"},
	{">", ">", false, "a pattern handed in as a subject never matches"},
	{"a.>", "a", false, "S7: the classic off-by-one"},
	{"a.>", "a.b", true, "S7: one trailing token"},
	{"a.>", "a.b.c", true, "S7: more than one trailing token"},
	{"a.>", "a.", false, "S2: a trailing dot leaves an empty token"},
	{"a.>", "a.b.", false, "S2: an empty token behind the wildcard"},
	{"a.>", "a..b", false, "S2: an empty token inside the wildcard tail"},
	{"a.>", "a.>", false, "a pattern handed in as a subject never matches"},
	{"a.*", "a.b", true, "S5: one token"},
	{"a.*", "a.b.c", false, "S5: never more than one"},
	{"a.*", "a", false, "S9: token counts differ"},
	{"a.*", "a.", false, "S2: a trailing dot leaves an empty token"},
	{"a.*", "a.*", false, "a pattern handed in as a subject never matches"},
	{"*", "a", true, "S5: exactly one token"},
	{"*", "a.b", false, "S5: never a separator"},
	{"*", "", false, "the empty subject is not a subject"},
	{"*", "*", false, "a pattern handed in as a subject never matches"},
	{"a.b", "a.b", true, "a literal pattern is string equality"},
	{"a.b", "a.B", false, "S4: case-sensitive"},
	{"a.b", "a.b.c", false, "S9: token counts differ"},
	{"a.b.c", "a.b", false, "S9: token counts differ"},
	{"a.b", ".a.b", false, "S2: a leading dot leaves an empty token"},
	{"a.b", "a.b.", false, "S2: a trailing dot leaves an empty token"},
	{"a.b", "a..b", false, "S2: a doubled dot leaves an empty token"},
	{"*.b", "a.b", true, "S5: a leading wildcard"},
	{"*.>", "a.b", true, "S5 and S7 together"},
	{"*.>", "a", false, "S7: `>` needs a token of its own"},
	{"a.*.c", "a.b.c", true, "S5: a wildcard in the middle"},
	{"a.*.c", "a..c", false, "S2: an empty token is not a token"},
	{"orders.eu.*.created", "orders.eu.de.created", true, "the worked example"},
	{"orders.>", "orders.eu.de.created", true, "the worked example, wildcard tail"},
	{"orders.>", "orders", false, "S7 on the worked example"},
	{"ordrer.å", "ordrer.å", true, "S3: any valid UTF-8 outside the reserved set"},
	{"ordrer.*", "ordrer.å", true, "S5: a wildcard covers a multi-byte token"},
	{"ordrer.å", "ordrer.a", false, "S4: bytes, not glyphs; no normalisation"},
}

func TestMatchTruthTable(t *testing.T) {
	t.Parallel()

	for _, row := range truthTable {
		t.Run(fmt.Sprintf("%s_matches_%s", displayName(row.Pattern), displayName(row.Subject)), func(t *testing.T) {
			t.Parallel()

			pat, err := subject.ParsePattern(row.Pattern)
			if err != nil {
				t.Fatalf("ParsePattern(%q) = %v, want a compiled pattern", row.Pattern, err)
			}
			if got := pat.Match(row.Subject); got != row.Want {
				t.Fatalf("ParsePattern(%q).Match(%q) = %t, want %t (%s)", row.Pattern, row.Subject, got, row.Want, row.Note)
			}
		})
	}
}

// TestGeneratedRulesDocIsCurrent keeps docs/generated/subject-rules.md equal to the tables
// above. Run `go test ./internal/subject -update` after changing a rule.
func TestGeneratedRulesDocIsCurrent(t *testing.T) {
	want := renderRulesDoc()

	if *update {
		if err := os.MkdirAll(filepath.Dir(generatedDoc), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(generatedDoc, []byte(want), 0o644); err != nil {
			t.Fatal(err)
		}
		t.Logf("wrote %s", generatedDoc)
		return
	}

	got, err := os.ReadFile(generatedDoc)
	if err != nil {
		t.Fatalf("%v\nrun: go test ./internal/subject -update", err)
	}
	if string(got) != want {
		t.Fatalf("%s is stale; run: go test ./internal/subject -update", generatedDoc)
	}
}

// renderRulesDoc writes the tables out as Markdown. docs/SEMANTICS.md includes the result.
func renderRulesDoc() string {
	var b strings.Builder
	b.WriteString("<!-- Generated by `go test ./internal/subject -update`. Do not edit. -->\n\n")
	b.WriteString("# Subject grammar and matcher truth table\n\n")
	b.WriteString("## Grammar\n\n| # | Rule |\n|---|---|\n")
	for _, r := range rules {
		fmt.Fprintf(&b, "| %s | %s |\n", r.ID, r.Text)
	}

	b.WriteString("\n## Truth table\n\n| Pattern | Subject | Matches | Why |\n|---|---|---|---|\n")
	for _, row := range truthTable {
		fmt.Fprintf(&b, "| `%s` | `%s` | %t | %s |\n", row.Pattern, displayEmpty(row.Subject), row.Want, row.Note)
	}

	b.WriteString("\n## Rejected input\n\n| Input | Parsed as | Error |\n|---|---|---|\n")
	for _, row := range rejectTable {
		fmt.Fprintf(&b, "| `%s` | %s | `%s` |\n", displayEmpty(row.label()), row.As, row.Err)
	}
	return b.String()
}

func displayEmpty(s string) string {
	if s == "" {
		return "(empty)"
	}
	return s
}

// displayName turns a pattern or subject into something readable in a test name.
func displayName(s string) string {
	if s == "" {
		return "empty"
	}
	r := strings.NewReplacer(".", "_", "*", "star", ">", "gt", " ", "sp")
	return r.Replace(s)
}
