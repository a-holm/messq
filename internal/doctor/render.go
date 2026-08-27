// SPDX-License-Identifier: Apache-2.0

package doctor

import (
	"fmt"
	"io"
	"sort"
	"strings"
	"time"
)

// Report is one doctor run handed to the renderer (issue #30 §8): the sorted
// findings of every check plus the facts the output faces need. The CLI fills
// Target and GeneratedAt from its own seams; the renderer stays pure.
type Report struct {
	GeneratedAt time.Time
	Source      Source
	Target      Target
	Findings    []Finding
	Checks      int // how many checks the registry evaluated
	Duration    time.Duration
}

// Target names what was diagnosed. Field names are frozen at schema 1.
type Target struct {
	Addr    string `json:"addr,omitempty"`
	DataDir string `json:"data_dir,omitempty"`
	Version string `json:"version,omitempty"`
}

// Summary is the frozen summary object closing every machine document.
// Skipped is honest cost, not attention: skips never raise the exit code
// below whatever --fail-on demands.
type Summary struct {
	Checks     int   `json:"checks"`
	OK         int   `json:"ok"`
	Info       int   `json:"info"`
	Warn       int   `json:"warn"`
	Fail       int   `json:"fail"`
	Skipped    int   `json:"skipped"`
	DurationMS int64 `json:"duration_ms"`
	ExitCode   int   `json:"exit_code"`
}

// JSONDocument is the frozen --output json shape: {schema, generated_at,
// source, target, findings[], summary}. Keys are compatibility surface;
// renaming any of them is a breaking change on the greppable contract.
type jsonDocument struct {
	Schema      int       `json:"schema"`
	GeneratedAt int64     `json:"generated_at"`
	Source      string    `json:"source"`
	Target      *Target   `json:"target,omitempty"`
	Findings    []Finding `json:"findings"`
	Summary     Summary   `json:"summary"`
}

const documentSchema = 1

func sourceName(s Source) string {
	if s == SourceLive {
		return "live"
	}
	return "data-dir"
}

// SortFindings orders deterministically for goldens: severity desc, then ID
// asc, then subject asc. It always copies — callers keep ownership of their
// slice. Skips sort dead last in human faces; order here matches that too, so
// both faces show identical sequences.
func SortFindings(in []Finding) []Finding {
	out := make([]Finding, len(in))
	copy(out, in)
	sort.SliceStable(out, func(i, j int) bool {
		a, b := out[i], out[j]
		ra, rb := SevRank(a.Severity), SevRank(b.Severity)
		if ra != rb {
			return ra > rb
		}
		if a.ID != b.ID {
			return a.ID < b.ID
		}
		return subjectKey(a.Subject) < subjectKey(b.Subject)
	})
	return out
}

// hoistID is the read-only latch finding. During an fsyncgate latch nothing
// else matters, so the face hoists it above the normal sort (§11).
const hoistID = "storage.readonly_latch"

// HoistReadonlyLatch moves the latch finding to the very front regardless of
// severity or sort order, preserving the relative order of everything else.
func HoistReadonlyLatch(sorted []Finding) []Finding {
	for i := range sorted {
		if sorted[i].ID != hoistID {
			continue
		}
		out := make([]Finding, 0, len(sorted))
		out = append(out, sorted[i])
		out = append(out, sorted[:i]...)
		out = append(out, sorted[i+1:]...)
		return out
	}
	return sorted
}

// SevRank orders severities for sorting and threshold comparison. The zero
// value SevSkipped deliberately ranks lowest: "could not judge" never outranks
// a verdict and never drives an exit code by itself.
func SevRank(s Severity) int {
	switch s {
	case SevFail:
		return 4
	case SevWarn:
		return 3
	case SevInfo:
		return 2
	case SevOK:
		return 1
	case SevSkipped:
		return 0
	default:
		return 0
	}
}

func subjectKey(s Subject) string {
	return s.Stream + "\x00" + s.Consumer + "\x00" + s.Path
}

// Summarize counts one run's findings and computes the process exit code from
// the default fail-on threshold (--fail-on warn). Use [Summary.ExitCode] with
// another threshold after parsing the flag.
func Summarize(findings []Finding, checks int, dur time.Duration) Summary {
	var sum Summary
	sum.Checks = checks
	sum.DurationMS = dur.Milliseconds()
	for _, f := range findings {
		switch f.Severity {
		case SevOK:
			sum.OK++
		case SevInfo:
			sum.Info++
		case SevWarn:
			sum.Warn++
		case SevFail:
			sum.Fail++
		case SevSkipped:
			sum.Skipped++
		default:
			sum.Skipped++
		}
	}
	sum.ExitCode = sum.ExitCodeFor("warn")
	return sum
}

// ExitCodeFor applies the --fail-on contract: 0 when nothing ranks at or above
// the threshold ("never" → always 0), else 1. Skips rank lowest and can never
// trip it — a doctor full of needs-more-data answers still exits 0 when it has
// no real findings to name.
func (s Summary) ExitCodeFor(failOn string) int {
	thresh, err := ParseFailOn(failOn)
	if err != nil || thresh == Severity(255) {
		return 0 // "never": unrankable by construction at parse time
	}
	counts := map[Severity]int{
		SevSkipped: s.Skipped, SevOK: s.OK, SevInfo: s.Info,
		SevWarn: s.Warn, SevFail: s.Fail,
	}
	total := 0
	for sev, n := range counts {
		if SevRank(sev) >= SevRank(thresh) {
			total += n
		}
	}
	if total > 0 {
		return 1
	}
	return 0
}

// ParseFailOn validates the --fail-on flag against its closed set. Any other
// value refuses as usage rather than silently widening the exit contract.
func ParseFailOn(v string) (Severity, error) {
	switch v {
	case "never":
		return Severity(255), nil
	case "info":
		return SevInfo, nil
	case "warn":
		return SevWarn, nil
	case "fail":
		return SevFail, nil
	default:
		return SevSkipped, fmt.Errorf("invalid --fail-on %q: want info|warn|fail|never", v)
	}
}

// JSONDocument renders the frozen machine shape with all findings, sorted and
// hoisted. Empty targets are omitted entirely rather than printed as {}.
func JSONDocument(rep Report) any {
	sorted := HoistReadonlyLatch(SortFindings(rep.Findings))
	doc := jsonDocument{
		Schema:      documentSchema,
		GeneratedAt: rep.GeneratedAt.UnixMilli(),
		Source:      sourceName(rep.Source),
		Findings:    sorted,
		Summary:     Summarize(rep.Findings, rep.Checks, rep.Duration),
	}
	var tgt *Target
	if rep.Target.Addr != "" || rep.Target.DataDir != "" || rep.Target.Version != "" {
		tgt = &Target{
			Addr:    rep.Target.Addr,
			DataDir: rep.Target.DataDir,
			Version: rep.Target.Version,
		}
	}
	doc.Target = tgt
	return doc
}

// NDJSONRecords returns one record per finding (sorted, hoisted) for the
// streaming face. The command emits the summary object as one final line
// itself — that trailing line is prose-adjacent state, not a finding, so it is
// kept off this slice which doubles as the three-faces agreement data.
func NDJSONRecords(rep Report) []any {
	sorted := HoistReadonlyLatch(SortFindings(rep.Findings))
	out := make([]any, len(sorted))
	for i := range sorted {
		out[i] = sorted[i]
	}
	return out
}

// HumanOpts tunes the prose face.
type HumanOpts struct {
	Quiet  bool // drop [ok]/[info] blocks
	Colour bool // ANSI tag styling; machine modes never set this
}

// ANSI colours per severity tag. Applied only around the bracketed tag so the
// body text survives NO_COLOR-based grepping even under --color always.
var sevANSI = map[Severity]string{
	SevFail:    "\x1b[31m",
	SevWarn:    "\x1b[33m",
	SevInfo:    "\x1b[36m",
	SevOK:      "\x1b[32m",
	SevSkipped: "\x1b[90m",
}

const ansiReset = "\x1b[0m"

// WriteHuman renders the prose face: header line, one block per finding —
// tag, id+subject, title, indented detail, arrowed fix commands or the NoFix
// sentence — then the counts footer. Bytes depend only on the report and opts:
// golden-safe by construction (the clock rides inside rep).
func WriteHuman(w io.Writer, rep Report, opts HumanOpts) error {
	sorted := HoistReadonlyLatch(SortFindings(rep.Findings))

	header := "messq doctor"
	var parts []string
	if rep.Target.Version != "" || rep.Target.Addr != "" {
		parts = append(parts, fmt.Sprintf("daemon %s at %s", rep.Target.Version, rep.Target.Addr))
	}
	if rep.Target.DataDir != "" {
		parts = append(parts, "data-dir "+rep.Target.DataDir)
	}
	if len(parts) > 0 {
		header += "   " + strings.Join(parts, "   ")
	}
	if _, err := fmt.Fprintln(w, header); err != nil {
		return err
	}

	shown := 0
	for _, f := range sorted {
		if opts.Quiet && (f.Severity == SevOK || f.Severity == SevInfo) {
			continue
		}
		shown++
		block, bErr := humanBlock(f, opts.Colour)
		if bErr != nil {
			return bErr
		}
		if _, err := io.WriteString(w, block); err != nil {
			return err
		}
	}

	if _, err := fmt.Fprintln(w); err != nil {
		return err
	}
	return printFooter(w, rep)
}

// humanBlock renders one finding's lines including the trailing newline.
// Skips carry their own layout — `[skip] <id> — reason` — because an operator
// greps for the exact check ID when deciding whether missing data matters;
// verdict findings keep the §8 prose shape without raw IDs.
func humanBlock(f Finding, colour bool) (string, error) {
	tag := "[" + f.Severity.String() + "]"
	if colour {
		if code, ok := sevANSI[f.Severity]; ok {
			tag = code + tag + ansiReset
		}
	}
	var b strings.Builder
	if f.Severity == SevSkipped {
		reason := f.Detail
		if reason == "" {
			reason = f.Title
		}
		fmt.Fprintf(&b, "%s %s — %s\n", tag, renderSafe(f.ID), renderSafe(reason))
		if f.NoFix != "" {
			fmt.Fprintf(&b, "       (%s)\n", renderSafe(f.NoFix))
		}
		return b.String(), nil
	}
	label := ""
	if sub := subjectLabel(f.Subject); sub != "" {
		label = sub + ": "
	}
	fmt.Fprintf(&b, "%s %s%s\n", tag, label, renderSafe(f.Title))
	for _, line := range wrapDetail(f.Detail) {
		fmt.Fprintf(&b, "       %s\n", renderSafe(line))
	}
	for _, cmd := range f.Fix {
		fmt.Fprintf(&b, "       -> %s\n", renderSafe(cmd))
	}
	if f.NoFix != "" && len(f.Fix) == 0 {
		fmt.Fprintf(&b, "       (%s)\n", renderSafe(f.NoFix))
	}
	return b.String(), nil
}

// printFooter closes the face: what needs attention and what it costs. The
// counts come from ALL findings, not the quiet-filtered view, so cron logs stay
// comparable between --quiet and verbose runs.
func printFooter(w io.Writer, rep Report) error {
	sec := formatSeconds(rep.Duration)
	all := Summarize(rep.Findings, rep.Checks, rep.Duration)
	attn := all.Info + all.Warn + all.Fail
	if attn > 0 {
		noun := "findings need"
		if attn == 1 {
			noun = "finding needs"
		}
		_, err := fmt.Fprintf(w, "%d %s attention (%d fail, %d warn, %d info) · %d checks · %s\n",
			attn, noun, all.Fail, all.Warn, all.Info, all.Checks, sec)
		return err
	}
	_, err := fmt.Fprintf(w, "no findings need attention · %d checks · %s\n", all.Checks, sec)
	return err
}

// subjectLabel renders the subject prefix for a finding line: stream/consumer
// for consumer findings, bare stream otherwise, path for storage subjects.
func subjectLabel(s Subject) string {
	switch {
	case s.Stream != "" && s.Consumer != "":
		return s.Stream + "/" + s.Consumer
	case s.Stream != "":
		return "stream " + s.Stream
	case s.Path != "":
		return s.Path
	default:
		return ""
	}
}

// wrapDetail splits a detail paragraph into display lines: doctor's details are
// hand-written prose of 1–3 sentences kept under roughly 100 columns by their
// authors; the renderer only breaks on explicit newline, never reflows.
func wrapDetail(detail string) []string {
	if strings.TrimSpace(detail) == "" {
		return nil
	}
	lines := strings.Split(strings.TrimRight(detail, "\n"), "\n")
	out := make([]string, 0, len(lines))
	for _, l := range lines {
		l = strings.TrimSpace(l)
		if l != "" {
			out = append(out, l)
		}
	}
	return out
}

// formatSeconds prints durations like the issue examples: 1.9s.
func formatSeconds(d time.Duration) string {
	return fmt.Sprintf("%.1fs", d.Seconds())
}

// renderSafe reuses the CLI sanitiser's contract without importing it (doctor
// may be linked by tools that must not reach internal/cli): titles, details and
// fix commands come from config and names operators chose, but subjects ride
// through config files too, so the same escape rule applies.
//
// Doctor's own Subject fields are already constrained to stream/consumer/path
// names; this is belt-and-braces for evidence-shaped Detail strings.
func renderSafe(s string) string { return safeText(s) }

func safeText(s string) string {
	var b strings.Builder
	for _, r := range s {
		if r < 0x20 || (r >= 0x7f && r <= 0x9f) {
			fmt.Fprintf(&b, "\\x%02x", r)
			continue
		}
		b.WriteRune(r)
	}
	return b.String()
}
