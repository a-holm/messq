// SPDX-License-Identifier: Apache-2.0

// Package help is the help-topic surface (issue #26 §4): seven embedded markdown
// topics rendered plainly to a terminal, wired as cobra additional-help-topic
// commands so `messq --help` lists them and `messq help <topic>` renders them.
//
// The topics are one source of truth for the terminal, the docs site (#35) and
// the executable-docs harness. Two of them are BOUND TO CODE and cannot drift:
//
//   - exit-codes is generated at render time from internal/cli/exit's tables —
//     a new code without a meaning fails TestExitCodeTopicMatchesCode;
//   - lifecycle is checked against internal/lifecycle's transition table by
//     TestLifecycleTopicCoversEveryTransition — an unmentioned move fails.
package help

import (
	"embed"
	"fmt"
	"io"
	"sort"
	"strings"
	"unicode/utf8"
)

//go:embed topics/*.md
var topicFS embed.FS

// Topic is one help topic: its name (what the user types), its one-line
// synopsis (root help), and its embedded markdown source.
type Topic struct {
	Name     string
	Synopsis string
	Source   string
}

// topics is the closed registry, in the order the root help lists them.
var topics = []Topic{
	{Name: "concepts", Synopsis: "the vocabulary, one paragraph each", Source: mustTopic("concepts.md")},
	{Name: "lifecycle", Synopsis: "the daemon state machine and the message lifecycle", Source: mustTopic("lifecycle.md")},
	{Name: "durability", Synopsis: "the two durability modes and what a 201 means", Source: mustTopic("durability.md")},
	{Name: "exit-codes", Synopsis: "the 0–7 table and the documented exceptions", Source: ""}, // generated
	{Name: "scripting", Synopsis: "output modes, jq recipes, the sysexits table, MESSQ_*", Source: mustTopic("scripting.md")},
	{Name: "subjects", Synopsis: "* and > matching with a truth table", Source: mustTopic("subjects.md")},
	{Name: "output", Synopsis: "the three modes, TTY detection, ULID abbreviation", Source: mustTopic("output.md")},
}

// Names returns every topic's name in registry order.
func Names() []string {
	out := make([]string, 0, len(topics))
	for _, t := range topics {
		out = append(out, t.Name)
	}
	return out
}

// Get returns the named topic, rendering the generated ones. Unknown names are
// an error carrying the topic list — the same text the CLI prints.
func Get(name string) (Topic, error) {
	for _, t := range topics {
		if t.Name == name {
			if t.Source == "" {
				t.Source = exitCodesSource()
			}
			return t, nil
		}
	}
	return Topic{}, fmt.Errorf("unknown help topic %q (topics: %s)", name, strings.Join(Names(), ", "))
}

// All returns every topic with generated sources rendered.
func All() []Topic {
	out := make([]Topic, 0, len(topics))
	for _, t := range topics {
		if t.Source == "" {
			t.Source = exitCodesSource()
		}
		out = append(out, t)
	}
	return out
}

func mustTopic(file string) string {
	raw, err := topicFS.ReadFile("topics/" + file)
	if err != nil {
		panic("help: embedded topic missing: " + err.Error())
	}
	return string(raw)
}

// exitCodesSource generates the exit-codes topic from internal/cli/exit's
// tables — the const block is the source of truth, the topic cannot invent or
// miss a code (issue §4: "the exit-code table is generated, not typed").
func exitCodesSource() string {
	var b strings.Builder
	fmt.Fprint(&b, "# exit codes\n\n")
	fmt.Fprint(&b, "Every messq command exits 0–7. Two documented exceptions sit outside\n"+
		"the table: `messq serve` keeps its sysexits values `74` (storage latch),\n"+
		"`75` (data dir locked) and `78` (config will never work) for systemd's\n"+
		"`RestartPreventExitStatus`, and an interrupt exits `130` (128+SIGINT).\n\n")
	fmt.Fprint(&b, "| exit | name | meaning |\n|---|---|---|\n")
	for _, d := range exitDocumented() {
		fmt.Fprintf(&b, "| %d | `%s` | %s |\n", d.Code, d.Name, d.Meaning)
	}
	fmt.Fprint(&b, "\nBranch on them in shell:\n\n```\n")
	fmt.Fprint(&b, "messq peek orders --last 1 || case $? in\n")
	fmt.Fprint(&b, "  0) echo got one ;;\n")
	fmt.Fprint(&b, "  5) echo stream empty ;;\n")
	fmt.Fprint(&b, "  6) echo daemon unreachable — is `messq serve` running? ;;\n")
	fmt.Fprint(&b, "esac\n```\n")
	return b.String()
}

// renderWidth is the fixed wrap width; COLUMNS-aware wrapping is #19's surface
// and the terminal renderer keeps to one honest column layout instead.
const renderWidth = 100

// Render turns a topic's markdown into terminal text: headings underlined, code
// fences indented four spaces, no ANSI unless colour is requested. The renderer
// supports exactly the constructs the topics use — headings, paragraphs, fences,
// tables and lists — because a renderer that guesses is a renderer that lies.
func Render(md string, colour bool) string {
	return render(md, colour, renderWidth)
}

func render(md string, colour bool, width int) string {
	lines := strings.Split(strings.TrimRight(md, "\n"), "\n")
	var out []string
	inFence := false
	var fence []string
	for _, line := range lines {
		switch {
		case strings.HasPrefix(line, "```"):
			if inFence {
				// Close: emit the fence body indented four.
				for _, fl := range fence {
					out = append(out, "    "+fl)
				}
				fence = fence[:0]
				inFence = false
			} else {
				inFence = true
			}
		case inFence:
			fence = append(fence, line)
		case strings.HasPrefix(line, "# "):
			out = append(out, heading(strings.TrimPrefix(line, "# "), colour)...)
		case strings.HasPrefix(line, "## "):
			out = append(out, heading(strings.TrimPrefix(line, "## "), colour)...)
		case strings.HasPrefix(line, "|"):
			out = append(out, tableRow(line)...)
		case line == "":
			out = append(out, "")
		default:
			out = append(out, wrap(stripInline(line), width)...)
		}
	}
	return strings.TrimRight(strings.Join(out, "\n"), "\n") + "\n"
}

// stripInline drops the markdown inline markers the terminal renderer does not
// reproduce: **bold** keeps its text (the emphasis survives as paragraph lead-in
// prose), everything else passes through verbatim including backticks.
func stripInline(line string) string {
	for strings.Contains(line, "**") {
		line = strings.Replace(line, "**", "", 2)
	}
	return line
}

// heading renders an ATX heading underlined with ─ to its display width.
func heading(text string, colour bool) []string {
	text = strings.TrimSpace(text)
	w := utf8.RuneCountInString(text)
	rule := strings.Repeat("─", w)
	if colour {
		return []string{"\x1b[1m" + text + "\x1b[0m", rule}
	}
	return []string{text, rule}
}

// tableRow renders one markdown table row as aligned text: the leading/trailing
// pipes go, cells are separated by two spaces, and a separator row collapses to
// the same two-space gap. The topics' tables are short; alignment across rows
// arrives with #35's doc set if it ever needs it.
func tableRow(line string) []string {
	trimmed := strings.Trim(strings.TrimSpace(line), "|")
	cells := strings.Split(trimmed, "|")
	for i := range cells {
		cells[i] = strings.TrimSpace(cells[i])
	}
	if isSeparator(cells) {
		return nil // the |---|---| line carries no information on a terminal
	}
	return []string{strings.Join(cells, "  ")}
}

func isSeparator(cells []string) bool {
	for _, c := range cells {
		if strings.Trim(c, "-: ") != "" {
			return false
		}
	}
	return true
}

// wrap wraps one paragraph to width on spaces, never breaking a word.
func wrap(line string, width int) []string {
	if utf8.RuneCountInString(line) <= width {
		return []string{line}
	}
	words := strings.Fields(line)
	var out []string
	cur := ""
	for _, w := range words {
		switch {
		case cur == "":
			cur = w
		case utf8.RuneCountInString(cur)+1+utf8.RuneCountInString(w) <= width:
			cur += " " + w
		default:
			out = append(out, cur)
			cur = w
		}
	}
	if cur != "" {
		out = append(out, cur)
	}
	return out
}

// SortedNames is Names() sorted — for suggestions on an unknown topic.
func SortedNames() []string {
	n := Names()
	sort.Strings(n)
	return n
}

var _ = io.Discard
