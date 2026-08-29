// SPDX-License-Identifier: Apache-2.0

// Package script hosts the testscript (.txtar) golden suite — the CLI's contract
// (issue #26 §5). Every command the tree exposes must be exercised by at least one
// script, every documented exit code must be produced by at least one `exitcode`
// line, and the goldens must stay machine-lintable: no ANSI escapes, no trailing
// whitespace, no raw temp paths.
//
// The bindings in this file are deliberately stdlib-only: they must compile and
// fail with named output even before the harness exists, which is how the suite
// was bootstrapped red-first.
package script

import (
	"bufio"
	"bytes"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"testing"

	"github.com/a-holm/messq/internal/cli"
	"github.com/rogpeppe/go-internal/txtar"
	"github.com/spf13/cobra"
)

// scriptDir is where the .txtar scripts live, relative to this package.
const scriptDir = "."

// optOutDir holds the reviewed opt-out files: no-script.txt (command<TAB>reason)
// and no-exitcode.txt (code<TAB>reason). Opt-outs are reviewed like code; each one
// names the issue or slice that removes it.
const optOutDir = "testdata"

// TestEveryCommandHasAScript walks the real cobra tree produced by cli.NewRoot and
// requires every command path to be exercised by at least one script line, or to
// carry a reasoned opt-out. A new command without a script fails here naming it —
// that failure is the gate, so a silent tree growth cannot skip the golden suite.
func TestEveryCommandHasAScript(t *testing.T) {
	paths := commandPaths(t)
	scripts := loadScripts(t)
	exercised := exercisedPaths(scripts)
	optOut := readOptOuts(t, "no-script.txt")

	var missing []string
	for _, p := range paths {
		if exercised[p] {
			continue
		}
		if reason, ok := optOut[p]; ok && reason != "" {
			t.Logf("opt-out: %s — %s", p, reason)
			continue
		}
		missing = append(missing, p)
	}
	if len(missing) > 0 {
		sort.Strings(missing)
		t.Fatalf("commands with no script and no opt-out (add a .txtar or a reasoned\n"+
			"entry in %s):\n  %s", filepath.Join(optOutDir, "no-script.txt"),
			strings.Join(missing, "\n  "))
	}
}

// TestEveryExitCodeIsExercised requires every documented exit code 0–7 to be
// produced by at least one `exitcode <n> messq ...` line across the suite. A code
// the current tree cannot produce yet carries a reasoned opt-out in
// testdata/no-exitcode.txt until the command wave that produces it lands.
func TestEveryExitCodeIsExercised(t *testing.T) {
	scripts := loadScripts(t)
	seen := map[int]string{} // code → script that exercises it
	for _, s := range scripts {
		for _, line := range splitLines(s.data) {
			code, ok := exitcodeLine(line)
			if !ok {
				continue
			}
			if _, dup := seen[code]; !dup {
				seen[code] = s.name
			}
		}
	}
	optOut := readOptOuts(t, "no-exitcode.txt")

	var missing []string
	for c := 0; c <= 7; c++ {
		if _, ok := seen[c]; ok {
			continue
		}
		if reason, ok := optOut[strconv.Itoa(c)]; ok && reason != "" {
			t.Logf("opt-out: exit %d — %s", c, reason)
			continue
		}
		missing = append(missing, strconv.Itoa(c))
	}
	if len(missing) > 0 {
		t.Fatalf("exit codes never exercised by an `exitcode` line (add a script or a\n"+
			"reasoned entry in %s): %s", filepath.Join(optOutDir, "no-exitcode.txt"),
			strings.Join(missing, ", "))
	}
}

// TestNoANSIInGoldens fails when an escape byte reaches a golden section: colour
// must never leak past render.Safe and NO_COLOR, and a golden with ANSI churns on
// every editor.
func TestNoANSIInGoldens(t *testing.T) {
	for _, s := range loadScripts(t) {
		for _, f := range s.archive.Files {
			if bytes.ContainsRune(f.Data, '\x1b') {
				t.Errorf("%s: golden %q contains an ANSI escape byte", s.name, f.Name)
			}
		}
	}
}

// TestNoTrailingSpaceInGoldens fails when a golden line ends in whitespace: the
// renderers must not pad, and editors would churn these files forever.
func TestNoTrailingSpaceInGoldens(t *testing.T) {
	for _, s := range loadScripts(t) {
		for _, f := range s.archive.Files {
			for i, line := range splitLines(string(f.Data)) {
				if line != strings.TrimRight(line, " \t") {
					t.Errorf("%s: golden %q line %d has trailing whitespace",
						s.name, f.Name, i+1)
				}
			}
		}
	}
}

// TestNoRawTempPathsInGoldens fails when a golden carries a raw $WORK-looking
// absolute temp path: those are normalised by `mask`/cmpenv before comparison, so a
// literal one means the normaliser was skipped and the golden cannot be stable.
func TestNoRawTempPathsInGoldens(t *testing.T) {
	markers := []string{"/tmp/go-build", "/tmp/go-test-script", "/tmp/messq-test"}
	for _, s := range loadScripts(t) {
		for _, f := range s.archive.Files {
			for _, m := range markers {
				if bytes.Contains(f.Data, []byte(m)) {
					t.Errorf("%s: golden %q contains a raw temp path (%s); run `mask` first",
						s.name, f.Name, m)
				}
			}
		}
	}
}

// ---- helpers ----

type scriptFile struct {
	name    string // base name, e.g. version.txtar
	path    string
	data    string
	archive *txtar.Archive
}

func loadScripts(t *testing.T) []scriptFile {
	t.Helper()
	entries, err := os.ReadDir(scriptDir)
	if err != nil {
		t.Fatalf("read script dir: %v", err)
	}
	var out []scriptFile
	for _, e := range entries {
		if !strings.HasSuffix(e.Name(), ".txtar") && !strings.HasSuffix(e.Name(), ".txt") {
			continue
		}
		raw, err := os.ReadFile(filepath.Join(scriptDir, e.Name()))
		if err != nil {
			t.Fatalf("read script %s: %v", e.Name(), err)
		}
		ar := txtar.Parse(raw)
		out = append(out, scriptFile{name: e.Name(), path: filepath.Join(scriptDir, e.Name()), data: string(ar.Comment), archive: ar})
	}
	return out
}

// commandPaths returns every command path in the real tree, including the bare
// root, in stable order.
func commandPaths(t *testing.T) []string {
	t.Helper()
	root := cli.NewRoot(&cli.Env{})
	var out []string
	var walk func(c *cobra.Command, words []string)
	walk = func(c *cobra.Command, words []string) {
		out = append(out, "messq"+joinWords(words))
		for _, sub := range c.Commands() {
			if sub.Hidden {
				// Hidden helpers (quickstart-handler) are excluded by name in
				// no-script.txt if they need an exemption; walking them keeps the
				// gate honest about everything a user can reach through completion
				// of the help output.
				walk(sub, append(append([]string{}, words...), sub.Name()))
				continue
			}
			walk(sub, append(append([]string{}, words...), sub.Name()))
		}
	}
	walk(root, nil)
	return out
}

func joinWords(words []string) string {
	if len(words) == 0 {
		return ""
	}
	b := &strings.Builder{}
	for _, w := range words {
		b.WriteByte(' ')
		b.WriteString(w)
	}
	return b.String()
}

// exercisedPaths scans script bodies for lines that invoke `messq <words...>` —
// directly, under `exitcode <n>`, or negated with `!`. It maps each exercised
// command path to true.
func exercisedPaths(scripts []scriptFile) map[string]bool {
	out := map[string]bool{}
	for _, s := range scripts {
		for _, line := range splitLines(s.data) {
			toks, ok := cliTokens(line)
			if !ok {
				continue
			}
			out["messq"] = true // a bare `messq` line exercises the root
			for i := 1; i <= len(toks); i++ {
				out["messq"+joinWords(toks[:i])] = true
			}
		}
	}
	return out
}

// cliTokens strips the wrapper prefixes a script line may carry (`!`, `-`, `+`,
// `exitcode <n>`) and returns the command tokens when the line runs messq.
func cliTokens(line string) ([]string, bool) {
	fields := strings.Fields(line)
	for len(fields) > 0 {
		switch {
		case fields[0] == "!" || fields[0] == "-" || fields[0] == "+":
			fields = fields[1:]
		case fields[0] == "exitcode" && len(fields) >= 2:
			fields = fields[2:]
		default:
			// redirections and flags never lead a messq line in our scripts
			if len(fields) > 0 && fields[0] == "messq" {
				return fields[1:], true
			}
			return nil, false
		}
	}
	return nil, false
}

// exitcodeLine reports the code asserted by an `exitcode <n> messq ...` line.
func exitcodeLine(line string) (int, bool) {
	fields := strings.Fields(line)
	if len(fields) < 3 || fields[0] != "exitcode" {
		return 0, false
	}
	n, err := strconv.Atoi(fields[1])
	if err != nil {
		return 0, false
	}
	toks, ok := cliTokens(line)
	if !ok || len(toks) == 0 {
		return 0, false
	}
	return n, true
}

func splitLines(s string) []string {
	var out []string
	sc := bufio.NewScanner(strings.NewReader(s))
	for sc.Scan() {
		out = append(out, sc.Text())
	}
	return out
}

// readOptOuts parses `key<TAB>reason` lines, skipping blanks and #-comments.
// A key present with an EMPTY reason is treated as absent: an opt-out without a
// reason is not an opt-out.
func readOptOuts(t *testing.T, name string) map[string]string {
	t.Helper()
	path := filepath.Join(optOutDir, name)
	raw, err := os.ReadFile(path)
	if os.IsNotExist(err) {
		return map[string]string{}
	}
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	out := map[string]string{}
	sc := bufio.NewScanner(bytes.NewReader(raw))
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		key, reason, found := strings.Cut(line, "\t")
		if !found {
			key, reason, found = strings.Cut(line, "  ")
		}
		key = strings.TrimSpace(key)
		reason = strings.TrimSpace(reason)
		if key == "" || !found || reason == "" {
			t.Errorf("%s: entry %q must be key<TAB>reason with a non-empty reason", path, line)
			continue
		}
		out[key] = reason
	}
	return out
}
