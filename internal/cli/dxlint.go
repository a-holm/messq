// SPDX-License-Identifier: Apache-2.0

package cli

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"

	"github.com/a-holm/messq/internal/cli/exit"

	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
)

// lintCommandTree walks the whole tree and reports every violation of the §8 DX
// rules. The linter is what keeps the contract from eroding one command at a time:
// a command that skips its teaching duties cannot ship.
//
// Exemptions: cobra's generated help command (its phrasing is cobra's, not ours);
// the completion command is disabled at the root so it can never appear here before
// #26 gives it a proper face.
func lintCommandTree(root *cobra.Command) []string {
	var problems []string
	var walk func(cmd *cobra.Command)
	walk = func(cmd *cobra.Command) {
		if !dxExempt[cmd.Name()] {
			problems = append(problems, lintOne(cmd)...)
		}
		problems = append(problems, lintSiblingAliases(cmd)...)
		for _, sub := range cmd.Commands() {
			walk(sub)
		}
	}
	walk(root)
	return problems
}

// lintSiblingAliases enforces §8's alias rule within one command group: two siblings
// must not share an alias, and an alias must not swallow a sibling's name — either
// way `messq <alias>` resolves to a command the operator did not mean. Exempt
// commands still count as names: aliasing to "help" collides even though help's own
// phrasing is cobra's.
func lintSiblingAliases(parent *cobra.Command) []string {
	kids := parent.Commands()
	if len(kids) < 2 {
		return nil
	}
	names := make(map[string]bool, len(kids))
	for _, c := range kids {
		names[c.Name()] = true
	}
	var problems []string
	firstAlias := make(map[string]string, len(kids)) // alias -> its first owner
	for _, c := range kids {
		for _, a := range c.Aliases {
			if other, taken := firstAlias[a]; taken {
				problems = append(problems,
					fmt.Sprintf("%s: alias %q collides between %s and %s", c.Name(), a, other, c.Name()))
				continue
			}
			if a != c.Name() && names[a] {
				problems = append(problems,
					fmt.Sprintf("%s: alias %q collides with sibling command's name", c.Name(), a))
				continue
			}
			firstAlias[a] = c.Name()
		}
	}
	return problems
}

var (
	nameShape = regexp.MustCompile(`^[a-z][a-z0-9-]*$`)
	dxExempt  = map[string]bool{"help": true, "completion": true}
	// documented is the closed set annExits may name: the 0–7 contract plus its
	// two documented exceptions — serve's sysexits 74/75/78 and interrupt's 130.
	dxDocumented = map[int]bool{
		exit.OK: true, exit.Error: true, exit.Usage: true, exit.NotFound: true,
		exit.Conflict: true, exit.Empty: true, exit.Unreachable: true, exit.Denied: true,
		74: true, 75: true, 78: true, 130: true,
	}
)

func lintOne(cmd *cobra.Command) []string {
	// Hidden helpers are not help surface: the DX rules below teach through
	// help output a user never sees for a hidden command. The quickstart tour's
	// demo worker (issue #26 §1) is the reason this exemption exists — its name
	// is issue-mandated and its documentation lives in its own Long.
	if cmd.Hidden {
		return nil
	}
	var problems []string
	add := func(format string, args ...any) {
		problems = append(problems, fmt.Sprintf("%s: %s", cmd.Name(), fmt.Sprintf(format, args...)))
	}

	if cmd.Short == "" {
		add("empty Short")
	} else {
		first := firstRune(cmd.Short)
		if first != strings.ToLower(first) {
			add("Short must start lowercase, got %q", first)
		}
		if strings.HasSuffix(cmd.Short, ".") {
			add("Short must not end in a period")
		}
	}
	if runes := len([]rune(cmd.Long)); runes < 120 {
		add("Long is only %d characters; help must teach the concept (>=120)", runes)
	}

	runnable := cmd.RunE != nil || cmd.Run != nil || len(cmd.Commands()) > 0
	if runnable && strings.TrimSpace(cmd.Example) == "" {
		add("runnable command has no Example")
	}
	if cmd.Run != nil {
		add("uses Run; every command uses RunE so failures reach the exit funnel")
	}
	if cmd.PersistentPreRun != nil {
		add("uses PersistentPreRun; use PersistentPreRunE (traverse hooks)")
	}
	if runnable && cmd.Args == nil {
		add("Args is nil; declare an Args validator so typos are refused")
	}
	if !nameShape.MatchString(cmd.Name()) || len(cmd.Name()) > 12 {
		add("name %q is not lowercase kebab or exceeds 12 characters", cmd.Name())
	}

	checkFlags(cmd.LocalFlags(), add)
	checkAnnExits(cmd, add)
	return problems
}

// checkFlags lints one flag set: usage strings are teaching surfaces too.
func checkFlags(fs *pflag.FlagSet, add func(string, ...any)) {
	if fs == nil {
		return
	}
	fs.VisitAll(func(f *pflag.Flag) {
		usage := f.Usage
		switch {
		case strings.TrimSpace(usage) == "":
			add("flag --%s has empty usage", f.Name)
		case strings.HasSuffix(usage, "."):
			add("flag --%s usage must not end in a period", f.Name)
		}
	})
}

func checkAnnExits(cmd *cobra.Command, add func(string, ...any)) {
	code, ok := cmd.Annotations[annExits]
	if !ok {
		return
	}
	for _, part := range strings.Split(code, ",") {
		n, err := strconv.Atoi(strings.TrimSpace(part))
		if err != nil {
			add("annotation messq.exits=%q is not a comma-separated integer list", code)
			continue
		}
		if !dxDocumented[n] {
			add("annotation messq.exits names %d, outside the documented table", n)
		}
	}
}

func firstRune(s string) string {
	for _, r := range s {
		return string(r)
	}
	return ""
}

// findCmd returns the subcommand with the given first-level name, for tests.
func findCmd(root *cobra.Command, name string) *cobra.Command {
	for _, c := range root.Commands() {
		if c.Name() == name {
			return c
		}
	}
	return nil
}
