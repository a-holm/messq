// SPDX-License-Identifier: Apache-2.0

// Package conf resolves every CLI setting through the same three layers —
//
//	flag > MESSQ_<UPPER_SNAKE> env > built-in default
//
// with no config file, no context file and no precedence engine (D8/ADR-0009: "this
// kills a parser, a precedence bug class, and a 'which setting won?' support
// category"). The environment name is DERIVED from the flag name, never registered by
// hand: --commit-window → MESSQ_COMMIT_WINDOW. An invalid env value is an error that
// names the ENVIRONMENT VARIABLE and the value, never the flag — the operator fixes
// their shell, not their alias.
package conf

import (
	"errors"
	"fmt"
	"io"
	"sort"
	"strings"

	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
)

// EnvName derives the MESSQ_* variable name for a flag name.
func EnvName(flag string) string {
	return "MESSQ_" + strings.ToUpper(strings.ReplaceAll(flag, "-", "_"))
}

// Source records where one resolved setting came from — the -vv dump's answer to
// "which setting won?".
type Source int

const (
	SourceDefault Source = iota
	SourceEnv
	SourceFlag
)

func (s Source) String() string {
	switch s {
	case SourceDefault:
		return "default"
	case SourceEnv:
		return "env"
	case SourceFlag:
		return "flag"
	default:
		return "unknown"
	}
}

// ApplyEnv applies the environment layer over cmd's flags. The flag layer has already
// run by the time this is called (pflag set f.Changed), and the default layer needs no
// work at all, so this function is the whole precedence engine:
//
//   - a flag given on the command line wins and is left untouched;
//   - otherwise a non-empty MESSQ_* value is parsed into the flag's own Value;
//   - an empty value counts as UNSET — never as "false" or "0";
//   - string-slice flags split the variable on commas (no quoting games);
//   - a value that will not parse is an error naming the variable, not the flag.
func ApplyEnv(cmd *cobra.Command, getenv func(string) string) error {
	var errs []error
	cmd.Flags().VisitAll(func(f *pflag.Flag) {
		if f.Changed { // layer 1 wins; nothing to resolve
			return
		}
		name := EnvName(f.Name)
		v := getenv(name)
		if v == "" { // empty env = unset, never a zero value
			return
		}
		var err error
		if sv, ok := f.Value.(pflag.SliceValue); ok {
			err = sv.Replace(strings.Split(v, ","))
		} else {
			err = f.Value.Set(v)
		}
		if err != nil {
			errs = append(errs, fmt.Errorf("%s=%q: %w", name, v, err))
			return
		}
		f.Changed = false // the flag was not set by a flag; -vv must report env
	})
	return errors.Join(errs...)
}

// Sources maps each of cmd's flags to the layer that produced its current value,
// computed AFTER [ApplyEnv] ran: Changed means a command-line flag, else a non-empty
// environment variable, else the built-in default.
func Sources(cmd *cobra.Command, getenv func(string) string) map[string]Source {
	out := make(map[string]Source)
	cmd.Flags().VisitAll(func(f *pflag.Flag) {
		switch {
		case f.Changed:
			out[f.Name] = SourceFlag
		case getenv(EnvName(f.Name)) != "":
			out[f.Name] = SourceEnv
		default:
			out[f.Name] = SourceDefault
		}
	})
	return out
}

// Dump renders the resolved configuration with its source per line, the -vv answer to
// "which setting won?":
//
//	addr        unix:///run/messq/messq.sock   (default)
//	output      json                           (env MESSQ_OUTPUT)
//	timeout     5s                             (flag --timeout)
//
// Rows are sorted by flag name so the dump is byte-stable for goldens.
func Dump(w io.Writer, cmd *cobra.Command, getenv func(string) string) {
	srcs := Sources(cmd, getenv)
	names := make([]string, 0, len(srcs))
	for name := range srcs {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		f := cmd.Flags().Lookup(name)
		src := srcs[name]
		attribution := "(default)"
		switch src {
		case SourceFlag:
			attribution = "(flag --" + name + ")"
		case SourceEnv:
			attribution = "(env " + EnvName(name) + ")"
		case SourceDefault:
		}
		line := fmt.Sprintf("%-12s%-31s%s\n", nameCol(name), elide(f.Value.String()), attribution)
		fmt.Fprint(w, line)
	}
}

// nameCol pads a flag name to the dump's first column, always leaving at least one
// separating space for names at or beyond the column width.
func nameCol(name string) string {
	if len(name) < 12 {
		return fmt.Sprintf("%-12s", name)
	}
	return name + " "
}

// elide trims absurdly long values so one bad paste cannot wreck the dump layout.
func elide(s string) string {
	if len(s) <= 31 {
		return s
	}
	return s[:28] + "..."
}
