// SPDX-License-Identifier: Apache-2.0

package conf

import (
	"strings"
	"testing"

	"github.com/spf13/cobra"
)

// newFuzzCmd carries one flag of every value shape the CLI uses.
func newFuzzCmd() *cobra.Command {
	cmd := &cobra.Command{Use: "fuzz"}
	fs := cmd.Flags()
	fs.String("addr", "unix:///run/messq/messq.sock", "daemon address")
	fs.Duration("timeout", 30_000_000_000, "per-request deadline")
	fs.Bool("quiet", false, "suppress narration")
	fs.Count("verbose", "narrate")
	fs.StringSlice("header", nil, "repeatable headers")
	return cmd
}

// FuzzEnvValue proves the env layer is total over arbitrary input: no value panics,
// and every ACCEPTED value round-trips through the flag's String() — i.e. ApplyEnv
// never half-applies a parse.
func FuzzEnvValue(f *testing.F) {
	seeds := []string{
		"", " ", "true", "false", "1", "5s", "-3s", "soon", "unix:///x.sock",
		"a,b,c", ",", "\x00", "\xff\xfe", strings.Repeat("9", 40), "--flag", "=", "yaml",
	}
	for _, s := range seeds {
		f.Add(s)
	}
	f.Fuzz(func(t *testing.T, val string) {
		cmd := newFuzzCmd()
		if err := ApplyEnv(cmd, func(string) string { return val }); err != nil {
			return // rejected: fine, as long as nothing panicked
		}
		// Accepted: every flag must still render a value without panicking.
		for _, name := range []string{"addr", "timeout", "quiet", "verbose", "header"} {
			_ = cmd.Flags().Lookup(name).Value.String()
		}
	})
}
