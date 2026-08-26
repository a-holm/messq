// SPDX-License-Identifier: Apache-2.0

package cli

import (
	"strings"
	"testing"

	"github.com/spf13/cobra"
)

// TestCommandTreeIsWellFormed walks the real tree through every §8 rule; one
// violation fails the build with the offending command named.
func TestCommandTreeIsWellFormed(t *testing.T) {
	root := NewRoot(&Env{})
	if problems := lintCommandTree(root); len(problems) > 0 {
		t.Errorf("command tree violates the DX contract:\n%s", strings.Join(problems, "\n"))
	}
}

// TestDXLinterBites sabotages one rule at a time and proves the linter names it —
// each row is one deliberate breakage and must produce exactly its named catch
// (issue test-plan §8).
func TestDXLinterBites(t *testing.T) {
	tests := []struct {
		name     string
		sabotage func(root *cobra.Command) *cobra.Command
		want     string // fragment the reported problem must contain
	}{
		{
			name: "empty Short",
			sabotage: func(root *cobra.Command) *cobra.Command {
				version := findCmd(root, "version")
				version.Short = ""
				return version
			},
			want: `empty Short`,
		},
		{
			name: "capitalised Short",
			sabotage: func(root *cobra.Command) *cobra.Command {
				version := findCmd(root, "version")
				version.Short = "Print build information"
				return version
			},
			want: `Short`,
		},
		{
			name: "Long too short to teach",
			sabotage: func(root *cobra.Command) *cobra.Command {
				version := findCmd(root, "version")
				version.Long = "prints the version"
				return version
			},
			want: `Long`,
		},
		{
			name: "Example missing",
			sabotage: func(root *cobra.Command) *cobra.Command {
				version := findCmd(root, "version")
				version.Example = ""
				return version
			},
			want: `Example`,
		},
		{
			name: "Run instead of RunE",
			sabotage: func(root *cobra.Command) *cobra.Command {
				version := findCmd(root, "version")
				version.RunE = nil
				version.Run = func(*cobra.Command, []string) {}
				return version
			},
			want: `RunE`,
		},
		{
			name: "Args nil",
			sabotage: func(root *cobra.Command) *cobra.Command {
				version := findCmd(root, "version")
				version.Args = nil
				return version
			},
			want: `Args`,
		},
		{
			name: "name not kebab",
			sabotage: func(root *cobra.Command) *cobra.Command {
				version := findCmd(root, "version")
				version.Use = "Version"
				return version
			},
			want: `kebab`,
		},
		{
			name: "flag usage ends in a period",
			sabotage: func(root *cobra.Command) *cobra.Command {
				root.PersistentFlags().Lookup("output").Usage = "Output mode."
				return root
			},
			want: `usage`,
		},
		{
			name: "annExits names an undocumented code",
			sabotage: func(root *cobra.Command) *cobra.Command {
				version := findCmd(root, "version")
				version.Annotations[annExits] = "9"
				return version
			},
			want: `messq.exits`,
		},
		{
			name: "alias collides with a sibling's name",
			sabotage: func(root *cobra.Command) *cobra.Command {
				findCmd(root, "version").Aliases = []string{"verify"}
				return findCmd(root, "version")
			},
			want: `collides with sibling`,
		},
		{
			name: "aliases collide between siblings",
			sabotage: func(root *cobra.Command) *cobra.Command {
				findCmd(root, "version").Aliases = []string{"ver"}
				findCmd(root, "serve").Aliases = []string{"ver"}
				return findCmd(root, "version")
			},
			want: `collides between`,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			root := NewRoot(&Env{})
			target := tt.sabotage(root)
			problems := lintCommandTree(root)
			found := false
			for _, p := range problems {
				if strings.Contains(p, target.Name()) && strings.Contains(p, tt.want) {
					found = true
				}
			}
			if !found {
				t.Errorf("linter did not catch %q; it reported:\n%s", tt.want, strings.Join(problems, "\n"))
			}
		})
	}
}
