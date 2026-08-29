// SPDX-License-Identifier: Apache-2.0

package cli

import (
	"github.com/spf13/cobra"
)

// newSneakyCmd is the gates-sabotage fixture (row G44): a command that joins the
// tree without a script in test/script. TestEveryCommandHasAScript must fail
// naming it; the file exists only in the gate matrix's scratch copies.
func newSneakyCmd(env *Env) *cobra.Command {
	return &cobra.Command{
		Use:   "sneaky",
		Short: "a command the golden suite has never seen",
		Long: "A sabotage fixture: joins the command tree without a .txtar script, so\n" +
			"the command-coverage binding must fail naming it. Never registered outside\n" +
			"the gates self-test's scratch copies.",
		Args: cobra.NoArgs,
		RunE: func(c *cobra.Command, _ []string) error {
			return c.Help()
		},
	}
}
