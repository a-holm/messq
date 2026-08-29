// SPDX-License-Identifier: Apache-2.0

package complete

import (
	"context"
	"os"
	"strings"

	"github.com/spf13/cobra"

	"github.com/a-holm/messq/internal/cli/uierr"
	"github.com/a-holm/messq/pkg/client"
)

// cobra script names this package ships, exactly the shells the suite tests.
// PowerShell and Nushell are deferred (issue §9): shipping untested scripts is
// worse than not shipping them.
var shells = []string{"bash", "zsh", "fish"}

// NewCompletionCommand replaces cobra's disabled default completion command:
// `messq completion bash|zsh|fish` emits the generated script. The generators
// are cobra's own; the scripts are NOT golden-tested byte-for-byte (that would
// pin a dependency's internals) — the __complete protocol output is.
func NewCompletionCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "completion <shell>",
		Short: "emit the shell completion script (bash, zsh or fish)",
		Long: "Emit the completion script for the named shell. Source it in your\n" +
			"shell's startup file, e.g.:\n\n" +
			"  source <(messq completion bash)\n\n" +
			"Completion never blocks a shell: live stream and consumer names are\n" +
			"fetched on a 200ms budget, and a dead or unauthorised daemon simply\n" +
			"completes nothing, silently.",
		Args: func(_ *cobra.Command, args []string) error {
			if len(args) != 1 {
				return errUsage("completion takes exactly one shell: %s", strings.Join(shells, "|"))
			}
			for _, s := range shells {
				if args[0] == s {
					return nil
				}
			}
			return errUsage("unknown shell %q: want %s", args[0], strings.Join(shells, "|"))
		},
		RunE: func(_ *cobra.Command, args []string) error {
			// Reached only for a first token that is not one of the shell
			// subcommands: the honest refusal beats cobra's silent help dump.
			return errUsage("unknown shell %q: want %s",
				strings.Join(args, " "), strings.Join(shells, "|"))
		},
	}
	for _, s := range shells {
		shell := s
		sub := &cobra.Command{
			Use:   shell,
			Short: "emit the " + shell + " completion script",
			Long: "Emit the " + shell + " completion script for the messq command tree onto stdout.\n" +
				"Source it from your " + shell + " startup file; it wires the __complete protocol so\n" +
				"flags, commands and help topics finish on TAB. The script itself is static; the\n" +
				"live stream and consumer names it offers are fetched per keystroke on the\n" +
				"resolver's 200ms budget, and a dead daemon simply completes nothing, silently.",
			Example: "  source <(messq completion " + shell + ") # noexec: sources in a shell",
			Args:    cobra.NoArgs,
			RunE: func(c *cobra.Command, _ []string) error {
				out := c.OutOrStdout()
				switch shell {
				case "bash":
					return c.Root().GenBashCompletionV2(out, true)
				case "zsh":
					return c.Root().GenZshCompletion(out)
				case "fish":
					return c.Root().GenFishCompletion(out, true)
				}
				return nil
			},
		}
		cmd.AddCommand(sub)
	}
	return cmd
}

// errUsage renders through the same teaching-error funnel as every other
// command failure: exit 2, the sentence, the fix.
func errUsage(format string, args ...any) error {
	return uierr.Usage(format, args...)
}

// RegisterFlagCompletion wires every closed-enum flag's completion at the root,
// so each command inherits it (issue §3's "a new enum member completes the day
// it exists" — the enums are the same const blocks the commands validate).
func RegisterFlagCompletion(root *cobra.Command) {
	register := func(name string, values ...string) {
		regErr := root.RegisterFlagCompletionFunc(name, func(_ *cobra.Command, _ []string, toComplete string) ([]string, cobraDirective) {
			var out []string
			for _, v := range values {
				if strings.HasPrefix(v, toComplete) {
					out = append(out, v)
				}
			}
			return out, cobra.ShellCompDirectiveNoFileComp
		})
		// Registration can only fail for a flag name that does not exist; the
		// three names above are the root's own persistent flags.
		if regErr != nil {
			panic("complete: register flag completion for --" + name + ": " + regErr.Error())
		}
	}
	register("output", "auto", "table", "json", "ndjson")
	register("color", "auto", "always", "never")
	// --durability is serve's hand-parsed §8 surface; its completion joins when
	// the cobra migration (#17's serve slice) makes it a real flag.
}

// NewResolver builds the production resolver from the resolved flag values.
// TokenFile is the --token-file value (or MESSQ_TOKEN_FILE); the credentials
// transport reads the file itself, the resolver only hashes its path for the
// cache key.
func NewResolver(addr, tokenFile string) *Resolver {
	return &Resolver{
		Addr:      addr,
		TokenFile: tokenFile,
		Dial: func(ctx context.Context) (*client.Client, error) {
			if tokenFile != "" {
				if _, err := os.Stat(tokenFile); err != nil {
					return nil, err
				}
			}
			return client.New(addr)
		},
		Cache: NewDiskCache(),
	}
}

// StreamArg is the ValidArgsFunction for a first positional stream argument:
// live stream names over the resolver built from the command's resolved flags.
// args are the already-typed positionals (none before the stream); toComplete
// is the prefix to filter by.
func StreamArg(r *Resolver) cobra.CompletionFunc {
	return func(cmd *cobra.Command, args []string, toComplete string) ([]string, cobra.ShellCompDirective) {
		got, directive := r.Streams(cmd.Context(), toComplete)
		return completionValues(got), directive
	}
}

// ConsumerArg is the ValidArgsFunction for a second positional consumer
// argument, scoped to the stream typed at position streamArg. An absent or
// unknown stream completes empty, silently.
func ConsumerArg(r *Resolver, streamArg int) cobra.CompletionFunc {
	return func(cmd *cobra.Command, args []string, toComplete string) ([]string, cobra.ShellCompDirective) {
		stream := ""
		if len(args) > streamArg {
			stream = args[streamArg]
		}
		got, directive := r.Consumers(cmd.Context(), stream, toComplete)
		return completionValues(got), directive
	}
}

// completionValues flattens candidates to the strings the __complete protocol
// wants ("value	description" per line is cobra's business, not ours).
func completionValues(got []Completion) []string {
	out := make([]string, 0, len(got))
	for _, c := range got {
		if c.Desc != "" {
			out = append(out, c.Value+"	"+c.Desc)
			continue
		}
		out = append(out, c.Value)
	}
	return out
}
