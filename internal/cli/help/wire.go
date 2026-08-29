// SPDX-License-Identifier: Apache-2.0

package help

import (
	"fmt"
	"io"
	"strings"

	"github.com/spf13/cobra"

	"github.com/a-holm/messq/internal/cli/uierr"
)

// NewTopicCommands builds one cobra command per topic. Deliberately NO Run and
// no children — that is cobra's additional-help-topic shape: they render under
// *Additional help topics* in `messq --help`, invoking one directly shows its
// rendered text via Long, and `messq help <topic>` renders through the
// overridden help command.
func NewTopicCommands(env TopicEnv) []*cobra.Command {
	out := make([]*cobra.Command, 0, len(All()))
	for _, t := range All() {
		topic := t
		cmd := &cobra.Command{
			Use:   topic.Name,
			Short: topic.Synopsis,
			Long:  Render(topic.Source, false),
		}
		out = append(out, cmd)
	}
	return out
}

// TopicEnv is the slice of cli.Env the help surface needs. It is an interface
// so the help package never imports internal/cli (no import cycle).
type TopicEnv interface {
	Stdout() io.Writer
	Colour() bool
}

// renderTo writes one rendered topic to w.
func renderTo(w io.Writer, name string) error {
	t, err := Get(name)
	if err != nil {
		return err
	}
	_, wErr := io.WriteString(w, Render(t.Source, false))
	return wErr
}

// NewHelpCommand overrides cobra's default help command: `messq help` prints the
// root help, `messq help <topic>` renders a topic, and `messq help nosuchtopic`
// exits 2 with did-you-mean candidates AND the topic list — not cobra's bare
// "Unknown help topic" with exit 0 (issue §8's edge table).
func NewHelpCommand(env TopicEnv, root *cobra.Command) *cobra.Command {
	return &cobra.Command{
		Use:   "help [command|topic]",
		Short: "help about any command or help topic",
		Long: "Help about any command, or about one of the help topics: " +
			strings.Join(Names(), ", ") + ".\n" +
			"Topics render the same markdown the docs site (#35) serves — one source of truth.",
		SilenceUsage: true,
		RunE: func(c *cobra.Command, args []string) error {
			root := c.Root()
			switch {
			case len(args) == 0:
				c.SetOut(env.Stdout())
				return root.Help()
			default:
				name := args[0]
				if IsTopic(name) {
					return renderTo(env.Stdout(), name)
				}
				// A real command: cobra's own help rendering.
				target, _, e := root.Find(args)
				if target != nil && e == nil && target != root {
					target.SetOut(env.Stdout())
					return target.Help()
				}
				ue := uierr.Usage("unknown help topic %q", name)
				ue.Suggest = suggestTopics(name)
				// The topic list IS the teaching payload: suggestions first, then
				// every topic as the command that renders it. next[] is data (the
				// machine face renders it), which satisfies the issue's "suggestion,
				// then the full topic list" in both faces.
				ue.Next = append(ue.Suggest, topicListNext()...)
				return ue
			}
		},
	}
}

// IsTopic reports whether name is a registered help topic.
func IsTopic(name string) bool {
	for _, t := range Names() {
		if t == name {
			return true
		}
	}
	return false
}

// suggestTopics picks did-you-mean candidates from the topic names plus the
// root's own command suggestions.
func suggestTopics(name string) []string {
	var out []string
	for _, t := range SortedNames() {
		if levenshtein(name, t) <= 3 {
			out = append(out, "messq help "+t)
		}
	}
	return out
}

// topicListNext renders the full topic list as next commands, in registry order.
func topicListNext() []string {
	out := make([]string, 0, len(Names()))
	for _, t := range Names() {
		out = append(out, "messq help "+t)
	}
	return out
}

// levenshtein is the classic edit distance; help topics are few and short, so
// the small DP beats pulling a dependency.
func levenshtein(a, b string) int {
	ra, rb := []rune(a), []rune(b)
	prev := make([]int, len(rb)+1)
	cur := make([]int, len(rb)+1)
	for j := range prev {
		prev[j] = j
	}
	for i := 1; i <= len(ra); i++ {
		cur[0] = i
		for j := 1; j <= len(rb); j++ {
			cost := 1
			if ra[i-1] == rb[j-1] {
				cost = 0
			}
			cur[j] = min(prev[j]+1, cur[j-1]+1, prev[j-1]+cost)
		}
		prev, cur = cur, prev
	}
	return prev[len(rb)]
}

// ListTopics renders the topic list for the unknown-topic error and `messq help
// topics`-style discovery.
func ListTopics() string {
	var b strings.Builder
	for _, t := range All() {
		fmt.Fprintf(&b, "  %-12s %s\n", t.Name, t.Synopsis)
	}
	return strings.TrimRight(b.String(), "\n")
}
