// SPDX-License-Identifier: Apache-2.0

package quickstart

import (
	"fmt"
	"os"
	"strconv"
	"strings"

	"github.com/a-holm/messq/internal/cli/exit"
	"github.com/a-holm/messq/internal/cli/uierr"
	"github.com/spf13/cobra"
)

// handlerEnv vars are #25's --exec contract: the worker reads the subject and
// attempt from its environment, never from argv, so the same command line works
// for every delivery.
const (
	envSubject = "MESSQ_SUBJECT"
	envAttempt = "MESSQ_ATTEMPT"
)

// handlerExitTemp is the sysexits code the tour's flaky worker fails with on
// its first attempt: nak, retry with backoff (issue #25's contract — 75 is
// EX_TEMPFAIL; only 65 would terminate).
const handlerExitTemp = 75

// newHandler builds `messq quickstart-handler`, the HIDDEN helper step 6 uses
// as its --exec worker so the tour needs no shell and no temp script. It reads
// #25's MESSQ_SUBJECT/MESSQ_ATTEMPT environment and deliberately fails the
// first delivery of the flaky subject with exit 75, then succeeds: the nak-with-
// backoff path, seen live. Hidden from help, documented in its own Long, and
// excluded from the command-coverage binding by name.
func NewHandlerCmd(deps Deps) *cobra.Command {
	stdout, stderr := deps.Stdout, deps.Stderr
	return &cobra.Command{
		Use:    "quickstart-handler",
		Short:  "the tour's flaky demo worker (hidden helper; not for scripts)",
		Hidden: true,
		Long: "The quickstart tour's demo worker: a worker that fails its first delivery\n" +
			"on purpose (exit 75 — nak, retry with backoff) and succeeds on the second\n" +
			"(exit 0 — ack). It reads MESSQ_SUBJECT and MESSQ_ATTEMPT, the --exec\n" +
			"contract from issue #25, and exists so the tour needs no shell, no $SCRIPT,\n" +
			"no temp file: the command above is the whole worker.\n\n" +
			"This command is a teaching prop, not an API: it is hidden from help,\n" +
			"excluded from the command-coverage binding, and its only consumer is the\n" +
			"tour's step 6.",
		Args: cobra.NoArgs,
		RunE: func(_ *cobra.Command, _ []string) error {
			subject := strings.TrimSpace(os.Getenv(envSubject))
			attempt := strings.TrimSpace(os.Getenv(envAttempt))
			switch {
			case subject == "" || attempt == "":
				return usageExit(handlerExitTemp, "quickstart-handler: missing %s or %s; it runs under messq sub --exec",
					envSubject, envAttempt)
			case isFlakyFirstAttempt(subject, attempt):
				fmt.Fprintf(stderr, "upstream returned 503\n")
				return exitWith(handlerExitTemp)
			default:
				fmt.Fprintln(stdout, "ok")
				return nil
			}
		},
	}
}

// usageExit is the handler's teaching error.
func usageExit(code int, format string, args ...any) error {
	ue := uierr.Usage(format, args...)
	ue.Exit = code
	return ue
}

// exitWith carries an explicit sysexits code through the funnel.
func exitWith(code int) error {
	return &exit.Err{Code: code}
}

// isFlakyFirstAttempt pins the demonstration: the flaky subject's FIRST attempt
// fails, every later attempt succeeds. The tour shows the nak AND the ack.
func isFlakyFirstAttempt(subject, attempt string) bool {
	if subject != "demo.flaky" {
		return false
	}
	n, err := strconv.Atoi(attempt)
	return err == nil && n == 1
}
