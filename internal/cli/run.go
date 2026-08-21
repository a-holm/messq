// SPDX-License-Identifier: Apache-2.0

package cli

import (
	"encoding/json"
	"fmt"
	"io"
	"strings"

	"github.com/a-holm/messq/internal/buildinfo"
)

// Exit codes are a documented contract (PLAN.md section 8): 0 ok, 1 error, 2 usage,
// 3 not found, 4 conflict/stale, 5 empty/timeout, 6 daemon unreachable, 7 permission.
// Only the codes this command surface can reach are defined.
const (
	exitOK    = 0
	exitError = 1
	exitUsage = 2
)

const usage = `messq is a lightweight, single-binary queue daemon for Linux.

Usage:
  messq <command> [flags]

Commands:
  version    Print build information.
  help       Print this message.

Flags for version:
  --output text|json    Output format. Default text.

Exit codes: 0 ok, 1 error, 2 usage.
`

// Run executes one messq invocation and returns the process exit code. Returning an int rather
// than an error keeps the exit-code contract testable without spawning a process. Data goes to
// stdout, narration to stderr.
func Run(args []string, stdin io.Reader, stdout, stderr io.Writer) int {
	switch {
	case len(args) == 0, args[0] == "-h", args[0] == "--help", args[0] == "help":
		fmt.Fprint(stdout, usage)
		return exitOK
	case args[0] == "version", args[0] == "--version":
		return runVersion(args[1:], stdout, stderr)
	default:
		return usageError(stderr, fmt.Sprintf("unknown command %q", args[0]))
	}
}

func runVersion(args []string, stdout, stderr io.Writer) int {
	format := "text"
	for i := 0; i < len(args); i++ {
		switch arg := args[i]; {
		case arg == "--output":
			if i+1 >= len(args) {
				return usageError(stderr, "--output needs a value: text or json")
			}
			i++
			format = args[i]
		case strings.HasPrefix(arg, "--output="):
			format = strings.TrimPrefix(arg, "--output=")
		default:
			return usageError(stderr, fmt.Sprintf("unexpected argument %q", arg))
		}
	}

	switch format {
	case "text":
		fmt.Fprintln(stdout, buildinfo.Short())
	case "json":
		if err := json.NewEncoder(stdout).Encode(buildinfo.Get()); err != nil {
			fmt.Fprintf(stderr, "messq: cannot write version json: %v\n", err)
			return exitError
		}
	default:
		return usageError(stderr, fmt.Sprintf("unsupported --output %q: use text or json", format))
	}
	return exitOK
}

func usageError(stderr io.Writer, msg string) int {
	fmt.Fprintf(stderr, "messq: %s\nrun 'messq help' for usage\n", msg)
	return exitUsage
}
