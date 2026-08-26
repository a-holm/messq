// SPDX-License-Identifier: Apache-2.0

package cli

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/a-holm/messq/internal/buildinfo"
)

// Exit codes are a documented contract (PLAN.md section 8): 0 ok, 1 error, 2 usage,
// 3 not found, 4 conflict/stale, 5 empty/timeout, 6 daemon unreachable, 7 permission.
// Only the codes this command surface can reach are defined.
const (
	exitOK         = 0
	exitError      = 1
	exitUsage      = 2
	exitNotFound   = 3
	exitConflict   = 4 // conflict/stale: would_lose_data, stale_ack, wrong_generation, confirm_mismatch
	exitEmpty      = 5 // empty/timeout: a listing with no rows, sub's idle timeout
	exitPermission = 7
)

const usage = `messq is a lightweight, single-binary queue daemon for Linux.

Usage:
  messq <command> [flags]

Commands:
  version    Print build information.
  serve      Run the daemon (messq serve --data-dir DIR).
  verify     Check a data directory's invariants (messq verify --data-dir DIR).
  help       Print this message.

Flags for version:
  --output text|json    Output format. Default text.

Flags for serve:
  --data-dir DIR        Data directory (required; or MESSQ_DATA_DIR).
  --listen ADDR         unix://PATH or tcp://HOST:PORT. Default unix:///run/messq/messq.sock.
  --drain-timeout D     Budget for in-flight handlers on SIGTERM (PLAN §4.4). Default 10s.

Flags for verify:
  --data-dir DIR        Data directory (or MESSQ_DATA_DIR, else /var/lib/messq).
  --deep                Add integrity_check, I8 and the I10 event fold.
  --output table|json   Output format. Default table on a TTY, json otherwise.
  --fail-fast           Stop at the first violating check.
  --limit N             Violating rows printed per check. Default 100.

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
	case args[0] == "serve":
		return runServe(args[1:], os.Getenv, stdout, stderr)
	case args[0] == "verify":
		return runVerify(args[1:], os.Getenv, stdout, stderr)
	default:
		return usageError(stderr, fmt.Sprintf("unknown command %q", args[0]))
	}
}

func runVersion(args []string, stdout, stderr io.Writer) int {
	format := "text"
	for len(args) > 0 {
		arg := args[0]
		args = args[1:]
		switch {
		case arg == "--output":
			if len(args) == 0 {
				return usageError(stderr, "--output needs a value: text or json")
			}
			format = args[0]
			args = args[1:]
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
