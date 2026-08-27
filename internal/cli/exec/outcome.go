// SPDX-License-Identifier: Apache-2.0

package exec

import (
	"os"
	"strconv"
	"syscall"
)

// Outcome is the settle decision the Worker acts on — the four shapes issue #25
// §5 allows Handle to return. It exists so render/summary/hint can name the
// outcome without touching error values.
type Outcome uint8

const (
	OutcomeAck     Outcome = iota // child exited 0
	OutcomeNak                    // retryable failure (75, other non-zero, signal, timeout, spawn)
	OutcomeTerm                   // poison payload: straight to the DLQ (65 only)
	OutcomeAbandon                // lease lost: nothing may be settled, ever
)

func (o Outcome) String() string {
	switch o {
	case OutcomeAck:
		return "ack"
	case OutcomeNak:
		return "nak"
	case OutcomeTerm:
		return "term"
	case OutcomeAbandon:
		return "abandon"
	default:
		return "unknown"
	}
}

// CauseKind explains WHY a child stopped outside its own honest exit code.
// These are the rows of the §3 table whose decision does not come from the
// wait status alone.
type CauseKind uint8

const (
	CauseNone         CauseKind = iota // the child stopped on its own
	CauseExecTimeout                   // --exec-timeout or the lease ceiling fired first
	CauseSpawnFailure                  // fork/exec itself failed at runtime (EAGAIN, ENOMEM…)
	CauseLeaseLoss                     // token is dead (seek/restart/unreachable); settle NOTHING
)

// Result is one classified child end: what the Worker should do, what actually
// happened to the process, and the operator-facing sentence.
type Result struct {
	Outcome  Outcome
	ExitCode int            // -1 when the child was killed by a signal
	Signal   syscall.Signal // 0 when the child exited normally
	Reason   string         // sanitised stderr or the classifier's sentence
}

// joinReason glues a prefix onto captured stderr exactly once, with ": "
// between them; empty parts drop out so reasons never end in stray separators.
func joinReason(prefix, stderr string) string {
	switch {
	case prefix == "":
		return stderr
	case stderr == "":
		return prefix
	default:
		return prefix + ": " + stderr
	}
}

// Classify maps one finished child onto a Result by the §3 table — pure,
// no clock, no I/O, no globals. One test row per table line lives in
// outcome_test.go, including the two deliberate refinements from the brief:
//
//   - ONLY 65 terminates. The rest of the sysexits block (77 EX_NOPERM,
//     78 EX_CONFIG…) is operator misconfiguration; dead-lettering fine
//     payloads because the environment was wrong would be the worst
//     possible outcome. 77/78 ride "other non-zero → nak".
//   - "128+N" from shells is NOT special-cased: mapping is defined on the
//     DIRECT child only, so --exec-shell's signalled grandchild surfaces as
//     ordinary exit 137 → nak.
//
// CauseKind beats the wait status whenever both speak: a lease-lost kill
// produces an ExitError we must not read as message feedback, and an
// --exec-timeout SIGKILL is ours, not the payload's fault.
func Classify(st *os.ProcessState, kind CauseKind, detail string, stderr string) Result {
	code := -1 // nothing known yet
	if st != nil {
		code = st.ExitCode()
	}

	switch kind {
	case CauseLeaseLoss:
		return Result{Outcome: OutcomeAbandon, ExitCode: code}
	case CauseSpawnFailure:
		return Result{
			Outcome:  OutcomeNak,
			ExitCode: -1,
			Reason:   joinReason("could not start "+detail, ""),
		}
	case CauseExecTimeout:
		return Result{
			Outcome:  OutcomeNak,
			ExitCode: code,
			Reason:   joinReason("exec timeout after "+detail, stderr),
		}
	case CauseNone:
	}

	// No honest exit state at all: fork/exec succeeded but reaping saw nothing.
	if st == nil {
		return Result{Outcome: OutcomeNak, ExitCode: -1, Reason: "child vanished without an exit status"}
	}

	if code < 0 {
		ws, ok := st.Sys().(syscall.WaitStatus)
		if ok && ws.Signaled() {
			sig := ws.Signal()
			return Result{
				Outcome:  OutcomeNak,
				ExitCode: -1,
				Signal:   sig,
				Reason:   joinReason("killed by "+signalName(sig), stderr),
			}
		}
		return Result{
			Outcome:  OutcomeNak,
			ExitCode: code,
			Reason:   "child ended without an exit or signal status",
		}
	}

	switch code {
	case 0:
		return Result{Outcome: OutcomeAck, ExitCode: 0}
	case 75:
		return Result{Outcome: OutcomeNak, ExitCode: 75, Reason: stderr}
	case 65:
		return Result{Outcome: OutcomeTerm, ExitCode: 65, Reason: stderr}
	default:
		return Result{Outcome: OutcomeNak, ExitCode: code, Reason: joinReason("exit "+itoa(code), stderr)}
	}
}

func itoa(n int) string { return strconv.Itoa(n) }

// signalNames is the closed Linux/POSIX set the direct child can die by; the
// reason sentence spells names the way operators read logs (SIGTERM, not
// syscall's lowercase "terminated").
var signalNames = map[syscall.Signal]string{
	syscall.SIGHUP:    "SIGHUP",
	syscall.SIGINT:    "SIGINT",
	syscall.SIGQUIT:   "SIGQUIT",
	syscall.SIGILL:    "SIGILL",
	syscall.SIGTRAP:   "SIGTRAP",
	syscall.SIGABRT:   "SIGABRT",
	syscall.SIGBUS:    "SIGBUS",
	syscall.SIGFPE:    "SIGFPE",
	syscall.SIGKILL:   "SIGKILL",
	syscall.SIGUSR1:   "SIGUSR1",
	syscall.SIGSEGV:   "SIGSEGV",
	syscall.SIGUSR2:   "SIGUSR2",
	syscall.SIGPIPE:   "SIGPIPE",
	syscall.SIGALRM:   "SIGALRM",
	syscall.SIGTERM:   "SIGTERM",
	syscall.SIGCHLD:   "SIGCHLD",
	syscall.SIGCONT:   "SIGCONT",
	syscall.SIGSTOP:   "SIGSTOP",
	syscall.SIGTSTP:   "SIGTSTP",
	syscall.SIGTTIN:   "SIGTTIN",
	syscall.SIGTTOU:   "SIGTTOU",
	syscall.SIGURG:    "SIGURG",
	syscall.SIGXCPU:   "SIGXCPU",
	syscall.SIGXFSZ:   "SIGXFSZ",
	syscall.SIGVTALRM: "SIGVTALRM",
	syscall.SIGPROF:   "SIGPROF",
	syscall.SIGWINCH:  "SIGWINCH",
	syscall.SIGIO:     "SIGIO",
	syscall.SIGPWR:    "SIGPWR",
	syscall.SIGSYS:    "SIGSYS",
	syscall.SIGSTKFLT: "SIGSTKFLT",
}

// signalName renders one signal for a reason sentence.
func signalName(s syscall.Signal) string {
	if n, ok := signalNames[s]; ok {
		return n
	}
	return "signal " + strconv.Itoa(int(s))
}
