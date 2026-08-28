// SPDX-License-Identifier: Apache-2.0

package exec

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"os/signal"
	"strconv"
	"strings"
	"testing"

	"golang.org/x/sys/unix"
)

// Hermetic helper-child battery (issue #25 §11.3): every cross-process test
// re-execs THIS binary with MESSQ_TEST_CHILD=<behaviour>. No shell, no sleep,
// no jq — the suite must pass in a scratch container. Behaviours grow only as
// a test needs them; adding one means teaching this switch to speak it.
const envTestChild = "MESSQ_TEST_CHILD"

func TestMain(m *testing.M) {
	if bev := strings.TrimSpace(os.Getenv(envTestChild)); bev != "" {
		runHelperChild(bev)
		return
	}
	os.Exit(m.Run())
}

// runHelperChild executes the requested scripted behaviour inside the child.
// Every branch is deterministic: fixed exit codes, fixed stderr bytes, or a
// synchronous self-signal that kills the process outright.
func runHelperChild(behaviour string) {
	switch {
	case bevIs(behaviour, "exit0"):
		os.Exit(0)
	case bevIs(behaviour, "exit75"):
		fmt.Fprint(os.Stderr, "upstream 503")
		os.Exit(75)
	case bevIs(behaviour, "exit65"):
		fmt.Fprint(os.Stderr, "bad json at offset 12")
		os.Exit(65)
	case bevIs(behaviour, "exit77"):
		os.Exit(77)
	case bevIs(behaviour, "exit137"):
		os.Exit(137)
	case bevIs(behaviour, "exit-other"):
		fmt.Fprint(os.Stderr, "mystery failure mode")
		os.Exit(42)
	case bevIs(behaviour, "stderr-flood"):
		floodStderr()
	case bevIs(behaviour, "grandkid-blocker"), bevIs(behaviour, "grandkid-waiter"):
		blockOnStdinForever()
	case strings.HasPrefix(behaviour, "trap-term-grandkid"):
		trapTermThenSpawnGrandkid()
	case bevIs(behaviour, "kill-self-term"):
		if err := unix.Kill(os.Getpid(), unix.SIGTERM); err != nil {
			os.Exit(9)
		}
		select {} // the SIGTERM must end us; hanging fails loudly via outer timeout
	default:
		fmt.Fprintf(os.Stderr, "messq-exec-helper: unknown behaviour %q\n", behaviour)
		os.Exit(9)
	}
}

func floodStderr() {
	// 64KB default: 16× the 4096-byte capture cap — the clamp semantics are
	// byte-count-based, so the over-cap proof needs bounded volume, not a
	// multi-megabyte torrent that burns CPU and pipe buffers for seconds.
	total := 65_536
	if n, err := strconv.Atoi(os.Getenv("MESSQ_FLOOD_BYTES")); err == nil && n > 0 {
		total = n
	}
	chunk := make([]byte, 4096)
	for i := range chunk {
		chunk[i] = byte('a' + i%26)
	}
	written := 0
	for written < total {
		end := minInt(len(chunk), total-written)
		n, err := os.Stderr.Write(chunk[:end])
		written += n
		if err != nil {
			os.Exit(8) // capture closed early: parent broke the drain contract
		}
	}
	os.Exit(7)
}

func blockOnStdinForever() {
	buf := make([]byte, 4096)
	for {
		// Read-until-EOF: the parked child exits BY ITSELF the moment its
		// stdin write-end lineage dies (test exit, panic, SIGKILL — the
		// kernel closes fds on any death), so no external kill sweep is ever
		// needed to avoid leaks. Surviving DATA is what the group-sweep
		// killer asserts against; EOF is the self-cleanup path.
		if _, err := os.Stdin.Read(buf); err != nil {
			os.Exit(0) // EOF: parent lineage gone — self-terminate
		}
	}
}

// trapTermThenSpawnGrandkid installs the SIGTERM swallow BEFORE anything else,
// re-execs a grandchild that inherits stderr and blocks on stdin forever,
// announces its pid as GK=<pid>, then waits for KILL inside its own body.
func trapTermThenSpawnGrandkid() {
	swallow := make(chan os.Signal, 4)
	signal.Notify(swallow, unix.SIGTERM)

	exe, err := os.Executable()
	fatalIf(err != nil, "exec path")
	ctxAll := context.Background()
	cmd := exec.CommandContext(ctxAll, exe)
	cmd.Env = append(os.Environ(), envTestChild+"=grandkid-blocker")
	cmd.Stderr = os.Stderr
	stdinGK, err := cmd.StdinPipe()
	fatalIf(err != nil, "grandkid stdin pipe")
	err = cmd.Start()
	fatalIf(err != nil, "start grandkid")
	fmt.Fprintf(os.Stderr,
		"TRAP=%d PGRP=%d GK=%d GKPGRP=%d\n",
		os.Getpid(), unix.Getpgrp(), cmd.Process.Pid, gkpgrpOf(cmd.Process.Pid))
	_ = stdinGK.Close() //nolint:errcheck // the grandkid parks forever regardless of this close

	<-make(chan struct{}) // park until SIGKILL sweeps the group
}

func gkpgrpOf(pid int) int {
	raw, err := os.ReadFile("/proc/" + strconv.Itoa(pid) + "/stat")
	if err != nil {
		return -1
	}
	s := string(raw)
	i := strings.LastIndexByte(s, ')')
	if i < 0 {
		return -1
	}
	fields := strings.Fields(s[i+2:])
	if len(fields) >= 3 {
		if n, err := strconv.Atoi(fields[2]); err == nil { // after "state": ppid pgrp sid
			return n
		}
	}
	return -1
}

func minInt(a, b int) int {
	if b < a {
		return b
	}
	return a
}

func fatalIf(cond bool, what string) {
	if cond {
		fmt.Fprintln(os.Stderr, "helper-child fatal:", what)
		os.Exit(98)
	}
}

// bevIs matches behaviours; future parameterised ones can switch here.
func bevIs(behaviour, base string) bool { return behaviour == base }

// newTestChildProc returns the argv/env pair that re-execs the test binary as
// the given helper child behaviour.
func newTestChildProc(t *testing.T, behaviour string) ([]string, []string) {
	t.Helper()
	exe, err := os.Executable()
	if err != nil {
		t.Fatalf("executable path: %v", err)
	}
	return []string{exe}, append(os.Environ(), envTestChild+"="+behaviour)
}
