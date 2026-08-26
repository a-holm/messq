// SPDX-License-Identifier: Apache-2.0

package cli

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/spf13/cobra"
)

// helperInterruptEnv marks a re-executed test binary as the interrupt child: the
// child runs the REAL RunEnv entry point — signal installation included — with one
// scratch command that hangs until its context is cancelled, exactly like a long
// poll would.
const helperInterruptEnv = "MESSQ_TEST_HELPER_INTERRUPT"

// TestHelperInterruptProcess is the re-exec entry point. It is a no-op for the
// parent run (env unset) and `messq hang` for the child.
func TestHelperInterruptProcess(t *testing.T) {
	if os.Getenv(helperInterruptEnv) != "1" {
		t.Skip("helper process only")
	}
	env := DefaultEnv(os.Stdin, os.Stdout, os.Stderr)
	env.build = func(e *Env) *cobra.Command {
		root := NewRoot(e)
		hang := &cobra.Command{
			Use:   "hang",
			Short: "hang until the invocation is cancelled",
			Long: "A scratch long poll used by the subprocess interrupt probe: it blocks on " +
				"the command context and unwinds with that cancellation, which is how every " +
				"genuine long-poll failure reaches the funnel after an operator hits ^C once.",
			Example: "  messq hang",
			Args:    cobra.NoArgs,
			RunE: func(cmd *cobra.Command, _ []string) error {
				fmt.Fprintln(os.Stdout, "READY") // the probe's handshake line
				<-cmd.Context().Done()
				return fmt.Errorf("long poll aborted: %w", cmd.Context().Err())
			},
		}
		hang.Annotations = map[string]string{annExits: "130"}
		root.AddCommand(hang)
		return root
	}
	os.Exit(RunEnv(context.Background(), env, []string{"--output", "json", "hang"})) //nolint:forbidigo // re-exec child's process boundary
}

// killChild reaps the probe after a failed step; the failure itself is reported by
// the caller, so a refused kill is only logged.
func killChild(t *testing.T, cmd *exec.Cmd) {
	t.Helper()
	if err := cmd.Process.Kill(); err != nil {
		t.Logf("kill interrupt child: %v", err)
	}
	_ = cmd.Wait() //nolint:errcheck // the exit code is not meaningful once we killed it
}

// waitForHandshake consumes the child's readiness line, bounded by a wall-clock cap
// that is a hang backstop, never a latency assertion.
func waitForHandshake(t *testing.T, stdout io.Reader) error {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	lines := make(chan string, 1)
	readErr := make(chan error, 1)
	go func() {
		line, err := bufio.NewReader(stdout).ReadString('\n')
		if err != nil {
			readErr <- err
			return
		}
		lines <- line
	}()
	select {
	case err := <-readErr:
		return fmt.Errorf("read readiness line: %w", err)
	case line := <-lines:
		if strings.TrimSpace(line) != "READY" {
			return fmt.Errorf("first child line was %q, want READY", line)
		}
		return nil
	case <-ctx.Done():
		return fmt.Errorf("child did not print READY within 30s")
	}
}

// TestFirstSignalExits130 pins §7's documented exception end to end through a real
// process: ONE SIGINT into a hung long poll must unwind gracefully and exit 130
// (128+SIGINT), never 6 — rc=6 invites cron retry loops against a command the
// operator explicitly interrupted. A second signal still hard-exits via the default
// disposition, which this probe deliberately does not exercise.
func TestFirstSignalExits130(t *testing.T) {
	exe, err := os.Executable()
	if err != nil {
		t.Fatalf("os.Executable: %v", err)
	}
	cmd := exec.CommandContext(context.Background(), exe, "-test.run=^TestHelperInterruptProcess$", "-test.timeout=2m")
	cmd.Env = append(os.Environ(), helperInterruptEnv+"=1")
	stdout, pipeErr := cmd.StdoutPipe()
	if pipeErr != nil {
		t.Fatalf("stdout pipe: %v", pipeErr)
	}
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	if startErr := cmd.Start(); startErr != nil {
		t.Fatalf("start interrupt child: %v", startErr)
	}

	// Readiness gate: signal only after the child is parked inside its long poll,
	// so the probe can never race the window before signal.Notify is installed.
	if rErr := waitForHandshake(t, stdout); rErr != nil {
		killChild(t, cmd)
		t.Fatalf("interrupt child never became ready: %v\n--- child stderr ---\n%s", rErr, stderr.String())
	}

	if sigErr := cmd.Process.Signal(os.Interrupt); sigErr != nil {
		t.Fatalf("signal SIGINT: %v", sigErr)
	}

	waitCtx, cancelWait := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancelWait()
	done := make(chan struct{})
	go func() {
		_ = cmd.Wait() //nolint:errcheck // the exit code is asserted below, not via Wait's error
		close(done)
	}()
	select {
	case <-done:
	case <-waitCtx.Done():
		_ = cmd.Process.Kill() //nolint:errcheck // watchdog reaping before failing hard
		t.Fatalf("child ignored one SIGINT for 30s\n--- child stderr ---\n%s", stderr.String())
	}

	code := cmd.ProcessState.ExitCode()
	if code != 130 {
		t.Fatalf("one SIGINT exited %d, want 130 per §7 (stderr %q)", code, stderr.String())
	}
	line := strings.TrimSpace(stderr.String())
	var doc map[string]any
	if jsonErr := json.Unmarshal([]byte(line), &doc); jsonErr != nil {
		t.Fatalf("machine stderr is not one JSON document (%v):\n%q", jsonErr, stderr.String())
	}
	inner, innerOK := doc["error"].(map[string]any)
	if !innerOK {
		t.Fatalf("stderr carries no error object:\n%q", stderr.String())
	}
	if inner["code"] != "interrupted" {
		t.Errorf("envelope code = %v, want interrupted (no Go internals on the error face)", inner["code"])
	}
	if inner["message"] != "interrupted by signal" {
		t.Errorf("envelope message = %v, want the §7 phrasing", inner["message"])
	}
	if inner["exit"] != float64(130) {
		t.Errorf("envelope exit = %v, want 130", inner["exit"])
	}
	if strings.Contains(stderr.String(), "context canceled") {
		t.Errorf("error face leaked Go internals:\n%s", stderr.String())
	}
}
