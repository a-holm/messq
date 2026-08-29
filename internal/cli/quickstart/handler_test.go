// SPDX-License-Identifier: Apache-2.0

package quickstart

import (
	"bytes"
	"errors"
	"io"
	"strings"
	"testing"

	"github.com/a-holm/messq/internal/cli/exit"
)

// TestQuickstartHandlerAttemptSemantics drives the demo worker through the
// --exec contract: attempt 1 of the flaky subject fails with 75 and a reason on
// stderr; attempt 2 (and every later one) acks with 0; a foreign subject always
// acks.
func TestQuickstartHandlerAttemptSemantics(t *testing.T) {
	t.Setenv(envSubject, "demo.flaky")

	t.Setenv(envAttempt, "1")
	var out, errOut bytes.Buffer
	err := NewHandlerCmd(Deps{Stdout: &out, Stderr: &errOut}).Execute()
	if !hasExitCode(err, handlerExitTemp) {
		t.Fatalf("attempt 1: err = %v, want exit 75", err)
	}
	if !strings.Contains(errOut.String(), "upstream returned 503") {
		t.Errorf("attempt 1 stderr = %q, want the failure reason", errOut.String())
	}
	if out.String() != "" {
		t.Errorf("attempt 1 wrote %q to stdout; a failing worker prints nothing", out.String())
	}

	t.Setenv(envAttempt, "2")
	out.Reset()
	err = NewHandlerCmd(Deps{Stdout: &out, Stderr: &errOut}).Execute()
	if err != nil {
		t.Fatalf("attempt 2: %v", err)
	}
	if !strings.Contains(out.String(), "ok") {
		t.Errorf("attempt 2 stdout = %q, want ok", out.String())
	}
}

// TestQuickstartHandlerForeignSubjectAlwaysAcks pins that only the flaky
// subject fails: a poison demonstration scoped to one subject, not a trap.
func TestQuickstartHandlerForeignSubjectAlwaysAcks(t *testing.T) {
	t.Setenv(envSubject, "demo.hello")
	t.Setenv(envAttempt, "1")
	var out bytes.Buffer
	if err := NewHandlerCmd(Deps{Stdout: &out}).Execute(); err != nil {
		t.Fatalf("foreign subject: %v", err)
	}
	if !strings.Contains(out.String(), "ok") {
		t.Errorf("stdout = %q", out.String())
	}
}

// TestQuickstartHandlerMissingEnvTeaches pins the misuse face: without the
// --exec environment the worker refuses with exit 75 and names the fix.
func TestQuickstartHandlerMissingEnvTeaches(t *testing.T) {
	t.Setenv(envSubject, "")
	t.Setenv(envAttempt, "")
	err := NewHandlerCmd(Deps{Stderr: io.Discard}).Execute()
	if !hasExitCode(err, handlerExitTemp) {
		t.Fatalf("err = %v, want exit 75", err)
	}
	if !strings.Contains(err.Error(), "MESSQ_SUBJECT") {
		t.Errorf("err = %q, want the env contract named", err.Error())
	}
}

// hasExitCode reports whether err carries exactly the given exit code — a
// command-local ExitCoder, a uierr.UserError with Exit set, or the exit
// package's explicit Err override.
func hasExitCode(err error, want int) bool {
	var coder interface{ ExitCode() int }
	if errors.As(err, &coder) && coder.ExitCode() == want {
		return true
	}
	var e *exit.Err
	if errors.As(err, &e) {
		return e.Code == want
	}
	return false
}
