// SPDX-License-Identifier: Apache-2.0

package exec

import (
	"errors"
	"strings"
	"testing"

	"github.com/a-holm/messq/internal/cli/uierr"
)

// ---- startup guards (slice 7) ----------------------------------------------

// The verbatim §2 refusals: usage error exit 2, teaching copy, no invented text.
func TestValidateFlagsMutualExclusions(t *testing.T) {
	base := ExecConfig{Cmd: "./h.sh"}
	errManual := ValidateFlags(ExecConfig{Cmd: "./h.sh", Manual: true})
	var ue *uierr.UserError
	if !errors.As(errManual, &ue) || ue.ExitCode() != 2 {
		t.Fatalf("--exec+--manual must be usage error 2, got %T", errManual)
	}
	for _, frag := range []string{"--manual prints ack tokens", "Pick one"} {
		if !strings.Contains(errManual.Error(), frag) {
			t.Fatalf("manual conflict message lost %q: %v", frag, errManual)
		}
	}
	errAuto := ValidateFlags(ExecConfig{Cmd: "./h.sh", AutoAck: true})
	if !errors.As(errAuto, &ue) || ue.ExitCode() != 2 {
		t.Fatalf("--exec+--auto-ack must be usage error 2, got %T", errAuto)
	}
	if !strings.Contains(errAuto.Error(), "would ack before the child ran") {
		t.Fatalf("auto-ack copy drifted: %v", errAuto)
	}
	if err := ValidateFlags(base); err != nil {
		t.Fatalf("plain --exec must validate, got %v", err)
	}
	if err := ValidateFlags(ExecConfig{Cmd: ""}); err != nil {
		t.Fatal("non-exec mode is none of this package's business")
	}
}

// Missing/non-executable targets fail at STARTUP with a teaching error — before
// any fetch burns attempts.
func TestResolveTargetStartupGuard(t *testing.T) {
	missing := "--no-such-worker-anywhere"
	_, err := ResolveTarget(missing, "")
	if err == nil {
		t.Fatal("missing binary must refuse at startup")
	}
	if !strings.Contains(err.Error(), "--exec") || !strings.Contains(err.Error(), "fix the path") {
		t.Fatalf("teaching text drifted: %v", err)
	}
	// Shell mode delegates quoting and skips binary resolution.
	argv, err := ResolveTarget("echo hi", "/bin/sh")
	if err != nil || argv[0] != "/bin/sh" || argv[1] != "-c" {
		t.Fatalf("shell argv = %#v err=%v", argv, err)
	}
}

// §7 clamp: max_ack_pending ceiling with the consumer-edit note; ordered caps
// at 1; unknown bounds pass through; zero/negative normalise to 1.
func TestClampConcurrencyRows(t *testing.T) {
	got, note := ClampConcurrency(16, 8, false)
	if got != 8 || !strings.Contains(note, "max_ack_pending=8") || !strings.Contains(note, "consumer edit") {
		t.Fatalf("clamp = %d note=%q", got, note)
	}
	if got, _ := ClampConcurrency(4, 32, true); got != 1 {
		t.Fatalf("ordered must cap to 1, got %d", got)
	}
	if got, _ := ClampConcurrency(0, 0, false); got != 1 {
		t.Fatalf("non-positive concurrency normalises to 1, got %d", got)
	}
	if got, note := ClampConcurrency(3, 0, false); got != 3 || note != "" {
		t.Fatalf("unknown server bound passes through untouched (%d,%q)", got, note)
	}
	if got, note := ClampConcurrency(8, 16, false); got != 8 || note != "" {
		t.Fatalf("under-ceiling value untouched; got %d note=%q", got, note)
	}
}
