// SPDX-License-Identifier: Apache-2.0

//go:build hookcheck

package hooks

import (
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

// The stdin shapes below were captured verbatim from a live git pushing into a scratch bare
// repository, not invented. A branch creation arrives as "<local-ref> <local-oid>
// <remote-ref> <zero-oid>"; a deletion as "(delete) <zero-oid> <remote-ref> <old-oid>"; and a
// mixed push as ONE hook invocation carrying both kinds of lines. The all-zero field is 40
// hex digits on SHA-1 repositories and 64 on SHA-256 ones, which is why the hook must accept
// any all-zero local oid rather than compare against one pinned constant.
const (
	deleteLine = "(delete) 0000000000000000000000000000000000000000 refs/heads/todelete 1a0d5506c7a5361cccda85bbcda8a913987b992c\n"
	commitLine = "HEAD 1a0d5506c7a5361cccda85bbcda8a913987b992c refs/heads/topic 0000000000000000000000000000000000000000\n"
)

func TestPrePushHook(t *testing.T) {
	t.Parallel()

	hookPath, err := filepath.Abs(filepath.Join("..", "..", ".githooks", "pre-push"))
	if err != nil {
		t.Fatal(err)
	}

	for _, tc := range []struct {
		name string
		// stdin is fed to the hook verbatim, in git's pre-push refspec format.
		stdin string
		// wantGate asserts that make ci ran; false asserts that it did not.
		wantGate bool
		// wantNote must appear in the hook's output; empty means the hook is
		// expected to stay silent.
		wantNote string
	}{
		{
			name:     "deletion-only push skips the gate",
			stdin:    deleteLine,
			wantNote: "pre-push: deletion-only push, skipping make ci",
		},
		{
			name:     "a normal push runs the gate",
			stdin:    commitLine,
			wantGate: true,
		},
		{
			name:     "a mixed delete-and-push still runs the gate",
			stdin:    deleteLine + commitLine,
			wantGate: true,
		},
		{
			name:     "empty stdin has nothing to validate and skips with a note",
			stdin:    "",
			wantNote: "pre-push: no ref updates on stdin, skipping make ci",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			code, output, makeLog := runPrePush(t, hookPath, tc.stdin)

			if code != 0 {
				t.Fatalf("hooks: exit=%d, want 0\n%s", code, output)
			}
			if tc.wantGate && makeLog == "" {
				t.Fatalf("hooks: make was never invoked; the gate must run for this input")
			}
			if !tc.wantGate && makeLog != "" {
				t.Fatalf("hooks: make was invoked (%q); this input carries nothing to validate", makeLog)
			}
			if tc.wantGate && !strings.Contains(makeLog, "ci") {
				t.Fatalf("hooks: make was invoked with %q, want the ci target", makeLog)
			}
			if tc.wantNote != "" && !strings.Contains(output, tc.wantNote) {
				t.Fatalf("hooks: output does not contain %q\n%s", tc.wantNote, output)
			}
			if !tc.wantGate {
				return
			}
			if note := "skipping"; strings.Contains(output, note) {
				t.Fatalf("hooks: a gated push must not print %q\n%s", note, output)
			}
		})
	}
}

// runPrePush executes the hook script directly, the way git would: stdin carrying the
// refspec lines, working directory inside an initialised git repository so the script's
// rev-parse --show-toplevel resolves. A stub make shadows the real one ahead of it on PATH
// and records every invocation to a log file, so the test observes whether the gate ran
// without ever paying for it.
func runPrePush(t *testing.T, hookPath, stdin string) (int, string, string) {
	t.Helper()

	repo := t.TempDir()
	if out, err := exec.Command("git", "init", "-q", repo).CombinedOutput(); err != nil {
		t.Fatalf("git init: %v\n%s", err, out)
	}

	stubBin := filepath.Join(t.TempDir(), "bin")
	if err := os.MkdirAll(stubBin, 0o755); err != nil {
		t.Fatal(err)
	}
	makeLog := filepath.Join(t.TempDir(), "make.log")
	stub := "#!/usr/bin/env bash\nprintf '%s\\n' \"$*\" >> \"$HOOKTEST_MAKE_LOG\"\nexit ${HOOKTEST_MAKE_EXIT:-0}\n"
	if err := os.WriteFile(filepath.Join(stubBin, "make"), []byte(stub), 0o755); err != nil {
		t.Fatal(err)
	}

	env := make([]string, 0, len(os.Environ())+2)
	for _, kv := range os.Environ() {
		if strings.HasPrefix(kv, "PATH=") || strings.HasPrefix(kv, "HOOKTEST_") {
			continue
		}
		env = append(env, kv)
	}
	env = append(env,
		"PATH="+stubBin+string(os.PathListSeparator)+os.Getenv("PATH"),
		"HOOKTEST_MAKE_LOG="+makeLog,
	)

	cmd := exec.Command("bash", hookPath)
	cmd.Dir = repo
	cmd.Env = env
	cmd.Stdin = strings.NewReader(stdin)
	output, err := cmd.CombinedOutput()

	log, logErr := os.ReadFile(makeLog)
	if logErr != nil && !errors.Is(logErr, os.ErrNotExist) {
		t.Fatal(logErr)
	}

	var exitErr *exec.ExitError
	switch {
	case err == nil:
	case errors.As(err, &exitErr):
		return exitErr.ExitCode(), string(output), string(log)
	default:
		t.Fatalf("run hook: %v\n%s", err, output)
	}
	return 0, string(output), string(log)
}
