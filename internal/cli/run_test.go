// SPDX-License-Identifier: Apache-2.0

package cli

import (
	"bytes"
	"encoding/json"
	"slices"
	"strings"
	"testing"
)

// The tests run against buffers, which are not terminals: auto mode resolves to the
// machine faces there. Explicit --output table pins the human face deterministically.

func TestRun_Table(t *testing.T) {
	tests := []struct {
		name               string
		args               []string
		wantExit           int
		wantStdoutContains string
		wantStderrContains string
		wantEmptyStdout    bool
		wantEmptyStderr    bool
	}{
		{
			name:               "no arguments prints help and succeeds",
			args:               nil,
			wantExit:           0,
			wantStdoutContains: "Usage:",
			wantEmptyStderr:    true,
		},
		{
			name:               "-h flag prints help to stdout",
			args:               []string{"-h"},
			wantExit:           0,
			wantStdoutContains: "Usage:",
			wantEmptyStderr:    true,
		},
		{
			name:               "--help flag prints help to stdout",
			args:               []string{"--help"},
			wantExit:           0,
			wantStdoutContains: "Usage:",
			wantEmptyStderr:    true,
		},
		{
			name:               "--version flag prints the build line",
			args:               []string{"--version"},
			wantExit:           0,
			wantStdoutContains: "messq ",
			wantEmptyStderr:    true,
		},
		{
			name:               "version on a pipe resolves to json",
			args:               []string{"version"},
			wantExit:           0,
			wantStdoutContains: `"version"`,
			wantEmptyStderr:    true,
		},
		{
			name:               "version table face is the text line",
			args:               []string{"--output", "table", "version"},
			wantExit:           0,
			wantStdoutContains: "messq ",
			wantEmptyStderr:    true,
		},
		{
			name:               "version json",
			args:               []string{"version", "--output", "json"},
			wantExit:           0,
			wantStdoutContains: `"version"`,
			wantEmptyStderr:    true,
		},
		{
			name:               "version json with an equals sign",
			args:               []string{"version", "--output=json"},
			wantExit:           0,
			wantStdoutContains: `"version"`,
			wantEmptyStderr:    true,
		},
		{
			name:               "unknown command is a usage error naming it",
			args:               []string{"--output", "table", "bogus"},
			wantExit:           2,
			wantStderrContains: `unknown command "bogus"`,
			wantEmptyStdout:    true,
		},
		{
			name:               "a fifth output mode is refused listing all four",
			args:               []string{"version", "--output", "yaml"},
			wantExit:           2,
			wantStderrContains: "auto|table|json|ndjson",
			wantEmptyStdout:    true,
		},
		{
			name:               "missing output value is a usage error",
			args:               []string{"version", "--output"},
			wantExit:           2,
			wantStderrContains: "flag needs an argument",
			wantEmptyStdout:    true,
		},
		{
			name:               "unexpected version argument is a usage error",
			args:               []string{"version", "extra"},
			wantExit:           2,
			wantStderrContains: "unexpected argument",
			wantEmptyStdout:    true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var stdout, stderr bytes.Buffer

			got := Run(tt.args, strings.NewReader(""), &stdout, &stderr)

			if got != tt.wantExit {
				t.Errorf("Run(%q) = %d, want %d (stderr: %q)", tt.args, got, tt.wantExit, stderr.String())
			}
			if tt.wantStdoutContains != "" && !strings.Contains(stdout.String(), tt.wantStdoutContains) {
				t.Errorf("stdout = %q, want it to contain %q", stdout.String(), tt.wantStdoutContains)
			}
			if tt.wantStderrContains != "" && !strings.Contains(stderr.String(), tt.wantStderrContains) {
				t.Errorf("stderr = %q, want it to contain %q", stderr.String(), tt.wantStderrContains)
			}
			if tt.wantEmptyStdout && stdout.Len() != 0 {
				t.Errorf("stdout = %q, want empty: data goes to stdout, narration to stderr", stdout.String())
			}
			if tt.wantEmptyStderr && stderr.Len() != 0 {
				t.Errorf("stderr = %q, want empty", stderr.String())
			}
		})
	}
}

func TestHelpListsEveryCurrentCommand(t *testing.T) {
	var stdout, stderr bytes.Buffer
	if exit := Run([]string{"--help"}, strings.NewReader(""), &stdout, &stderr); exit != 0 {
		t.Fatalf("exit = %d, want 0 (stderr %q)", exit, stderr.String())
	}
	out := stdout.String()
	for _, cmd := range []string{"version", "serve", "verify"} {
		if !strings.Contains(out, cmd) {
			t.Errorf("help does not list %q:\n%s", cmd, out)
		}
	}
}

// TestRun_VersionJSONKeys checks the rendered command output, not just the struct tags, so a
// change of renderer cannot silently change the contract frozen in PLAN.md section 8.
func TestRun_VersionJSONKeys(t *testing.T) {
	var stdout, stderr bytes.Buffer

	if exit := Run([]string{"version", "--output", "json"}, strings.NewReader(""), &stdout, &stderr); exit != 0 {
		t.Fatalf("exit = %d, want 0 (stderr: %q)", exit, stderr.String())
	}

	var fields map[string]json.RawMessage
	if err := json.Unmarshal(stdout.Bytes(), &fields); err != nil {
		t.Fatalf("stdout is not valid JSON (%v): %q", err, stdout.String())
	}
	got := make([]string, 0, len(fields))
	for k := range fields {
		got = append(got, k)
	}
	slices.Sort(got)

	want := []string{"commit", "date", "dirty", "go_version", "platform", "version"}
	if !slices.Equal(got, want) {
		t.Errorf("JSON keys = %v, want %v", got, want)
	}
	if !strings.HasSuffix(stdout.String(), "\n") {
		t.Error("JSON output does not end with a newline")
	}
}

// TestRun_VersionTableFaceIsTheTextLine pins the human face's byte shape.
func TestRun_VersionTableFaceIsTheTextLine(t *testing.T) {
	var stdout, stderr bytes.Buffer
	if exit := Run([]string{"--output", "table", "version"}, strings.NewReader(""), &stdout, &stderr); exit != 0 {
		t.Fatalf("exit = %d, want 0 (stderr: %q)", exit, stderr.String())
	}
	line := strings.TrimSpace(stdout.String())
	rest, ok := strings.CutPrefix(line, "messq ")
	if !ok {
		t.Fatalf("version line = %q, want it to start with %q", line, "messq ")
	}
	if version, _, _ := strings.Cut(rest, " ("); version == "" {
		t.Errorf("version line = %q, want a non-empty version field", line)
	}
}

// TestRun_VersionIsNeverEmpty guards the `go run ./cmd/messq version` case, where no ldflags
// are injected at all.
func TestRun_VersionIsNeverEmpty(t *testing.T) {
	var stdout, stderr bytes.Buffer

	if exit := Run([]string{"--output", "table", "version"}, strings.NewReader(""), &stdout, &stderr); exit != 0 {
		t.Fatalf("exit = %d, want 0 (stderr: %q)", exit, stderr.String())
	}

	line := strings.TrimSpace(stdout.String())
	if !strings.HasPrefix(line, "messq ") {
		t.Fatalf("version line = %q, want it to start with %q", line, "messq ")
	}
}

// TestServeAndVerifyRoutedThroughTree proves the two pre-chassis commands answer on
// the new tree with their contracts intact (their own suites exercise the bodies).
func TestServeAndVerifyRoutedThroughTree(t *testing.T) {
	t.Run("serve without data dir keeps its own usage error and code 2", func(t *testing.T) {
		var stdout, stderr bytes.Buffer
		code := Run([]string{"serve"}, strings.NewReader(""), &stdout, &stderr)
		if code != 2 {
			t.Errorf("exit = %d, want 2 (stderr %q)", code, stderr.String())
		}
		if got := stderr.String(); !strings.Contains(got, "--data-dir is required") || strings.Contains(got, "Error:") {
			t.Errorf("serve's bytes moved or were double-rendered: %q", got)
		}
	})
	t.Run("unknown flag reaches serve's own parser untouched", func(t *testing.T) {
		var stdout, stderr bytes.Buffer
		code := Run([]string{"serve", "--data-dir", t.TempDir(), "--wat"}, strings.NewReader(""), &stdout, &stderr)
		if code != 2 {
			t.Errorf("exit = %d, want 2 from serve's hand-parsed layer (stderr %q)", code, stderr.String())
		}
		if strings.Contains(stderr.String(), "Error:") {
			t.Errorf("funnel re-rendered serve's own message: %q", stderr.String())
		}
	})
}
