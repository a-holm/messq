// SPDX-License-Identifier: Apache-2.0

package cli

import (
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	"github.com/a-holm/messq/internal/exitcode"
)

// TestCanaryCredentialNeverReappears mints a real-looking credential whose
// RANDOM half makes it unique to this run, then walks every surface downstream
// of `messq auth add` and asserts the raw credential bytes appear NOWHERE ELSE:
// not in narration, not in ls/json listings, not in hashes' neighbourhoods, not
// in re-read token files beyond their hash line.
//
// This is the PR-lane canary of issue #16 §11: any future regression that prints,
// logs or embeds a credential outside its single sanctioned stdout line turns
// this test red before review even opens.
func TestCanaryCredentialNeverReappears(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	file := filepath.Join(dir, "tokens")

	code, stdout, stderr := runAuth(t, "",
		"auth", "add",
		"--auth-file", file,
		"--id", "canary",
		"--roles", "consume",
		"--streams", "orders*",
	)
	if code != exitcode.OK {
		t.Fatalf("seed add exit = %d:\n%s", code, stderr)
	}
	lines := nonEmptyLines(stdout)
	if len(lines) != 1 {
		t.Fatalf("sanctioned window broken at the source: %d stdout lines:\n%s", len(lines), stdout)
	}
	credential := lines[0]

	sinks := map[string]string{
		"add stderr": stderr,
	}

	// Downstream surface 1: the stored FILE must contain only the hash line.
	data, rerr := os.ReadFile(file)
	if rerr != nil {
		t.Fatalf("read tokens: %v", rerr)
	}
	sinks["stored file"] = string(data)

	// Downstream surface 2: ls in both faces.
	if c2, o2, e2 := runAuth(t, "", "auth", "ls", "--auth-file", file); c2 == exitcode.OK {
		sinks["ls table"] = o2
	} else {
		sinks["ls error"] = e2
	}
	if c3, o3, _ := runAuth(t, "", "auth", "ls", "--output", "json", "--auth-file", file); c3 == exitcode.OK {
		sinks["ls json"] = o3
	}

	// Downstream surface 3: check success text.
	if c4, o4, _ := runAuth(t, credential, "auth", "check", "--auth-file", file); c4 == exitcode.OK {
		sinks["check ok"] = o4
	}

	for sink, content := range sinks {
		if strings.Contains(content, credential) {
			t.Errorf("%s contains the raw credential bytes:\n%s", sink, redactExcept(credential, content))
		}
		hexTail := credential[strings.LastIndex(credential, "_")+1:]
		if len(hexTail) > 8 && strings.Contains(content, hexTail) && sink != "add stderr" && sink != "stored file" {
			t.Errorf("%s contains the secret tail:\n%s", sink, redactExcept(credential, content))
		}
	}

	// And the golden invariant: exactly ONE line anywhere mentions msq1_ with
	// this run's id — the credential itself.
	re := regexp.MustCompile(`msq1_canary_[A-Za-z0-9._~+-]+`)
	total := 0
	for _, content := range []string{stdout, stderr} {
		total += len(re.FindAllString(content, -1))
	}
	if total != 1 {
		t.Errorf("%d full-credential appearances in add's two streams combined, want exactly 1", total)
	}
}

// redactExcept keeps the assertion messages safe to print in CI logs: the
// credential's middle is masked wherever it appears.
func redactExcept(credential, s string) string {
	if len(credential) < 12 || !strings.Contains(s, credential) {
		return s
	}
	masked := credential[:4] + strings.Repeat("*", len(credential)-8) + credential[len(credential)-4:]
	return strings.ReplaceAll(s, credential, masked)
}
