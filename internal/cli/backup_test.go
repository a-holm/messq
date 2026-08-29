// SPDX-License-Identifier: Apache-2.0

package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/a-holm/messq/internal/cli/exit"
	"github.com/a-holm/messq/internal/queue"
	"github.com/a-holm/messq/internal/store"
)

// backupClock is the frozen seam every seeded store builds under: nothing in
// this file may read the wall clock (tests are never flaky).
var backupClock = time.Date(2026, 8, 26, 12, 0, 0, 0, time.UTC)

// seedBackupStore builds a 0700 data dir holding one stream with one message —
// the minimum messq can honestly snapshot.
func seedBackupStore(t *testing.T) string {
	t.Helper()
	dir := filepath.Join(t.TempDir(), "data")
	ctx := context.Background()
	st, _, openErr := store.Open(ctx, store.Options{DataDir: dir})
	if openErr != nil {
		t.Fatalf("open store: %v", openErr)
	}
	if _, _, crErr := st.CreateStream(ctx,
		queue.DefaultConfig("orders"), "orders"); crErr != nil {
		t.Fatalf("create stream: %v", crErr)
	}
	if _, pubErr := st.PublishBatch(ctx, store.BatchCmd{
		Stream: "orders",
		Reqs:   []queue.PublishReq{{Subject: "orders.created", Body: []byte("x")}},
	}); pubErr != nil {
		t.Fatalf("publish: %v", pubErr)
	}
	if closeErr := st.Close(ctx); closeErr != nil {
		t.Fatalf("close: %v", closeErr)
	}
	return dir
}

// cliResult captures one in-process invocation through the production funnel.
type cliResult struct {
	Exit   int
	Stdout string
	Stderr string
}

// runTree executes messq once with neutral seams — non-TTY, empty environment,
// frozen clock. It rides ExecuteTree so runs take exactly the production path
// (the clitest harness does the same for external packages).
func runTree(t *testing.T, env map[string]string, args ...string) cliResult {
	t.Helper()
	var stdout, stderr bytes.Buffer
	e := &Env{
		Stdin:      strings.NewReader(""),
		Stdout:     &stdout,
		Stderr:     &stderr,
		Getenv:     func(k string) string { return env[k] },
		Now:        func() time.Time { return backupClock },
		IsTerminal: func(io.Writer) bool { return false },
		Width:      func() int { return 0 },
	}
	root := NewRoot(e)
	res := cliResult{Exit: ExecuteTree(context.Background(), e, root, args)}
	res.Stdout = stdout.String()
	res.Stderr = stderr.String()
	return res
}

func writeProbeFile(t *testing.T, path string, body []byte) {
	t.Helper()
	if wErr := os.WriteFile(path, body, 0o600); wErr != nil {
		t.Fatalf("write probe file: %v", wErr)
	}
}

func readSmallFile(t *testing.T, path string) string {
	t.Helper()
	body, rErr := os.ReadFile(path)
	if rErr != nil {
		t.Fatalf("read %s: %v", path, rErr)
	}
	return string(body)
}

func TestBackupSuccessTableFace(t *testing.T) {
	dataDir := seedBackupStore(t)
	dest := filepath.Join(t.TempDir(), "out.db")

	res := runTree(t, nil, "backup", dest, "--data-dir", dataDir)
	if res.Exit != exit.OK {
		t.Fatalf("exit = %d, want 0\nstdout:\n%s\nstderr:\n%s", res.Exit, res.Stdout, res.Stderr)
	}
	for _, want := range []string{"dest", "quick_check", "node"} {
		if !strings.Contains(res.Stdout, want) {
			t.Fatalf("table receipt lacks %q:\n%s", want, res.Stdout)
		}
	}
	info, statErr := os.Stat(dest)
	if statErr != nil {
		t.Fatalf("snapshot missing after success: %v", statErr)
	}
	if info.Mode().Perm() != 0o600 {
		t.Fatalf("snapshot mode = %v, want 0600", info.Mode().Perm())
	}
}

func TestBackupJSONFaceFrozenKeys(t *testing.T) {
	dataDir := seedBackupStore(t)
	dest := filepath.Join(t.TempDir(), "out.db")

	res := runTree(t, nil, "backup", dest, "--data-dir", dataDir, "--output", "json")
	if res.Exit != exit.OK {
		t.Fatalf("exit = %d\nstderr:\n%s", res.Exit, res.Stderr)
	}
	var doc map[string]any
	if uErr := json.Unmarshal([]byte(res.Stdout), &doc); uErr != nil {
		t.Fatalf("json face is not one document: %v\n%s", uErr, res.Stdout)
	}
	for _, key := range []string{
		"schema", "dest", "bytes", "pages", "taken_at",
		"duration_ms", "verified", "source_node_id", "stream_heads", "inflight_at_snapshot",
	} {
		if _, ok := doc[key]; !ok {
			t.Fatalf("json face lacks frozen key %q: %v", key, doc)
		}
	}
	if doc["schema"] != float64(1) || doc["verified"] != "quick_check" {
		t.Fatalf("frozen values drifted: %v", doc)
	}
	destField, destOK := doc["dest"].(string)
	if !destOK || destField == "" || !filepath.IsAbs(destField) {
		t.Fatalf("dest = %v, want the absolute path", doc["dest"])
	}
	heads, ok := doc["stream_heads"].(map[string]any)
	if !ok || heads["orders"] == nil {
		t.Fatalf("stream_heads = %v, want orders stamped", doc["stream_heads"])
	}
}

func TestBackupDestinationExistsWithoutForceExits4(t *testing.T) {
	dataDir := seedBackupStore(t)
	dest := filepath.Join(t.TempDir(), "out.db")
	writeProbeFile(t, dest, []byte("old"))

	res := runTree(t, nil, "backup", dest, "--data-dir", dataDir)
	if res.Exit != exit.Conflict {
		t.Fatalf("exit = %d, want 4\nstderr:\n%s", res.Exit, res.Stderr)
	}
	if !strings.Contains(res.Stderr, "--force") {
		t.Fatalf("teaching error should name --force:\n%s", res.Stderr)
	}
	// The old file is untouched: a refused run never destroys the good copy.
	if got := readSmallFile(t, dest); got != "old" {
		t.Fatalf("refused run clobbered the destination: %q", got)
	}
}

func TestBackupForceOverwritesViaRename(t *testing.T) {
	dataDir := seedBackupStore(t)
	dest := filepath.Join(t.TempDir(), "out.db")
	writeProbeFile(t, dest, []byte("old"))

	res := runTree(t, nil, "backup", dest, "--data-dir", dataDir, "--force")
	if res.Exit != exit.OK {
		t.Fatalf("exit = %d\nstderr:\n%s", res.Exit, res.Stderr)
	}
	if got := readSmallFile(t, dest); got == "old" {
		t.Fatal("--force left the old contents in place")
	}
}

func TestBackupInsideDataDirRefusedExit2(t *testing.T) {
	dataDir := seedBackupStore(t)
	dest := filepath.Join(dataDir, "snap.db")

	res := runTree(t, nil, "backup", dest, "--data-dir", dataDir)
	if res.Exit != exit.Usage {
		t.Fatalf("exit = %d, want 2\nstderr:\n%s", res.Exit, res.Stderr)
	}
	if !strings.Contains(res.Stderr, "inside the data directory") {
		t.Fatalf("teaching error should say why inside is wrong:\n%s", res.Stderr)
	}
}

func TestBackupUnwritableDestDirExit7(t *testing.T) {
	dataDir := seedBackupStore(t)
	// A destination whose parent does not exist fails the writable-probe for
	// every uid — deterministic where a chmod 0500 game depends on running
	// as non-root.
	dest := filepath.Join(t.TempDir(), "no-such-dir", "out.db")

	res := runTree(t, nil, "backup", dest, "--data-dir", dataDir)
	if res.Exit != exit.Denied {
		t.Fatalf("exit = %d, want 7\nstderr:\n%s", res.Exit, res.Stderr)
	}
	if !strings.Contains(res.Stderr, "not writable") {
		t.Fatalf("teaching error should name the unwritable directory:\n%s", res.Stderr)
	}
}

func TestBackupUsageRefusals(t *testing.T) {
	dataDir := seedBackupStore(t)
	cases := []struct {
		name string
		args []string
	}{
		{"stdout streaming is not a backup", []string{"backup", "-", "--data-dir", dataDir}},
		{"relative destination", []string{"backup", "rel/out.db", "--data-dir", dataDir}},
		{"missing destination argument", []string{"backup", "--data-dir", dataDir}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			res := runTree(t, nil, tc.args...)
			if res.Exit != exit.Usage {
				t.Fatalf("exit = %d, want 2\nstderr:\n%s", res.Exit, res.Stderr)
			}
		})
	}
}

func TestBackupVerifyFlagValuesAndSkippedWarning(t *testing.T) {
	dataDir := seedBackupStore(t)

	res := runTree(t, nil, "backup", filepath.Join(t.TempDir(), "out.db"),
		"--data-dir", dataDir, "--verify", "none")
	if res.Exit != exit.OK {
		t.Fatalf("exit = %d\nstderr:\n%s", res.Exit, res.Stderr)
	}
	if !strings.Contains(strings.ToLower(res.Stdout+res.Stderr), "skipped") {
		t.Fatalf("--verify none must own its skipped self-check visibly:\n%s|%s", res.Stdout, res.Stderr)
	}

	res = runTree(t, nil, "backup", filepath.Join(t.TempDir(), "b.db"),
		"--data-dir", dataDir, "--verify", "loudly")
	if res.Exit != exit.Usage {
		t.Fatalf("bad --verify value exit = %d, want 2", res.Exit)
	}
}

func TestBackupNotAMessqDataDirTeachesDataDirFlag(t *testing.T) {
	bare := t.TempDir()

	res := runTree(t, nil, "backup", filepath.Join(t.TempDir(), "out.db"), "--data-dir", bare)
	if res.Exit != exit.Error {
		t.Fatalf("exit = %d, want 1\nstderr:\n%s", res.Exit, res.Stderr)
	}
	if !strings.Contains(res.Stderr, "--data-dir") {
		t.Fatalf("error should point at --data-dir:\n%s", res.Stderr)
	}
}
