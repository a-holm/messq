// SPDX-License-Identifier: Apache-2.0

package cli

import (
	"context"
	"encoding/json"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/a-holm/messq/internal/cli/exit"
	"github.com/a-holm/messq/internal/queue"
	"github.com/a-holm/messq/internal/store"
)

// seedDoctorStoreForCLI seeds a misconfigured dir so a KNOWN check fires
// through the whole command path: consumer invoices drops with no ceiling,
// which is max_deliver_unlimited_no_dlq's exact profile.
func seedDoctorStoreForCLI(t *testing.T) string {
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
	cfg := queue.ConsumerConfig{
		Name:          "invoices",
		Filters:       []string{">"},
		AckWait:       30 * time.Second,
		MaxAckPending: 1000,
		Backoff:       []time.Duration{time.Second},
		DeadPolicy:    queue.DeadPolicyDrop,
		MaxDeliver:    0,
	}
	if _, crErr := st.CreateConsumer(ctx, "orders", cfg,
		queue.StartPosition{Kind: queue.StartFirst}, "test"); crErr != nil {
		t.Fatalf("create consumer: %v", crErr)
	}
	if closeErr := st.Close(ctx); closeErr != nil {
		t.Fatalf("close: %v", closeErr)
	}
	return dir
}

func TestDoctorOfflineCleanFixtureExitsZero(t *testing.T) {
	dataDir := seedBackupStore(t)

	res := runTree(t, nil, "doctor", "--data-dir", dataDir, "--output", "table")
	if res.Exit != exit.OK {
		t.Fatalf("exit = %d\nstdout:\n%s\nstderr:\n%s", res.Exit, res.Stdout, res.Stderr)
	}
	if !strings.Contains(res.Stdout, "no findings need attention") &&
		!strings.Contains(res.Stdout, " · ") {
		t.Fatalf("footer missing from offline run:\n%s", res.Stdout)
	}
}

func TestDoctorOfflineSeededFixtureFiresFinding(t *testing.T) {
	dataDir := seedDoctorStoreForCLI(t)

	res := runTree(t, nil, "doctor", "--data-dir", dataDir, "--output", "table")
	if res.Exit != exit.Error { // fail finding under --fail-on warn defaults exit 1
		t.Fatalf("exit = %d\nstdout:\n%s\nstderr:\n%s", res.Exit, res.Stdout, res.Stderr)
	}
	if !strings.Contains(res.Stdout, "[fail]") {
		t.Fatalf("human face lacks the fail block:\n%s", res.Stdout)
	}
	if !strings.Contains(res.Stdout+res.Stderr, "dead_policy=drop") &&
		!strings.Contains(res.Stdout, "drop") {
		t.Fatalf("finding prose should name the drop policy:\n%s", res.Stdout)
	}

	// The greppable contract rides the machine face.
	resJSON := runTree(t, nil, "doctor", "--data-dir", dataDir, "--output", "json")
	if !strings.Contains(resJSON.Stdout, `"id":"consumer.max_deliver_unlimited_no_dlq"`) {
		t.Fatalf("json face lost the seeded fail id:\n%s", resJSON.Stdout)
	}
}

func TestDoctorJSONFaceFrozenShape(t *testing.T) {
	dataDir := seedDoctorStoreForCLI(t)

	res := runTree(t, nil, "doctor", "--data-dir", dataDir, "--output", "json")
	if res.Exit != exit.Error {
		t.Fatalf("exit = %d\nstderr:\n%s", res.Exit, res.Stderr)
	}
	var doc map[string]any
	if uErr := json.Unmarshal([]byte(res.Stdout), &doc); uErr != nil {
		t.Fatalf("json face is not one document: %v\n%s", uErr, res.Stdout)
	}
	for _, key := range []string{"schema", "generated_at", "source", "findings", "summary"} {
		if _, ok := doc[key]; !ok {
			t.Fatalf("json face lacks frozen key %q", key)
		}
	}
	target, ok := doc["target"].(map[string]any)
	if !ok || target["data_dir"] != dataDir {
		t.Fatalf("target = %v, want data_dir %s", doc["target"], dataDir)
	}
	sum, ok := doc["summary"].(map[string]any)
	if !ok {
		t.Fatalf("summary missing or not an object: %v", doc["summary"])
	}
	if sum["fail"] == float64(0) {
		t.Fatalf("summary counts lost the seeded failure: %v", sum)
	}
}

func TestDoctorNDJSONFindingsThenSummaryLast(t *testing.T) {
	dataDir := seedDoctorStoreForCLI(t)

	res := runTree(t, nil, "doctor", "--data-dir", dataDir, "--output", "ndjson")
	if res.Exit != exit.Error {
		t.Fatalf("exit = %d\nstderr:\n%s", res.Exit, res.Stderr)
	}
	lines := nonEmptyLinesDoctor(res.Stdout)
	if len(lines) < 3 {
		t.Fatalf("expected finding lines plus summary line:\n%s", res.Stdout)
	}
	last := map[string]any{}
	if uErr := json.Unmarshal([]byte(lines[len(lines)-1]), &last); uErr != nil ||
		last["summary"] == nil {
		t.Fatalf("ndjson must end with the summary object, got %q (%v)",
			lines[len(lines)-1], uErr)
	}
	finding := map[string]any{}
	if uErr := json.Unmarshal([]byte(lines[0]), &finding); uErr != nil || finding["id"] == nil {
		t.Fatalf("ndjson line 0 is not a finding: %q (%v)", lines[0], uErr)
	}
}

func TestDoctorFailOnNeverAndThresholds(t *testing.T) {
	dataDir := seedDoctorStoreForCLI(t)

	res := runTree(t, nil, "doctor", "--data-dir", dataDir, "--fail-on", "never")
	if res.Exit != exit.OK {
		t.Fatalf("--fail-on never must always exit 0, got %d", res.Exit)
	}
	res = runTree(t, nil, "doctor", "--data-dir", dataDir, "--fail-on", "bogus")
	if res.Exit != exit.Usage {
		t.Fatalf("bad --fail-on exit = %d, want 2", res.Exit)
	}
}

func TestDoctorQuietFace(t *testing.T) {
	dataDir := seedDoctorStoreForCLI(t)

	res := runTree(t, nil, "doctor", "--data-dir", dataDir, "--quiet", "--output", "table")
	if res.Exit != exit.Error {
		t.Fatalf("exit = %d", res.Exit)
	}
	if strings.Contains(res.Stdout, "[info]") || strings.Contains(res.Stdout, "[ok]") {
		t.Fatalf("--quiet leaked ok/info blocks:\n%s", res.Stdout)
	}
	if !strings.Contains(res.Stdout, "[fail]") {
		t.Fatalf("--quiet dropped actionable findings:\n%s", res.Stdout)
	}
}

func TestDoctorListExplainsTheRegistry(t *testing.T) {
	res := runTree(t, nil, "doctor", "--list")
	if res.Exit != exit.OK {
		t.Fatalf("exit = %d\nstderr:\n%s", res.Exit, res.Stderr)
	}
	if !strings.Contains(res.Stdout, "consumer.max_deliver_unlimited") {
		t.Fatalf("--list missed a registered check:\n%s", res.Stdout)
	}
	res = runTree(t, nil, "doctor", "--explain", "consumer.max_deliver_unlimited")
	if res.Exit != exit.OK || !strings.Contains(res.Stdout, "max_deliver=0") {
		t.Fatalf("--explain output wrong (exit %d):\n%s", res.Exit, res.Stdout)
	}
	res = runTree(t, nil, "doctor", "--explain", "nosuch.check")
	if res.Exit != exit.Usage {
		t.Fatalf("unknown --explain id exit = %d, want 2", res.Exit)
	}
}

func TestDoctorOnlySkipFiltersValidateIDs(t *testing.T) {
	dataDir := seedDoctorStoreForCLI(t)

	res := runTree(t, nil, "doctor", "--data-dir", dataDir,
		"--only", "consumer.max_deliver_unlimited_no_dlq")
	if res.Exit != exit.Error {
		t.Fatalf("--only scoped run should still find the failure, exit %d:\n%s", res.Exit, res.Stdout)
	}
	if strings.Contains(res.Stdout, "stream.typo_suspect") &&
		strings.Contains(res.Stdout, "[info]") {
		t.Logf("note: out-of-family info findings present under --only:\n%s", res.Stdout)
	}

	res = runTree(t, nil, "doctor", "--data-dir", dataDir, "--skip", "consumer.*")
	if res.Exit != exit.OK {
		t.Fatalf("--skip family.* should suppress the failing check, exit %d\nstdout:\n%s",
			res.Exit, res.Stdout)
	}
	res = runTree(t, nil, "doctor", "--data-dir", dataDir, "--only", "nosuch.id")
	if res.Exit != exit.Usage {
		t.Fatalf("unknown --only id exit = %d, want 2", res.Exit)
	}
}

func TestDoctorUnreachableDaemonNeverSix(t *testing.T) {
	dead := "unix://" + filepath.Join(t.TempDir(), "missing.sock")

	res := runTree(t, nil, "doctor", "--addr", dead, "--output", "table")
	if res.Exit == exit.Unreachable {
		t.Fatal("doctor exited 6 for an unreachable daemon — §10 forbids this forever")
	}
	if res.Exit != exit.Error {
		t.Fatalf("exit = %d, want 1 (a fail finding), stderr:\n%s", res.Exit, res.Stderr)
	}
	if !strings.Contains(res.Stdout, "[fail] no daemon answered") {
		t.Fatalf("unreachable daemon must surface as a fail finding:\n%s", res.Stdout)
	}
	resJSON := runTree(t, nil, "doctor", "--addr", dead, "--output", "json")
	if !strings.Contains(resJSON.Stdout, `"id":"server.unreachable"`) ||
		!strings.Contains(resJSON.Stdout, `"severity":"fail"`) {
		t.Fatalf("json face lost the unreachable id:\n%s", resJSON.Stdout)
	}
}

// nonEmptyLines strips blank lines; ndjson writers never emit them but the
// trailing newline of stdout would otherwise produce an empty tail entry.
func nonEmptyLinesDoctor(s string) []string {
	var out []string
	for _, l := range strings.Split(strings.TrimRight(s, "\n"), "\n") {
		if strings.TrimSpace(l) != "" {
			out = append(out, l)
		}
	}
	return out
}

func TestDoctorMissingDataDirExitSeven(t *testing.T) {
	absent := filepath.Join(t.TempDir(), "no-such-data")

	res := runTree(t, nil, "doctor", "--data-dir", absent)
	if res.Exit != exit.Denied {
		t.Fatalf("exit = %d, want 7\nstderr:\n%s", res.Exit, res.Stderr)
	}
	if !strings.Contains(res.Stderr, absent) {
		t.Fatalf("teaching error should name the directory:\n%s", res.Stderr)
	}
}
