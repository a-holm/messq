// SPDX-License-Identifier: Apache-2.0

package scriptenv

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/a-holm/messq/internal/wirecheck"
	"github.com/a-holm/messq/pkg/client"
)

// jsonNumber is the decode-side number type documentDigest and stringify see.
func jsonNumber(s string) json.Number { return json.Number(s) }

// TestDeterministicEnvDropsMessqVars pins the G2 sibling rule for scripts: the
// outer environment's MESSQ_* variables never leak into a script unless the
// script sets them itself.
func TestDeterministicEnvDropsMessqVars(t *testing.T) {
	outer := []string{
		"PATH=/usr/bin",
		"MESSQ_ADDR=unix:///run/messq/messq.sock",
		"MESSQ_TOKEN_FILE=/run/messq/token",
		"HOME=/root",
	}
	got, dirs := deterministicEnv("/work", outer)
	for _, kv := range got {
		if strings.HasPrefix(kv, "MESSQ_") {
			t.Errorf("deterministicEnv leaked %q into the script environment", kv)
		}
	}
	have := func(prefix string) bool {
		for _, kv := range got {
			if strings.HasPrefix(kv, prefix+"=") {
				return true
			}
		}
		return false
	}
	for _, want := range []string{
		"TZ", "LC_ALL", "NO_COLOR", "TERM", "COLUMNS", "HOME",
		"XDG_CONFIG_HOME", "XDG_CACHE_HOME", "XDG_STATE_HOME",
	} {
		if !have(want) {
			t.Errorf("deterministicEnv is missing %s", want)
		}
	}
	if len(dirs) != 4 {
		t.Errorf("deterministicEnv returned %d dirs, want 4", len(dirs))
	}
}

// TestMaskTextNeverMasksContractValues pins the never-mask list on the textual
// path: ack tokens, seq numbers and counts survive; volatile values do not.
func TestMaskTextNeverMasksContractValues(t *testing.T) {
	work := "/tmp/go-test-script123/001"
	in := "published 01K3QW8F2M9V0X7Y3B5N6C8D1E  seq 1  demo.hello  11 B\n" +
		"token demo/worker/3/2/1 attempt 2/3  cause=timeout\n" +
		"trace 4bf92f3577b34da6a3ce929d0e0e4736  at " + work + "/data\n" +
		"2026-08-26T12:00:00.114Z durable\n"
	got := maskText(in, work)
	if strings.Contains(got, "01K3QW8F2M9V0X7Y3B5N6C8D1E") {
		t.Error("maskText left a ULID in place")
	}
	if !strings.Contains(got, "token demo/worker/3/2/1") {
		t.Error("maskText masked an ack token — tokens are never masked")
	}
	if !strings.Contains(got, "seq 1") || !strings.Contains(got, "attempt 2/3") {
		t.Error("maskText masked seq/attempt — counts are never masked")
	}
	if strings.Contains(got, work) {
		t.Error("maskText left the work dir in place")
	}
	if !strings.Contains(got, "$WORK") || !strings.Contains(got, "<TRACE>") || !strings.Contains(got, "<TS>") {
		t.Errorf("maskText missed a placeholder: %q", got)
	}
}

// TestJSONPathWalksAndFails covers the capture command's path walker.
func TestJSONPathWalksAndFails(t *testing.T) {
	doc := map[string]any{
		"id":  "01K3",
		"seq": jsonNumber("7"),
		"items": []any{
			map[string]any{"seq": jsonNumber("1")},
		},
	}
	for _, tc := range []struct {
		path string
		want string
	}{
		{"$.id", "01K3"},
		{"$.seq", "7"},
		{"$.items[0].seq", "1"},
	} {
		got, err := jsonPath(doc, tc.path)
		if err != nil {
			t.Fatalf("jsonPath(%q): %v", tc.path, err)
		}
		if stringify(got) != tc.want {
			t.Errorf("jsonPath(%q) = %q, want %q", tc.path, stringify(got), tc.want)
		}
	}
	for _, bad := range []string{"id", "$.missing", "$.items[5].seq", "$.items[0].nope"} {
		if _, err := jsonPath(doc, bad); err == nil {
			t.Errorf("jsonPath(%q) unexpectedly succeeded", bad)
		}
	}
}

// TestShapeDiffDetectsRename is the cmpshape guarantee in miniature: a renamed
// field shows up as a document path the type digest does not know.
func TestShapeDiffDetectsRename(t *testing.T) {
	wantDigest, err := wirecheck.DigestOf(client.PublishAck{})
	if err != nil {
		t.Fatalf("digest PublishAck: %v", err)
	}
	// The honest document: every field of PublishAck under its real name.
	honest := documentDigest(map[string]any{
		"stream": "s", "seq": jsonNumber("1"), "id": "x", "trace_id": "t",
		"duplicate": false, "published_at": jsonNumber("42"),
	})
	if diff := shapeDiff(wantDigest, honest); diff != "" {
		t.Errorf("shapeDiff flagged an honest document: %s", diff)
	}
	// The renamed document: `seq` became `sequence` — a break, never a refresh.
	renamed := documentDigest(map[string]any{
		"stream": "s", "sequence": jsonNumber("1"), "id": "x", "trace_id": "t",
		"duplicate": false, "published_at": jsonNumber("42"),
	})
	if diff := shapeDiff(wantDigest, renamed); diff == "" {
		t.Error("shapeDiff missed a renamed field")
	}
}

// TestShapesRegistryHasPrototypes keeps every registered shape digestable.
func TestShapesRegistryHasPrototypes(t *testing.T) {
	for name, proto := range Shapes() {
		if _, err := wirecheck.DigestOf(proto); err != nil {
			t.Errorf("shape %s: digest of %T: %v", name, proto, err)
		}
	}
	if _, ok := Shapes()["BuildInfo"]; !ok {
		t.Error("Shapes lost the BuildInfo entry")
	}
}

// TestWriteFileAtomicLeavesNoTempFiles covers the -update write path.
func TestWriteFileAtomicLeavesNoTempFiles(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "script.txtar")
	if err := os.WriteFile(path, []byte("first"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := writeFileAtomic(path, []byte("second")); err != nil {
		t.Fatalf("writeFileAtomic: %v", err)
	}
	got, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != "second" {
		t.Errorf("writeFileAtomic wrote %q", got)
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 1 {
		t.Errorf("writeFileAtomic left %d files behind, want exactly the target", len(entries))
	}
}
