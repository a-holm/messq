// SPDX-License-Identifier: Apache-2.0

package cli

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/a-holm/messq/internal/queue"
	"github.com/a-holm/messq/internal/store"
)

// verifyFixture builds a migrated data dir with one stream and one message, for a clean
// verify run.
func verifyFixture(t *testing.T) string {
	t.Helper()
	ctx := context.Background()
	dir := filepath.Join(t.TempDir(), "data")
	st, _, err := store.Open(ctx, store.Options{
		DataDir:    dir,
		Durability: store.DurabilityFull,
		Logger:     slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	if _, _, createErr := st.CreateStream(ctx, queue.DefaultConfig("orders"), "test"); createErr != nil {
		t.Fatalf("create stream: %v", createErr)
	}
	if _, pubErr := st.Publish(ctx, store.PublishCmd{Stream: "orders", Req: queue.PublishReq{Subject: "orders.a", Body: []byte("hi")}}); pubErr != nil {
		t.Fatalf("publish: %v", pubErr)
	}
	if closeErr := st.Close(ctx); closeErr != nil {
		t.Fatalf("close store: %v", closeErr)
	}
	return dir
}

func runVerifyForTest(t *testing.T, args []string, getenv func(string) string) (int, string, string) {
	t.Helper()
	var stdout, stderr bytes.Buffer
	code := runVerify(args, getenv, &stdout, &stderr)
	return code, stdout.String(), stderr.String()
}

// TestVerifyCleanDir proves a freshly migrated dir exits 0 with the OK summary.
func TestVerifyCleanDir(t *testing.T) {
	dir := verifyFixture(t)
	code, out, _ := runVerifyForTest(t, []string{"--data-dir", dir, "--output", "table"}, nil)
	if code != exitOK {
		t.Fatalf("verify of a clean dir exited %d, want 0", code)
	}
	if !strings.Contains(out, "OK") {
		t.Errorf("table output lacks the OK summary: %s", out)
	}
}

// TestVerifyMissingDirExit3 proves a missing data dir exits 3.
func TestVerifyMissingDirExit3(t *testing.T) {
	code, _, _ := runVerifyForTest(t, []string{"--data-dir", filepath.Join(t.TempDir(), "nope")}, nil)
	if code != exitNotFound {
		t.Fatalf("verify of a missing dir exited %d, want 3", code)
	}
}

// TestVerifyUnknownFlagExit2 proves an unknown flag exits 2.
func TestVerifyUnknownFlagExit2(t *testing.T) {
	code, _, _ := runVerifyForTest(t, []string{"--bogus"}, nil)
	if code != exitUsage {
		t.Fatalf("verify with an unknown flag exited %d, want 2", code)
	}
}

// TestVerifyViolationExit1 proves a sabotaged dir exits 1 with the violation's frozen JSON
// field names.
func TestVerifyViolationExit1(t *testing.T) {
	dir := verifyFixture(t)
	db, err := sql.Open("sqlite", "file:"+filepath.Join(dir, "messq.db"))
	if err != nil {
		t.Fatalf("open writable: %v", err)
	}
	if _, err := db.ExecContext(context.Background(), `UPDATE meta SET v = '99' WHERE k = 'schema_version'`); err != nil {
		t.Fatalf("plant newer schema: %v", err)
	}
	if cerr := db.Close(); cerr != nil {
		t.Fatalf("close writable: %v", cerr)
	}

	code, out, _ := runVerifyForTest(t, []string{"--data-dir", dir, "--output", "json"}, nil)
	if code != exitError {
		t.Fatalf("verify of a sabotaged dir exited %d, want 1", code)
	}
	var got struct {
		OK         bool `json:"ok"`
		Violations []struct {
			ID     string `json:"id"`
			Detail string `json:"detail"`
		} `json:"violations"`
	}
	if err := json.Unmarshal([]byte(out), &got); err != nil {
		t.Fatalf("json output does not parse: %v\n%s", err, out)
	}
	if got.OK {
		t.Error("json ok flag is true for a violating dir")
	}
	if len(got.Violations) == 0 || got.Violations[0].ID != "V1" {
		t.Errorf("violations = %+v, want a V1 violation with the frozen field names", got.Violations)
	}
}

// TestVerifyIncompleteCopy proves edge case 15: a dir whose clean_shutdown marker says a
// -wal should exist but whose -wal is missing is diagnosed as an incomplete copy, not as
// data loss.
func TestVerifyIncompleteCopy(t *testing.T) {
	dir := verifyFixture(t)
	db, err := sql.Open("sqlite", "file:"+filepath.Join(dir, "messq.db"))
	if err != nil {
		t.Fatalf("open writable: %v", err)
	}
	if _, err := db.ExecContext(context.Background(), `UPDATE meta SET v = '0' WHERE k = 'clean_shutdown'`); err != nil {
		t.Fatalf("mark unclean: %v", err)
	}
	// Checkpoint the WAL into the main .db so the marker survives the -wal removal below.
	if _, err := db.ExecContext(context.Background(), `PRAGMA wal_checkpoint(TRUNCATE)`); err != nil {
		t.Fatalf("checkpoint: %v", err)
	}
	if cerr := db.Close(); cerr != nil {
		t.Fatalf("close writable: %v", cerr)
	}
	// Simulate a copy that took only the .db: drop the -wal/-shm siblings the marker says
	// must exist.
	for _, suffix := range []string{"-wal", "-shm"} {
		if rmErr := os.Remove(filepath.Join(dir, "messq.db"+suffix)); rmErr != nil && !errors.Is(rmErr, os.ErrNotExist) {
			t.Fatalf("remove %s: %v", suffix, rmErr)
		}
	}

	code, _, stderr := runVerifyForTest(t, []string{"--data-dir", dir, "--output", "table"}, nil)
	if code != exitError {
		t.Fatalf("verify of an incomplete copy exited %d, want 1", code)
	}
	if !strings.Contains(stderr, "incomplete copy") {
		t.Errorf("stderr does not name the incomplete copy: %s", stderr)
	}
}

// TestParseVerifyFlags proves the hand-rolled flag parser resolves flag -> env -> default
// and rejects bad values.
func TestParseVerifyFlags(t *testing.T) {
	tests := []struct {
		name    string
		args    []string
		env     map[string]string
		wantDir string
		wantErr bool
	}{
		{"flag wins", []string{"--data-dir", "/a"}, map[string]string{"MESSQ_DATA_DIR": "/b"}, "/a", false},
		{"env fallback", nil, map[string]string{"MESSQ_DATA_DIR": "/b"}, "/b", false},
		{"default", nil, nil, "/var/lib/messq", false},
		{"deep flag", []string{"--deep"}, nil, "/var/lib/messq", false},
		{"bad output", []string{"--output", "yaml"}, nil, "", true},
		{"unknown flag", []string{"--nope"}, nil, "", true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			getenv := func(k string) string { return tt.env[k] }
			cfg, err := parseVerifyFlags(tt.args, getenv)
			if (err != nil) != tt.wantErr {
				t.Fatalf("parseVerifyFlags(%v) error = %v, wantErr %v", tt.args, err, tt.wantErr)
			}
			if err == nil && cfg.dataDir != tt.wantDir {
				t.Errorf("dataDir = %q, want %q", cfg.dataDir, tt.wantDir)
			}
		})
	}
}
