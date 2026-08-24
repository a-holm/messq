// SPDX-License-Identifier: Apache-2.0

package cli

import (
	"context"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/a-holm/messq/internal/store"
)

func noEnv(string) string { return "" }

func TestParseServeFlagsDefaults(t *testing.T) {
	cfg, err := parseServeFlags([]string{"--data-dir", "/tmp/x"}, noEnv)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if cfg.dataDir != "/tmp/x" {
		t.Errorf("dataDir = %q, want %q", cfg.dataDir, "/tmp/x")
	}
	if cfg.listen != "unix:///run/messq/messq.sock" {
		t.Errorf("listen = %q, want the default unix socket", cfg.listen)
	}
	if cfg.durability != store.DurabilityFull {
		t.Errorf("durability = %v, want full", cfg.durability)
	}
	if cfg.maxMsgSizeCeiling != 8<<20 {
		t.Errorf("maxMsgSizeCeiling = %d, want 8 MiB", cfg.maxMsgSizeCeiling)
	}
	if cfg.maxHeaderBytes != 4<<10 {
		t.Errorf("maxHeaderBytes = %d, want 4 KiB", cfg.maxHeaderBytes)
	}
	if cfg.maxBatchMessages != 1000 {
		t.Errorf("maxBatchMessages = %d, want 1000", cfg.maxBatchMessages)
	}
	if cfg.maxBatchBytes != 8<<20 {
		t.Errorf("maxBatchBytes = %d, want 8 MiB", cfg.maxBatchBytes)
	}
	if cfg.peekScanLimit != 10000 {
		t.Errorf("peekScanLimit = %d, want 10000", cfg.peekScanLimit)
	}
	if cfg.peekMaxLimit != 1000 {
		t.Errorf("peekMaxLimit = %d, want 1000", cfg.peekMaxLimit)
	}
	if cfg.dedupSweepInterval != 60*time.Second {
		t.Errorf("dedupSweepInterval = %v, want 60s", cfg.dedupSweepInterval)
	}
	// A1's register value is the flag's default: the SEMANTICS bounds row is the source
	// of truth and --drain-timeout only overrides it at runtime (orchestrator ruling on
	// brief-17 §8 Q1).
	if cfg.drainTimeout != 10*time.Second {
		t.Errorf("drainTimeout = %v, want the A1 default 10s", cfg.drainTimeout)
	}
	// #14 §9 transport bounds, one assertion per flag: a resolve() default can drift
	// (4096 → 10000 survived a whole review suite) without any other test noticing.
	// --max-waiters is pinned to SEMANTICS §A1's 4096 by name.
	if cfg.maxWaiters != 4096 {
		t.Errorf("maxWaiters = %d, want 4096 (SEMANTICS A1)", cfg.maxWaiters)
	}
	if cfg.maxWaitersPerConsumer != 256 {
		t.Errorf("maxWaitersPerConsumer = %d, want 256", cfg.maxWaitersPerConsumer)
	}
	if cfg.maxFetchWait != 5*time.Minute {
		t.Errorf("maxFetchWait = %v, want 5m", cfg.maxFetchWait)
	}
	if cfg.fetchEmptyDamper != 5*time.Millisecond {
		t.Errorf("fetchEmptyDamper = %v, want 5ms", cfg.fetchEmptyDamper)
	}
	if cfg.maxRequestBytes != 1<<20 {
		t.Errorf("maxRequestBytes = %d, want 1 MiB", cfg.maxRequestBytes)
	}
	if cfg.readHeaderTimeout != 10*time.Second {
		t.Errorf("readHeaderTimeout = %v, want 10s", cfg.readHeaderTimeout)
	}
	if cfg.idleTimeout != 120*time.Second {
		t.Errorf("idleTimeout = %v, want 120s", cfg.idleTimeout)
	}
	if cfg.maxRequestHeaderBytes != 16<<10 {
		t.Errorf("maxRequestHeaderBytes = %d, want 16 KiB", cfg.maxRequestHeaderBytes)
	}
	if cfg.maxConns != 1024 {
		t.Errorf("maxConns = %d, want 1024", cfg.maxConns)
	}
	if cfg.writerSubmitTimeout != 5*time.Second {
		t.Errorf("writerSubmitTimeout = %v, want 5s", cfg.writerSubmitTimeout)
	}
}

func TestParseServeFlagsEnvFallback(t *testing.T) {
	env := map[string]string{
		"MESSQ_DATA_DIR":             "/env/data",
		"MESSQ_LISTEN":               "tcp://127.0.0.1:4390",
		"MESSQ_DURABILITY":           "relaxed",
		"MESSQ_MAX_MSG_SIZE_CEILING": "16MiB",
		"MESSQ_MAX_HEADER_BYTES":     "8KiB",
		"MESSQ_MAX_BATCH_MESSAGES":   "500",
		"MESSQ_MAX_BATCH_BYTES":      "4MiB",
		"MESSQ_PEEK_SCAN_LIMIT":      "2000",
		"MESSQ_PEEK_MAX_LIMIT":       "200",
		"MESSQ_DEDUP_SWEEP_INTERVAL": "30s",
		"MESSQ_DRAIN_TIMEOUT":        "25s",
	}
	cfg, err := parseServeFlags(nil, func(k string) string { return env[k] })
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if cfg.dataDir != "/env/data" {
		t.Errorf("dataDir = %q, want /env/data", cfg.dataDir)
	}
	if cfg.listen != "tcp://127.0.0.1:4390" {
		t.Errorf("listen = %q", cfg.listen)
	}
	if cfg.durability != store.DurabilityRelaxed {
		t.Errorf("durability = %v, want relaxed", cfg.durability)
	}
	if cfg.maxMsgSizeCeiling != 16<<20 {
		t.Errorf("maxMsgSizeCeiling = %d", cfg.maxMsgSizeCeiling)
	}
	if cfg.maxHeaderBytes != 8<<10 {
		t.Errorf("maxHeaderBytes = %d", cfg.maxHeaderBytes)
	}
	if cfg.maxBatchMessages != 500 {
		t.Errorf("maxBatchMessages = %d", cfg.maxBatchMessages)
	}
	if cfg.maxBatchBytes != 4<<20 {
		t.Errorf("maxBatchBytes = %d", cfg.maxBatchBytes)
	}
	if cfg.peekScanLimit != 2000 {
		t.Errorf("peekScanLimit = %d", cfg.peekScanLimit)
	}
	if cfg.peekMaxLimit != 200 {
		t.Errorf("peekMaxLimit = %d", cfg.peekMaxLimit)
	}
	if cfg.dedupSweepInterval != 30*time.Second {
		t.Errorf("dedupSweepInterval = %v", cfg.dedupSweepInterval)
	}
	if cfg.drainTimeout != 25*time.Second {
		t.Errorf("drainTimeout = %v, want 25s from MESSQ_DRAIN_TIMEOUT", cfg.drainTimeout)
	}
}

func TestParseServeFlagsFlagOverridesEnv(t *testing.T) {
	env := func(k string) string {
		switch k {
		case "MESSQ_LISTEN":
			return "tcp://127.0.0.1:9999"
		case "MESSQ_DURABILITY":
			return "relaxed"
		default:
			return ""
		}
	}
	cfg, err := parseServeFlags([]string{
		"--data-dir", "/d",
		"--listen", "unix:///x.sock",
		"--durability", "full",
		"--drain-timeout", "3s",
	}, env)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if cfg.listen != "unix:///x.sock" {
		t.Errorf("listen = %q, want the flag to win over MESSQ_LISTEN", cfg.listen)
	}
	if cfg.durability != store.DurabilityFull {
		t.Errorf("durability = %v, want the flag to win over MESSQ_DURABILITY", cfg.durability)
	}
	if cfg.drainTimeout != 3*time.Second {
		t.Errorf("drainTimeout = %v, want the flag to win over MESSQ_DRAIN_TIMEOUT", cfg.drainTimeout)
	}
}

func TestParseServeFlagsErrors(t *testing.T) {
	tests := []struct {
		name string
		args []string
		env  map[string]string
		want string
	}{
		{"missing data dir", nil, nil, "--data-dir is required"},
		{"unknown flag", []string{"--data-dir", "/d", "--bogus", "x"}, nil, "unknown flag"},
		{"missing value", []string{"--data-dir", "/d", "--listen"}, nil, "--listen needs a value"},
		{"bad durability", []string{"--data-dir", "/d", "--durability", "nope"}, nil, "durability"},
		{"bad size", []string{"--data-dir", "/d", "--max-msg-size-ceiling", "12XY"}, nil, "max-msg-size-ceiling"},
		{"bad int", []string{"--data-dir", "/d", "--max-batch-messages", "abc"}, nil, "max-batch-messages"},
		{"bad duration", []string{"--data-dir", "/d", "--dedup-sweep-interval", "soon"}, nil, "dedup-sweep-interval"},
		{"zero sweep", []string{"--data-dir", "/d", "--dedup-sweep-interval", "0s"}, nil, "dedup-sweep-interval"},
		{"bad drain timeout", []string{"--data-dir", "/d", "--drain-timeout", "ten"}, nil, "drain-timeout"},
		{"zero drain timeout", []string{"--data-dir", "/d", "--drain-timeout", "0s"}, nil, "drain-timeout"},
		{"negative drain timeout", []string{"--data-dir", "/d", "--drain-timeout", "-5s"}, nil, "drain-timeout"},
		{"negative ceiling", []string{"--data-dir", "/d", "--max-msg-size-ceiling", "-1MiB"}, nil, "max-msg-size-ceiling"},
		{"positional", []string{"--data-dir", "/d", "extra"}, nil, "unexpected argument"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			getenv := func(k string) string { return tc.env[k] }
			_, err := parseServeFlags(tc.args, getenv)
			if err == nil {
				t.Fatalf("parse succeeded, want error containing %q", tc.want)
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Errorf("error = %q, want it to contain %q", err, tc.want)
			}
		})
	}
}

func TestParseByteSize(t *testing.T) {
	good := []struct {
		in   string
		want int64
	}{
		{"8MiB", 8 << 20},
		{"4KiB", 4 << 10},
		{"1GiB", 1 << 30},
		{"2TiB", 2 << 40},
		{"8B", 8},
		{"8388608", 8388608},
		{"0", 0},
	}
	for _, tc := range good {
		got, err := parseByteSize(tc.in)
		if err != nil {
			t.Errorf("parseByteSize(%q) = %v, want %d", tc.in, err, tc.want)
			continue
		}
		if got != tc.want {
			t.Errorf("parseByteSize(%q) = %d, want %d", tc.in, got, tc.want)
		}
	}
	bad := []string{"", "MiB", "8M", "8MiBx", "abc", "-1MiB", "1.5MiB"}
	for _, in := range bad {
		if _, err := parseByteSize(in); err == nil {
			t.Errorf("parseByteSize(%q) = nil error, want failure", in)
		}
	}
}

func TestServeConfigStoreOptions(t *testing.T) {
	cfg := serveConfig{
		dataDir:            "/d",
		durability:         store.DurabilityRelaxed,
		maxMsgSizeCeiling:  16 << 20,
		maxHeaderBytes:     8 << 10,
		maxBatchMessages:   500,
		peekScanLimit:      2000,
		peekMaxLimit:       200,
		dedupSweepInterval: 30 * time.Second,
	}
	opt := cfg.storeOptions()
	if opt.DataDir != "/d" {
		t.Errorf("DataDir = %q", opt.DataDir)
	}
	if opt.Durability != store.DurabilityRelaxed {
		t.Errorf("Durability = %v", opt.Durability)
	}
	if opt.Limits.MaxMsgSizeCeiling != 16<<20 {
		t.Errorf("Limits.MaxMsgSizeCeiling = %d", opt.Limits.MaxMsgSizeCeiling)
	}
	if opt.Limits.MaxHeaderBytes != 8<<10 {
		t.Errorf("Limits.MaxHeaderBytes = %d", opt.Limits.MaxHeaderBytes)
	}
	if opt.Limits.MaxHeaders != 32 {
		t.Errorf("Limits.MaxHeaders = %d, want the DefaultLimits value of 32", opt.Limits.MaxHeaders)
	}
	if opt.Limits.MaxSubjects != 32 {
		t.Errorf("Limits.MaxSubjects = %d, want the DefaultLimits value of 32", opt.Limits.MaxSubjects)
	}
	if opt.Limits.MaxDedupWindow != 24*time.Hour {
		t.Errorf("Limits.MaxDedupWindow = %v, want the DefaultLimits value of 24h", opt.Limits.MaxDedupWindow)
	}
	if opt.MaxBatchMessages != 500 {
		t.Errorf("MaxBatchMessages = %d", opt.MaxBatchMessages)
	}
	if opt.PeekScanLimit != 2000 {
		t.Errorf("PeekScanLimit = %d", opt.PeekScanLimit)
	}
	if opt.PeekMaxLimit != 200 {
		t.Errorf("PeekMaxLimit = %d", opt.PeekMaxLimit)
	}
	if opt.DedupSweepInterval != 30*time.Second {
		t.Errorf("DedupSweepInterval = %v", opt.DedupSweepInterval)
	}
}

func TestListenUnixSocket(t *testing.T) {
	dir := t.TempDir()
	sock := filepath.Join(dir, "messq.sock")

	ln, err := listen(context.Background(), "unix://"+sock)
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer func() {
		if closeErr := ln.Close(); closeErr != nil {
			t.Logf("close: %v", closeErr)
		}
	}()

	fi, err := os.Stat(sock)
	if err != nil {
		t.Fatalf("stat socket: %v", err)
	}
	if fi.Mode().Perm() != 0o660 {
		t.Errorf("socket mode = %o, want 0660", fi.Mode().Perm())
	}
}

func TestListenStaleSocket(t *testing.T) {
	dir := t.TempDir()
	sock := filepath.Join(dir, "messq.sock")

	// A crashed run leaves a plain file where the socket path used to be.
	if err := os.WriteFile(sock, []byte("stale"), 0o600); err != nil {
		t.Fatalf("plant stale file: %v", err)
	}

	ln, err := listen(context.Background(), "unix://"+sock)
	if err != nil {
		t.Fatalf("listen over stale path: %v", err)
	}
	defer func() {
		if closeErr := ln.Close(); closeErr != nil {
			t.Logf("close: %v", closeErr)
		}
	}()

	fi, err := os.Stat(sock)
	if err != nil {
		t.Fatalf("stat: %v", err)
	}
	if fi.Mode()&os.ModeSocket == 0 {
		t.Errorf("path is %v, want a socket", fi.Mode())
	}
}

func TestListenLoopbackTCP(t *testing.T) {
	ln, err := listen(context.Background(), "tcp://127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer func() {
		if closeErr := ln.Close(); closeErr != nil {
			t.Logf("close: %v", closeErr)
		}
	}()

	addr, ok := ln.Addr().(*net.TCPAddr)
	if !ok {
		t.Fatalf("addr = %T, want a TCP address", ln.Addr())
	}
	if !addr.IP.IsLoopback() {
		t.Errorf("bound to %v, want loopback", addr)
	}
}

func TestListenRefusesNonLoopback(t *testing.T) {
	for _, addr := range []string{
		"tcp://0.0.0.0:1234",
		"tcp://192.0.2.1:1234",
		"tcp://:1234",
	} {
		t.Run(addr, func(t *testing.T) {
			ln, err := listen(context.Background(), addr)
			if err == nil {
				if closeErr := ln.Close(); closeErr != nil {
					t.Logf("close: %v", closeErr)
				}
				t.Fatalf("listen(%q) succeeded, want a refusal", addr)
			}
		})
	}
}

func TestListenUnsupportedScheme(t *testing.T) {
	if _, err := listen(context.Background(), "http://127.0.0.1:8080"); err == nil {
		t.Fatal("listen(http://...) succeeded, want an unsupported-scheme error")
	}
}

// TestRefuseNonLoopback is the pure decision behind the bind refusal: loopback addresses
// pass, everything else (all-interfaces, a public address, an empty host) is refused.
func TestRefuseNonLoopback(t *testing.T) {
	for _, ok := range []string{"127.0.0.1:0", "127.0.0.1:4390", "[::1]:0"} {
		if err := refuseNonLoopback(context.Background(), ok); err != nil {
			t.Errorf("refuseNonLoopback(%q) = %v, want nil", ok, err)
		}
	}
	for _, bad := range []string{"0.0.0.0:0", "192.0.2.1:4390", ":4390", "10.0.0.1:80"} {
		if err := refuseNonLoopback(context.Background(), bad); err == nil {
			t.Errorf("refuseNonLoopback(%q) = nil, want a refusal", bad)
		}
	}
}
