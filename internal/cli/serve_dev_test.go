// SPDX-License-Identifier: Apache-2.0

package cli

import (
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// --dev is the composite flag of issue #26 §2. Its implications on EXISTING
// serve flags are exactly two — the ephemeral data dir and #17's drain timeout —
// plus the dev-mode daemon behaviour (auto-create, /v1/info "dev") that lives in
// internal/api. The precedence rule is total: an explicitly-set flag always
// beats a --dev implication, and the table tests below are the pinned contract.

// TestServeDevParsesWithNoDataDir pins the acceptance-critical shape:
// `messq serve --dev` with no arguments parses. The parser records that the dir
// is ephemeral; runServe creates and names it (so the banner can print the real
// path) — that is why the parser must not invent one.
func TestServeDevParsesWithNoDataDir(t *testing.T) {
	cfg, err := parseServeFlags([]string{"--dev"}, func(string) string { return "" })
	if err != nil {
		t.Fatalf("parse `serve --dev`: %v", err)
	}
	if !cfg.dev {
		t.Fatal("--dev did not reach the config")
	}
	if !cfg.devEphemeral {
		t.Error("--dev without --data-dir must imply an ephemeral dir")
	}
	if cfg.dataDir != "" {
		t.Errorf("parser invented a data dir %q; ephemeral creation is runServe's job", cfg.dataDir)
	}
}

// TestServeDevImpliesDrainTimeout checks the one flag-value implication that
// exists today: Ctrl-C is instant in dev (2s drain, #17's flag, this issue's
// composition).
func TestServeDevImpliesDrainTimeout(t *testing.T) {
	cfg, err := parseServeFlags([]string{"--dev", "--data-dir", t.TempDir()}, func(string) string { return "" })
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if cfg.drainTimeout != devDrainTimeout {
		t.Errorf("--dev drain timeout = %s, want %s", cfg.drainTimeout, devDrainTimeout)
	}
	if devDrainTimeout != 2*time.Second {
		t.Errorf("devDrainTimeout = %s, want the issue's pinned 2s", devDrainTimeout)
	}
}

// TestServeDevExplicitFlagBeatsDevImplication is THE precedence test: an
// explicitly-set flag always beats a --dev implication (--dev --drain-timeout=9s
// gives 9s). The bug class this kills: applying --dev defaults over an
// explicitly-set flag.
func TestServeDevExplicitFlagBeatsDevImplication(t *testing.T) {
	cfg, err := parseServeFlags(
		[]string{"--dev", "--data-dir", filepath.Join(t.TempDir(), "d"), "--drain-timeout", "9s"},
		func(string) string { return "" })
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if cfg.drainTimeout != 9*time.Second {
		t.Errorf("explicit --drain-timeout lost to --dev: got %s, want 9s", cfg.drainTimeout)
	}
}

// TestServeDevExplicitDataDirWinsAndIsKept pins the banner-critical rule: an
// explicit --data-dir survives --dev and is NOT marked ephemeral — the daemon
// must never delete a directory the operator named.
func TestServeDevExplicitDataDirWinsAndIsKept(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "keep-me")
	cfg, err := parseServeFlags([]string{"--dev", "--data-dir", dir}, func(string) string { return "" })
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if cfg.dataDir != dir {
		t.Errorf("explicit --data-dir was overridden by --dev: %q", cfg.dataDir)
	}
	if cfg.devEphemeral {
		t.Error("--dev marked an explicitly-named data dir ephemeral")
	}
}

// TestServeDevEnvFallbackNotEatenByDev checks the resolution layering under
// --dev: flags win, then MESSQ_* env, then the dev default. --dev implies the
// drain timeout, so an env fallback for it is replaced — but settings --dev does
// not imply (durability) keep their env fallback untouched.
func TestServeDevEnvFallbackNotEatenByDev(t *testing.T) {
	env := map[string]string{"MESSQ_DURABILITY": "relaxed", "MESSQ_DRAIN_TIMEOUT": "7s"}
	cfg, err := parseServeFlags([]string{"--dev", "--data-dir", t.TempDir()},
		func(k string) string { return env[k] })
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if cfg.durability.String() != "relaxed" {
		t.Errorf("--dev overrode the MESSQ_DURABILITY env fallback: %q", cfg.durability.String())
	}
	if cfg.drainTimeout != devDrainTimeout {
		t.Errorf("dev implication lost to the MESSQ_DRAIN_TIMEOUT env: %s (dev replaces env, only flags beat dev)", cfg.drainTimeout)
	}
}

// TestServeDevFlagBeatsEnvDataDir pins the data-dir layering: an explicit
// --data-dir beats MESSQ_DATA_DIR (ADR-0009), and --dev beats a bare env value —
// a MESSQ_DATA_DIR pointing at production must not be silently adopted by a
// throwaway dev daemon.
func TestServeDevFlagBeatsEnvDataDir(t *testing.T) {
	env := map[string]string{"MESSQ_DATA_DIR": "/var/lib/messq-prod"}
	cfg, err := parseServeFlags([]string{"--dev"}, func(k string) string { return env[k] })
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if !cfg.devEphemeral || cfg.dataDir != "" {
		t.Errorf("--dev adopted the env data dir %q; dev implies ephemeral over env", cfg.dataDir)
	}
}

// TestDevBannerNamesItsCosts pins the banner content: unmissable, names the
// deletion (or the keep), and points at the real serve line.
func TestDevBannerNamesItsCosts(t *testing.T) {
	b := devBanner("/tmp/messq-dev-01K3", devDrainTimeout, false)
	for _, want := range []string{"DEVELOPMENT MODE", "DELETED", "--data-dir"} {
		if !strings.Contains(b, want) {
			t.Errorf("dev banner is missing %q:\n%s", want, b)
		}
	}
	kept := devBanner("/var/lib/messq", devDrainTimeout, true)
	if !strings.Contains(kept, "KEPT") {
		t.Errorf("dev banner with an explicit --data-dir must say the dir is kept:\n%s", kept)
	}
	if strings.Contains(kept, "DELETED") {
		t.Errorf("dev banner threatens deletion of an explicitly-named dir:\n%s", kept)
	}
}
