// SPDX-License-Identifier: Apache-2.0

package cli

import (
	"strings"
	"testing"
	"time"
)

// Issue #27 §8 flags for the housekeeping and disk-safety subsystem. Values come
// from PLAN §4.5 / SEMANTICS S15's A1 register rows (#27-owned) and the issue body's
// job-bound table; 0 for --janitor-interval is a documented dev-only disable.

func TestParseServeFlagsHousekeepingDefaults(t *testing.T) {
	cfg, err := parseServeFlags([]string{"--data-dir", "/tmp/x"}, noEnv)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if cfg.janitorInterval != time.Minute {
		t.Errorf("janitorInterval = %v, want the census's 60s tick", cfg.janitorInterval)
	}
	if cfg.eventRetention != 72*time.Hour {
		t.Errorf("eventRetention = %v, want 72h (A1)", cfg.eventRetention)
	}
	if cfg.eventMaxRows != 1_000_000 {
		t.Errorf("eventMaxRows = %d, want 1_000_000 (I11: bounded by default)", cfg.eventMaxRows)
	}
	if cfg.walMaxBytes != 256<<20 {
		t.Errorf("walMaxBytes = %d, want 256MiB (A1)", cfg.walMaxBytes)
	}
	if cfg.minFreeBytes != 256<<20 {
		t.Errorf("minFreeBytes = %d, want 256MiB (A1)", cfg.minFreeBytes)
	}
	if cfg.diskReserveBytes != 64<<20 {
		t.Errorf("diskReserveBytes = %d, want 64MiB (issue body §disk-reserve-bytes)", cfg.diskReserveBytes)
	}
	if cfg.vacuumFreelistPages != 10_000 {
		t.Errorf("vacuumFreelistPages = %d, want 10000 (~40MiB)", cfg.vacuumFreelistPages)
	}
	if cfg.vacuumPagesPerTick != 2_000 {
		t.Errorf("vacuumPagesPerTick = %d, want 2000", cfg.vacuumPagesPerTick)
	}
	if cfg.lagReportThreshold != 10_000 {
		t.Errorf("lagReportThreshold = %d, want 10000", cfg.lagReportThreshold)
	}
	if cfg.lagReportInterval != time.Minute {
		t.Errorf("lagReportInterval = %v, want 60s", cfg.lagReportInterval)
	}
}

func TestParseServeFlagsJanitorIntervalZeroIsLegal(t *testing.T) {
	cfg, err := parseServeFlags([]string{"--data-dir", "/tmp/x", "--janitor-interval", "0"}, noEnv)
	if err != nil {
		t.Fatalf("0 must parse (documented dev-only disable): %v", err)
	}
	if cfg.janitorInterval != 0 {
		t.Fatalf("janitorInterval = %v, want exactly 0 to arm no jobs", cfg.janitorInterval)
	}
}

func TestParseServeFlagsNegativeAndMalformedRefused(t *testing.T) {
	tests := []struct {
		name string
		args []string
		want string
	}{
		{"negative wal-max-bytes", []string{"--data-dir", "d", "--wal-max-bytes", "-1B"}, "--wal-max-bytes"},
		{"negative min-free", []string{"--data-dir", "d", "--min-free-bytes", "-1KiB"}, "--min-free-bytes"},
		{"negative event-max-rows", []string{"--data-dir", "d", "--event-max-rows", "-5"}, "--event-max-rows"},
		{"garbage janitor interval", []string{"--data-dir", "d", "--janitor-interval", "soon"}, "--janitor-interval"},
		{"negative vacuum pages", []string{"--data-dir", "d", "--vacuum-pages-per-tick", "-1"}, "--vacuum-pages-per-tick"},
		{"negative freelist threshold", []string{"--data-dir", "d", "--vacuum-freelist-pages", "-9"}, "--vacuum-freelist-pages"},
		{"negative lag threshold", []string{"--data-dir", "d", "--lag-report-threshold", "-2"}, "--lag-report-threshold"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := parseServeFlags(tc.args, noEnv)
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("err = %v, want a refusal naming %s", err, tc.want)
			}
		})
	}
}

func TestParseServeFlagsEnvFallbackForDiskFlags(t *testing.T) {
	env := func(k string) string {
		switch k {
		case "MESSQ_WAL_MAX_BYTES":
			return "64MiB"
		case "MESSQ_MIN_FREE_BYTES":
			return "128MiB"
		case "MESSQ_EVENT_RETENTION":
			return "24h"
		default:
			return ""
		}
	}
	cfg, err := parseServeFlags([]string{"--data-dir", "/tmp/x"}, env)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if cfg.walMaxBytes != 64<<20 || cfg.minFreeBytes != 128<<20 || cfg.eventRetention != 24*time.Hour {
		t.Fatalf("env fallback missed: %+v", cfg)
	}
}
