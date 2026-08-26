// SPDX-License-Identifier: Apache-2.0

// Package crash_test is the crash harness's entry points: the tests CI lanes run with
// different -crash.* flag values (a smoke subset on PRs, a 100-cycle kill9 on main, a
// 1 000-cycle sweep nightly). The harness itself lives in internal/testutil/crash and is
// driven by these flags, never by a hard-coded cycle count.
package crash_test

import (
	"context"
	"flag"
	"path/filepath"
	"testing"

	"github.com/a-holm/messq/internal/testutil/crash"
)

// The -crash.* flags compose with ordinary go test flags, so `go test -run TestCrashKill9
// -crash.cycles=100` runs the same test the nightly lane runs.
var (
	crashCycles     = flag.Int("crash.cycles", 8, "kill/restart cycles to run")
	crashSeed       = flag.Int64("crash.seed", 0, "seed; cycle N uses seed+N (0 = time-derived)")
	crashDurability = flag.String("crash.durability", "full", "full, relaxed, or both")
	crashPublishers = flag.Int("crash.publishers", 8, "concurrent publisher goroutines")
	// crashSurvivorship opts into the both-outcome SURVIVORSHIP vacuity guard. Whether an
	// UNKNOWN record survives recovery depends on whether a publish was mid-commit when
	// the kill landed — runner-scheduling-dependent (see Config.SkipSurvivorship). Under a
	// full make-cover parallel run the starved daemon legitimately produces whole sweeps
	// with zero surviving UNKNOWNs ("SURVIVORSHIP: 0 UNKNOWN present, 68 absent" fired
	// twice in CI), so short smokes assert only the deterministic guards; the nightly
	// 1 000-cycle sweep passes -crash.survivorship, where the law of large numbers makes
	// both outcomes certain in every healthy run.
	crashSurvivorship = flag.Bool("crash.survivorship", false, "enforce the both-outcome SURVIVORSHIP guard (nightly sweep lane; scheduling-dependent on short smokes)")
)

// durabilities expands the --crash.durability flag into the modes a sweep runs.
func durabilities() []string {
	if *crashDurability == "both" {
		return []string{"full", "relaxed"}
	}
	return []string{*crashDurability}
}

// newConfig builds the crash harness config from the flags for one durability mode.
func newConfig(t *testing.T, durability string) crash.Config {
	t.Helper()
	return crash.Config{
		Durability: durability,
		Root:       filepath.Join(t.TempDir(), "root"),
		Publishers: *crashPublishers,
		Cycles:     *crashCycles,
		Seed:       *crashSeed,
		// The deterministic guards (LIVENESS, KILL-LANDS-LOW/HIGH, WAL-TAIL) always run;
		// only the scheduling-dependent SURVIVORSHIP both-outcome requirement is opt-in.
		SkipSurvivorship: !*crashSurvivorship,
	}
}

// TestCrashKill9 is the named I1 test: seeded kill/restart cycles across all three kill
// strategies, asserting zero acknowledged loss in both durability modes. It is the
// per-merge, main and nightly lane — the same test, different -crash.cycles.
func TestCrashKill9(t *testing.T) {
	for _, d := range durabilities() {
		t.Run(d, func(t *testing.T) {
			report, err := crash.Run(context.Background(), newConfig(t, d))
			if err != nil {
				t.Fatalf("crash sweep (%s): %v", d, err)
			}
			t.Logf("%s: ok=%d unknown=%d failed=%d present=%d absent=%d wal_tail=%t",
				d, report.OK, report.Unknown, report.Failed,
				report.UnknownPresent, report.UnknownAbsent, report.WALTailObserved)
		})
	}
}

// TestRestartNoCleanup is G4: consecutive kill/restart cycles with zero filesystem
// intervention — the stale socket, the -wal/-shm files and the flock must all be handled by
// the daemon itself, never by the harness.
func TestRestartNoCleanup(t *testing.T) {
	ctx := context.Background()
	dir := filepath.Join(t.TempDir(), "root")
	dataDir := filepath.Join(dir, "data")
	sock := filepath.Join(dir, "messq.sock")
	cfg := crash.Config{Durability: "full"}

	for i := 0; i < 20; i++ {
		s, startErr := crash.Start(ctx, cfg, dataDir, sock)
		if startErr != nil {
			t.Fatalf("cycle %d start: %v", i, startErr)
		}
		if readyErr := s.Ready(ctx); readyErr != nil {
			t.Fatalf("cycle %d ready: %v", i, readyErr)
		}
		if killErr := s.Kill(); killErr != nil {
			t.Fatalf("cycle %d kill: %v", i, killErr)
		}
		s2, restartErr := crash.Start(ctx, cfg, dataDir, sock)
		if restartErr != nil {
			t.Fatalf("cycle %d restart: %v", i, restartErr)
		}
		if readyErr := s2.Ready(ctx); readyErr != nil {
			t.Fatalf("cycle %d restart ready: %v", i, readyErr)
		}
		if stopErr := s2.Stop(); stopErr != nil {
			t.Fatalf("cycle %d stop: %v", i, stopErr)
		}
	}
}

// TestCrashDuringMigration is G11's migration-window coverage: the immediate strategy
// against a fresh data dir, every cycle ending integrity-clean and verify-clean. The
// default 8 cycles is the smoke subset; the nightly lane raises -crash.cycles.
func TestCrashDuringMigration(t *testing.T) {
	ctx := context.Background()
	dir := filepath.Join(t.TempDir(), "root")
	cfg := crash.Config{
		Durability: "full",
		Root:       dir,
		Publishers: 4,
		Cycles:     *crashCycles,
		Seed:       *crashSeed + 1000, // a distinct seed family from the kill9 sweep
		Kill:       crash.Immediate{},
		SkipGuards: true, // immediate kills do not sustain load; the guards belong to the kill9 sweep
	}
	if _, err := crash.Run(ctx, cfg); err != nil {
		t.Fatalf("migration-window sweep: %v", err)
	}
}
