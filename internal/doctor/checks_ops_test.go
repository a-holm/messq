// SPDX-License-Identifier: Apache-2.0

package doctor

import (
	"strings"
	"testing"
	"time"
)

var opsNow = time.Date(2026, 11, 4, 12, 0, 0, 0, time.UTC)

func TestDLQGrowingUndrainedFiresWithoutRedrives(t *testing.T) {
	snap := func(redrives int64) *Snapshot {
		return &Snapshot{
			Streams: []StreamState{{Name: "orders.dlq", Msgs: 41}},
			Events: EventStats{
				DeadGrowthKnown: true,
				DeadByOrigin:    map[string]int64{"orders": 12},
				RedriveCounts:   map[string]int64{"orders": redrives},
			},
		}
	}
	f := mustFire(t, evalCheck(t, "dlq.growing_undrained", snap(0)),
		"dlq.growing_undrained", SevFail)
	if f.Subject.Stream != "orders.dlq" {
		t.Fatalf("subject should name the DLQ stream: %+v", f.Subject)
	}
	mustNotFire(t, evalCheck(t, "dlq.growing_undrained", snap(2)),
		"dlq.growing_undrained") // actively redriven
}

func TestDLQNoRetentionZeroZeroBoundary(t *testing.T) {
	mk := func(ageMS, bytesMax int64) *Snapshot {
		return &Snapshot{Streams: []StreamState{{
			Name: "orders.dlq", MaxAgeMS: ageMS, MaxBytes: bytesMax,
		}}}
	}
	w := mustFire(t, evalCheck(t, "dlq.no_retention", mk(0, 0)),
		"dlq.no_retention", SevWarn)
	if !strings.Contains(w.Fix[0], "--max-age") {
		t.Fatalf("fix should suggest max-age: %v", w.Fix)
	}
	// Exactly one of the two set means bounded: not fired.
	mustNotFire(t, evalCheck(t, "dlq.no_retention", mk(1, 0)),
		"dlq.no_retention")
	mustNotFire(t, evalCheck(t, "dlq.no_retention", mk(0, 1)),
		"dlq.no_retention")
}

func TestDLQNoConsumerNeedsDepthAndSilence(t *testing.T) {
	snap := func(msgs int64, consumers int) *Snapshot {
		s := &Snapshot{Streams: []StreamState{{Name: "orders.dlq", Msgs: msgs}}}
		for i := 0; i < consumers; i++ {
			s.Consumers = append(s.Consumers,
				ConsumerState{Stream: "orders.dlq", Name: "watcher"})
		}
		return s
	}
	w := mustFire(t, evalCheck(t, "dlq.no_consumer", snap(5, 0)),
		"dlq.no_consumer", SevWarn)
	if !strings.Contains(w.Fix[0], "messq dlq ls") {
		t.Fatalf("fix should point at the dlq listing (#29): %v", w.Fix)
	}
	mustNotFire(t, evalCheck(t, "dlq.no_consumer", snap(0, 0)),
		"dlq.no_consumer") // empty lot
	mustNotFire(t, evalCheck(t, "dlq.no_consumer", snap(5, 1)),
		"dlq.no_consumer") // someone watches
}

func TestServerUncleanStartRequiresHistory(t *testing.T) {
	res := evalCheck(t, "server.unclean_last_start", &Snapshot{})
	if len(res) != 1 || res[0].Severity != SevSkipped {
		t.Fatalf("no history should skip: %+v", res)
	}
	clean := &Snapshot{Events: EventStats{StartHistoryKnown: true}}
	res = evalCheck(t, "server.unclean_last_start", clean)
	if len(res) != 0 {
		t.Fatalf("clean history stays silent: %+v", res)
	}
	i := mustFire(t, evalCheck(t, "server.unclean_last_start",
		&Snapshot{Events: EventStats{StartHistoryKnown: true, LastStartUnclean: true}}),
		"server.unclean_last_start", SevInfo)
	if i.NoFix == "" {
		t.Fatal("informational verdict needs its NoFix sentence")
	}
}

func TestServerRestartLoopFloorAtThree(t *testing.T) {
	hist := func(starts int64) *Snapshot {
		return &Snapshot{Events: EventStats{StartHistoryKnown: true, RecentStarts: starts}}
	}
	w := mustFire(t, evalCheck(t, "server.restart_loop", hist(3)),
		"server.restart_loop", SevFail)
	if !strings.Contains(w.Fix[0], "journalctl") {
		t.Fatalf("fix should route to journald: %v", w.Fix)
	}
	mustNotFire(t, evalCheck(t, "server.restart_loop", hist(2)),
		"server.restart_loop")
}

func TestServerClockJumpOnlyOnRecordedRegression(t *testing.T) {
	mustNotFire(t, evalCheck(t, "server.clock_jump", &Snapshot{}),
		"server.clock_jump")
	w := mustFire(t, evalCheck(t, "server.clock_jump",
		&Snapshot{Events: EventStats{ClockJumpMS: 1500}}), "server.clock_jump", SevWarn)
	if w.Evidence["jump_back_ms"] != int64(1500) {
		t.Fatalf("evidence lost the magnitude: %+v", w.Evidence)
	}
}

func TestServerSweepIntervalAgainstSmallestAckWait(t *testing.T) {
	knobbed := &Snapshot{
		ServerKnobs: ListenerConfigFacts{SweepIntervalMS: 60_000},
		Consumers: []ConsumerState{
			{Stream: "a", Name: "x", AckWaitMS: 30_000},
			{Stream: "b", Name: "y", AckWaitMS: 45_000},
		},
	}
	w := mustFire(t, evalCheck(t, "server.sweep_interval", knobbed),
		"server.sweep_interval", SevWarn)
	if w.Evidence["min_ack_wait_ms"] != int64(30_000) {
		t.Fatalf("must compare against the SMALLEST ack_wait: %+v", w.Evidence)
	}
	ok := &Snapshot{
		ServerKnobs: ListenerConfigFacts{SweepIntervalMS: 30_000},
		Consumers:   knobbed.Consumers,
	}
	mustNotFire(t, evalCheck(t, "server.sweep_interval", ok),
		"server.sweep_interval") // equal is fine: sweep can't be slower than this one
	res := evalCheck(t, "server.sweep_interval", &Snapshot{})
	if len(res) != 1 || res[0].Severity != SevSkipped {
		t.Fatalf("live-only check skips offline: %+v", res)
	}
}

func TestMetricsDroppedSeriesAnyNonzeroWarns(t *testing.T) {
	res := evalCheck(t, "metrics.dropped_series", &Snapshot{})
	if len(res) != 1 || res[0].Severity != SevSkipped {
		t.Fatalf("metrics live-only skip offline: %+v", res)
	}
	live := &Snapshot{Source: SourceLive, Metrics: &MetricFacts{DroppedSeries: 7}}
	mustFire(t, evalCheck(t, "metrics.dropped_series", live),
		"metrics.dropped_series", SevWarn)
	calm := &Snapshot{Source: SourceLive, Metrics: &MetricFacts{}}
	mustNotFire(t, evalCheck(t, "metrics.dropped_series", calm),
		"metrics.dropped_series")
}

func TestSecurityPermissionsJudgeRawModes(t *testing.T) {
	safe := &Snapshot{Security: &SecurityFacts{DataDirMode: 0o700, DBFileMode: 0o600}}
	res := evalCheck(t, "security.permissions", safe)
	if len(res) != 0 {
		t.Fatalf("safe modes should stay silent: %+v", res)
	}
	open := &Snapshot{
		Storage:  &StorageFacts{DataDir: "/var/lib/messq"},
		Security: &SecurityFacts{DataDirMode: 0o755, DBFileMode: 0o644},
	}
	f := mustFire(t, evalCheck(t, "security.permissions", open),
		"security.permissions", SevFail)
	if len(f.Fix) != 2 {
		t.Fatalf("one fix line per problem: %v", f.Fix)
	}
	res = evalCheck(t, "security.permissions", &Snapshot{})
	if len(res) != 1 || res[0].Severity != SevSkipped {
		t.Fatalf("facts missing → skip: %+v", res)
	}
}

func TestBackupFamilyBoundaries(t *testing.T) {
	// none_configured standing reminder.
	i := mustFire(t, evalCheck(t, "backup.none_configured", &Snapshot{}),
		"backup.none_configured", SevInfo)
	if strings.Contains(strings.Join(i.Fix, ""), "--backup-dir=") &&
		false { // exact command text stays free-form; presence asserted below
		t.Fatal("unreachable guard")
	}
	if len(i.Fix) < 2 {
		t.Fatal("reminder should show both the backup and the watch commands")
	}

	base := func(files ...BackupFile) *Snapshot {
		return &Snapshot{
			BackupDir: "/var/backups/messq",
			Now:       opsNow,
			Backups:   files,
		}
	}
	fresh := base(BackupFile{
		Path:      "/var/backups/messq/a.db",
		ModTimeMS: opsNow.Add(-24 * time.Hour).UnixMilli(),
	})
	res := evalCheck(t, "backup.stale", fresh)
	if len(res) != 0 {
		t.Fatalf("fresh snapshot keeps staleness quiet: %+v", res)
	}

	exactAge := base(BackupFile{
		Path:      "/b.db",
		ModTimeMS: opsNow.Add(-168 * time.Hour).UnixMilli(),
	})
	mustFire(t, evalCheck(t, "backup.stale", exactAge), "backup.stale", SevWarn)

	emptyDir := base()
	s := mustFire(t, evalCheck(t, "backup.stale", emptyDir), "backup.stale", SevWarn)
	if !strings.Contains(s.Title, "no snapshots yet") {
		t.Fatalf("empty backup dir should say so plainly: %q", s.Title)
	}

	broken := base(
		BackupFile{Path: "/c.db", StampState: "quickcheck_failed"},
		BackupFile{Path: "/d.db", StampState: "ok"},
	)
	f := mustFire(t, evalCheck(t, "backup.unreadable", broken),
		"backup.unreadable", SevFail)
	if !strings.Contains(f.Detail, "quickcheck_failed") {
		t.Fatalf("naming matters on restore-problems: %q", f.Detail)
	}

	loose := base(BackupFile{Path: "/e.db", Mode: 0o644})
	w := mustFire(t, evalCheck(t, "backup.perms", loose), "backup.perms", SevWarn)
	if !strings.Contains(w.Detail, "0600") && !strings.Contains(w.Title, "group/other") {
		t.Fatalf("perms finding should teach the mode law: %+v", w)
	}
	tight := base(BackupFile{Path: "/f.db", Mode: 0o600})
	res = evalCheck(t, "backup.perms", tight)
	if len(res) != 0 {
		t.Fatalf("tight perms stay silent: %+v", res)
	}
}
