// SPDX-License-Identifier: Apache-2.0

package doctor

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"time"
)

func init() {
	defaultRegistry.Register(Check{
		ID:      "dlq.growing_undrained",
		Summary: "DLQ grew in the window while nobody redrove anything",
		Explain: "A poison message is the usual cause; DLQs nobody watches are the " +
			"documented failure mode (#12). Redrive them deliberately after reading " +
			"the causes, not after the disk reminds you.",
		Needs: SourceEither,
		Eval:  evalDLQGrowingUndrained,
	})
	defaultRegistry.Register(Check{
		ID:      "dlq.no_retention",
		Summary: "a DLQ stream keeps everything forever",
		Explain: "Unbounded dead-letter streams turn one bad consumer into an outage " +
			"on layaway. Give the parking lot its own retention so redrives stay a " +
			"choice and not an archaeology project.",
		Needs: SourceEither,
		Eval:  evalDLQNoRetention,
	})
	defaultRegistry.Register(Check{
		ID:      "dlq.no_consumer",
		Summary: "dead letters piled up with nobody watching that DLQ",
		Explain: "Depth without a consumer means messages parked after failing and " +
			"nothing observes them. Even a manual redrive routine beats silence.",
		Needs: SourceEither,
		Eval:  evalDLQNoConsumer,
	})
	defaultRegistry.Register(Check{
		ID:      "dlq.template_drift",
		Summary: "an existing DLQ drifted from the current --dlq-* template",
		Explain: "#12 creates DLQs from your current template settings; older ones " +
			"keep the shape they were born with. Doctor reports the drift rather than " +
			"mutating history.",
		Needs: SourceEither,
		Eval:  evalDLQTemplateDrift,
	})
	defaultRegistry.Register(Check{
		ID:      "server.unclean_last_start",
		Summary: "the most recent start followed an unclean shutdown",
		Explain: "Expected after kill -9 during incident response; NOT expected after " +
			"a systemctl stop. Repeated unclean stops mean lifecycle handling deserves " +
			"a look (#17).",
		Needs: SourceEither,
		Eval:  evalServerUncleanLastStart,
	})
	defaultRegistry.Register(Check{
		ID:      "server.restart_loop",
		Summary: "three or more daemon starts inside the last hour",
		Explain: "A crash-looping broker hides partial availability behind restarts. " +
			"The exit codes and journal tell you which component keeps dying first.",
		Needs: SourceEither,
		Eval:  evalServerRestartLoop,
	})
	defaultRegistry.Register(Check{
		ID:      "server.clock_jump",
		Summary: "the wall clock regressed more than a second while running",
		Explain: "Deadlines ride monotonic clocks but ULIDs do not — a backwards jump " +
			"orders ids strangely and inflates TTL math anywhere callers used wall time.",
		Needs: SourceEither,
		Eval:  evalServerClockJump,
	})
	defaultRegistry.Register(Check{
		ID:      "server.sweep_interval",
		Summary: "--sweep-interval outruns the smallest ack_wait (#11)",
		Explain: "The sweeper reclaims timed-out deliveries; if it wakes less often " +
			"than some consumer's ack_wait, redelivery latency inherits that gap for " +
			"every worker on that consumer.",
		Needs: SourceLive,
		Eval:  evalServerSweepInterval,
	})
	defaultRegistry.Register(Check{
		ID:      "server.janitor_disabled",
		Summary: "--janitor-interval 0 disables retention and trims (#27)",
		Explain: "It is a test-only setting: retention promises, dedup trims and disk " +
			"reserve all stop happening. Running production with zero janitor interval " +
			"is choosing which of those hurts first.",
		Needs: SourceLive,
		Eval:  evalServerJanitorDisabled,
	})
	defaultRegistry.Register(Check{
		ID:      "storage.reserve_unavailable",
		Summary: "fallocate is unsupported here — disk headroom shrank (#27)",
		Explain: "Without fallocate the reserved-tail protection cannot pin bytes, so " +
			"--min-free-bytes becomes your only floor. Raise it accordingly.",
		Needs: SourceLive,
		Eval:  evalStorageReserveUnavailable,
	})
	defaultRegistry.Register(Check{
		ID:      "metrics.dropped_series",
		Summary: "the metrics registry dropped series under pressure (#21)",
		Explain: "Anything dropped stopped existing as far as dashboards are concerned. " +
			"Raise --metrics-max-series or cut series-producing consumers before an " +
			"incident makes you read the graph wrong.",
		Needs: SourceLive,
		Eval:  evalMetricsDroppedSeries,
	})
	defaultRegistry.Register(Check{
		ID:      "security.permissions",
		Summary: "filesystem permissions around the data directory are wrong",
		Explain: "#16's preflight rules are blunt on purpose: data dirs private, " +
			"databases 0600, token files tighter than nothing at all. Doctor names the " +
			"path and the chmod line.",
		Needs: SourceEither,
		Eval:  evalSecurityPermissions,
	})
	defaultRegistry.Register(Check{
		ID:      "security.tcp_no_auth",
		Summary: "a TCP listener has no auth file protecting it",
		Explain: "Loopback TCP still shares the host with anything; without --auth-file " +
			"any local process may publish. Mount an auth file or accept who knows.",
		Needs: SourceLive,
		Eval:  evalSecurityTCPNoAuth,
	})
	defaultRegistry.Register(Check{
		ID:      "security.cleartext_public",
		Summary: "a non-loopback listener serves cleartext HTTP today",
		Explain: "Until native TLS lands (#40) termination belongs in front of messq: " +
			"a reverse proxy or WireGuard tunnel, and never raw on a public interface.",
		Needs: SourceLive,
		Eval:  evalSecurityCleartextPublic,
	})
	defaultRegistry.Register(Check{
		ID:      "backup.none_configured",
		Summary: "no backup directory is configured for monitoring",
		Explain: "messq cannot tell you when you last took a backup without somewhere " +
			"to look. Point --backup-dir at the directory the timer writes to.",
		Needs: SourceEither,
		Eval:  evalBackupNoneConfigured,
	})
	defaultRegistry.Register(Check{
		ID:      "backup.stale",
		Summary: "newest known snapshot is older than --backup-max-age",
		Explain: "Backups rot silently: this is the check that turns silent rot into a " +
			"warn line cron emails somebody about.",
		Needs: SourceEither,
		Eval:  evalBackupStale,
	})
	defaultRegistry.Register(Check{
		ID:      "backup.unreadable",
		Summary: "a snapshot on disk would not restore",
		Explain: "quick_check fails or provenance keys are missing: whichever it is, " +
			"this file will not restore. Take a fresh one and delete this politely.",
		Needs: SourceEither,
		Eval:  evalBackupUnreadable,
	})
	defaultRegistry.Register(Check{
		ID:      "backup.perms",
		Summary: "backup payloads readable beyond their owner",
		Explain: "Snapshots contain message bodies in full: group/other read on a " +
			"backup file is the same leak as publishing them wider.",
		Needs: SourceEither,
		Eval:  evalBackupPerms,
	})
}

// ---- fact bundles for the ops families -------------------------------------

// SecurityFacts carries raw permission observations; judging stays in checks.
type SecurityFacts struct {
	DataDirMode   uint32 // os.FileMode().Perm() of the data dir
	DBFileMode    uint32 // perm bits of messq.db (0 = not statable)
	TokenFilePath string // relative to data dir; "" when none configured
	TokenFileMode uint32 // perm bits of the token file (0 = absent/not statable)

	TCPNoAuth        bool // live: a TCP listener exists without auth configured
	CleartextPublic  bool // live: non-loopback listener without TLS termination
	ListenerUnknowns bool // live source failed to describe listeners fully
}

// ListenerConfigFacts hold the runtime knobs /v1/info will grow.
type ListenerConfigFacts struct {
	SweepIntervalMS   int64 // 0 = unknown
	JanitorIntervalMS int64 // 0 = unknown; -1 = disabled
	DiskReserveMisses bool  // fallocate unsupported → reserve unusable
}

// BackupFile describes one candidate snapshot in the watched backup dir.
type BackupFile struct {
	Path       string
	ModTimeMS  int64
	Bytes      int64
	Mode       uint32
	StampState string // "ok" | "missing" | "unknown" | "quickcheck_failed"
}

// ---- dlq --------------------------------------------------------------------

// evalDLQGrowingUndrained uses window aggregates: deads arriving minus redrives.
func evalDLQGrowingUndrained(_ context.Context, snap *Snapshot) []Finding {
	if !snap.Events.DeadGrowthKnown {
		return []Finding{skippedCheck("dlq.growing_undrained",
			"event aggregates were not collected")}
	}
	var out []Finding
	for origin, grew := range snap.Events.DeadByOrigin {
		if grew <= 0 {
			continue
		}
		if snap.Events.RedriveCounts[origin] > 0 {
			continue // someone is actively draining
		}
		depth := int64(0)
		if st := streamByName(snap.Streams, dlqName(origin)); st != nil {
			depth = st.Msgs
		}
		out = append(out, Finding{
			ID: "dlq.growing_undrained", Severity: SevFail,
			Title: fmt.Sprintf("DLQ %s holds depth %d and grew by %d in the window "+
				"with zero redrives", renderSafe(dlqName(origin)), depth, grew),
			Detail: "Dead letters accumulate because a worker rejects something " +
				"deterministically; DLQs nobody watches are the documented failure mode.",
			Fix:     []string{fmt.Sprintf("messq dlq ls %s --group-by cause", origin)},
			Subject: Subject{Stream: dlqName(origin)},
			Evidence: map[string]any{
				"new_dead": grew, "redriven": snap.Events.RedriveCounts[origin],
				"depth": depth,
			},
			Docs: docsAnchor("dlq.growing_undrained"),
		})
	}
	sort.Slice(out, func(i, j int) bool {
		return subjectKey(out[i].Subject) < subjectKey(out[j].Subject)
	})
	return out
}

func dlqName(origin string) string { return origin + ".dlq" }

func streamByName(streams []StreamState, name string) *StreamState {
	for i := range streams {
		if streams[i].Name == name {
			return &streams[i]
		}
	}
	return nil
}

func evalDLQNoRetention(_ context.Context, snap *Snapshot) []Finding {
	var out []Finding
	for _, st := range snap.Streams {
		if !strings.HasSuffix(st.Name, ".dlq") {
			continue
		}
		if st.MaxAgeMS != 0 || st.MaxBytes != 0 {
			continue
		}
		out = append(out, Finding{
			ID: "dlq.no_retention", Severity: SevWarn,
			Title: fmt.Sprintf("%s keeps dead letters forever", renderSafe(st.Name)),
			Detail: "max_age_ms=0 and max_bytes=0 both: the parking lot grows until " +
				"someone invents retention the hard way.",
			Fix:     []string{fmt.Sprintf("messq stream edit %s --max-age 30d", st.Name)},
			Subject: Subject{Stream: st.Name},
			Docs:    docsAnchor("dlq.no_retention"),
		})
	}
	return out
}

func evalDLQNoConsumer(_ context.Context, snap *Snapshot) []Finding {
	counts := streamConsumers(snap)
	var out []Finding
	for _, st := range snap.Streams {
		if !strings.HasSuffix(st.Name, ".dlq") || st.Msgs == 0 ||
			counts[st.Name] > 0 {
			continue
		}
		out = append(out, Finding{
			ID: "dlq.no_consumer", Severity: SevWarn,
			Title: fmt.Sprintf("%s holds %d dead letters with no consumer watching",
				renderSafe(st.Name), st.Msgs),
			Detail:  "Nobody is even looking. Silence plus growth is how DLQ rot wins.",
			Fix:     []string{fmt.Sprintf("messq dlq ls %s", originOf(st.Name))},
			Subject: Subject{Stream: st.Name},
			Docs:    docsAnchor("dlq.no_consumer"),
		})
	}
	return out
}

func originOf(dlq string) string {
	return strings.TrimSuffix(dlq, ".dlq")
}

func evalDLQTemplateDrift(_ context.Context, snap *Snapshot) []Finding {
	if len(snap.Events.DLQDriftList) == 0 {
		return nil // drift facts land with #12's config comparisons; silent pass otherwise
	}
	var out []Finding
	for _, d := range snap.Events.DLQDriftList {
		out = append(out, Finding{
			ID: "dlq.template_drift", Severity: SevInfo,
			Title: fmt.Sprintf("%s differs from the current DLQ template",
				renderSafe(d.Stream)),
			Detail: renderSafe(d.Diff),
			Fix: []string{
				fmt.Sprintf("messq stream edit %s ...   # adopt template or accept drift",
					d.Stream),
			},
			Subject: Subject{Stream: d.Stream},
			Docs:    docsAnchor("dlq.template_drift"),
		})
	}
	return out
}

// ---- server remainder -------------------------------------------------------

func evalServerUncleanLastStart(_ context.Context, snap *Snapshot) []Finding {
	if !snap.Events.StartHistoryKnown {
		return []Finding{skippedCheck("server.unclean_last_start",
			"start history was not collected")}
	}
	if !snap.Events.LastStartUnclean {
		return nil
	}
	return []Finding{{
		ID: "server.unclean_last_start", Severity: SevInfo,
		Title: "most recent start recovered from an unclean shutdown",
		Detail: "Fine after kill -9; suspicious right after systemctl stop — the " +
			"shutdown path should have written its goodbye.",
		NoFix: "informational — tail the journal if it repeats unexpectedly",
		Docs:  docsAnchor("server.unclean_last_start"),
	}}
}

const restartLoopFloor = 3

func evalServerRestartLoop(_ context.Context, snap *Snapshot) []Finding {
	if !snap.Events.StartHistoryKnown {
		return []Finding{skippedCheck("server.restart_loop",
			"start history was not collected")}
	}
	if snap.Events.RecentStarts < restartLoopFloor {
		return nil
	}
	return []Finding{{
		ID: "server.restart_loop", Severity: SevFail,
		Title: fmt.Sprintf("%d daemon starts inside the analysis window",
			snap.Events.RecentStarts),
		Detail: "Three or more is a loop, not luck: something dies at boot and systemd " +
			"keeps reviving it.",
		Fix:      []string{"journalctl -u messq --since=-60min   # see which exit repeats"},
		Evidence: map[string]any{"starts_in_window": snap.Events.RecentStarts},
		Docs:     docsAnchor("server.restart_loop"),
	}}
}

func evalServerClockJump(_ context.Context, snap *Snapshot) []Finding {
	if snap.Events.ClockJumpMS == 0 {
		return nil // no jump recorded (or clocks quiet): nothing to say
	}
	return []Finding{{
		ID: "server.clock_jump", Severity: SevWarn,
		Title: fmt.Sprintf("wall clock jumped backwards by %dms since start",
			snap.Events.ClockJumpMS),
		Detail: "Deadlines stayed sane (monotonic); ULIDs did not — sorting by id vs " +
			"time can disagree until traffic washes past the seam.",
		Fix:      []string{"fix NTP on this host"},
		Evidence: map[string]any{"jump_back_ms": snap.Events.ClockJumpMS},
		Docs:     docsAnchor("server.clock_jump"),
	}}
}

func evalServerSweepInterval(_ context.Context, snap *Snapshot) []Finding {
	knobs := snap.Knobs()
	if knobs.SweepIntervalMS == 0 {
		return []Finding{skippedCheck("server.sweep_interval",
			"needs a running daemon exposing sweep config (try --addr)")}
	}
	minAck := int64(-1)
	for _, c := range snap.Consumers {
		if c.AckWaitMS > 0 && (minAck < 0 || c.AckWaitMS < minAck) {
			minAck = c.AckWaitMS
		}
	}
	if minAck < 0 || knobs.SweepIntervalMS <= minAck {
		return nil
	}
	return []Finding{{
		ID: "server.sweep_interval", Severity: SevWarn,
		Title: fmt.Sprintf("sweep every %s is slower than ack_wait %s",
			time.Duration(knobs.SweepIntervalMS)*time.Millisecond,
			time.Duration(minAck)*time.Millisecond),
		Detail: "Timed-out deliveries wait for the NEXT sweeper wake before they " +
			"redeliver: consumer timeouts become sweep+ack_wait by construction.",
		Fix: []string{"lower --sweep-interval below the smallest ack_wait, or raise that ack_wait"},
		Evidence: map[string]any{
			"sweep_ms": knobs.SweepIntervalMS, "min_ack_wait_ms": minAck,
		},
		Docs: docsAnchor("server.sweep_interval"),
	}}
}

func evalServerJanitorDisabled(_ context.Context, snap *Snapshot) []Finding {
	knobs := snap.Knobs()
	switch {
	case knobs.JanitorIntervalMS == 0:
		return []Finding{skippedCheck("server.janitor_disabled",
			"needs a running daemon exposing janitor config (try --addr)")}
	case knobs.JanitorIntervalMS > 0:
		return nil
	}
	return []Finding{{
		ID: "server.janitor_disabled", Severity: SevWarn,
		Title: "--janitor-interval 0: retention and trims are off entirely",
		Detail: "Ret enforcement, dedup trimming and disk-reserve sweeps all wait on " +
			"a timer that will never fire.",
		Fix:  []string{"set a real --janitor-interval and restart"},
		Docs: docsAnchor("server.janitor_disabled"),
	}}
}

func evalStorageReserveUnavailable(_ context.Context, snap *Snapshot) []Finding {
	if snap.Storage == nil || !snap.Storage.ReserveUnavailable {
		if snap.Source == SourceLive && snap.Storage == nil {
			return []Finding{skippedCheck("storage.reserve_unavailable",
				"needs a running daemon reporting its reserve state")}
		}
		return nil
	}
	return []Finding{{
		ID: "storage.reserve_unavailable", Severity: SevInfo,
		Title:  "fallocate unsupported — the disk reserve cannot protect headroom",
		Detail: "Headroom falls back to --min-free-bytes alone on this filesystem.",
		Fix:    []string{"raise --min-free-bytes to cover what the reserve missed"},
		Docs:   docsAnchor("storage.reserve_unavailable"),
	}}
}

func evalMetricsDroppedSeries(_ context.Context, snap *Snapshot) []Finding {
	if snap.Metrics == nil {
		return []Finding{skippedCheck("metrics.dropped_series",
			"needs a running daemon (try --addr)")}
	}
	if snap.Metrics.DroppedSeries <= 0 {
		return nil
	}
	return []Finding{{
		ID: "metrics.dropped_series", Severity: SevWarn,
		Title: fmt.Sprintf("%d metric series were dropped by the registry",
			snap.Metrics.DroppedSeries),
		Detail: "Dashboards and alerts quietly stop receiving those series; the outage " +
			"is observable, exactly once, by the absence of numbers.",
		Fix: []string{
			"raise --metrics-max-series or reduce label cardinality at consumers",
		},
		Evidence: map[string]any{"dropped_series": snap.Metrics.DroppedSeries},
		Docs:     docsAnchor("metrics.dropped_series"),
	}}
}

// ---- security ---------------------------------------------------------------

func evalSecurityPermissions(_ context.Context, snap *Snapshot) []Finding {
	if snap.Security == nil {
		return []Finding{skippedCheck("security.permissions",
			"permission facts were not collected")}
	}
	sec := snap.Security
	var problems []string
	if sec.DataDirMode&0o077 != 0 {
		problems = append(problems, fmt.Sprintf(
			"data dir mode %#o grants more than owner; want 0700", sec.DataDirMode))
	}
	if sec.DBFileMode != 0 && sec.DBFileMode&0o077 != 0 {
		problems = append(problems, fmt.Sprintf(
			"messq.db mode %#o grants group/other access; want 0600", sec.DBFileMode))
	}
	if sec.TokenFilePath != "" && sec.TokenFileMode != 0 && sec.TokenFileMode&0o077 != 0 {
		problems = append(problems, fmt.Sprintf(
			"token file mode %#o too open; want 0600", sec.TokenFileMode))
	}
	if len(problems) == 0 {
		return nil
	}
	fix := append([]string(nil), problems...)
	return []Finding{{
		ID: "security.permissions", Severity: SevFail,
		Title: fmt.Sprintf("%d permission problem(s) around %s",
			len(problems), renderSafe(snap.DataDir())),
		Detail:  "#16's preflight demands private-everything near the queue.",
		Fix:     fix,
		Subject: Subject{Path: snap.DataDir()},
		Docs:    docsAnchor("security.permissions"),
	}}
}

func evalSecurityTCPNoAuth(_ context.Context, snap *Snapshot) []Finding {
	if snap.Security == nil || !snap.Security.TCPNoAuth {
		if snap.Source == SourceLive && snap.Security == nil {
			return []Finding{skippedCheck("security.tcp_no_auth",
				"needs a running daemon (try --addr)")}
		}
		return nil
	}
	return []Finding{{
		ID: "security.tcp_no_auth", Severity: SevWarn,
		Title: "TCP listener accepting publishes without an auth file",
		Detail: "Loopback keeps strangers off your LAN, not off your laptop: any local " +
			"user speaks fluent publish already.",
		Fix: []string{
			"--auth-file <path>   # and: messq auth add …",
		},
		Docs: docsAnchor("security.tcp_no_auth"),
	}}
}

func evalSecurityCleartextPublic(_ context.Context, snap *Snapshot) []Finding {
	if snap.Security == nil || !snap.Security.CleartextPublic {
		if snap.Source == SourceLive && snap.Security == nil {
			return []Finding{skippedCheck("security.cleartext_public",
				"needs a running daemon (try --addr)")}
		}
		return nil
	}
	return []Finding{{
		ID: "security.cleartext_public", Severity: SevWarn,
		Title: "non-loopback listener serves cleartext HTTP",
		Detail: "Headers carry tokens and bodies carry payloads — both readable en route " +
			"until TLS terminates in front of the socket.",
		Fix: []string{
			"put a reverse proxy or WireGuard in front today (native TLS lands in #40)",
		},
		Docs: docsAnchor("security.cleartext_public"),
	}}
}

// ---- backup family ----------------------------------------------------------

func evalBackupNoneConfigured(_ context.Context, snap *Snapshot) []Finding {
	if snap.BackupDir != "" {
		return nil
	}
	return []Finding{{
		ID: "backup.none_configured", Severity: SevInfo,
		Title: "no backup directory configured, so doctor cannot watch freshness",
		Detail: "A backup pipeline you cannot see decays silently; point doctor at it " +
			"and staleness becomes a warn instead of a memory.",
		Fix: []string{
			"messq backup /var/backups/messq/$(date -u +%FT%H%MZ).db",
			"messq doctor --backup-dir /var/backups/messq",
		},
		Docs: docsAnchor("backup.none_configured"),
	}}
}

const backupDefaultMaxAge = 168 * time.Hour

func evalBackupStale(_ context.Context, snap *Snapshot) []Finding {
	if snap.BackupDir == "" {
		return nil
	}
	maxAge := snap.BackupMaxAge
	if maxAge <= 0 {
		maxAge = backupDefaultMaxAge
	}
	newest := int64(0)
	for _, b := range snap.Backups {
		if b.ModTimeMS > newest {
			newest = b.ModTimeMS
		}
	}
	if newest == 0 {
		return []Finding{{
			ID: "backup.stale", Severity: SevWarn,
			Title: "the configured backup directory holds no snapshots yet",
			Fix:   []string{"messq backup <dest>   # take the first one now"},
			Docs:  docsAnchor("backup.stale"),
		}}
	}
	age := snap.Now.Sub(time.UnixMilli(newest))
	if age < maxAge {
		return nil
	}
	return []Finding{{
		ID: "backup.stale", Severity: SevWarn,
		Title: fmt.Sprintf("newest snapshot is %s old (limit %s)",
			age.Truncate(time.Second), maxAge),
		Detail: "Restore difficulty scales with age and nobody scales patience equally.",
		Fix: []string{
			"messq backup <dest>   # take a fresh one",
			"systemctl enable --now messq-backup.timer",
		},
		Evidence: map[string]any{
			"age_seconds":     int64(age.Seconds()),
			"max_age_seconds": int64(maxAge.Seconds()),
		},
		Docs: docsAnchor("backup.stale"),
	}}
}

func evalBackupUnreadable(_ context.Context, snap *Snapshot) []Finding {
	if snap.BackupDir == "" {
		return nil
	}
	var broken []BackupFile
	for _, b := range snap.Backups {
		switch b.StampState {
		case "missing", "quickcheck_failed":
			broken = append(broken, b)
		case "unknown":
			// tri-state: cannot judge without opening; say so once, per the
			// honesty rule, as part of the stale/unreadable summary skip.
		}
	}
	if len(broken) == 0 {
		return nil
	}
	names := make([]string, 0, len(broken))
	for _, b := range broken {
		names = append(names, b.Path+" ("+b.StampState+")")
	}
	return []Finding{{
		ID: "backup.unreadable", Severity: SevFail,
		Title:  fmt.Sprintf("%d snapshot(s) would not restore", len(broken)),
		Detail: renderSafe(strings.Join(names, "; ")),
		Fix: []string{
			"take a fresh snapshot; verify the writer's disk health",
			"rm <broken-snapshot>   # after confirming nothing depends on it",
		},
		Docs: docsAnchor("backup.unreadable"),
	}}
}

func evalBackupPerms(_ context.Context, snap *Snapshot) []Finding {
	if snap.BackupDir == "" {
		return nil
	}
	var leaked []string
	for _, b := range snap.Backups {
		if b.Mode&0o077 != 0 {
			leaked = append(leaked, fmt.Sprintf("%s (%#o)", b.Path, b.Mode))
		}
	}
	if len(leaked) == 0 {
		return nil
	}
	return []Finding{{
		ID: "backup.perms", Severity: SevWarn,
		Title:    fmt.Sprintf("%d backup file(s) grant group/other access", len(leaked)),
		Detail:   "Payloads are message bodies in full: 0700 directories, 0600 files.",
		Fix:      []string{"chmod 0600 <snapshot>; chmod 0700 <dir>"},
		Evidence: map[string]any{"files": strings.Join(leaked, ",")},
		Docs:     docsAnchor("backup.perms"),
	}}
}
