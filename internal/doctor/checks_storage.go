// SPDX-License-Identifier: Apache-2.0

package doctor

import (
	"context"
	"fmt"
	"strings"
	"time"
)

func init() {
	defaultRegistry.Register(Check{
		ID:      "storage.fs_type",
		Summary: "names the filesystem under the data directory and refuses liars",
		Explain: "flock on NFS/CIFS/FUSE/overlay is a cooperative fiction (#5): two " +
			"processes can each believe they hold it. On those filesystems messq's " +
			"data-directory locking cannot do its job — move the data dir to a local " +
			"filesystem before trusting single-writer guarantees.",
		Needs: SourceEither,
		Eval:  evalStorageFsType,
	})
	defaultRegistry.Register(Check{
		ID:      "storage.mount_options",
		Summary: "nobarrier/barrier=0 void every durability claim on this mount",
		Explain: "Mount options are where lying hardware hides in plain sight. A " +
			"'nobarrier' or 'barrier=0' tells the kernel writes need not hit stable " +
			"storage before acking — exactly what durability=full assumed true.",
		Needs: SourceEither,
		Eval:  evalStorageMountOptions,
	})
	defaultRegistry.Register(Check{
		ID:      "storage.disk_headroom",
		Summary: "free space versus minimum headroom and growth rate",
		Explain: "Doctor computes days-to-full from the observed growth rate when it " +
			"is known and always checks the absolute floor at four times --min-free-bytes. " +
			"The ETA rides the evidence so cron logs become graphs for free.",
		Needs: SourceEither,
		Eval:  evalStorageDiskHeadroom,
	})
	defaultRegistry.Register(Check{
		ID:      "storage.wal_size",
		Summary: "WAL size above budget, or a reader pinning checkpoints too long",
		Explain: "A long-running read transaction (often a backup) pins the WAL tail: " +
			"checkpoints stall and the file grows with write volume. This is a cost, " +
			"not corruption — doctor names who is holding it.",
		Needs: SourceEither,
		Eval:  evalStorageWALSizeAndPinned,
	})
	defaultRegistry.Register(Check{
		ID:      "storage.events_share",
		Summary: "the events table eating more than its share of the database",
		Explain: "Events are the audit trail (§8), but past ~40% of database bytes they " +
			"are retention policy failure wearing a tux. Raise --event-retention's teeth " +
			"or lower the row cap; #27 trims for you once configured.",
		Needs: SourceEither,
		Eval:  evalStorageEventsShare,
	})
	defaultRegistry.Register(Check{
		ID:      "storage.readonly_latch",
		Summary: "the daemon latched itself read-only after an fsyncgate event",
		Explain: "After fdatasync stopped being trustworthy the daemon refused writes " +
			"(D4) rather than lie about durability. This is a page, not a chore: clear " +
			"the storage fault first, restart, and only then look at anything else.",
		Needs: SourceEither,
		Eval:  evalStorageReadonlyLatch,
	})
}

// evalStorageFsType judges from statfs magic: the flock-liars fail outright,
// everything else reports healthy as information.
func evalStorageFsType(_ context.Context, snap *Snapshot) []Finding {
	if snap.Storage == nil || snap.Storage.FsName == "" {
		return []Finding{skippedCheck("storage.fs_type",
			"filesystem identity was not collected")}
	}
	st := snap.Storage
	if isFlockLiar(st.FsName) {
		return []Finding{{
			ID: "storage.fs_type", Severity: SevFail,
			Title:    fmt.Sprintf("data dir sits on %s — flock does not protect it there", st.FsName),
			Detail:   "Two processes can hold the lock simultaneously across this layer; messq's single-writer guarantee rests on that lock (#5).",
			Fix:      []string{"move --data-dir to a local filesystem and restart"},
			Subject:  Subject{Path: st.DataDir},
			Evidence: map[string]any{"fs": st.FsName},
			Docs:     docsAnchor("storage.fs_type"),
		}}
	}
	return []Finding{{
		ID: "storage.fs_type", Severity: SevOK,
		Title:    fmt.Sprintf("%s filesystem — flock semantics hold", st.FsName),
		NoFix:    "informational — nothing to fix here",
		Evidence: map[string]any{"fs": st.FsName},
		Docs:     docsAnchor("storage.fs_type"),
	}}
}

func isFlockLiar(fs string) bool {
	switch strings.ToLower(fs) {
	case "nfs", "cifs", "fuse", "overlay":
		return true
	default:
		return false
	}
}

func evalStorageMountOptions(_ context.Context, snap *Snapshot) []Finding {
	if snap.Storage == nil {
		return []Finding{skippedCheck("storage.mount_options",
			"mount options were not collected")}
	}
	for _, opt := range snap.Storage.MountOptions {
		if opt == "nobarrier" || opt == "barrier=0" {
			return []Finding{{
				ID: "storage.mount_options", Severity: SevWarn,
				Title: fmt.Sprintf("mount option %q voids the durability claim", opt),
				Detail: "The mount itself says barriers can be skipped; no pragma set " +
					"can outrank that.",
				Fix: []string{
					fmt.Sprintf("remount without %q", opt),
					"messq doctor --data-dir <dir>   # verify the option is gone",
				},
				Subject:  Subject{Path: snap.Storage.DataDir},
				Evidence: map[string]any{"mount_options": strings.Join(snap.Storage.MountOptions, ",")},
				Docs:     docsAnchor("storage.mount_options"),
			}}
		}
	}
	return nil
}

// daysFloorWarnDays is doctor's conservative ceiling while no operator-facing
// max-age knob exists yet (#27 owns the retention setting).
const daysFloorWarnDays = 14

func evalStorageDiskHeadroom(_ context.Context, snap *Snapshot) []Finding {
	if snap.Storage == nil {
		return []Finding{skippedCheck("storage.disk_headroom",
			"free-space facts were not collected")}
	}
	st := snap.Storage
	if st.TotalBytes <= 0 && st.FreeBytes <= 0 {
		return []Finding{skippedCheck("storage.disk_headroom",
			"statfs numbers unavailable")}
	}
	minFree := snap.MinFreeBytes

	switch {
	case minFree > 0 && st.FreeBytes < minFree:
		return []Finding{{
			ID: "storage.disk_headroom", Severity: SevFail,
			Title: fmt.Sprintf("only %s free — below the configured minimum of %s",
				humanBytes(st.FreeBytes), humanBytes(minFree)),
			Detail: "Below the floor you set: the next WAL spike is unbooked.",
			Fix: []string{
				"raise retention levers or add disk space",
				"messq doctor --data-dir <dir>   # confirm numbers moved",
			},
			Subject: Subject{Path: st.DataDir},
			Evidence: map[string]any{
				"free_bytes": st.FreeBytes, "min_free_bytes": minFree,
			},
			Docs: docsAnchor("storage.disk_headroom"),
		}}
	case minFree > 0 && st.FreeBytes < minFree*4:
		return headroomWarn(st, map[string]any{
			"free_bytes": st.FreeBytes, "min_free_bytes": minFree,
		})
	}

	if st.GrowthBytesPerDay > 0 {
		daysToFull := int64(st.FreeBytes / st.GrowthBytesPerDay)
		ev := map[string]any{
			"free_bytes": st.FreeBytes, "growth_bytes_per_day": st.GrowthBytesPerDay,
			"days_to_full": daysToFull,
		}
		if daysToFull < daysFloorWarnDays {
			f := headroomWarn(st, ev)[0]
			return []Finding{f}
		}
		return []Finding{{
			ID: "storage.disk_headroom", Severity: SevOK,
			Title: fmt.Sprintf("%s free, growing ~%s/day → %d days at this rate",
				humanBytes(st.FreeBytes), humanBytes(st.GrowthBytesPerDay), daysToFull),
			NoFix:    "informational — ETA recomputed on every run",
			Evidence: ev,
			Docs:     docsAnchor("storage.disk_headroom"),
		}}
	}
	return nil
}

func headroomWarn(st *StorageFacts, ev map[string]any) []Finding {
	days, hasETA := ev["days_to_full"].(int64)
	extra := ""
	if hasETA && days > 0 {
		extra = fmt.Sprintf(" — about %d days left at this rate", days)
	}
	return []Finding{{
		ID: "storage.disk_headroom", Severity: SevWarn,
		Title: fmt.Sprintf("free space below headroom target%s", extra),
		Detail: "Warnings at 4× your --min-free-bytes exist so actual action stays " +
			"unhurried when the floor trips later.",
		Fix: []string{
			"raise --min-free-bytes only if you have reconsidered",
			"add disk space or lower retention",
		},
		Subject:  Subject{Path: st.DataDir},
		Evidence: ev,
		Docs:     docsAnchor("storage.disk_headroom"),
	}}
}

// pinnedCheckpointFloorMS is §7's ">10 min" for a stalled checkpoint.
const pinnedCheckpointFloorMS = 10 * 60 * int64(timeSecond)

const timeSecond = 1000 // ms, named so the const block reads like prose

func evalStorageWALSize(_ context.Context, snap *Snapshot) []Finding {
	if snap.Storage == nil {
		return []Finding{skippedCheck("storage.wal_size",
			"WAL facts were not collected")}
	}
	st := snap.Storage

	var fired []Finding
	if snap.WalMaxBytes > 0 && st.WALBytes > snap.WalMaxBytes {
		fired = append(fired, Finding{
			ID: "storage.wal_size", Severity: SevWarn,
			Title: fmt.Sprintf("WAL is %s, over the %s budget",
				humanBytes(st.WALBytes), humanBytes(snap.WalMaxBytes)),
			Detail:  "Beyond the budget the next crash replay costs proportionally more.",
			Fix:     []string{"run a checkpoint by restarting the daemon cleanly, then re-measure"},
			Subject: Subject{Path: st.DataDir + "/messq.db-wal"},
			Evidence: map[string]any{
				"wal_bytes": st.WALBytes, "wal_max_bytes": snap.WalMaxBytes,
			},
			Docs: docsAnchor("storage.wal_size"),
		})
	}
	return fired
}

// evalStorageWALPinned is the live-only branch of the WAL story: a -wal whose
// mtime ages while a daemon runs means checkpoints are pinned by a reader.
func evalStorageWALPinned(_ context.Context, snap *Snapshot) []Finding {
	if snap.Storage == nil || snap.Source != SourceLive ||
		snap.Storage.WALLastModifiedMS == 0 || snap.Storage.WALBytes == 0 {
		return nil
	}
	idle := snap.Now.Sub(unixMilliToTime(snap.Storage.WALLastModifiedMS))
	if idle <= time.Duration(pinnedCheckpointFloorMS)*time.Millisecond {
		return nil
	}
	return []Finding{{
		ID: "storage.wal_size", Severity: SevWarn,
		Title: fmt.Sprintf("WAL untouched for %s while the daemon runs — checkpoints look pinned",
			idle.Truncate(time.Second)),
		Detail: "A long read transaction (often a backup connection) holds the tail; " +
			"the file grows with every write until it exits.",
		Fix: []string{
			"find the long reader (backup, peek with held conn) and end it",
			"messq trace <msg-id>   # if you suspect a stuck worker",
		},
		Subject: Subject{Path: snap.Storage.DataDir + "/messq.db-wal"},
		Evidence: map[string]any{
			"idle_seconds": int64(idle.Seconds()), "wal_bytes": snap.Storage.WALBytes,
		},
		Docs: docsAnchor("storage.wal_size"),
	}}
}

// evalStorageWALSizeAndPinned emits both WAL findings in family order.
func evalStorageWALSizeAndPinned(ctx context.Context, snap *Snapshot) []Finding {
	out := evalStorageWALSize(ctx, snap)
	return append(out, evalStorageWALPinned(ctx, snap)...)
}

func evalStorageEventsShare(_ context.Context, snap *Snapshot) []Finding {
	if snap.Storage == nil || !snap.Storage.ShareKnown {
		return []Finding{skippedCheck("storage.events_share",
			"events-table size was not measured")}
	}
	st := snap.Storage
	if st.DBBytes <= 0 {
		return nil
	}
	pct := st.EventsBytes * 100 / st.DBBytes
	if pct <= 40 {
		return nil
	}
	return []Finding{{
		ID: "storage.events_share", Severity: SevWarn,
		Title: fmt.Sprintf("events take %d%% of database bytes (%d%%+ is policy failure)",
			int(pct), 40),
		Detail: "Trim the audit trail before it trims your headroom: retention and " +
			"row caps both feed the janitor.",
		Fix: []string{
			"--event-retention <shorter>",
			"--event-max-rows <cap>",
		},
		Subject: Subject{Path: st.DataDir},
		Evidence: map[string]any{
			"events_bytes": st.EventsBytes, "db_bytes": st.DBBytes, "share_pct": int(pct),
		},
		Docs: docsAnchor("storage.events_share"),
	}}
}

func evalStorageReadonlyLatch(_ context.Context, snap *Snapshot) []Finding {
	if snap.Storage == nil {
		return []Finding{skippedCheck("storage.readonly_latch",
			"latch state requires collection doctor did not run")}
	}
	st := snap.Storage
	if !st.ReadonlyLatched {
		return nil
	}
	cause := st.ReadonlyCause
	if cause == "" {
		cause = "unknown storage fault"
	}
	return []Finding{{
		ID: hoistID, Severity: SevFail,
		Title: "the write path is LATCHED READ-ONLY",
		Detail: renderSafe(cause) +
			" — publishes will refuse until the fault clears and the daemon restarts.",
		Fix: []string{
			"clear the underlying storage fault (fsync failures, ENOSPC, EIO)",
			"restart the daemon to release the latch",
			"journalctl -u messq   # confirm the latch reason matches the fault",
		},
		Subject:  Subject{Path: st.DataDir},
		Evidence: map[string]any{"latched": true},
		Docs:     docsAnchor(hoistID),
	}}
}

func unixMilliToTime(ms int64) time.Time { return time.UnixMilli(ms) }
