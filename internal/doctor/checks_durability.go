// SPDX-License-Identifier: Apache-2.0

package doctor

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"golang.org/x/sys/unix"
)

// StorageFacts is what doctor knows about the disk under the data directory.
// Fields stay pointers-of-presence by collection: a collector that could not
// measure something leaves the fact zeroed and the consuming check degrades.
type StorageFacts struct {
	DataDir string
	// FsName is the human filesystem name derived from statfs magic: ext4,
	// xfs, tmpfs, nfs, cifs, fuse, overlay ...
	FsName string
	// OnTmpfs mirrors magic == tmpfs so fsync findings can trust fast numbers.
	OnTmpfs bool
	// MountOptions lists the mount point's options (/proc/mounts order).
	MountOptions []string

	TotalBytes int64
	FreeBytes  int64

	DBBytes  int64 // messq.db size
	WALBytes int64 // messq.db-wal size (0 when absent)
	// EventsBytes measures the events table's share when measurable;
	// ShareKnown marks measurability because 0 bytes is a real answer.
	EventsBytes int64
	ShareKnown  bool

	// WALLastModifiedMS is messq.db-wal's mtime; feeds the long-reader
	// heuristic in live mode (a stopped daemon legitimately ages its WAL).
	WALLastModifiedMS int64

	// GrowthBytesPerDay is the measured database growth rate when available;
	// 0 means unknown (days-to-full then reports only the absolute floor).
	GrowthBytesPerDay int64

	// ReadonlyLatched records the D4 fsyncgate latch state; the latch owner
	// is #27's daemon runtime, which publishes this fact when it lands.
	ReadonlyLatched bool
	ReadonlyCause   string

	// ReserveUnavailable reports fallocate-unsupported (the reserve side of
	// #27); a live daemon publishes it, offline storage facts leave it false.
	ReserveUnavailable bool
}

// DurabilityFacts is the pragma story. Offline, Synchronous reads back the
// COLLECTOR'S OWN connection (OwnConnection=true) — honest labeling required
// by §6: that value proves nothing about the daemon's writer.
type DurabilityFacts struct {
	Mode          string // live: "full"|"relaxed"; offline: "" (unknown)
	Synchronous   int    // SQLite synchronous: 0 OFF, 1 NORMAL, 2 FULL
	OwnConnection bool
}

// FsyncFacts carries one fdatasync measurement run on the data dir.
type FsyncFacts struct {
	Samples           int64
	MedianUs          int64
	P50us             int64
	P99us             int64
	P999us            int64
	DurableMsgsPerSec float64
}

func init() {
	defaultRegistry.Register(Check{
		ID:      "durability.pragma",
		Summary: "what the synchronous pragma actually read back",
		Explain: "A publish that answered 201 survives power loss only when SQLite's " +
			"synchronous pragma matches the durability mode. Doctor reads it back " +
			"instead of trusting configuration. Offline it can only report its own " +
			"connection — which is labeled as such, because that value cannot prove " +
			"anything about the daemon's writer.",
		Needs: SourceEither,
		Eval:  evalDurabilityPragma,
	})
	defaultRegistry.Register(Check{
		ID:      "durability.mode_relaxed",
		Summary: "--durability=relaxed trades crash safety for latency",
		Explain: "Relaxed lets a crash between the OS page cache and disk lose recent " +
			"publishes (D4). That can be a deliberate choice for a dev box; doctor " +
			"names it loudly precisely so nobody inherits it unknowingly.",
		Needs: SourceLive,
		Eval:  evalDurabilityModeRelaxed,
	})
	defaultRegistry.Register(Check{
		ID:      "durability.fsync_probe",
		Summary: "measures fdatasync latency and projects durable msg/s",
		Explain: "PLAN §12 promises re-baselined numbers nobody has to take on faith: " +
			"doctor writes 4 KiB blocks plus fdatasync against the data dir and prints " +
			"p50/p99/p99.9 together with the achievable commit rate at the observed " +
			"batch size. --fsync-samples 0 turns the probe off.",
		Needs: SourceDataDir,
		Eval:  evalDurabilityFsyncProbe,
	})
	defaultRegistry.Register(Check{
		ID:      "durability.fsync_implausible",
		Summary: "an fdatasync faster than RAM suggests lying hardware",
		Explain: "Real spinning disks and NAND cannot acknowledge an fdatasync in " +
			"under ~25 µs; a median below that means a VM write-back cache, a " +
			"'nobarrier' mount or consumer SSD flush-simulating firmware is lying. " +
			"The cost arrives during the power cut you took the setting for.",
		Needs: SourceDataDir,
		Eval:  evalDurabilityFsyncImplausible,
	})
	defaultRegistry.Register(Check{
		ID:      "durability.tmpfs",
		Summary: "the data dir lives on tmpfs while durability demands otherwise",
		Explain: "tmpfs is RAM: a reboot IS a power loss. Under --durability=full the " +
			"durability promise is void no matter what pragmas say — move the data dir " +
			"to a real filesystem.",
		Needs: SourceEither,
		Eval:  evalDurabilityTmpfs,
	})
}

// evalDurabilityPragma labels its own epistemology: live verdicts judge the
// daemon; offline verdicts only describe doctor's own connection.
func evalDurabilityPragma(_ context.Context, snap *Snapshot) []Finding {
	if snap.Durability == nil {
		return []Finding{skippedCheck("durability.pragma",
			"pragma state was not collected")}
	}
	d := snap.Durability
	if d.OwnConnection || d.Mode == "" {
		sev := SevOK
		verdict := "survives power loss: YES"
		if d.Synchronous < 2 {
			sev = SevInfo
			verdict = fmt.Sprintf(
				"but note this only describes this process's connection (%d), "+
					"not proof about any daemon's writer", d.Synchronous)
		}
		return []Finding{{
			ID: "durability.pragma", Severity: sev,
			Title: fmt.Sprintf("synchronous read back as %s on MY OWN connection",
				synchronousName(d.Synchronous)),
			Detail: fmt.Sprintf("Offline doctor opens its own read-only handle, so the "+
				"value above is about that handle: %s. This says nothing about any "+
				"daemon's writer connection.", verdict),
			NoFix: "this is informational; start the daemon for a judged verdict",
			Evidence: map[string]any{
				"synchronous": d.Synchronous, "source": "own_connection",
			},
			Docs: docsAnchor("durability.pragma"),
		}}
	}
	if d.Mode == "full" && d.Synchronous < 2 {
		return []Finding{{
			ID: "durability.pragma", Severity: SevFail,
			Title: fmt.Sprintf("--durability=full but synchronous read back as %s",
				synchronousName(d.Synchronous)),
			Detail: "A 201 from publish does NOT survive power loss in this state.",
			Fix: []string{
				"restart the daemon so the pragma set reapplies",
				"messq doctor --addr <addr>   # re-read the live pragma after restart",
			},
			Evidence: map[string]any{
				"synchronous": d.Synchronous, "mode": d.Mode,
			},
			Docs: docsAnchor("durability.pragma"),
		}}
	}
	return []Finding{{
		ID: "durability.pragma", Severity: SevOK,
		Title: fmt.Sprintf("%s — synchronous read back as FULL on a live pooled connection",
			modeSentence(d.Mode)),
		Detail: "A 201 from publish survives power loss: YES",
		NoFix:  "informational — this is the healthy path",
		Evidence: map[string]any{
			"synchronous": d.Synchronous, "mode": d.Mode,
		},
		Docs: docsAnchor("durability.pragma"),
	}}
}

func synchronousName(v int) string {
	switch v {
	case 0:
		return "OFF"
	case 1:
		return "NORMAL"
	case 2:
		return "FULL"
	default:
		return fmt.Sprintf("%d", v)
	}
}

func modeSentence(mode string) string {
	if mode == "" {
		return "durability"
	}
	return "durability=" + mode
}

func evalDurabilityModeRelaxed(_ context.Context, snap *Snapshot) []Finding {
	if snap.Source != SourceLive || snap.Durability == nil ||
		snap.Durability.Mode != "relaxed" {
		if snap.Source == SourceLive && (snap.Durability == nil || snap.Durability.Mode == "") {
			return []Finding{skippedCheck("durability.mode_relaxed",
				"needs a running daemon (try --addr)")}
		}
		return nil
	}
	return []Finding{{
		ID: "durability.mode_relaxed", Severity: SevWarn,
		Title: "--durability=relaxed accepts losing recent publishes on crash",
		Detail: "Fine for throwaway environments; a lie for anything you would mourn. " +
			"What it buys is latency; what it costs is the tail of every ack window.",
		Fix:      []string{"restart the daemon with --durability=full"},
		Evidence: map[string]any{"mode": snap.Durability.Mode},
		Docs:     docsAnchor("durability.mode_relaxed"),
	}}
}

func evalDurabilityFsyncProbe(_ context.Context, snap *Snapshot) []Finding {
	if snap.Fsync == nil {
		return []Finding{skippedCheck("durability.fsync_probe",
			"needs a writable data dir (run offline with --data-dir, or as the daemon user)")}
	}
	f := snap.Fsync
	_ = f
	return []Finding{{
		ID: "durability.fsync_probe", Severity: SevOK,
		Title: fmt.Sprintf("fdatasync p50 %dµs p99 %dµs p99.9 %dµs (%d samples)",
			f.P50us, f.P99us, f.P999us, f.Samples),
		Detail: fmt.Sprintf("projected ≈ %.0f durable msg/s at the observed commit batch",
			f.DurableMsgsPerSec),
		NoFix: "informational — these are YOUR disk's numbers, nobody has to trust our README",
		Evidence: map[string]any{
			"samples": f.Samples, "p50_us": f.P50us, "p99_us": f.P99us,
			"p999_us": f.P999us, "durable_msgs_per_sec": f.DurableMsgsPerSec,
		},
		Docs: docsAnchor("durability.fsync_probe"),
	}}
}

// implausibleMedianUs sits below every spinning disk and NAND answer recorded
// in PLAN §12's own baselines.
const implausibleMedianUs = 25

func evalDurabilityFsyncImplausible(_ context.Context, snap *Snapshot) []Finding {
	if snap.Fsync == nil {
		return []Finding{skippedCheck("durability.fsync_implausible",
			"no fdatasync measurement available")}
	}
	if snap.Storage == nil {
		return []Finding{skippedCheck("durability.fsync_implausible",
			"filesystem type unknown, so plausibility cannot be judged")}
	}
	f := snap.Fsync
	if f.MedianUs >= implausibleMedianUs || snap.Storage.OnTmpfs {
		return nil // tmpfs IS ram: a fast answer there is honest
	}
	return []Finding{{
		ID: "durability.fsync_implausible", Severity: SevWarn,
		Title: fmt.Sprintf("median fdatasync of %dµs is implausibly fast on %s",
			f.MedianUs, renderSafe(snap.Storage.FsName)),
		Detail: "Something between the kernel and the platters is acknowledging " +
			"before it should: VM write-back cache, nobarrier mount, or SSD firmware.",
		Fix: []string{
			"check the VM/disk for write-back caching and barrier disabling",
			"re-run messq doctor --data-dir <dir> after fixing the layer",
		},
		Evidence: map[string]any{"median_us": f.MedianUs},
		Docs:     docsAnchor("durability.fsync_implausible"),
	}}
}

func evalDurabilityTmpfs(_ context.Context, snap *Snapshot) []Finding {
	if snap.Storage == nil {
		return []Finding{skippedCheck("durability.tmpfs",
			"filesystem type was not collected")}
	}
	if snap.Durability == nil || snap.Durability.Mode != "full" {
		return nil // mode unknown: judge nothing rather than nag everyone
	}
	if !snap.Storage.OnTmpfs {
		return nil
	}
	return []Finding{{
		ID: "durability.tmpfs", Severity: SevFail,
		Title: "the data directory lives on tmpfs under --durability=full",
		Detail: "RAM holds the pages: reboot, OOM eviction or container teardown " +
			"loses the tail exactly like pulling the power cord.",
		Fix: []string{
			"move --data-dir to a persistent filesystem and restart",
			"messq doctor --data-dir <new-dir>   # confirm the fs type changed",
		},
		Subject: Subject{Path: snap.Storage.DataDir},
		Docs:    docsAnchor("durability.tmpfs"),
	}}
}

// collectStorageFacts fills the disk-side facts the OfflineCollector can get
// without holding any lock: file sizes, statfs identity and free space.
func collectStorageFacts(dir string) (*StorageFacts, error) {
	f := &StorageFacts{DataDir: dir}
	var st unix.Statfs_t
	if err := unix.Statfs(dir, &st); err == nil {
		// Kernel block counts fit in int64 by construction (see ops/backup's
		// FreeBytes for the same concession); a real filesystem cannot return
		// counts that overflow, so gosec's G115 row is silenced with reason.
		f.TotalBytes = int64(st.Blocks) * int64(st.Bsize) //nolint:gosec // G115: kernel counts fit in int64
		f.FreeBytes = int64(st.Bavail) * int64(st.Bsize)  //nolint:gosec // G115: kernel counts fit in int64
		f.FsName = fsMagicName(st.Type)
		f.OnTmpfs = st.Type == unix.TMPFS_MAGIC
	} else {
		return nil, fmt.Errorf("statfs %s: %w", dir, err)
	}
	dbInfo, err := os.Stat(filepath.Join(dir, "messq.db"))
	switch {
	case err == nil:
		f.DBBytes = dbInfo.Size()
	case os.IsNotExist(err):
		return nil, fmt.Errorf("%s holds no messq.db: %w", dir, err)
	default:
		return nil, fmt.Errorf("stat messq.db: %w", err)
	}
	if wal, wErr := os.Stat(filepath.Join(dir, "messq.db-wal")); wErr == nil {
		f.WALBytes = wal.Size()
		f.WALLastModifiedMS = wal.ModTime().UnixMilli()
	}
	return f, nil
}

// fsMagicName maps the kernels' statfs magics onto the names operators type;
// the dangerous ones drive storage.fs_type.
func fsMagicName(magic int64) string {
	switch magic {
	case unix.TMPFS_MAGIC:
		return "tmpfs"
	case 0xEF53:
		return "ext4"
	case unix.XFS_SUPER_MAGIC:
		return "xfs"
	case unix.NFS_SUPER_MAGIC:
		return "nfs"
	case unix.CIFS_SUPER_MAGIC:
		return "cifs"
	case unix.FUSE_SUPER_MAGIC:
		return "fuse"
	case unix.OVERLAYFS_SUPER_MAGIC:
		return "overlay"
	case unix.BTRFS_SUPER_MAGIC:
		return "btrfs"
	default:
		return fmt.Sprintf("fs(%#x)", magic)
	}
}

var _ = time.Second
