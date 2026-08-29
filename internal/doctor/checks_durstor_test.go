// SPDX-License-Identifier: Apache-2.0

package doctor

import (
	"strings"
	"testing"
	"time"
)

func TestDurabilityPragmaLiveFullBelowFull(t *testing.T) {
	live := func(sync int) *Snapshot {
		return &Snapshot{
			Source: SourceLive,
			Durability: &DurabilityFacts{
				Mode:        "full",
				Synchronous: sync,
			},
		}
	}
	f := mustFire(t, evalCheck(t, "durability.pragma", live(1)),
		"durability.pragma", SevFail)
	if !containsEvidenceKey(f.Evidence, "synchronous") {
		t.Fatalf("evidence must carry the numeric pragma: %+v", f.Evidence)
	}
	healthy := mustFire(t, evalCheck(t, "durability.pragma", live(2)), "durability.pragma", SevOK)
	if !strings.Contains(healthy.Detail, "YES") {
		t.Fatalf("healthy pragma should answer the power-loss question: %+v", healthy)
	}
}

func TestDurabilityPragmaOfflineIsHonestNotFatal(t *testing.T) {
	// Own-connection FULL cannot prove anything about the daemon's writer,
	// so the finding stays informational and says which one it did.
	snap := &Snapshot{
		Source:     SourceDataDir,
		Durability: &DurabilityFacts{Synchronous: 2, OwnConnection: true},
	}
	f := mustFire(t, evalCheck(t, "durability.pragma", snap), "durability.pragma", SevOK)
	if !strings.Contains(f.Detail, "own read-only handle") {
		t.Fatalf("offline verdict must label its source of truth: %q", f.Detail)
	}
	// A sub-FULL own connection is still only an honest observation.
	snap.Durability.Synchronous = 1
	mustFire(t, evalCheck(t, "durability.pragma", snap), "durability.pragma", SevInfo)

	// Facts entirely absent: skip with a reason, never a guess.
	res := evalCheck(t, "durability.pragma", &Snapshot{})
	if len(res) != 1 || res[0].Severity != SevSkipped {
		t.Fatalf("missing durability facts should skip, got %+v", res)
	}
}

func TestDurabilityModeRelaxed(t *testing.T) {
	live := &Snapshot{
		Source:     SourceLive,
		Durability: &DurabilityFacts{Mode: "relaxed"},
	}
	w := mustFire(t, evalCheck(t, "durability.mode_relaxed", live),
		"durability.mode_relaxed", SevWarn)
	if !anyFixContains(w.Fix, "--durability=full") {
		t.Fatalf("fix must name the mode switch: %v", w.Fix)
	}
	mustNotFire(t, evalCheck(t, "durability.mode_relaxed", &Snapshot{
		Source:     SourceLive,
		Durability: &DurabilityFacts{Mode: "full"},
	}), "durability.mode_relaxed")
}

func TestDurabilityFsyncProbeInformationalAndSkips(t *testing.T) {
	withProbe := &Snapshot{
		Fsync: &FsyncFacts{
			Samples: 1000, P50us: 78, P99us: 240, P999us: 1100,
			MedianUs: 78, DurableMsgsPerSec: 5900,
		},
	}
	ok := mustFire(t, evalCheck(t, "durability.fsync_probe", withProbe),
		"durability.fsync_probe", SevOK)
	if ok.NoFix == "" {
		t.Fatal("the probe is informational; it needs a NoFix sentence")
	}
	res := evalCheck(t, "durability.fsync_probe", &Snapshot{})
	if len(res) != 1 || res[0].Severity != SevSkipped {
		t.Fatalf("no probe data should skip, got %+v", res)
	}
}

func TestDurabilityFsyncImplausibleBoundaryAt25Micros(t *testing.T) {
	plausible := func(med int64, tmpfs bool) *Snapshot {
		return &Snapshot{
			Storage: &StorageFacts{OnTmpfs: tmpfs},
			Fsync:   &FsyncFacts{MedianUs: med},
		}
	}
	mustNotFire(t, evalCheck(t, "durability.fsync_implausible", plausible(25, false)),
		"durability.fsync_implausible")
	mustFire(t, evalCheck(t, "durability.fsync_implausible", plausible(24, false)),
		"durability.fsync_implausible", SevWarn)
	// tmpfs IS ram: fast is honest there.
	mustNotFire(t, evalCheck(t, "durability.fsync_implausible", plausible(10, true)),
		"durability.fsync_implausible")
}

func TestDurabilityTmpfsUnderFull(t *testing.T) {
	full := &Snapshot{
		Source:     SourceDataDir,
		Storage:    &StorageFacts{OnTmpfs: true},
		Durability: &DurabilityFacts{Mode: "full"},
	}
	mustFire(t, evalCheck(t, "durability.tmpfs", full), "durability.tmpfs", SevFail)
	mustNotFire(t, evalCheck(t, "durability.tmpfs", &Snapshot{
		Storage: &StorageFacts{OnTmpfs: true},
	}), "durability.tmpfs") // unknown mode: judge nothing
}

func TestStorageFsTypeMagicMatrix(t *testing.T) {
	cases := []struct {
		name   string
		fsName string
		want   Severity
	}{
		{"nfs lies about flock", "nfs", SevFail},
		{"cifs lies too", "cifs", SevFail},
		{"fuse layers a fuse", "fuse", SevFail},
		{"overlayfs docker cliché", "overlay", SevFail},
		{"ext4 healthy", "ext4", SevOK},
		{"tmpfs handled by its own check", "tmpfs", SevOK},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			snap := &Snapshot{Storage: &StorageFacts{FsName: tc.fsName}}
			findings := evalCheck(t, "storage.fs_type", snap)
			if tc.want == SevFail {
				mustFire(t, findings, "storage.fs_type", tc.want)
				return
			}
			ok := mustFire(t, findings, "storage.fs_type", tc.want)
			if ok.NoFix == "" {
				t.Fatalf("healthy fs verdict should be informational: %+v", ok)
			}
		})
	}
	res := evalCheck(t, "storage.fs_type", &Snapshot{})
	if len(res) != 1 || res[0].Severity != SevSkipped {
		t.Fatalf("missing storage facts should skip: %+v", res)
	}
}

func TestStorageMountOptionsBarrierWords(t *testing.T) {
	fire := func(opts ...string) *Snapshot {
		return &Snapshot{Storage: &StorageFacts{MountOptions: opts}}
	}
	for _, opts := range [][]string{
		{"rw", "nobarrier"}, {"rw", "barrier=0"}, {"commit=999", "nobarrier"},
	} {
		w := mustFire(t, evalCheck(t, "storage.mount_options", fire(opts...)),
			"storage.mount_options", SevWarn)
		if !anyFixContains(w.Fix, "remount") {
			t.Fatalf("fix should suggest remounting: %v", w.Fix)
		}
	}
	mustNotFire(t, evalCheck(t, "storage.mount_options",
		fire("rw", "relatime")), "storage.mount_options")
}

func TestStorageDiskHeadroomBoundaries(t *testing.T) {
	const GiB = 1 << 30
	base := func(free int64) *Snapshot {
		return &Snapshot{
			Storage: &StorageFacts{
				FreeBytes: free, TotalBytes: 100 * GiB,
				GrowthBytesPerDay: 10 * GiB,
			},
		}
	}
	// min-free default 0: absolute branch idle; days-to-full drives here.
	// 40 GiB / 10 GiB-per-day = 4 days remaining.
	w := mustFire(t, evalCheck(t, "storage.disk_headroom", base(GiB)),
		"storage.disk_headroom", SevWarn)
	if !containsEvidenceKey(w.Evidence, "days_to_full") {
		t.Fatalf("ETA must be evidence: %v", w.Evidence)
	}
	// Zero growth makes days-to-full infinite: absolute threshold only.
	calm := &Snapshot{Storage: &StorageFacts{FreeBytes: GiB, TotalBytes: 100 * GiB}}
	if res := evalCheck(t, "storage.disk_headroom", calm); len(res) > 0 &&
		res[0].Severity != SevSkipped && res[0].ID == "storage.disk_headroom" &&
		res[0].Severity >= SevWarn {
		t.Fatalf("no growth and big free space fired wrongly: %+v", res)
	}
	// A free-space absolute floor below 4×min-free fires regardless.
	knobbed := base(8 * GiB)
	knobbed.MinFreeBytes = 4 * GiB
	mustFire(t, evalCheck(t, "storage.disk_headroom", knobbed),
		"storage.disk_headroom", SevWarn) // 8 GiB < 4×4GiB... sees 16GiB line
}

func TestStorageWALSizeBoundary(t *testing.T) {
	knob := &Snapshot{Storage: &StorageFacts{
		DataDir: "/var/lib/messq", WALBytes: 512 << 20,
	}}
	knob.WalMaxBytes = 512 << 20
	mustNotFire(t, evalCheck(t, "storage.wal_size", knob), "storage.wal_size")

	over := &Snapshot{Storage: &StorageFacts{
		DataDir: "/var/lib/messq", WALBytes: 1 << 30,
		WALLastModifiedMS: time.Date(2026, 11, 4, 12, 0, 0, 0, time.UTC).UnixMilli(),
	}}
	over.WalMaxBytes = 512 << 20
	over.Now = time.Date(2026, 11, 4, 12, 15, 0, 0, time.UTC) // 15m untouched
	w := mustFire(t, evalCheck(t, "storage.wal_size", over), "storage.wal_size", SevWarn)
	if w.Subject.Path == "" {
		t.Fatalf("wal finding should name what it measured: %+v", w.Subject)
	}
}

func TestStorageEventsShareAtFourtyPercent(t *testing.T) {
	atThreshold := func(events int64) *Snapshot {
		return &Snapshot{Storage: &StorageFacts{
			DBBytes: 1000, EventsBytes: events, ShareKnown: true,
		}}
	}
	// 400/1000 == 40% == not fired (strictly greater fires).
	mustNotFire(t, evalCheck(t, "storage.events_share", atThreshold(400)),
		"storage.events_share")
	w := mustFire(t, evalCheck(t, "storage.events_share", atThreshold(500)),
		"storage.events_share", SevWarn)
	if !anyFixContains(w.Fix, "--event-retention") && !anyFixContains(w.Fix, "--event-max-rows") {
		t.Fatalf("fix should name retention levers: %v", w.Fix)
	}
}

func TestStorageReadonlyLatchHoistsAboveEverything(t *testing.T) {
	latched := &Snapshot{Storage: &StorageFacts{
		ReadonlyLatched: true, ReadonlyCause: "fdatasync failed on journal commit",
	}}
	f := mustFire(t, evalCheck(t, "storage.readonly_latch", latched),
		"storage.readonly_latch", SevFail)
	if f.ID != hoistID {
		t.Fatalf("latch id drifted out of the renderer's hoist constant")
	}
	if !strings.Contains(latched.Storage.ReadonlyCause, "sync") {
		t.Fatalf("cause should ride along: %q", latched.Storage.ReadonlyCause)
	}
}

// containsEvidenceKey asserts a numeric-evidence contract survived edits.
func containsEvidenceKey(ev map[string]any, key string) bool {
	_, ok := ev[key]
	return ok
}

// anyFixContains greps the fix commands for a suggested lever.
func anyFixContains(fix []string, needle string) bool {
	for _, f := range fix {
		if strings.Contains(f, needle) {
			return true
		}
	}
	return false
}
