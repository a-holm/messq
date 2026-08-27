// SPDX-License-Identifier: Apache-2.0

package doctor

import (
	"context"
	"strings"
	"testing"
	"time"
)

// Regression: cancel() used to run BEFORE the expiry read, so every healthy
// check that produced zero findings was branded "timed out after 2s". The CLI
// end-to-end run caught it; this pins it at unit level.
func TestRunChecksHealthyEmptyCheckEmitsNothing(t *testing.T) {
	r := NewRegistry()
	r.Register(Check{
		ID: "healthy.quiet", Summary: "fine",
		Eval: func(context.Context, *Snapshot) []Finding { return nil },
	})
	findings := RunChecks(context.Background(), r, &Snapshot{})
	if len(findings) != 0 {
		t.Fatalf("healthy empty check produced findings: %+v", findings)
	}
}

func TestRunChecksWholeRunCancelSkipsRemainder(t *testing.T) {
	r := NewRegistry()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	r.Register(Check{
		ID: "slow.blocked", Summary: "blocks", Budget: 50 * time.Millisecond,
		Eval: func(cctx context.Context, _ *Snapshot) []Finding {
			cancel() // the operator hits ^C while this check is mid-flight
			<-cctx.Done()
			return nil
		},
	})
	r.Register(Check{
		ID: "later.never", Summary: "after a cancelled whole-run budget",
		Eval: func(context.Context, *Snapshot) []Finding { return nil },
	})

	findings := RunChecks(ctx, r, &Snapshot{})
	var blocked Finding
	for _, f := range findings {
		if f.ID == "slow.blocked" {
			blocked = f
		}
	}
	if blocked.Severity != SevSkipped || blocked.ID == "" {
		t.Fatalf("whole-run expiry should still yield an honest skip: %+v", findings)
	}
	if !strings.Contains(blocked.Detail, "run window closed") {
		t.Fatalf("skip detail should explain the closed window: %q", blocked.Detail)
	}
	for _, f := range findings {
		if f.ID == "later.never" {
			t.Fatal("a check started after window close leaked a verdict")
		}
	}
}

func TestFilterRegistryExactAndFamilyPatterns(t *testing.T) {
	src := NewRegistry()
	for _, id := range []string{
		"consumer.max_deliver_unlimited", "stream.no_consumers",
		"stream.typo_suspect", "server.restored",
	} {
		src.Register(Check{
			ID: id, Summary: "s",
			Eval: func(context.Context, *Snapshot) []Finding { return nil },
		})
	}

	got, unknown := FilterRegistry(src, []string{"stream.*"}, nil)
	if len(unknown) != 0 {
		t.Fatalf("family pattern flagged unknown: %v", unknown)
	}
	if ids := got.List(); len(ids) != 2 ||
		!contains(ids, "stream.no_consumers") || !contains(ids, "stream.typo_suspect") {
		t.Fatalf("family --only kept %v, want exactly the stream family", ids)
	}

	// skip wins over only.
	got, _ = FilterRegistry(src, []string{"stream.*"}, []string{"stream.typo_suspect"})
	if !contains(got.List(), "stream.no_consumers") ||
		contains(got.List(), "stream.typo_suspect") {
		t.Fatalf("skip did not prune inside only-family: %v", got.List())
	}
}

func TestFilterRegistryRefusesUnknownIDs(t *testing.T) {
	src := NewRegistry()
	src.Register(Check{
		ID: "a.real", Summary: "s",
		Eval: func(context.Context, *Snapshot) []Finding { return nil },
	})

	_, unknown := FilterRegistry(src, []string{"a.reel", "b.missing"}, nil)
	if len(unknown) != 2 {
		t.Fatalf("unknown = %v, want both typos returned", unknown)
	}
	// A family glob matching nothing is a typo too.
	if _, unknown = FilterRegistry(src, nil, []string{"stream.*"}); len(unknown) == 0 {
		t.Fatal("unknown family pattern passed silently")
	}
}

func contains(haystack []string, needle string) bool {
	for _, s := range haystack {
		if s == needle {
			return true
		}
	}
	return false
}

func TestOfflineCollectFillsStorageAndDurabilityFacts(t *testing.T) {
	dir := seedDoctorStore(t)
	snap, err := OfflineCollector{DataDir: dir}.Collect(context.Background())
	if err != nil {
		t.Fatalf("Collect: %v", err)
	}
	if snap.Storage == nil {
		t.Fatal("storage facts missing from offline snapshot")
	}
	if snap.Storage.DBBytes <= 0 || snap.Storage.FreeBytes <= 0 {
		t.Fatalf("disk numbers empty: %+v", snap.Storage)
	}
	if snap.Storage.FsName == "" {
		t.Fatalf("filesystem name not derived: %+v", snap.Storage)
	}
	if snap.Durability == nil || !snap.Durability.OwnConnection {
		t.Fatalf("own-connection durability facts missing: %+v", snap.Durability)
	}
	if snap.Durability.Synchronous < 0 || snap.Durability.Synchronous > 3 {
		t.Fatalf("synchronous out of SQLite range: %+v", snap.Durability)
	}
}
