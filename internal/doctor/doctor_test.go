// SPDX-License-Identifier: Apache-2.0

package doctor

import (
	"context"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/a-holm/messq/internal/clock"
	"github.com/a-holm/messq/internal/queue"
	"github.com/a-holm/messq/internal/store"
)

// seedDoctorStore builds a data dir with one stream and two consumers.
func seedDoctorStore(t *testing.T) string {
	t.Helper()
	dir := filepath.Join(t.TempDir(), "data")
	st, _, openErr := store.Open(context.Background(), store.Options{DataDir: dir})
	if openErr != nil {
		t.Fatalf("open: %v", openErr)
	}
	if _, _, crErr := st.CreateStream(context.Background(),
		queue.DefaultConfig("orders"), "orders"); crErr != nil {
		t.Fatalf("create stream: %v", crErr)
	}
	if _, pubErr := st.PublishBatch(context.Background(), store.BatchCmd{
		Stream: "orders",
		Reqs:   []queue.PublishReq{{Subject: "orders.created", Body: []byte("x")}},
	}); pubErr != nil {
		t.Fatalf("publish: %v", pubErr)
	}
	if _, crErr := st.CreateConsumer(context.Background(), "orders",
		queue.ConsumerConfig{
			Name:          "invoices",
			Filters:       []string{">"},
			AckWait:       30 * time.Second,
			MaxAckPending: 1000,
			Backoff:       []time.Duration{time.Second, 5 * time.Second},
			DeadPolicy:    queue.DeadPolicyDLQ,
			MaxDeliver:    5,
		},
		queue.StartPosition{Kind: queue.StartFirst}, "test"); crErr != nil {
		t.Fatalf("create consumer: %v", crErr)
	}
	if closeErr := st.Close(context.Background()); closeErr != nil {
		t.Fatalf("close: %v", closeErr)
	}
	return dir
}

func TestRegistryRejectsDuplicateIDs(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatal("registering a duplicate check ID did not panic; IDs are a greppable contract")
		}
	}()
	r := NewRegistry()
	r.Register(Check{ID: "x.dup", Summary: "first", Eval: func(context.Context, *Snapshot) []Finding { return nil }})
	r.Register(Check{ID: "x.dup", Summary: "second", Eval: func(context.Context, *Snapshot) []Finding { return nil }})
}

func TestRegistryListIsSortedAndExplainTeaches(t *testing.T) {
	r := NewRegistry()
	for _, c := range []Check{
		{ID: "b.check", Summary: "second", Explain: "second explanation"},
		{ID: "a.check", Summary: "first", Explain: "first explanation"},
	} {
		r.Register(c)
	}
	ids := r.List()
	if len(ids) != 2 || ids[0] != "a.check" || ids[1] != "b.check" {
		t.Fatalf("List() = %v, want sorted [a.check b.check]", ids)
	}
	explain, ok := r.Explain("b.check")
	if !ok || !strings.Contains(explain, "second explanation") {
		t.Fatalf("Explain(b.check) = %q,%v", explain, ok)
	}
	if _, ok := r.Explain("nope.check"); ok {
		t.Fatal("Explain of an unknown ID returned ok")
	}
}

func TestChecksArePureOverEmptySnapshot(t *testing.T) {
	// G6: every registered Eval must run against a bare Snapshot without
	// panicking or performing I/O — SevSkipped where data is missing.
	snap := &Snapshot{Now: clock.NewFake(time.Date(2026, 11, 4, 2, 0, 0, 0, time.UTC)).Now()}
	for _, id := range defaultRegistry.List() {
		check := defaultRegistry.mustGet(id)
		findings := check.Eval(context.Background(), snap)
		for _, f := range findings {
			if f.ID == "" {
				t.Fatalf("check %s emitted a finding with an empty ID", id)
			}
			switch f.Severity {
			case SevSkipped, SevOK, SevInfo, SevWarn, SevFail:
			default:
				t.Fatalf("check %s emitted severity %d", id, f.Severity)
			}
		}
	}
}

func TestOfflineCollectorReadsState(t *testing.T) {
	dir := seedDoctorStore(t)

	collector := OfflineCollector{DataDir: dir}
	snap, err := collector.Collect(context.Background())
	if err != nil {
		t.Fatalf("Collect: %v", err)
	}
	if snap.Source != SourceDataDir {
		t.Fatalf("snap.Source = %v, want SourceDataDir", snap.Source)
	}
	if len(snap.Streams) != 1 || snap.Streams[0].Name != "orders" || snap.Streams[0].Msgs != 1 {
		t.Fatalf("Streams = %+v, want orders with 1 message", snap.Streams)
	}
	if len(snap.Consumers) != 1 || snap.Consumers[0].Stream != "orders" ||
		snap.Consumers[0].Name != "invoices" {
		t.Fatalf("Consumers = %+v, want orders/invoices", snap.Consumers)
	}
	if snap.Consumers[0].MaxDeliver <= 0 || snap.Consumers[0].AckWaitMS == 0 {
		t.Fatalf("consumer facts incomplete: %+v", snap.Consumers[0])
	}
	if snap.Restored != nil {
		t.Fatalf("fresh dir reported provenance %+v", snap.Restored)
	}
}

func TestOfflineCollectorReportsProvenance(t *testing.T) {
	ctx := context.Background()
	dir := seedDoctorStore(t)

	st, _, openErr := store.Open(ctx, store.Options{DataDir: dir})
	if openErr != nil {
		t.Fatalf("reopen: %v", openErr)
	}
	if closeErr := st.Close(ctx); closeErr != nil {
		t.Fatalf("close: %v", closeErr)
	}

	collector := OfflineCollector{DataDir: dir}
	snap, err := collector.Collect(ctx)
	if err != nil {
		t.Fatalf("Collect: %v", err)
	}
	if snap.Restored != nil {
		t.Fatalf("unrestored dir reported provenance %+v", snap.Restored)
	}
}

func TestRunChecksAppliesBudgetAndSkip(t *testing.T) {
	r := NewRegistry()
	r.Register(Check{
		ID: "fast.ok", Summary: "fast",
		Eval: func(context.Context, *Snapshot) []Finding {
			return []Finding{{ID: "fast.ok", Severity: SevOK, Title: "fine"}}
		},
	})
	r.Register(Check{
		ID: "slow.skip", Summary: "slow", Budget: time.Millisecond,
		Eval: func(ctx context.Context, _ *Snapshot) []Finding {
			<-ctx.Done() // block until the budget cancels us
			return nil
		},
	})

	snap := &Snapshot{}
	findings := RunChecks(context.Background(), r, snap)
	byID := map[string]Finding{}
	for _, f := range findings {
		byID[f.ID] = f
	}
	if byID["fast.ok"].Severity != SevOK {
		t.Fatalf("fast.ok = %+v, want its own finding", byID["fast.ok"])
	}
	got := byID["slow.skip"]
	if got.Severity != SevSkipped {
		t.Fatalf("slow.skip = %+v, want SevSkipped on budget expiry", got)
	}
	if !strings.Contains(got.Detail, "timed out") {
		t.Fatalf("skip detail %q, want it to say why (timed out)", got.Detail)
	}
}
