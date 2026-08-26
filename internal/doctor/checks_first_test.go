// SPDX-License-Identifier: Apache-2.0

package doctor

import (
	"context"
	"strings"
	"testing"
	"time"
)

// mustFire asserts one expected finding exists with the right severity and a
// well-formed shape (Docs anchor + Fix-or-NoFix), per the registry contract.
func mustFire(t *testing.T, findings []Finding, id string, sev Severity) Finding {
	t.Helper()
	for _, f := range findings {
		if f.ID == id {
			if f.Severity != sev {
				t.Fatalf("finding %s severity = %v, want %v", id, f.Severity, sev)
			}
			if !strings.HasPrefix(f.Docs, "docs/doctor.md#") {
				t.Fatalf("finding %s docs anchor %q", id, f.Docs)
			}
			if len(f.Fix) == 0 && f.NoFix == "" {
				t.Fatalf("finding %s has neither Fix nor NoFix", id)
			}
			return f
		}
	}
	t.Fatalf("finding %s missing from %+v", id, findings)
	return Finding{}
}

func mustNotFire(t *testing.T, findings []Finding, id string) {
	t.Helper()
	for _, f := range findings {
		if f.ID == id {
			t.Fatalf("finding %s fired but must not: %+v", id, f)
		}
	}
}

func evalCheck(t *testing.T, id string, snap *Snapshot) []Finding {
	t.Helper()
	return defaultRegistry.mustGet(id).Eval(context.Background(), snap)
}

func TestConsumerMaxDeliverUnlimited(t *testing.T) {
	base := func(maxDeliver int32, policy string) *Snapshot {
		return &Snapshot{
			Consumers: []ConsumerState{{
				Stream: "orders", Name: "invoices",
				MaxDeliver: maxDeliver, DeadPolicy: policy,
			}},
		}
	}

	findings := evalCheck(t, "consumer.max_deliver_unlimited", base(0, "dlq"))
	mustFire(t, findings, "consumer.max_deliver_unlimited", SevWarn)

	mustNotFire(t, evalCheck(t, "consumer.max_deliver_unlimited", base(5, "dlq")),
		"consumer.max_deliver_unlimited")

	// Combined case escalates to fail.
	combined := evalCheck(t, "consumer.max_deliver_unlimited", base(0, "drop"))
	mustFire(t, combined, "consumer.max_deliver_unlimited_no_dlq", SevFail)

	// A bounded consumer with drop does not fire either check.
	bounded := evalCheck(t, "consumer.max_deliver_unlimited", base(5, "drop"))
	mustNotFire(t, bounded, "consumer.max_deliver_unlimited")
	mustNotFire(t, bounded, "consumer.max_deliver_unlimited_no_dlq")
}

func TestServerRestored(t *testing.T) {
	restored := &Snapshot{
		Restored: &RestoredProvenance{
			SnapshotAtMS: time.Date(2026, 11, 4, 2, 0, 11, 0, time.UTC).UnixMilli(),
			SourceNodeID: "01HTEST",
			StreamHeads:  map[string]int64{"orders": 1057201},
		},
	}
	f := mustFire(t, evalCheck(t, "server.restored", restored), "server.restored", SevInfo)
	if f.Evidence["snapshot_at_ms"] == nil {
		t.Fatalf("restored finding evidence %+v lacks snapshot_at_ms", f.Evidence)
	}
	mustNotFire(t, evalCheck(t, "server.restored", &Snapshot{}), "server.restored")
}

func TestStreamNoConsumers(t *testing.T) {
	now := time.Date(2026, 11, 11, 2, 0, 0, 0, time.UTC)
	old := now.Add(-8 * 24 * time.Hour).UnixMilli()
	fresh := now.Add(-1 * 24 * time.Hour).UnixMilli()

	snap := func(msgs int64, createdMS int64, consumers int) *Snapshot {
		s := &Snapshot{Now: now, Streams: []StreamState{{Name: "quiet", Msgs: msgs, CreatedAtMS: createdMS}}}
		for i := 0; i < consumers; i++ {
			snapConsumers := ConsumerState{Stream: "quiet", Name: string(rune('a' + i))}
			s.Consumers = append(s.Consumers, snapConsumers)
		}
		return s
	}

	mustFire(t, evalCheck(t, "stream.no_consumers", snap(50, old, 0)),
		"stream.no_consumers", SevInfo)
	mustNotFire(t, evalCheck(t, "stream.no_consumers", snap(50, fresh, 0)),
		"stream.no_consumers") // younger than 7d: give it time
	mustNotFire(t, evalCheck(t, "stream.no_consumers", snap(50, old, 1)),
		"stream.no_consumers") // someone consumes it
	mustNotFire(t, evalCheck(t, "stream.no_consumers", snap(0, old, 0)),
		"stream.no_consumers") // empty stream: nothing to lose
}

func TestStreamTypoSuspect(t *testing.T) {
	now := time.Date(2026, 11, 11, 2, 0, 0, 0, time.UTC)
	young := now.Add(-2 * 24 * time.Hour).UnixMilli()

	snapWith := func(name string, msgs int64, createdMS int64, consumers int) *Snapshot {
		s := &Snapshot{
			Now: now,
			Streams: []StreamState{
				{Name: name, Msgs: msgs, CreatedAtMS: createdMS},
				{Name: "orders", Msgs: 1000, CreatedAtMS: young},
			},
		}
		for i := 0; i < consumers; i++ {
			s.Consumers = append(s.Consumers,
				ConsumerState{Stream: name, Name: string(rune('a' + i))})
		}
		return s
	}

	// "ordres" sits at edit distance 1 from "orders": young, tiny, unconsumed.
	f := mustFire(t, evalCheck(t, "stream.typo_suspect", snapWith("ordres", 2, young, 0)),
		"stream.typo_suspect", SevInfo)
	if !strings.Contains(f.Detail, "orders") {
		t.Fatalf("typo detail %q should name the likely intended stream", f.Detail)
	}

	// Too many messages to be a typo.
	mustNotFire(t, evalCheck(t, "stream.typo_suspect", snapWith("ordres", 11, young, 0)),
		"stream.typo_suspect")

	// Old enough that it looks deliberate.
	old := now.Add(-9 * 24 * time.Hour).UnixMilli()
	mustNotFire(t, evalCheck(t, "stream.typo_suspect", snapWith("ordres", 2, old, 0)),
		"stream.typo_suspect")

	// Has consumers: it is used, whatever its name.
	mustNotFire(t, evalCheck(t, "stream.typo_suspect", snapWith("ordres", 2, young, 1)),
		"stream.typo_suspect")

	// Distance 3 from every other stream: not a typo candidate.
	mustNotFire(t, evalCheck(t, "stream.typo_suspect", snapWith("zzzzzz", 2, young, 0)),
		"stream.typo_suspect")
}

func TestLevenshteinDistance(t *testing.T) {
	tests := []struct {
		a, b string
		want int
	}{
		{"orders", "orders", 0},
		{"ordres", "orders", 2}, // transposition = 1 insert + 1 delete under plain Levenshtein
		{"order", "orders", 1},
		{"cat", "cart", 1},
		{"zzz", "orders", 6},
	}
	for _, tt := range tests {
		if got := levenshtein(tt.a, tt.b); got != tt.want {
			t.Fatalf("levenshtein(%q,%q) = %d, want %d", tt.a, tt.b, got, tt.want)
		}
	}
}
