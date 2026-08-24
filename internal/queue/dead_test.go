// SPDX-License-Identifier: Apache-2.0

package queue

import (
	"strings"
	"testing"
)

// The pure DLQ planner tests (issue #12 slice 1): PlanDead's policy matrix, the
// DLQ naming helpers' round-trip property, and the copy budget's
// decrement-before-write contract.

func deadCtx() DeadCtx {
	return DeadCtx{
		Stream: "orders", Consumer: "worker", Subject: "orders.created",
		Seq: 10493, MsgID: "01J8ZQ4K2M9V0X7Y3B5N6C8D1E", TraceID: "4bf92f3577b34da6a3ce929d0e0e4736",
		Attempts: 5, Generation: 1, MaxDeliver: 5,
		Cause: DeadCauseMaxDeliver, Trigger: DeadTriggerAckWait,
		Policy: DeadPolicyDLQ, LastReason: "upstream 503",
	}
}

// TestPlanDeadMatrix pins the policy x cause decision: dlq on a plain stream copies into
// the derived stream under the original subject/trace; drop copies nothing.
// Red direction: a mutant that copies on a drop policy would be caught, and a mutant
// that routes a dlq death to anything but "<stream>.dlq" would be caught by the name.
func TestPlanDeadMatrix(t *testing.T) {
	cfg := DefaultDLQConfig(DefaultLimits())
	base := deadCtx()
	for _, tc := range []struct {
		name        string
		policy      DeadPolicy
		wantCopy    bool
		wantDLQ     string
		wantSubject string
		wantTrace   string
	}{
		{"dlq_copies", DeadPolicyDLQ, true, "orders.dlq", "orders.created", "4bf92f3577b34da6a3ce929d0e0e4736"},
		{"drop_no_copy", DeadPolicyDrop, false, "", "orders.created", "4bf92f3577b34da6a3ce929d0e0e4736"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			d := base
			d.Policy = tc.policy
			p := PlanDead(d, cfg)
			if p.Copy != tc.wantCopy {
				t.Fatalf("Copy = %v, want %v", p.Copy, tc.wantCopy)
			}
			if p.DLQStream != tc.wantDLQ {
				t.Fatalf("DLQStream = %q, want %q", p.DLQStream, tc.wantDLQ)
			}
			if p.Subject != tc.wantSubject {
				t.Fatalf("Subject = %q, want %q (copy stays under the original subject)", p.Subject, tc.wantSubject)
			}
			if p.TraceID != tc.wantTrace {
				t.Fatalf("TraceID = %q, want %q (preserved byte-for-byte, S4.4)", p.TraceID, tc.wantTrace)
			}
		})
	}
}

// TestDLQNameHelpers round-trips the naming contract: derive, detect, unwind.
func TestDLQNameHelpers(t *testing.T) {
	for _, s := range []string{"orders", "a", "very.long.stream.name.with.many.segments", "abc 123"} {
		dlq := DLQName(s)
		if !IsDLQ(dlq) {
			t.Fatalf("IsDLQ(%q) = false, want true", dlq)
		}
		orig, ok := OriginOf(dlq)
		if !ok || orig != s {
			t.Fatalf("OriginOf(%q) = %q,%v, want %q,true", dlq, orig, ok, s)
		}
	}
	if IsDLQ("orders") {
		t.Fatal("IsDLQ(\"orders\") = true, want false")
	}
	if _, ok := OriginOf("orders"); ok {
		t.Fatal("OriginOf(\"orders\") = _,true, want _,false")
	}
	if !strings.HasSuffix(DLQName("x"), DLQSuffix) {
		t.Fatal("DLQName does not carry the suffix")
	}
}

// TestDeadBudgetContract pins decrement-before-write: CanCopy never mutates (two asks
// give the same answer), and a take past zero is impossible by construction because CanCopy
// refuses first.
func TestDeadBudgetContract(t *testing.T) {
	b := &DeadBudget{Copies: 2, Bytes: 100}
	for i := 0; i < 2; i++ {
		if !b.CanCopy(50) {
			t.Fatalf("CanCopy(50) must be true on ask %d (no mutation on can)", i+1)
		}
	}
	b.Take(50)
	for i := 0; i < 2; i++ {
		if !b.CanCopy(50) {
			t.Fatalf("after take 50, CanCopy(50) must still be true on ask %d", i+1)
		}
	}
	b.Take(50)
	if b.CanCopy(50) {
		t.Fatal("CanCopy after 100/100 bytes must be false")
	}
	if b.CanCopy(1) {
		t.Fatal("CanCopy with 0 copies must be false")
	}
	if b.Copies != 0 || b.Bytes != 0 {
		t.Fatalf("budget after exhaustion = %+v, want exhausted copy=0 bytes=0", b)
	}
}

// TestNewDeadBudgetFromConfig seeds the per-commit defaults.
func TestNewDeadBudgetFromConfig(t *testing.T) {
	b := NewDeadBudget(DefaultDLQConfig(DefaultLimits()))
	if b.Copies != 128 || b.Bytes != 32<<20 {
		t.Fatalf("default budget = copies=%d bytes=%d, want 128/32MiB", b.Copies, b.Bytes)
	}
}
