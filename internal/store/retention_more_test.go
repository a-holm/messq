// SPDX-License-Identifier: Apache-2.0

package store

import (
	"context"
	"testing"
)

// TestRetentionSweepMoreFlagConverges drives the batch bound hard: every call may
// delete at most Batch rows, and Result.More must be true exactly while a SATURATED
// window still leaves a violated limit behind — never forever once enforcement has
// drained all it can (the byte pass always keeps one message).
func TestRetentionSweepMoreFlagConverges(t *testing.T) {
	ctx := context.Background()
	st, _ := openWithStore(t)
	defer func() {
		if cerr := st.Close(ctx); cerr != nil {
			t.Logf("close store: %v", cerr)
		}
	}()

	cfg := limitsConfig("orders", 0, 50) // byte pressure far beyond what stays possible
	if _, _, err := st.CreateStream(ctx, cfg, "test"); err != nil {
		t.Fatalf("create: %v", err)
	}
	publishOrders(t, st, "orders", 8)

	moreCalls := 0
	for i := 0; i < 12; i++ {
		res, err := st.Retention(ctx, RetentionCmd{Batch: 3})
		if err != nil {
			t.Fatalf("Retention call %d: %v", i, err)
		}
		if res.Deleted == 0 && res.More {
			t.Fatalf("call %d reports More with zero progress", i)
		}
		if res.More {
			moreCalls++
		}
		if !res.More {
			break
		}
	}
	if moreCalls == 0 {
		t.Fatal("a batch-bound saturated sweep never reported More")
	}
	// Terminal state under byte pressure with 16-byte messages: 50-byte ceiling keeps
	// three messages (48 bytes); a two-message tail leaves that boundary honestly
	// unsaturated, so More turned false the moment enforcement finished draining.
	s := statsOf(t, st, "orders")
	if s.msgs != 3 || s.bytes != 3*fakeSize {
		t.Fatalf("terminal stats = %+v, want msgs=3 bytes=%d", s, 3*fakeSize)
	}
}
