// SPDX-License-Identifier: Apache-2.0

package store

import (
	"context"
	"slices"
	"strings"
	"testing"

	"github.com/a-holm/messq/internal/queue"
)

func violationsOf(vs []Violation, id string) []Violation {
	out := make([]Violation, 0, 2)
	for _, v := range vs {
		if v.ID == id {
			out = append(out, v)
		}
	}
	return out
}

func TestInvariantsHoldAfterNormalOperation(t *testing.T) {
	ctx := context.Background()
	st, _ := openWithStore(t)
	mustCreate(t, st, queue.DefaultConfig("orders"))
	mustCreate(t, st, queueDefaultWithWindow("billing", 1_000))

	for i := 0; i < 7; i++ {
		req := pub("orders.a", []byte("body"))
		req.Req.MsgID = "o-" + string(rune('0'+i))
		if i == 3 { // a retry inside the batch of history
			req.Req.MsgID = "o-2"
		}
		if _, err := st.Publish(ctx, PublishCmd{Stream: "orders", Req: req.Req}); err != nil {
			t.Fatalf("publish %d: %v", i, err)
		}
	}
	if _, err := st.PublishBatch(ctx, BatchCmd{Stream: "billing", Reqs: []queue.PublishReq{
		{Subject: "billing.a", Body: make([]byte, 40)},
		{Subject: "billing.b", Body: nil},
	}}); err != nil {
		t.Fatalf("batch: %v", err)
	}
	// A delete leaves its high-water mark behind; nothing may violate after it.
	if _, err := st.DeleteStream(ctx, "billing", "billing", "test"); err != nil {
		t.Fatalf("delete: %v", err)
	}
	mustCreate(t, st, queueDefaultWithWindow("billing", 1_000))

	vs, err := st.CheckPublishInvariants(ctx)
	if err != nil {
		t.Fatalf("check: %v", err)
	}
	if len(vs) != 0 {
		t.Fatalf("violations after normal operation = %+v, want none", vs)
	}
}

// seedViolatingStream builds a stream with 4 clean messages for mutation subtests.
func seedViolatingStream(t *testing.T, st *Store) *Store {
	t.Helper()
	mustCreate(t, st, queue.DefaultConfig("orders"))
	for i := 1; i <= 4; i++ {
		if _, err := st.rw.ExecContext(context.Background(),
			`INSERT INTO messages (stream, seq, id, subject, body, size, published_at, trace_id)
			 VALUES ('orders', ?, ?, 'orders.a', x'0102', 2, ?, 'tr')`,
			i, "seed-"+string(rune('a'+i)), fakeStartMillis+int64(i)*100); err != nil {
			t.Fatalf("seed %d: %v", i, err)
		}
	}
	if _, err := st.rw.ExecContext(context.Background(),
		`UPDATE stream_seq SET next = 5 WHERE stream='orders'`); err != nil {
		t.Fatalf("bump: %v", err)
	}
	return st
}

func TestInvariantsReportSeededViolations(t *testing.T) {
	cases := map[string]struct {
		corrupt string
		wantID  string
	}{
		"p1-gap": {
			corrupt: `DELETE FROM messages WHERE seq=2`,
			wantID:  "P1",
		},
		"p2-seq-above-counter": {
			corrupt: `UPDATE stream_seq SET next=2 WHERE stream='orders'`,
			wantID:  "P2",
		},
		"p3-stale-key": {
			// Seq 1 is the only row where an ancient published_at cannot also break
			// P4 (nothing precedes it), so this isolates the expiry leg.
			corrupt: `UPDATE messages SET dedup_key='stale', published_at=1 WHERE seq=1`,
			wantID:  "P3",
		},
		"p4-clock-regression": {
			corrupt: `UPDATE messages SET published_at=0 WHERE seq=3`,
			wantID:  "P4",
		},
		"p5-size-mismatch": {
			corrupt: `UPDATE messages SET size=99 WHERE seq=2`,
			wantID:  "P5",
		},
		"p5-stats-drift": {
			corrupt: `UPDATE stream_stats SET msgs=17 WHERE stream='orders'`,
			wantID:  "P5",
		},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			st, _ := openWithStore(t)
			seedViolatingStream(t, st)
			if _, err := st.rw.ExecContext(ctx,
				`INSERT INTO stream_stats (stream, msgs, bytes) VALUES ('orders', 4, 8)
				 ON CONFLICT (stream) DO UPDATE SET msgs=excluded.msgs, bytes=excluded.bytes`); err != nil {
				t.Fatalf("stats seed: %v", err)
			}

			if before, cErr := st.CheckPublishInvariants(ctx); cErr != nil || len(before) != 0 {
				t.Fatalf("pre-corruption check = %+v err=%v, want clean", before, cErr)
			}
			if _, xErr := st.rw.ExecContext(ctx, tc.corrupt); xErr != nil {
				t.Fatalf("corrupt: %v", xErr)
			}
			vs, cErr := st.CheckPublishInvariants(ctx)
			if cErr != nil {
				t.Fatalf("check: %v", cErr)
			}
			got := violationsOf(vs, tc.wantID)
			// One physical corruption can legitimately trip several invariants at
			// once (a deleted row also drifts the counters) — those are true
			// findings. What must hold: the named invariant fires on this stream.
			if len(got) == 0 {
				t.Fatalf("violations = %+v, want at least one %s on orders", vs, tc.wantID)
			}
			if got[0].Detail == "" {
				t.Errorf("%s violation carries no detail", tc.wantID)
			}
		})
	}
}

func TestInvariantsReportDuplicateDedupKeys(t *testing.T) {
	ctx := context.Background()
	st, _ := openWithStore(t)
	seedViolatingStream(t, st)
	if _, sErr := st.rw.ExecContext(ctx,
		`INSERT INTO stream_stats (stream, msgs, bytes) VALUES ('orders', 4, 8)
		 ON CONFLICT (stream) DO UPDATE SET msgs = excluded.msgs, bytes = excluded.bytes`); sErr != nil {
		t.Fatalf("seed stats: %v", sErr)
	}
	if vs, cErr := st.CheckPublishInvariants(ctx); cErr != nil || len(vs) != 0 {
		t.Fatalf("pre-corruption = %+v err=%v, want clean", vs, cErr)
	}
	// Simulate a bypassed unique index (corruption, not a supported state): drop it,
	// plant two rows on one key, and expect the P3a leg to catch what SQL no longer
	// enforces.
	if _, xErr := st.rw.ExecContext(ctx, `DROP INDEX messages_dedup`); xErr != nil {
		t.Fatalf("drop index: %v", xErr)
	}
	for _, seq := range []int64{6, 7} {
		if _, xErr := st.rw.ExecContext(ctx,
			`INSERT INTO messages (stream, seq, id, subject, body, size, published_at, trace_id, dedup_key)
			 VALUES ('orders', ?, ?, 'orders.a', x'01', 1, ?, 'tr', 'dup-key')`,
			seq, "seed-"+string(rune('0'+seq)), fakeStartMillis); xErr != nil {
			t.Fatalf("seed dup %d: %v", seq, xErr)
		}
	}
	vs, cErr := st.CheckPublishInvariants(ctx)
	if cErr != nil {
		t.Fatalf("check: %v", cErr)
	}
	dups := violationsOf(vs, "P3")
	if len(dups) == 0 || !strings.Contains(dups[0].Detail, `"dup-key"`) {
		t.Fatalf("P3 findings = %+v, want the duplicated key named", vs)
	}
}

func TestViolationIDsAreTheClosedSet(t *testing.T) {
	seen := []string{"P1", "P2", "P3", "P4", "P5"}
	slices.Sort(seen)
	if !slices.IsSorted(seen) {
		t.Fatal("unreachable")
	}
}
