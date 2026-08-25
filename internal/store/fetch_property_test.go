// SPDX-License-Identifier: Apache-2.0

//go:build rapid

package store

import (
	"context"
	"flag"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"pgregory.net/rapid"

	"github.com/a-holm/messq/internal/clock"
	"github.com/a-holm/messq/internal/queue"
)

// TestConsumerRapidDeliverySeed is the local rapid seed of #13's delivery suite (G11).
// It drives the delivery actions — publish, fetch, advance_time, pause, resume — against
// one consumer on a real store and, after every action, checks:
//
//  1. the C1–C6 invariants report no non-advisory violation, and
//  2. no matching message is ever skipped: every published sequence is either present
//     in the deliveries table (pending or inflight) or was returned by a fetch exactly
//     once — flow control must delay, never drop.
//
// The per-check action budget follows the central preset's nightly-depth contract
// (recovery_crash_test.go TestMain): the EFFECTIVE -rapid.steps value scales this
// machine ×10 — the preset's steps=6 yields 60 local actions per check — so an explicit
// nightly -rapid.steps deepens every check exactly as -rapid.checks multiplies how many
// fresh machines run. The former hardcoded 200 made -rapid.steps a dead knob for this,
// one of the package's heaviest property machines, and helped pin the suite against
// TEST_TIMEOUT.
//
// The whole file lives behind //go:build rapid: real-store, real-fsync property actions
// are nightly-depth work, so the default go test of the PR lane excludes it entirely —
// keeping the store suite inside TEST_TIMEOUT with margin — while -tags rapid compiles
// it back in at whatever depth -rapid.checks/-rapid.steps name.
func TestConsumerRapidDeliverySeed(t *testing.T) {
	stepsRaw := flag.Lookup("rapid.steps")
	if stepsRaw == nil {
		t.Fatal("rapid.steps flag is not registered")
	}
	steps, stepsErr := strconv.Atoi(stepsRaw.Value.String())
	if stepsErr != nil || steps <= 0 {
		t.Fatalf("rapid.steps = %q, want a positive integer", stepsRaw.Value.String())
	}
	actionsPerCheck := steps * 10

	rapid.Check(t, func(t *rapid.T) {
		ctx := context.Background()
		//nolint:usetesting // rapid.T predates testing.TB's TempDir and has no equivalent
		dir, mkErr := os.MkdirTemp("", "messq-consumer-prop-")
		if mkErr != nil {
			t.Fatalf("temp dir: %v", mkErr)
		}
		t.Cleanup(func() {
			if rmErr := os.RemoveAll(dir); rmErr != nil {
				t.Errorf("cleanup %s: %v", dir, rmErr)
			}
		})
		cl := queue.DefaultConsumerLimits()
		cl.ScanLimit = 4 // small scan window so flow control and multi-fetch paths engage
		st, _, err := Open(ctx, Options{
			DataDir:        filepath.Join(dir, "data"),
			Clock:          fakeClock(),
			Logger:         discardLoggerStore(),
			ConsumerLimits: cl,
		})
		if err != nil {
			t.Fatalf("open: %v", err)
		}
		t.Cleanup(func() {
			if cerr := st.Close(ctx); cerr != nil {
				t.Errorf("close: %v", cerr)
			}
		})
		if _, _, err := st.CreateStream(ctx, queue.DefaultConfig("orders"), "test"); err != nil {
			t.Fatalf("create stream: %v", err)
		}
		cfg := queue.DefaultConsumerConfig("worker")
		cfg.Filters = []string{">"}
		cfg.MaxAckPending = 3
		if _, err := st.CreateConsumer(ctx, "orders", cfg, queue.StartPosition{Kind: queue.StartFirst}, "test"); err != nil {
			t.Fatalf("create consumer: %v", err)
		}

		claimed := map[int64]int{} // seq -> times returned
		nextSeq := int64(0)        // our own publish counter, tracks the stream head

		// The store's fake clock, for advance_time.
		fk, ok := st.clk.(*clock.Fake)
		if !ok {
			t.Fatalf("store clock is not *clock.Fake")
		}

		for i := 0; i < actionsPerCheck; i++ {
			switch rapid.IntRange(0, 5).Draw(t, "action") {
			case 0, 1: // publish
				nextSeq++
				if _, pErr := st.Publish(ctx, PublishCmd{
					Stream: "orders",
					Req:    queue.PublishReq{Subject: "orders.1", Body: []byte("x")},
				}); pErr != nil {
					t.Fatalf("publish: %v", pErr)
				}
			case 2: // fetch
				res, fErr := st.Fetch(ctx, FetchReq{
					Stream: "orders", Consumer: "worker",
					Batch: rapid.IntRange(1, 5).Draw(t, "batch"),
				})
				if fErr != nil {
					t.Fatalf("fetch: %v", fErr)
				}
				for _, m := range res.Messages {
					claimed[m.Seq]++
				}
			case 3: // advance time (lets deadlines age; no-op in this issue)
				fk.Advance(time.Duration(rapid.IntRange(1, 10_000).Draw(t, "ms")) * time.Millisecond)
			case 4: // pause
				if _, pErr := st.SetPaused(ctx, "orders", "worker", true, "test"); pErr != nil {
					t.Fatalf("pause: %v", pErr)
				}
			case 5: // resume
				if _, pErr := st.SetPaused(ctx, "orders", "worker", false, "test"); pErr != nil {
					t.Fatalf("resume: %v", pErr)
				}
			}

			// 1. C1–C6 clean (advisory C5 is allowed).
			vs, cErr := st.CheckConsumerInvariants(ctx)
			if cErr != nil {
				t.Fatalf("check invariants: %v", cErr)
			}
			for _, v := range vs {
				if !v.Advisory {
					t.Fatalf("action %d left violation %+v", i, v)
				}
			}
		}

		// 2. No matching message skipped below the cursor: every published seq the
		// cursor has already passed must be either pending or returned exactly once.
		// Seqs at or above the cursor are legitimately held back by flow control (there
		// is no ack in this issue to drain the pending set), so they are not "skipped".
		info, gErr := st.GetConsumer(ctx, "orders", "worker")
		if gErr != nil {
			t.Fatalf("get consumer: %v", gErr)
		}
		pending := map[int64]bool{}
		rows, qErr := st.RO().QueryContext(ctx, `SELECT seq FROM deliveries WHERE stream='orders' AND consumer='worker'`)
		if qErr != nil {
			t.Fatalf("read deliveries: %v", qErr)
		}
		defer func() {
			if cerr := rows.Close(); cerr != nil {
				t.Errorf("close deliveries: %v", cerr)
			}
		}()
		for rows.Next() {
			var seq int64
			if sErr := rows.Scan(&seq); sErr != nil {
				t.Fatalf("scan: %v", sErr)
			}
			pending[seq] = true
		}
		if rErr := rows.Err(); rErr != nil {
			t.Fatalf("iterate: %v", rErr)
		}
		for seq := int64(1); seq < info.CursorSeq && seq <= nextSeq; seq++ {
			if pending[seq] {
				continue // still in flight or ready: not skipped
			}
			switch claimed[seq] {
			case 1:
				continue // returned exactly once and settled: not skipped
			case 0:
				t.Fatalf("seq %d (below cursor %d) was skipped: neither pending nor returned",
					seq, info.CursorSeq)
			default:
				t.Fatalf("seq %d was returned %d times, want at most once", seq, claimed[seq])
			}
		}
	})
}
