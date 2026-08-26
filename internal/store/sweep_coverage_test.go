// SPDX-License-Identifier: Apache-2.0

package store

import (
	"context"
	"errors"
	"runtime"
	"testing"
	"time"

	"github.com/a-holm/messq/internal/errs"
	"github.com/a-holm/messq/internal/queue"
)

// The coverage-restoring tests (issue #11): the swap of sweeper/retire/expiry code in
// issue #11 pushed internal/store below the 85% durability-and-recovery floor, so this
// file drives the remaining under-covered statements green — the command seams (Kind /
// Bytes / default-limit), the batch-bound rejection, SweepConfig defaulting, the
// startup/retire-ticker and RetireNow seams of the sweeper, the immediate-wake and
// next-due paths of SweepCmd.Apply, and (white-box, against a hand-built transaction)
// the DecideSweep skip fence and the orphaned-policy skip. No production behaviour
// changes; the floor is untouched.

// TestSweepRetireCmdSeams covers the one-line Kind/Bytes stubs of both writer commands.
func TestSweepRetireCmdSeams(t *testing.T) {
	var s SweepCmd
	if s.Kind() != kindSweep {
		t.Fatalf("SweepCmd.Kind() = %q, want %q", s.Kind(), kindSweep)
	}
	if s.Bytes() != 0 {
		t.Fatalf("SweepCmd.Bytes() = %d, want 0", s.Bytes())
	}
	var r RetireCmd
	if r.Kind() != kindRetire {
		t.Fatalf("RetireCmd.Kind() = %q, want %q", r.Kind(), kindRetire)
	}
	if r.Bytes() != 0 {
		t.Fatalf("RetireCmd.Bytes() = %d, want 0", r.Bytes())
	}
}

// TestSweepRejectsOversizedBatch asserts a sweep batch above the store's bound is a
// business rejection before any I/O (I11), not an infra error.
func TestSweepRejectsOversizedBatch(t *testing.T) {
	st, _, _ := openSweepStore(t)
	_, err := st.Sweep(context.Background(), SweepCmd{Limit: 1 << 20})
	if !errors.Is(err, errs.ErrBadRequest) {
		t.Fatalf("oversized sweep error = %v, want ErrBadRequest", err)
	}
}

// TestSweepRetireDefaultLimit covers the Limit<=0 default-resolve branch of both entry
// points on an empty store: a zero Limit means "use the configured batch" and must be a
// no-op, not an error.
func TestSweepRetireDefaultLimit(t *testing.T) {
	st, _, _ := openSweepStore(t)
	ctx := context.Background()
	if res, err := st.Sweep(ctx, SweepCmd{}); err != nil {
		t.Fatalf("sweep with zero limit: %v", err)
	} else if res.Expired != 0 || res.Redelivered != 0 {
		t.Fatalf("empty sweep result = %+v, want all-zero", res)
	}
	if res, err := st.Retire(ctx, RetireCmd{}); err != nil {
		t.Fatalf("retire with zero limit: %v", err)
	} else if res.Retired != 0 {
		t.Fatalf("empty retire result = %+v, want Retired 0", res)
	}
}

// TestSweepConfigFillDefaults exercises the whole defaulting ladder of SweepConfig when
// no field is set, and the negative-retire-interval clamp.
func TestSweepConfigFillDefaults(t *testing.T) {
	var cfg SweepConfig
	cfg.fillDefaults()
	if cfg.Interval != 250*time.Millisecond {
		t.Fatalf("default Interval = %v, want 250ms", cfg.Interval)
	}
	if cfg.Batch != 1024 {
		t.Fatalf("default Batch = %d, want 1024", cfg.Batch)
	}
	if cfg.Catchup != 8 {
		t.Fatalf("default Catchup = %d, want 8", cfg.Catchup)
	}
	if cfg.RetireInterval != 0 {
		t.Fatalf("default RetireInterval = %v, want 0", cfg.RetireInterval)
	}
	// A negative retire interval clamps to 0 (startup only), never to a live ticker.
	neg := SweepConfig{Interval: -1, Batch: -1, Catchup: -1, RetireInterval: -time.Second}
	neg.fillDefaults()
	if neg.Interval != 250*time.Millisecond || neg.Batch != 1024 || neg.Catchup != 8 {
		t.Fatalf("negative fillDefaults = %+v, want defaults", neg)
	}
	if neg.RetireInterval != 0 {
		t.Fatalf("negative RetireInterval clamped to %v, want 0", neg.RetireInterval)
	}
}

// TestNewSweeperNilSeams covers the nil waker/logger substitution in the constructor.
func TestNewSweeperNilSeams(t *testing.T) {
	st, _, _ := openSweepStore(t)
	sw := NewSweeper(st, SweepConfig{Interval: time.Second, Batch: 4, Catchup: 2}, nil, nil)
	if sw == nil {
		t.Fatal("NewSweeper with nil seams returned nil")
	}
	if sw.waker == nil {
		t.Fatal("nil Waker was not defaulted to NopWaker")
	}
}

// TestSweeperWakeSeam drives the post-commit wake fan-out directly (G8: wake after Do).
func TestSweeperWakeSeam(t *testing.T) {
	_, _, _, sw := newTestSweeper(t, SweepConfig{
		Interval: time.Second, Batch: 4, Catchup: 2,
	})
	wak := &recWaker{}
	sw.waker = wak
	sw.wake([]queue.ConsumerKey{{Stream: "orders", Consumer: "worker"}})
	if n := wak.wakeCount(); n != 1 {
		t.Fatalf("wake fan-out delivered %d wake(s), want 1", n)
	}
}

// TestSweeperRetireNow covers the on-demand startup-only retire seam (G7): a stranded
// READY row is retired without waiting for any ticker.
func TestSweeperRetireNow(t *testing.T) {
	st, _, _ := openSweepStore(t)
	toks := seedSettle(t, st, 2, 2)
	settles := make([]SettleItem, 0, len(toks))
	for _, tk := range toks {
		settles = append(settles, SettleItem{Token: tk, Verb: queue.VerbNak, Reason: "test"})
	}
	if _, err := st.Settle(context.Background(), settleCmd(settles...)); err != nil {
		t.Fatalf("nak: %v", err)
	}
	if _, err := st.UpdateConsumer(context.Background(), "orders", "worker",
		ConsumerPatch{MaxDeliver: int32ptr(1)}, "test"); err != nil {
		t.Fatalf("lower max_deliver: %v", err)
	}
	sw := NewSweeper(st, SweepConfig{Interval: time.Second, Batch: 10, Catchup: 2}, NopWaker{}, nil)
	sw.RetireNow(context.Background())
	if n := countRows(t, st, `SELECT count(*) FROM deliveries WHERE state = 0 AND attempts = 1`); n != 0 {
		t.Fatalf("READY rows after RetireNow = %d, want 0 (stranded rows retired)", n)
	}
}

// TestSweepWakeImmediatelyVisible drives the Woke append in SweepCmd.Apply: with a zero
// backoff the redelivered row is visible immediately, so its consumer joins the wake set.
func TestSweepWakeImmediatelyVisible(t *testing.T) {
	st, fk, rec := openSweepStore(t)
	seedSweep(t, st, func(c *queue.ConsumerConfig) { c.Backoff = []time.Duration{0} }, 2, 2)
	fk.Advance(31 * time.Second)

	res, err := st.Sweep(context.Background(), SweepCmd{Limit: 10})
	if err != nil {
		t.Fatalf("sweep: %v", err)
	}
	if res.Redelivered != 2 {
		t.Fatalf("redelivered = %d, want 2", res.Redelivered)
	}
	wantWoke := queue.ConsumerKey{Stream: "orders", Consumer: "worker"}
	if len(res.Woke) != 1 || res.Woke[0] != wantWoke {
		t.Fatalf("Woke = %v, want exactly [%+v]", res.Woke, wantWoke)
	}
	if to, re, de := rec.counts(); to != 2 || re != 2 || de != 0 {
		t.Fatalf("metrics timeouts=%d redelivered=%d dead=%d, want 2/2/0", to, re, de)
	}
}

// TestSweepReportsNextDueForRemainingInflight sweeps a bounded batch smaller than the
// expired set, so rows stay INFLIGHT and NextDueMS reports the earliest remaining lease.
func TestSweepReportsNextDueForRemainingInflight(t *testing.T) {
	st, fk, _ := openSweepStore(t)
	seedSweep(t, st, nil, 3, 3)
	fk.Advance(31 * time.Second)

	res, err := st.Sweep(context.Background(), SweepCmd{Limit: 1})
	if err != nil {
		t.Fatalf("sweep: %v", err)
	}
	// The scanned set is bounded by Limit, so one row is released; the other two stay
	// INFLIGHT at their original (now-expired) lease deadline.
	if res.Expired != 1 || res.Redelivered != 1 {
		t.Fatalf("result = %+v, want 1 expired/1 redelivered (batch bound = 1)", res)
	}
	if !res.More {
		t.Fatalf("More = false on a partially-drained batch, want true")
	}
	// Exactly one row released; the other two still INFLIGHT.
	if ready := countRows(t, st, `SELECT count(*) FROM deliveries WHERE state = 0`); ready != 1 {
		t.Fatalf("READY rows = %d, want 1", ready)
	}
	if inflight := countRows(t, st, `SELECT count(*) FROM deliveries WHERE state = 1`); inflight != 2 {
		t.Fatalf("INFLIGHT rows = %d, want 2", inflight)
	}
	// NextDueMS is the earliest remaining INFLIGHT lease deadline (still the original
	// 30s ack_wait, since the untouched rows were never re-fetched).
	var due int64
	if err := st.RO().QueryRowContext(context.Background(),
		`SELECT MIN(visible_at) FROM deliveries WHERE state = 1`).Scan(&due); err != nil {
		t.Fatalf("read remaining MIN(visible_at): %v", err)
	}
	if res.NextDueMS != due || due == 0 {
		t.Fatalf("NextDueMS = %d, want %d (earliest remaining INFLIGHT lease)", res.NextDueMS, due)
	}
}

// TestSweeperRunExecutesRetireTicker strands two rows before Run (so the startup pass is
// bounded by Batch=1), then advances the fake clock to fire the retire-interval ticker,
// which retires the second. This covers Run's retireTicker setup and its retireCh branch.
func TestSweeperRunExecutesRetireTicker(t *testing.T) {
	st, fk, _ := openSweepStore(t)
	seedSettle(t, st, 2, 2)
	if _, err := st.Settle(context.Background(), settleCmd(
		SettleItem{Token: qtok("orders", "worker", 1, 1, 1), Verb: queue.VerbNak, Reason: "t"},
		SettleItem{Token: qtok("orders", "worker", 2, 1, 1), Verb: queue.VerbNak, Reason: "t"},
	)); err != nil {
		t.Fatalf("nak: %v", err)
	}
	if _, err := st.UpdateConsumer(context.Background(), "orders", "worker",
		ConsumerPatch{MaxDeliver: int32ptr(1)}, "test"); err != nil {
		t.Fatalf("lower max_deliver: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- NewSweeper(st, SweepConfig{
			Interval: 10 * time.Second, Batch: 1, Catchup: 2, RetireInterval: 50 * time.Millisecond,
		}, NopWaker{}, nil).Run(ctx)
	}()

	// The startup retire pass (Batch=1) retires exactly one of the two stranded rows.
	waitSweep(t, func() bool {
		return countRows(t, st, `SELECT count(*) FROM deliveries WHERE state = 0`) == 1
	}, "startup retire to drain one row")

	// Fire the retire-interval ticker: the second stranded row goes. Run creates its
	// tickers only AFTER the synchronous startup pass returns, and the waitSweep above
	// gives no happens-before for that arming: under -race + -coverpkg parallel load the
	// first Advance could land before NewTicker(RetireInterval), and a fake ticker armed
	// afterwards fires only on FUTURE advances — of which this test makes none — so the
	// second row strands forever (green isolated, red under full make-cover load).
	// BlockUntil pins arming before any clock moves; the bounded advance retries are
	// TestSweeperRunSweepTick's defence-in-depth against any missed grid point. The
	// asserted property is unchanged: the retire TICKER must be what drains the row.
	fk.BlockUntil(2) // the interval and retire tickers
	fk.Advance(60 * time.Millisecond)
	for i := 0; i < 100 && countRows(t, st, `SELECT count(*) FROM deliveries WHERE state = 0`) != 0; i++ {
		runtime.Gosched()
		fk.Advance(20 * time.Millisecond)
	}
	waitSweep(t, func() bool {
		return countRows(t, st, `SELECT count(*) FROM deliveries WHERE state = 0`) == 0
	}, "retire ticker to drain the second row")
	cancel()
	if rErr := <-done; rErr != nil {
		t.Fatalf("sweeper Run returned error: %v", rErr)
	}
}

// TestSweeperRunSweepTick covers the interval-ticker select arm of Run (G6): an expired
// INFLIGHT row is redelivered through a live tick rather than a synchronous step call.
// RetireInterval is 0 so no retire ticker floods the select with retire passes.
func TestSweeperRunSweepTick(t *testing.T) {
	st, fk, _ := openSweepStore(t)
	seedSweep(t, st, nil, 1, 1)
	fk.Advance(31 * time.Second) // row's ack_wait deadline passes before Run starts

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- NewSweeper(st, SweepConfig{
			Interval: 100 * time.Millisecond, Batch: 4, Catchup: 2,
		}, NopWaker{}, nil).Run(ctx)
	}()
	// Fire interval ticks until the expired row is released. The Run goroutine may not
	// have armed its ticker yet, so retry with bounded advances (Gosched only — no Sleep).
	// BlockUntil pins arming before the first advance: a fake ticker armed after an
	// advance fires only on future ones (the same load-dependent stranding the retire
	// ticker test hit — CI 2026-08-26), and exactly one waiter exists here because
	// RetireInterval is 0.
	fk.BlockUntil(1)
	for i := 0; i < 100 && countRows(t, st, `SELECT count(*) FROM deliveries WHERE state = 0`) == 0; i++ {
		runtime.Gosched()
		fk.Advance(20 * time.Millisecond)
	}
	waitSweep(t, func() bool {
		return countRows(t, st, `SELECT count(*) FROM deliveries WHERE state = 0`) == 1
	}, "interval tick to release the expired row")
	cancel()
	if rErr := <-done; rErr != nil {
		t.Fatalf("sweeper Run returned error: %v", rErr)
	}
}
