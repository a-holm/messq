// SPDX-License-Identifier: Apache-2.0

package store

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/a-holm/messq/internal/queue"
)

// The SweepCmd behaviour tests (issue #11 G2/G5/G10): the timeout arm releases expired
// INFLIGHT rows to READY at the jittered backoff deadline, dead-letters them at the
// max_deliver bound, and never touches attempts (D6) — all in one transaction with a
// full msg.timeout event row.

// TestSweepRedeliversExpiredRow is the canonical worker-dies path: a claimed row whose
// ack_wait passes is released to READY at now + backoff[attempts-1], attempts unchanged,
// with a msg.timeout event carrying the full field set.
func TestSweepRedeliversExpiredRow(t *testing.T) {
	st, fk, rec := openSweepStore(t)
	seedSweep(t, st, nil, 1, 1)

	// After the claim the row is INFLIGHT at attempts=1, visible_at = now + 30s (ack_wait).
	if got := attemptsFor(t, st, 1); got != 1 {
		t.Fatalf("attempts after claim = %d, want 1", got)
	}
	before := visibleAtOf(t, st, 1)

	// Advance past the deadline.
	fk.Advance(30 * time.Second)
	nowMS := fk.Now().UnixMilli()

	res, err := st.Sweep(context.Background(), SweepCmd{Limit: 10})
	if err != nil {
		t.Fatalf("sweep: %v", err)
	}
	if res.Expired != 1 || res.Redelivered != 1 || res.Dead != 0 || res.Skipped != 0 {
		t.Fatalf("result = %+v, want 1 expired/1 redelivered/0 dead/0 skipped", res)
	}
	if res.More {
		t.Fatalf("More = true on a single-row sweep")
	}
	// Attempts untouched (D6): still 1.
	if got := attemptsFor(t, st, 1); got != 1 {
		t.Fatalf("attempts after sweep = %d, want 1 (sweeper never touches attempts)", got)
	}
	// Row released to READY at now + backoff[0] = now + 1s (default backoff, identity jitter).
	want := nowMS + 1000
	if got := visibleAtOf(t, st, 1); got != want {
		t.Fatalf("visible_at after sweep = %d, want %d (now + 1s)", got, want)
	}
	if before == visibleAtOf(t, st, 1) {
		t.Fatalf("visible_at did not move (%d)", before)
	}
	if to, re, de := rec.counts(); to != 1 || re != 1 || de != 0 {
		t.Fatalf("metrics timeouts=%d redelivered=%d dead=%d, want 1/1/0", to, re, de)
	}
}

// TestSweepEmitsFullTimeoutEvent pins the msg.timeout row field set (G10): WARN-level
// event with held_ms, lateness_ms, ack_wait_ms, schedule_ms, delay_ms, retry_at,
// attempt, max_deliver and cause.
func TestSweepEmitsFullTimeoutEvent(t *testing.T) {
	st, fk, _ := openSweepStore(t)
	seedSweep(t, st, nil, 1, 1)

	// Freeze so held/lateness are deterministic. Claim at T0, deadline T0+30s, sweep at
	// T0+31s -> held_ms = 31s, lateness_ms = 1s.
	fk.Advance(31 * time.Second)
	nowMS := fk.Now().UnixMilli()
	if _, err := st.Sweep(context.Background(), SweepCmd{Limit: 10}); err != nil {
		t.Fatalf("sweep: %v", err)
	}

	rows, err := st.RO().QueryContext(context.Background(),
		`SELECT attempt, msg_id, trace_id, detail FROM events WHERE event = 'msg.timeout'`)
	if err != nil {
		t.Fatalf("query msg.timeout: %v", err)
	}
	defer func() {
		if cerr := rows.Close(); cerr != nil {
			t.Errorf("close msg.timeout rows: %v", cerr)
		}
	}()
	var attempt int64
	var msgID, traceID, detail string
	if !rows.Next() {
		t.Fatal("no msg.timeout event row")
	}
	if err := rows.Scan(&attempt, &msgID, &traceID, &detail); err != nil {
		t.Fatalf("scan msg.timeout: %v", err)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate msg.timeout: %v", err)
	}
	if attempt != 1 {
		t.Fatalf("attempt = %d, want 1", attempt)
	}
	if msgID == "" || traceID == "" {
		t.Fatalf("msg_id/trace_id not carried: %q/%q", msgID, traceID)
	}
	for _, want := range []string{
		`"held_ms":31000`, `"lateness_ms":1000`, `"ack_wait_ms":30000`,
		`"schedule_ms":1000`, `"delay_ms":1000`, `"retry_at":` + itoa(nowMS+1000),
		`"attempt":1`, `"max_deliver":5`, `"cause":"ack_wait"`,
	} {
		if !containsStr(detail, want) {
			t.Fatalf("msg.timeout detail %q missing %q", detail, want)
		}
	}
}

// TestSweepDeadLettersAtBound drives a poison message to DEAD after exactly max_deliver
// deliveries by timeout, counting deliver events (G2): a max_deliver=1 consumer's single
// in-flight row dies on its first timeout.
func TestSweepDeadLettersAtBound(t *testing.T) {
	st, fk, rec := openSweepStore(t)
	seedSweep(t, st, func(c *queue.ConsumerConfig) { c.MaxDeliver = 1 }, 1, 1)
	fk.Advance(30 * time.Second)

	res, err := st.Sweep(context.Background(), SweepCmd{Limit: 10})
	if err != nil {
		t.Fatalf("sweep: %v", err)
	}
	if res.Dead != 1 || res.Redelivered != 0 || res.Skipped != 0 {
		t.Fatalf("result = %+v, want 1 dead/0 redelivered/0 skipped", res)
	}
	// The row is gone (DEAD is the row's absence via the sink's delete).
	if n := countDeliveryRows(t, st); n != 0 {
		t.Fatalf("deliveries after dead-letter = %d, want 0", n)
	}
	// A msg.dead audit row exists (DropSink writes it).
	var deadN int
	if err := st.RO().QueryRowContext(context.Background(),
		`SELECT count(*) FROM events WHERE event = 'msg.dead'`).Scan(&deadN); err != nil {
		t.Fatalf("count msg.dead: %v", err)
	}
	if deadN != 1 {
		t.Fatalf("msg.dead rows = %d, want 1", deadN)
	}
	if to, re, de := rec.counts(); to != 1 || re != 0 || de != 1 {
		t.Fatalf("metrics timeouts=%d redelivered=%d dead=%d, want 1/0/1", to, re, de)
	}
}

// TestSweepNeverIncrementsAttemptsAcrossSweeps sweeps the same row three times and
// asserts attempts stays 1 the whole way — the D6 contract (G2). A mutant that
// increments attempts on release reds this (and I4).
func TestSweepNeverIncrementsAttemptsAcrossSweeps(t *testing.T) {
	st, fk, _ := openSweepStore(t)
	seedSweep(t, st, func(c *queue.ConsumerConfig) { c.Backoff = []time.Duration{time.Second} }, 1, 1)
	for i := 1; i <= 3; i++ {
		fk.Advance(30 * time.Second)
		if _, err := st.Sweep(context.Background(), SweepCmd{Limit: 10}); err != nil {
			t.Fatalf("sweep %d: %v", i, err)
		}
		if got := attemptsFor(t, st, 1); got != 1 {
			t.Fatalf("attempts after sweep %d = %d, want 1 (sweeper must never increment)", i, got)
		}
	}
}

// itoa and containsStr are tiny test helpers to keep assertions readable.
func itoa(v int64) string { return fmt.Sprintf("%d", v) }

func containsStr(haystack, needle string) bool {
	for i := 0; i+len(needle) <= len(haystack); i++ {
		if haystack[i:i+len(needle)] == needle {
			return true
		}
	}
	return false
}
