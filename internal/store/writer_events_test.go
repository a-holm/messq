// SPDX-License-Identifier: Apache-2.0

package store

import (
	"context"
	"log/slog"
	"runtime"
	"strings"
	"sync"
	"testing"

	"github.com/a-holm/messq/internal/errs"
	"github.com/a-holm/messq/internal/obs"
)

// slowSink runs onPublish inside every Publish call.
type slowSink struct {
	onPublish func()
}

func (s *slowSink) Publish(evs []obs.Event) {
	s.onPublish()
	_ = evs
}

// TestEventsArePublishedOnlyAfterCommit pins the fan-out discipline two ways:
//
//  1. While the batch transaction is still open (frozen at store.tx.before_apply), the sink
//     has seen NOTHING — events produced by Apply are projections of a commit, not attempts.
//  2. A rejected command contributes no events even though its siblings commit: the sink
//     sees exactly the two survivors' events.
func TestEventsArePublishedOnlyAfterCommit(t *testing.T) {
	ctx := context.Background()
	handler := &logCapture{}
	fc := fakeClock()
	sink := &sinkRecorder{}
	opts := testOptions(testDataDir(t), fc, handler)
	st, _, err := Open(ctx, opts)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer func() {
		if closeErr := st.Close(ctx); closeErr != nil {
			t.Errorf("close store: %v", closeErr)
		}
	}()

	gate := make(chan struct{})
	var enteredOnce sync.Once
	entered := make(chan struct{})
	hks := hooks{beforeApply: func() {
		enteredOnce.Do(func() { close(entered) })
		<-gate
	}}
	w, err := st.NewWriter(Config{},
		withLogger(handler.asLogger()), withEventSink(sink), withHooks(hks))
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}

	r1 := submitAsync(w, 1, "first", 0)
	<-entered // P1 parked mid-transaction

	if got := len(sink.batches()); got != 0 {
		t.Fatalf("sink saw %d publishes while the transaction was still open", got)
	}

	// Two siblings join the open batch; the middle one is rejected.
	req2 := &request{cmd: &probeCmd{key: 2, val: "doomed", bizErr: errs.ErrStaleAck}, done: make(chan struct{})}
	req3 := &request{cmd: &probeCmd{key: 3, val: "third"}, done: make(chan struct{})}
	w.ch <- req2
	w.ch <- req3
	close(gate)

	if got := <-r1; got.Err != nil {
		t.Fatalf("Do(P1): %v", got.Err)
	}
	<-req2.done
	<-req3.done

	waitFor(func() bool { return len(sink.events()) == 2 })
	evs := sink.events()
	if len(evs) != 2 || evs[0].Detail["k"] != int64(1) || evs[1].Detail["k"] != int64(3) {
		t.Fatalf("sink events = %+v, want exactly keys [1 3] — a rejection leaked an event", evs)
	}
	if closeErr := w.Close(ctx); closeErr != nil {
		t.Errorf("close: %v", closeErr)
	}
}

// TestReplyPrecedesEventDelivery pins the ordering rule with an observable, not timing:
// while the fan-out pump is parked inside Publish for batch #1, the caller of batch #1 has
// ALREADY received its reply. Any reordering of reply and fan-out fails this.
func TestReplyPrecedesEventDelivery(t *testing.T) {
	ctx := context.Background()
	handler := &logCapture{}
	st, _, err := Open(ctx, testOptions(testDataDir(t), fakeClock(), handler))
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer func() {
		if closeErr := st.Close(ctx); closeErr != nil {
			t.Errorf("close store: %v", closeErr)
		}
	}()

	parked := make(chan struct{})
	release := make(chan struct{})
	parkOnce := sync.Once{}
	slow := &slowSink{onPublish: func() {
		parkOnce.Do(func() {
			close(parked)
			<-release
		})
	}}
	w, err := st.NewWriter(Config{}, withLogger(handler.asLogger()), withEventSink(slow))
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}

	r1 := submitAsync(w, 1, "a", 0)
	<-parked // the pump is stuck inside Publish for batch #1's events

	// The reply must be deliverable while the pump is parked. A bounded wait absorbs the
	// submitting goroutine's scheduling delay; a writer that put fan-out on the reply path
	// would never deliver and fail here.
	replied := false
	waitFor(func() bool {
		select {
		case got := <-r1:
			replied = true
			if got.Err != nil {
				t.Errorf("Do(P1) = %v — the slow sink blocked the reply path", got.Err)
			}
			return true
		default:
			return false
		}
	})
	if !replied {
		t.Fatal("P1 reply not delivered while the sink is parked: fan-out blocks replies")
	}

	close(release)
	if closeErr := w.Close(ctx); closeErr != nil {
		t.Fatalf("close: %v", closeErr)
	}
}

// TestSlowSinkCannotStallTheWriter pins the acceptance line verbatim: the sink blocks inside
// Publish for the FIRST event, and the writer keeps committing anyway.
func TestSlowSinkCannotStallTheWriter(t *testing.T) {
	ctx := context.Background()
	handler := &logCapture{}
	st, _, err := Open(ctx, testOptions(testDataDir(t), fakeClock(), handler))
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer func() {
		if closeErr := st.Close(ctx); closeErr != nil {
			t.Errorf("close store: %v", closeErr)
		}
	}()

	parked := make(chan struct{})
	release := make(chan struct{})
	parkOnce := sync.Once{}
	slow := &slowSink{onPublish: func() {
		parkOnce.Do(func() {
			close(parked)
			<-release
		})
	}}
	w, err := st.NewWriter(Config{}, withLogger(handler.asLogger()), withEventSink(slow))
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}

	r1 := submitAsync(w, 1, "a", 0)
	<-parked // pump stuck inside Publish for batch #1

	if got := <-r1; got.Err != nil {
		t.Fatalf("Do(P1) = %v", got.Err)
	}
	// The writer happily commits more work while the sink is still parked:
	if _, doErr := w.Do(ctx, &probeCmd{key: 2, val: "b"}); doErr != nil {
		t.Fatalf("Do(P2) while sink parked: %v", doErr)
	}

	close(release)
	if closeErr := w.Close(ctx); closeErr != nil {
		t.Fatalf("close: %v", closeErr)
	}
	if rows := readProbe(t, st.RO()); len(rows) != 2 {
		t.Errorf("%d rows, want 2 — work continued during the parked sink", len(rows))
	}
}

// TestFanOutOverflowDropsLoudly pins the bounded-pump behaviour: when the pump queue is full,
// the event hand-off drops loudly instead of wedging the writer. The events table remains
// complete either way — this surface is a projection.
func TestFanOutOverflowDropsLoudly(t *testing.T) {
	ctx := context.Background()
	handler := &logCapture{}
	st, _, err := Open(ctx, testOptions(testDataDir(t), fakeClock(), handler))
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer func() {
		if closeErr := st.Close(ctx); closeErr != nil {
			t.Errorf("close store: %v", closeErr)
		}
	}()

	parked := make(chan struct{})
	release := make(chan struct{})
	parkOnce := sync.Once{}
	blocking := &slowSink{onPublish: func() {
		parkOnce.Do(func() {
			close(parked)
			<-release
		})
	}}
	w, err := st.NewWriter(Config{}, withLogger(handler.asLogger()), withEventSink(blocking))
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}

	r1 := submitAsync(w, 1, "a", 0)
	<-parked // pump stuck on batch #1's events; P1's reply is already out
	if got := <-r1; got.Err != nil {
		t.Fatalf("Do(P1) = %v", got.Err)
	}
	for i := 0; i < eventBufferBatches; i++ { // fill the pump queue to the brim
		w.evCh <- []obs.Event{{Event: "filler", TS: int64(i)}}
	}

	// This batch's events cannot fit: the hand-off must DROP (loudly), not block.
	r2 := submitAsync(w, 2, "b", 0)
	if got := <-r2; got.Err != nil {
		t.Fatalf("Do(P2) = %v — overflow blocked the writer", got.Err)
	}
	// The engine closes replies BEFORE the fan-out hand-off on purpose (replies first,
	// fan-out off the latency path), so the drop warning can land a scheduling step
	// after the caller unblocks. Yield until the writer's hand-off becomes visible: a
	// bounded spin stays free of wall-clock sleeps and still fails fast if the drop
	// never happens.
	var drops []slog.Record
	for i := 0; i < 10_000; i++ {
		drops = handler.find(slog.LevelWarn, "events.dropped")
		if len(drops) > 0 {
			break
		}
		runtime.Gosched()
	}
	if len(drops) != 1 {
		t.Fatalf("events.dropped warnings = %d, want exactly 1", len(drops))
	}
	if !strings.Contains(attrString(drops[0], "reason"), "overflow") {
		t.Errorf("drop warning lacks a reason attribute: %+v", drops[0])
	}

	close(release)
	if closeErr := w.Close(ctx); closeErr != nil {
		t.Fatalf("close: %v", closeErr)
	}
	if rows := readProbe(t, st.RO()); len(rows) != 2 {
		t.Errorf("rows = %d, want 2 — dropped PROJECTIONS never mean dropped WORK", len(rows))
	}
}

// attrString pulls one attribute value out of a captured record as text.
func attrString(r slog.Record, key string) string {
	found := ""
	r.Attrs(func(a slog.Attr) bool {
		if a.Key == key {
			found = a.Value.String()
			return false
		}
		return true
	})
	return found
}
