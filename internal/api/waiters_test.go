// SPDX-License-Identifier: Apache-2.0

package api

import (
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/a-holm/messq/internal/clock"
	"github.com/a-holm/messq/internal/obs"
	"github.com/a-holm/messq/internal/queue"
	"github.com/a-holm/messq/internal/subject"
)

// G6/G7/G8: wakes are levels not edges, caps hold with honest counters, filter
// snapshots match publishes, and the sink path never blocks the writer.

func mustSet(t *testing.T, raw ...string) subject.Set {
	t.Helper()
	set, err := subject.ParseSet(raw)
	if err != nil {
		t.Fatalf("parse filters %v: %v", raw, err)
	}
	return set
}

func key(stream, consumer string) queue.ConsumerKey {
	return queue.ConsumerKey{Stream: stream, Consumer: consumer}
}

func TestWakeIsALevelNotAnEdge(t *testing.T) {
	t.Parallel()

	reg := NewRegistry(16, 8)
	sub, err := reg.Subscribe(key("orders", "w"), mustSet(t, ">"))
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	defer sub.Close()

	// N wakes collapse into at most one token.
	for range 5 {
		reg.Wake(key("orders", "w"))
	}
	select {
	case <-sub.C():
	default:
		t.Fatal("wake lost despite a parked subscriber")
	}
	// The buffered token is single: a second receive without another wake blocks.
	select {
	case <-sub.C():
		t.Fatal("edge semantics: one wake produced two tokens")
	default:
	}

	// A second wake after consumption delivers exactly one more token.
	reg.Wake(key("orders", "w"))
	select {
	case <-sub.C():
	case <-clock.System{}.NewTimer(time.Second).C():
		t.Fatal("second wake never arrived")
	}
}

func TestWakeWithoutSubscriberIsANoop(t *testing.T) {
	t.Parallel()

	reg := NewRegistry(16, 8)
	reg.Wake(key("ghost", "w")) // must not panic or block
	if got := reg.Waiting(); got != nil {
		t.Errorf("Waiting() = %v, want nil", got)
	}
}

func TestDoubleCloseAndNilWaitingWhenEmpty(t *testing.T) {
	t.Parallel()

	reg := NewRegistry(16, 8)
	sub, err := reg.Subscribe(key("orders", "w"), mustSet(t, ">"))
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	sub.Close()
	sub.Close() // idempotent

	if got := reg.Parked(); got != 0 {
		t.Errorf("Parked() = %d after close, want 0", got)
	}
	if got := reg.Waiting(); got != nil {
		t.Errorf("Waiting() = %v, want nil when nobody is parked (the sweeper probe must be free)", got)
	}
}

func TestWaiterCapsHoldWithHonestCounters(t *testing.T) {
	t.Parallel()

	reg := NewRegistry(3, 1)

	s1, err := reg.Subscribe(key("a", "w1"), mustSet(t, ">"))
	if err != nil {
		t.Fatalf("subscribe 1: %v", err)
	}
	defer s1.Close()

	// Per-key cap of 1 refuses a second waiter on the same consumer...
	if _, subErr := reg.Subscribe(key("a", "w1"), mustSet(t, ">")); subErr == nil {
		t.Fatal("per-consumer cap did not hold")
	} else if !isTooManyWaiters(subErr) {
		t.Fatalf("err = %v, want a too-many-waiters error", subErr)
	}

	// ...but other consumers still fit until the global cap.
	s2, err := reg.Subscribe(key("b", "w2"), mustSet(t, ">"))
	if err != nil {
		t.Fatalf("subscribe 2: %v", err)
	}
	defer s2.Close()
	s3, err := reg.Subscribe(key("c", "w3"), mustSet(t, ">"))
	if err != nil {
		t.Fatalf("subscribe 3: %v", err)
	}
	defer s3.Close()

	if _, err := reg.Subscribe(key("d", "w4"), mustSet(t, ">")); err == nil {
		t.Fatal("global cap did not hold")
	} else if !isTooManyWaiters(err) {
		t.Fatalf("err = %v, want a too-many-waiters error", err)
	}

	if got := reg.Parked(); got != 3 {
		t.Errorf("Parked() = %d, want 3 (the counter must match reality)", got)
	}
	waiting := reg.Waiting()
	if len(waiting) != 3 {
		t.Errorf("Waiting() = %v, want 3 distinct keys", waiting)
	}
}

func isTooManyWaiters(err error) bool {
	var tmw *tooManyWaitersError
	return errors.As(err, &tmw)
}

func TestFilterSnapshotMatching(t *testing.T) {
	t.Parallel()

	reg := NewRegistry(16, 16)
	eu, err := reg.Subscribe(key("orders", "eu"), mustSet(t, "orders.eu.*"))
	if err != nil {
		t.Fatalf("subscribe eu: %v", err)
	}
	defer eu.Close()
	us, err := reg.Subscribe(key("orders", "us"), mustSet(t, "orders.us.*"))
	if err != nil {
		t.Fatalf("subscribe us: %v", err)
	}
	defer us.Close()

	reg.Publish([]obs.Event{{
		Event: "msg.publish", Stream: "orders", Subject: "orders.eu.created",
	}})

	select {
	case <-eu.C():
	default:
		t.Fatal("publish matching orders.eu.* did not wake the eu waiter")
	}
	select {
	case <-us.C():
		t.Error("orders.us.* waiter woken by an orders.eu.* publish")
	default:
	}
	// Other vocabularies never wake anyone.
	reg.Publish([]obs.Event{{Event: "msg.ack", Stream: "orders", Subject: "orders.eu.created"}})
	select {
	case <-us.C():
		t.Error("non-publish event woke a waiter")
	default:
	}
}

func TestSinkPublishNeverBlocksTheWriter(t *testing.T) {
	t.Parallel()

	reg := NewRegistry(64, 64)

	// Parked waiters whose channels are already FULL — the worst case for a blocking
	// sink: every send attempt would block.
	subs := make([]*Sub, 0, 32)
	for i := range 32 {
		sub, err := reg.Subscribe(key("orders", "w"+string(rune('a'+i))), mustSet(t, ">"))
		if err != nil {
			t.Fatalf("subscribe %d: %v", i, err)
		}
		subs = append(subs, sub)
		reg.Wake(key("orders", "w"+string(rune('a'+i)))) // fill each cap-1 channel
	}
	for _, sub := range subs {
		defer sub.Close()
	}

	done := make(chan struct{})
	go func() {
		defer close(done)
		evs := make([]obs.Event, 0, 10_000)
		for i := range 10_000 {
			evs = append(evs, obs.Event{
				Event: "msg.publish", Stream: "orders",
				Subject: "orders.tick." + string(rune('a'+i%26)),
			})
		}
		reg.Publish(evs)
	}()
	select {
	case <-done:
	case <-clock.System{}.NewTimer(2 * time.Second).C():
		t.Fatal("Publish blocked on the writer goroutine's clock: sink is not non-blocking")
	}
}

func TestSubscriberChurnUnderPublishRaceClean(t *testing.T) {
	t.Parallel()

	reg := NewRegistry(256, 256)
	stop := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		evs := make([]obs.Event, 64)
		for i := range evs {
			evs[i] = obs.Event{
				Event: "msg.publish", Stream: "orders",
				Subject: "orders.s." + string(rune('a'+i%26)),
			}
		}
		for {
			select {
			case <-stop:
				return
			default:
				reg.Publish(evs)
			}
		}
	}()
	for i := range 200 {
		sub, err := reg.Subscribe(key("orders", "churn"+string(rune('a'+i%26))), mustSet(t, ">"))
		if err != nil {
			t.Fatalf("subscribe %d: %v", i, err)
		}
		reg.Wake(key("orders", "churn"+string(rune('a'+i%26))))
		sub.Close()
	}
	close(stop)
	wg.Wait()
	if got := reg.Parked(); got != 0 {
		t.Errorf("Parked() = %d after full churn, want 0 (no leaked waiters)", got)
	}
}

func TestReleaseAllReleasesEveryParkedWaiter(t *testing.T) {
	t.Parallel()

	reg := NewRegistry(16, 8)
	closed := make(chan struct{}, 4)
	for _, k := range []string{"w1", "w2", "w3", "w4"} {
		sub, err := reg.Subscribe(key("orders", k), mustSet(t, ">"))
		if err != nil {
			t.Fatalf("subscribe %s: %v", k, err)
		}
		go func() {
			<-sub.C() // unblocks either on close or a wake; ReleaseAll closes
			closed <- struct{}{}
		}()
	}
	reg.ReleaseAll()
	for range 4 {
		select {
		case <-closed:
		case <-clock.System{}.NewTimer(time.Second).C():
			t.Fatal("ReleaseAll left a parked waiter stuck")
		}
	}
	if got := reg.Parked(); got != 0 {
		t.Errorf("Parked() = %d after ReleaseAll, want 0", got)
	}
	if got := reg.Waiting(); got != nil {
		t.Errorf("Waiting() = %v after ReleaseAll, want nil", got)
	}

	// The registry stays usable afterwards (#17 drain semantics).
	if _, err := reg.Subscribe(key("orders", "w5"), mustSet(t, ">")); err != nil {
		t.Errorf("subscribe after ReleaseAll: %v", err)
	}
}
