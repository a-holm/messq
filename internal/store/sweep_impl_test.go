// SPDX-License-Identifier: Apache-2.0

package store

import (
	"context"
	"sync"
	"testing"

	"github.com/a-holm/messq/internal/clock"
	"github.com/a-holm/messq/internal/queue"
)

// The SweepCmd test harness (issue #11). A shared store with a deterministic identity
// jitter (so exact visible_at arithmetic is possible), a recording sweep-metrics twin,
// and a seed helper that publishes + claims a number of deliveries.

// recordSweepMetrics is the recording SweepMetrics twin for sweep tests.
type recordSweepMetrics struct {
	mu          sync.Mutex
	timeouts    int
	redelivered int
	dead        int
}

func (r *recordSweepMetrics) Timeout() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.timeouts++
}

func (r *recordSweepMetrics) Redelivered() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.redelivered++
}

func (r *recordSweepMetrics) Dead() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.dead++
}

func (r *recordSweepMetrics) counts() (timeouts, redelivered, dead int) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.timeouts, r.redelivered, r.dead
}

// openSweepStore opens an engine-less store with deterministic identity jitter and a
// recording sweep-metrics twin installed (like openSettleStore).
func openSweepStore(t testing.TB) (*Store, *clock.Fake, *recordSweepMetrics) {
	t.Helper()
	st, fk, _ := openSettleStore(t)
	rec := &recordSweepMetrics{}
	st.sweepMetrics = rec
	return st, fk, rec
}

// seedSweep publishes n messages to stream "orders", creates consumer "worker" (with
// the harness's config tweak applied), claims batch deliveries. After the claim the
// rows are INFLIGHT with visible_at = now + ack_wait.
func seedSweep(t testing.TB, st *Store, tweak func(*queue.ConsumerConfig), n, batch int) {
	t.Helper()
	for i := 1; i <= n; i++ {
		if _, err := st.Publish(context.Background(), PublishCmd{
			Stream: "orders", Req: queue.PublishReq{Subject: "orders.1", Body: []byte("x")},
		}); err != nil {
			t.Fatalf("publish %d: %v", i, err)
		}
	}
	cfg := queue.DefaultConsumerConfig("worker")
	cfg.Filters = []string{">"}
	if tweak != nil {
		tweak(&cfg)
	}
	if _, err := st.CreateConsumer(context.Background(), "orders", cfg, queue.StartPosition{Kind: queue.StartFirst}, "test"); err != nil {
		t.Fatalf("create consumer: %v", err)
	}
	if _, err := st.Fetch(context.Background(), FetchReq{Stream: "orders", Consumer: "worker", Batch: batch}); err != nil {
		t.Fatalf("fetch: %v", err)
	}
}
