// SPDX-License-Identifier: Apache-2.0

package api

import (
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/a-holm/messq/internal/obs"
	"github.com/a-holm/messq/internal/queue"
	"github.com/a-holm/messq/internal/subject"
)

// The bounded waiter registry (issue §7): the long-poll park/wake fabric. It
// implements store.Waker (#11 — the sweeper's Waiting/Wake seam) and obs.Sink (#6 —
// committed-event fan-out), so every wake source lands here:
//
//	publish committed  → Publish sees msg.publish and wakes matching filter snapshots
//	redelivery due     → the sweeper calls Waiting() then Wake(key)
//	nak visible now    → the settle handler calls Wake(key) after its commit
//	consumer update    → #15 wakes the key unconditionally (snapshot invalidation)
//	shutdown (#17)     → ReleaseAll closes everything; parked fetches drain

// eventMsgPublish is the vocabulary name of a committed publish (SEMANTICS S2.4).
// A local constant until #19 owns the event vocabulary.
const eventMsgPublish = "msg.publish"

// numShards is the shard count: sharded by STREAM so the writer-goroutine sink path
// contends only with the waiters parked on that one stream (issue sketches 64).
const numShards = 64

// Registry is the whole registry. Zero value is unusable; build with [NewRegistry].
type Registry struct {
	shards [numShards]shard
	total  atomic.Int64 // fast "is anyone parked?" for Waiting()
	max    int
	maxPer int
}

// shard holds one stream-hash bucket. The critical section is "iterate the parked
// waiters of one key" — never all waiters.
type shard struct {
	mu     sync.Mutex
	byKey  map[queue.ConsumerKey][]*waiter
	closed bool
}

// waiter is one parked fetch.
type waiter struct {
	key     queue.ConsumerKey
	filters subject.Set // snapshot at Subscribe; matches msg.publish subjects
	c       chan struct{}
}

// Sub is one subscription's handle. C yields at most one token per state change
// (level, not edge); Close is idempotent.
type Sub struct {
	w      *waiter
	reg    *Registry
	shardN uint32
	close  sync.Once
}

// C returns the wake channel. Receive with select alongside the fetch deadline,
// request cancellation and server shutdown — never alone.
func (s *Sub) C() <-chan struct{} { return s.w.c }

// Key returns the consumer this sub parks on.
func (s *Sub) Key() queue.ConsumerKey { return s.w.key }

// Close releases the slot. Idempotent; a double close neither panics nor double-counts.
func (s *Sub) Close() {
	s.close.Do(func() {
		sh := &s.reg.shards[s.shardN]
		sh.mu.Lock()
		defer sh.mu.Unlock()
		if sh.closed {
			return
		}
		list := sh.byKey[s.w.key]
		for i, w := range list {
			if w == s.w {
				sh.byKey[s.w.key] = append(list[:i], list[i+1:]...)
				break
			}
		}
		if len(sh.byKey[s.w.key]) == 0 {
			delete(sh.byKey, s.w.key)
		}
		s.reg.total.Add(-1)
	})
}

// tooManyWaitersError is the cap refusal. The fetch handler attaches the
// too_many_waiters wire code and Retry-After via writeError's 503 policy.
type tooManyWaitersError struct {
	scope string // "" = global cap; otherwise names the per-consumer bound
	limit int
}

func (e *tooManyWaitersError) Error() string {
	if e.scope == "" {
		return fmt.Sprintf("too many parked waiters process-wide (--max-waiters %d); "+
			"reduce worker concurrency or raise the flag", e.limit)
	}
	return fmt.Sprintf("too many parked waiters for %s (--max-waiters-per-consumer %d)",
		e.scope, e.limit)
}

// NewRegistry builds a registry with the given global and per-consumer caps
// (--max-waiters / --max-waiters-per-consumer). Both must be positive.
func NewRegistry(maxWaiters, maxPerConsumer int) *Registry {
	r := &Registry{max: maxWaiters, maxPer: maxPerConsumer}
	for i := range r.shards {
		r.shards[i].byKey = make(map[queue.ConsumerKey][]*waiter)
	}
	return r
}

func shardOf(stream string) uint32 {
	const (
		offset32 = 2166136261
		prime32  = 16777619
	)
	h := uint32(offset32)
	for i := 0; i < len(stream); i++ {
		h ^= uint32(stream[i])
		h *= prime32
	}
	return h % numShards
}

// Subscribe parks a waiter for key with a filter snapshot, or refuses with
// *tooManyWaitersError when either cap is full. Filters are matched against published
// subjects verbatim (#3's allocation-free matcher).
func (r *Registry) Subscribe(k queue.ConsumerKey, filters subject.Set) (*Sub, error) {
	n := shardOf(k.Stream)
	sh := &r.shards[n]
	sh.mu.Lock()
	defer sh.mu.Unlock()
	if sh.closed {
		return nil, &tooManyWaitersError{scope: "registry closed", limit: 0}
	}
	if int(r.total.Load()) >= r.max {
		return nil, &tooManyWaitersError{limit: r.max}
	}
	if len(sh.byKey[k]) >= r.maxPer {
		return nil, &tooManyWaitersError{scope: k.Stream + "/" + k.Consumer, limit: r.maxPer}
	}
	w := &waiter{
		key:     k,
		filters: filters,
		c:       make(chan struct{}, 1),
	}
	sh.byKey[k] = append(sh.byKey[k], w)
	r.total.Add(1)
	return &Sub{w: w, reg: r, shardN: n}, nil
}

// Wake signals every waiter parked on key that SOMETHING changed for them. It never
// blocks and never duplicates tokens: the cap-1 channel plus non-blocking send makes N
// wakes collapse into one retry, and the store — not the wake — decides whether work
// exists. Satisfies store.Waker.
func (r *Registry) Wake(k queue.ConsumerKey) {
	sh := &r.shards[shardOf(k.Stream)]
	sh.mu.Lock()
	defer sh.mu.Unlock()
	for _, w := range sh.byKey[k] {
		select {
		case w.c <- struct{}{}:
		default:
		}
	}
}

// Waiting returns the distinct keys with parked waiters, or NIL when total == 0 — the
// sweeper's 4 Hz probe costs one atomic load on an idle broker. Satisfies store.Waker.
func (r *Registry) Waiting() []queue.ConsumerKey {
	if r.total.Load() == 0 {
		return nil
	}
	var out []queue.ConsumerKey
	for i := range r.shards {
		sh := &r.shards[i]
		sh.mu.Lock()
		for k := range sh.byKey {
			out = append(out, k)
		}
		sh.mu.Unlock()
	}
	return out
}

// Parked returns the number of currently parked waiters. Tests assert caps against it;
// the sweeper probe and #21's messq_waiters gauge read it.
func (r *Registry) Parked() int64 { return r.total.Load() }

// ReleaseAll drains every waiter (shutdown, #17): each channel is closed so a parked
// fetch observes shutdown as a closed receive, and the registry returns to empty.
// Safe to call twice; the registry stays usable afterwards.
func (r *Registry) ReleaseAll() {
	for i := range r.shards {
		sh := &r.shards[i]
		sh.mu.Lock()
		for _, list := range sh.byKey {
			for _, w := range list {
				close(w.c)
			}
		}
		sh.byKey = make(map[queue.ConsumerKey][]*waiter)
		sh.closed = false
		sh.mu.Unlock()
	}
	r.total.Store(0)
}

// Publish consumes committed events off the writer's fan-out pump. It MUST NOT block
// (G8): it takes one shard lock at a time and only performs non-blocking sends, so ten
// thousand events cost ten thousand bounded sends no matter how many workers park.
// Satisfies obs.Sink.
func (r *Registry) Publish(evs []obs.Event) {
	for i := range evs {
		ev := &evs[i]
		if ev.Event != eventMsgPublish || ev.Stream == "" {
			continue
		}
		r.wakeMatching(ev.Stream, ev.Subject)
	}
}

// wakeMatching wakes the parked waiters of one stream whose snapshot matches subject.
func (r *Registry) wakeMatching(streamName, subj string) {
	sh := &r.shards[shardOf(streamName)]
	sh.mu.Lock()
	defer sh.mu.Unlock()
	for _, list := range sh.byKey {
		for _, w := range list {
			if w.key.Stream != streamName || !w.filters.Match(subj) {
				continue
			}
			select {
			case w.c <- struct{}{}:
			default:
			}
		}
	}
}
