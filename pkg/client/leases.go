// SPDX-License-Identifier: Apache-2.0

package client

import (
	"container/heap"
	"context"
	"errors"
	"time"
)

// The lease keeper (issue §7.3) — the point of the whole Worker. One goroutine owns
// every held lease in one min-heap of nextExtendAt, so ONE batched POST /v1/extend
// per 250 ms window serves any concurrency. Deadlines are anchored locally on the
// monotonic clock; the broker's deadline_ms is display and skew detection only.
//
// Give-up is honest: at MaxLease, on extend_capped, or when a retry budget is spent,
// the keeper stops extending, cancels the handler with cause ErrLeaseLost, marks the
// token lost so its late outcome is discarded rather than settled, and (for the
// worker's own give-ups) naks it so the work redelivers immediately instead of after
// a full ack_wait.

type lease struct {
	token         string
	item          *workItem
	cancel        context.CancelCauseFunc
	ackWait       time.Duration
	localDeadline time.Time // monotonic-anchored; NEVER deadline_ms
	nextExtendAt  time.Time
	extends       int
	totalHeld     time.Duration
	index         int
}

type leaseHeap []*lease

func (h leaseHeap) Len() int           { return len(h) }
func (h leaseHeap) Less(i, j int) bool { return h[i].nextExtendAt.Before(h[j].nextExtendAt) }
func (h leaseHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i]; h[i].index = i; h[j].index = j }
func (h *leaseHeap) Push(x any) {
	l, ok := x.(*lease)
	if !ok {
		return // unreachable by construction; heap.Interface hands back what we pushed
	}
	l.index = len(*h)
	*h = append(*h, l)
}

func (h *leaseHeap) Pop() any {
	old := *h
	n := len(old)
	l := old[n-1]
	old[n-1] = nil
	l.index = -1
	*h = old[:n-1]
	return l
}
func (h *leaseHeap) peek() *lease { return (*h)[0] }
func (h *leaseHeap) find(token string) *lease {
	for _, l := range *h {
		if l.token == token {
			return l
		}
	}
	return nil
}
func (h *leaseHeap) remove(l *lease) { heap.Remove(h, l.index) }

// keeperLoop runs for the whole life of Run. It exits when ctx ends, or when draining
// finished with an empty heap.
func (w *Worker) keeperLoop(ctx context.Context, st *workerState) {
	h := &leaseHeap{}
	heap.Init(h)

	draining := false
	var drainDeadline time.Time
	now := func() time.Time { return w.client.clk.Now() }

	// Edge-trigger the stop signal: a closed st.stopFetch would otherwise win every
	// select from that moment on, starving the extend timer mid-drain.
	stop := st.stopFetch

	for {
		if h.Len() == 0 {
			if draining {
				return
			}
			select {
			case l := <-st.addLease:
				heap.Push(h, l)
			case <-stop:
				stop = nil
				draining = true
				drainDeadline = now().Add(w.cfg.DrainTimeout)
			case <-ctx.Done():
				return
			}
			continue
		}

		if draining && !now().Before(drainDeadline) {
			// §7.6: whatever is still held when DrainTimeout expires is nakked with
			// reason="worker draining", delay=0 — visible to the next worker at once
			// instead of stranding for a full ack_wait.
			for _, l := range *h {
				w.giveUp(ctx, st, l, "worker draining")
			}
			return
		}

		top := h.peek()
		d := top.nextExtendAt.Sub(now())
		if draining {
			// Extends continue throughout the drain (§7.6), but never past its budget.
			if dd := drainDeadline.Sub(now()); dd < d {
				d = max(dd, 0)
			}
		}
		if d < 0 {
			d = 0
		}
		timer := w.client.clk.NewTimer(d)
		select {
		case l := <-st.addLease:
			timer.Stop()
			heap.Push(h, l)
		case tok := <-st.doneToken:
			timer.Stop()
			if l := h.find(tok); l != nil {
				h.remove(l)
			}
		case <-stop:
			timer.Stop()
			stop = nil
			if !draining {
				draining = true
				drainDeadline = now().Add(w.cfg.DrainTimeout)
			}
		case <-timer.C():
			w.extendDue(ctx, st, h)
		case <-ctx.Done():
			timer.Stop()
			return
		}
	}
}

// extendDue extends every lease due within the batch window in ONE request.
func (w *Worker) extendDue(ctx context.Context, st *workerState, h *leaseHeap) {
	clk := w.client.clk
	now := clk.Now()
	windowEnd := now.Add(defaultExtendWindow)

	var due []*lease
	for h.Len() > 0 && !h.peek().nextExtendAt.After(windowEnd) {
		popped, ok := heap.Pop(h).(*lease)
		if !ok { // unreachable by construction (see Push)
			continue
		}
		due = append(due, popped)
	}
	if len(due) == 0 {
		return
	}

	// T7 give-up: totalHeld + ackWait > MaxLease ⇒ stop extending THIS window honestly.
	live := due[:0]
	for _, l := range due {
		if w.cfg.MaxLease > 0 && l.totalHeld+l.ackWait > w.cfg.MaxLease {
			w.giveUp(ctx, st, l, "lease expired")
			continue
		}
		live = append(live, l)
	}
	if len(live) == 0 {
		return
	}

	res, err := w.extendWithRetry(ctx, live)
	if err != nil {
		if errors.Is(err, ErrExtendCapped) {
			// The daemon answered extend_capped: every token in this request hit the cap.
			for _, l := range live {
				w.capOut(st, l)
			}
			return
		}
		// Unrecoverable within margin (or a real verdict): presume the worst — the
		// alternative, silently running a handler whose message redelivers, is how
		// two workers end up on one job.
		for _, l := range live {
			w.loseLease(st, l)
		}
		return
	}

	byToken := map[string]SettleItem{}
	for _, item := range res.Results {
		byToken[item.Token] = item
	}
	requeue := make([]*lease, 0, len(live))
	for _, l := range live {
		item, ok := byToken[l.token]
		if ok && item.Status == SettleOK {
			l.extends++
			st.stats.extends.Add(1)
			l.totalHeld += l.ackWait
			l.localDeadline = l.localDeadline.Add(l.ackWait) // T7: visible_at += ack_wait
			margin := extendMargin(l.ackWait)
			next := now.Add(time.Duration(float64(l.ackWait) * w.cfg.ExtendAt))
			if cap := l.localDeadline.Add(-margin); next.After(cap) {
				next = cap
			}
			l.nextExtendAt = next
			requeue = append(requeue, l)
			continue
		}
		// stale / stale_ack / wrong_generation / unknown: the broker no longer honours
		// this lease. Cancel the handler, discard its eventual outcome, settle NOTHING.
		status := SettleUnknown
		if ok {
			status = item.Status
		}
		if status == SettleStale || status == SettleStaleAck {
			st.stats.staleacks.Add(1)
			w.emit(WorkerEvent{Kind: EventStaleAck, Msg: &l.item.msg})
		}
		w.loseLease(st, l)
	}
	for _, l := range requeue {
		heap.Push(h, l)
	}
	if len(requeue) > 0 {
		w.emit(WorkerEvent{Kind: EventExtended, Attempt: len(requeue)})
	}
}

// extendWithRetry retries transport-level failures with reconnect backoff until the
// earliest local deadline minus its margin; past that the leases are presumed lost.
func (w *Worker) extendWithRetry(ctx context.Context, live []*lease) (SettleResult, error) {
	tokens := make([]string, len(live))
	for i, l := range live {
		tokens[i] = l.token
	}
	earliest := live[0].localDeadline
	for _, l := range live[1:] {
		if l.localDeadline.Before(earliest) {
			earliest = l.localDeadline
		}
	}
	giveUpBy := earliest.Add(-extendMargin(live[0].ackWait))

	attempt := 0
	for {
		res, err := w.client.Extend(ctx, tokens...)
		if err == nil {
			return res, nil
		}
		if errors.Is(err, ErrExtendCapped) {
			return SettleResult{}, err
		}
		if Classify(err) != KindUnavailable && Classify(err) != KindTimeout {
			// A definitive non-extend answer we cannot interpret as transient.
			return SettleResult{}, err
		}
		if !w.client.clk.Now().Before(giveUpBy) {
			return SettleResult{}, errUnreachableExtend
		}
		if !wait(ctx, w.client.clk, w.cfg.Backoff.next(attempt)) {
			return SettleResult{}, errUnreachableExtend
		}
		attempt++
	}
}

var errUnreachableExtend = errors.New("messq: extend unreachable past the lease margin")

// loseLease fences a token whose lease the BROKER no longer honours: cancel with
// cause ErrLeaseLost, mark lost (late outcomes get discarded), settle nothing.
func (w *Worker) loseLease(st *workerState, l *lease) {
	if st.markLost(l.token) {
		st.stats.leaselost.Add(1)
		w.emit(WorkerEvent{Kind: EventLeaseLost, Msg: &l.item.msg})
	}
	l.cancel(ErrLeaseLost)
}

// capOut applies the T7 cap: stop extending, report, apply OnLeaseLost. Unlike a
// give-up there is no nak here — the server already refuses to renew, so the message
// redelivers on its own schedule.
func (w *Worker) capOut(st *workerState, l *lease) {
	if st.markLost(l.token) {
		st.stats.leaked.Add(1)
		w.emit(WorkerEvent{Kind: EventLeaseCapped, Msg: &l.item.msg})
	}
	l.cancel(ErrLeaseLost)
}

// giveUp is the WORKER deciding to release a token it still holds (MaxLease reached,
// drain leftover): fence the handler's late outcome, cancel it, and nak NOW with a
// named reason so the work redelivers immediately instead of after ack_wait.
func (w *Worker) giveUp(ctx context.Context, st *workerState, l *lease, reason string) {
	if st.markLost(l.token) {
		st.stats.leaked.Add(1)
		w.emit(WorkerEvent{
			Kind: EventHandlerTimeout, Msg: &l.item.msg,
			Err: errors.New(reason),
		})
		// Buffered; the settler is alive until after keeperDone.
		select {
		case st.forceNaks <- forceNak{token: l.token, reason: reason}:
		case <-ctx.Done():
		}
	}
	l.cancel(ErrLeaseLost)
}
