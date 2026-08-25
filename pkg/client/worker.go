// SPDX-License-Identifier: Apache-2.0

package client

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"runtime"
	"sync"
	"sync/atomic"
	"time"
)

// The Worker (issue §7): fetch → handle → settle with a CENTRAL lease keeper issuing
// batched extends at ack_wait/2. Goroutine census during Run is fixed at
// 3 + Concurrency beyond the caller: one fetcher, one keeper, one settler, N handlers
// — no goroutine and no timer per message.

// Handler processes one delivered message.
//
//	nil                  → the message is acked
//	wrapped by Permanent → termed straight to DLQ, reason = err
//	wrapped by RetryAfter→ nakked with that explicit delay
//	any other error      → nakked; the consumer's backoff schedule applies
//	panic                → recovered per OnPanic (default: nak + keep running)
type Handler func(ctx context.Context, m *Delivered) error

// ErrPermanent marks a handler failure as permanent: the delivery dead-letters via
// Term instead of retrying. Wrap causes with Permanent to carry them along.
var ErrPermanent = errors.New("messq: permanent handler failure")

// Permanent wraps err so errors.Is(err, ErrPermanent) is true while the original
// cause stays unwrappable.
func Permanent(err error) error {
	return fmt.Errorf("%w: %w", ErrPermanent, err)
}

// RetryAfterError carries an explicit nak delay past the handler boundary.
type RetryAfterError struct {
	Delay time.Duration
	Cause error
}

func (e *RetryAfterError) Error() string {
	if e.Cause != nil {
		return e.Cause.Error()
	}
	return "retry after " + e.Delay.String()
}

func (e *RetryAfterError) Unwrap() error { return e.Cause }

// RetryAfter returns an error that makes the Worker nak with exactly d of visibility
// delay (T4), overriding the consumer's backoff schedule for this one release.
func RetryAfter(d time.Duration, err error) error {
	return &RetryAfterError{Delay: d, Cause: err}
}

// LeasePolicy decides what happens to a handler whose lease is gone. CancelHandler
// (the default and today only policy) cancels the handler's context with cause
// [ErrLeaseLost]; the handler's eventual result is discarded, never settled (§7.5).
type LeasePolicy uint8

const CancelHandler LeasePolicy = iota

// PanicPolicy decides what a recovered handler panic does.
type PanicPolicy uint8

const (
	NakAndContinue PanicPolicy = iota // default: nak "panic: …", worker keeps running
	TermOnPanic                       // term straight to DLQ
	Repanic                           // propagate: the process dies loudly
)

// BackoffConfig is the reconnect/hold backoff: exponential ×2 from Initial to Max,
// ALWAYS ±20 % jittered (§5.1's rule applied client-side).
type BackoffConfig struct {
	Initial time.Duration // default 100ms
	Max     time.Duration // default 30s
}

func (b BackoffConfig) withDefaults() BackoffConfig {
	if b.Initial <= 0 {
		b.Initial = defaultBackoffInitial
	}
	if b.Max < b.Initial {
		b.Max = defaultBackoffMax
	}
	return b
}

// next returns the jittered backoff after n consecutive failures.
func (b BackoffConfig) next(n int) time.Duration {
	d := b.Initial
	for range min(n, 16) {
		d *= 2
		if d >= b.Max {
			return jitter(b.Max)
		}
	}
	return jitter(d)
}

// jitter applies ±20% multiplicative jitter.
func jitter(d time.Duration) time.Duration {
	if d <= 0 {
		return 0
	}
	f := 1 - backoffJitter + 2*backoffJitter*rand.Float64() // [0.8, 1.2]
	return time.Duration(float64(d) * f)
}

// WorkerConfig configures [Client.NewWorker]. Every field has a defensible zero value.
type WorkerConfig struct {
	Stream, Consumer string

	Concurrency int           // default 1
	Batch       int           // default 0 ⇒ min(free slots, server cap); never more than FREE slots
	Wait        time.Duration // long-poll wait, default 30s
	Prefetch    bool          // default false; relaxes the free-slot bound (keeper heartbeats queued leases)

	ExtendAt float64       // fraction of ack_wait for the extend schedule; default 0.5
	MaxLease time.Duration // stop extending past this total hold; default 0 ⇒ server cap (T7, 1h)

	OnLeaseLost LeasePolicy // CancelHandler (default)
	OnPanic     PanicPolicy // NakAndContinue (default)

	AckWindow    time.Duration // settle coalescing window; default 5ms; 0 disables batching
	AckRetries   int           // retries for an ack lost to transport failure; default 3 (T3a: repeats are safe)
	Backoff      BackoffConfig // 100ms → 30s ×2 ±20%
	DrainTimeout time.Duration // default 30s
	FailFast     bool          // a deleted consumer turns fatal instead of backing off

	OnEvent func(WorkerEvent)
	Logger  *slog.Logger // optional; nil ⇒ silent
}

func (cfg WorkerConfig) withDefaults() WorkerConfig {
	if cfg.Concurrency <= 0 {
		cfg.Concurrency = 1
	}
	if cfg.Wait == 0 {
		cfg.Wait = defaultFetchWait
	}
	if cfg.ExtendAt <= 0 || cfg.ExtendAt >= 1 {
		cfg.ExtendAt = 0.5
	}
	if cfg.AckRetries == 0 {
		cfg.AckRetries = 3
	}
	if cfg.DrainTimeout == 0 {
		cfg.DrainTimeout = defaultFetchWait
	}
	cfg.Backoff = cfg.Backoff.withDefaults()
	return cfg
}

// Worker runs the fetch→handle→settle loop for one stream/consumer pair.
// Construct with [Client.NewWorker]; Run is mutually exclusive per Worker value.
type Worker struct {
	client    *Client
	cfg       WorkerConfig
	st        *workerState
	drainOnce sync.Once
	finished  chan struct{}
}

// NewWorker validates the configuration against this client's daemon address.
func (c *Client) NewWorker(cfg WorkerConfig) (*Worker, error) {
	if err := validStreamName(cfg.Stream); err != nil {
		return nil, err
	}
	if err := validConsumerName(cfg.Consumer); err != nil {
		return nil, err
	}
	return &Worker{client: c, cfg: cfg.withDefaults(), finished: make(chan struct{})}, nil
}

type settleAction struct {
	kind   byte // 'a' ack · 'n' nak · 't' term · 0 nothing (lease was lost)
	delay  time.Duration
	reason string
}

type outcome struct {
	item *workItem
	act  settleAction
}

type workItem struct {
	msg   Delivered
	ctx   context.Context
	start time.Time
}

type forceNak struct {
	token  string
	reason string
}

// workerState carries the channels shared by Run's goroutines.
type workerState struct {
	slotsUsed      atomic.Int64
	slotNotify     chan struct{}
	work           chan *workItem
	outcomes       chan outcome
	addLease       chan *lease
	doneToken      chan string
	forceNaks      chan forceNak
	stopFetch      chan struct{}
	keeperDone     chan struct{}
	outcomesClosed chan struct{}
	handlersLeft   atomic.Int64
	handlersDone   chan struct{}

	lostMu sync.Mutex
	lost   map[string]bool // tokens whose lease died: their outcomes are discarded

	stats workerStatsCounters

	clockSkewOnce sync.Once
	settlerDone   chan struct{}
}

type workerStatsCounters struct {
	fetched, acked, naked, termed, extends, leaselost, staleacks, reconnects, leaked atomic.Int64
}

func (w *Worker) emit(e WorkerEvent) {
	if w.cfg.OnEvent != nil {
		w.cfg.OnEvent(e)
	}
	if w.cfg.Logger != nil {
		if e.Err != nil {
			w.cfg.Logger.Warn("messq worker", "event", e.Kind.String(), "err", e.Err.Error())
		} else {
			w.cfg.Logger.Info("messq worker", "event", e.Kind.String())
		}
	}
}

func (s *workerState) markLost(token string) bool {
	s.lostMu.Lock()
	defer s.lostMu.Unlock()
	if s.lost[token] {
		return false
	}
	s.lost[token] = true
	return true
}

func (s *workerState) isLost(token string) bool {
	s.lostMu.Lock()
	defer s.lostMu.Unlock()
	return s.lost[token]
}

// finishItem releases the handler slot and retires the token with the keeper.
func (s *workerState) finishItem(prefetch bool, it *workItem) {
	if !prefetch {
		s.slotsUsed.Add(-1)
		select {
		case s.slotNotify <- struct{}{}:
		default:
		}
	}
	s.doneToken <- it.msg.AckToken
}

// Stats snapshots the counters (#31 samples them).
func (w *Worker) Stats() WorkerStats {
	c := &w.st.stats
	return WorkerStats{
		Fetched:        c.fetched.Load(),
		Acked:          c.acked.Load(),
		Naked:          c.naked.Load(),
		Termed:         c.termed.Load(),
		Extends:        c.extends.Load(),
		LeaseLost:      c.leaselost.Load(),
		StaleAcks:      c.staleacks.Load(),
		Reconnects:     c.reconnects.Load(),
		LeakedHandlers: c.leaked.Load(),
	}
}

// Run blocks until ctx is done, Drain finishes the worker, or a fatal error occurs.
// A handler that outlives its lease's death is NOT killed (Go cannot); it is reported
// via LeakedHandlers and its late result is discarded.
func (w *Worker) Run(ctx context.Context, h Handler) (rerr error) {
	if h == nil {
		return &Error{Code: "config_error", Message: "Run needs a non-nil Handler", err: ErrConfig}
	}
	defer close(w.finished)

	runCtx, cancelRun := context.WithCancel(ctx)
	defer cancelRun()

	info, err := w.preflight(runCtx)
	if err != nil {
		return err
	}

	N := int64(w.cfg.Concurrency)
	st := &workerState{
		slotNotify:     make(chan struct{}, 1),
		work:           make(chan *workItem),
		outcomes:       make(chan outcome, N*4),
		addLease:       make(chan *lease, N*4),
		doneToken:      make(chan string, N*8),
		forceNaks:      make(chan forceNak, N*2),
		stopFetch:      make(chan struct{}),
		keeperDone:     make(chan struct{}),
		outcomesClosed: make(chan struct{}),
		handlersDone:   make(chan struct{}),
		lost:           map[string]bool{},
		settlerDone:    make(chan struct{}),
	}
	st.handlersLeft.Store(N)
	w.st = st

	var wg sync.WaitGroup
	wg.Add(int(N))
	for range int(N) {
		go func() {
			defer func() {
				if st.handlersLeft.Add(-1) == 0 {
					close(st.handlersDone)
				}
				wg.Done()
			}()
			w.handlerLoop(runCtx, h, st)
		}()
	}
	go func() {
		w.settlerLoop(runCtx, st)
	}()
	go func() {
		defer close(st.keeperDone)
		w.keeperLoop(runCtx, st)
	}()

	fetchErr := w.fetcherLoop(runCtx, info, st)
	close(st.work) // handlers finish what was dispatched, then exit

	select {
	case <-st.handlersDone:
	case <-st.keeperDone:
		// The drain deadline passed with handlers still running: they are leaked,
		// already nakked, and their late outcomes will be discarded below.
	case <-runCtx.Done():
	}
	cancelRun() // abort any straggling outcome sends
	close(st.outcomesClosed)
	<-st.settlerDone
	<-st.keeperDone

	w.emit(WorkerEvent{Kind: EventDrained})
	if fetchErr != nil {
		return fetchErr
	}
	return ctx.Err()
}

// Drain stops fetching, lets in-flight handlers finish (extending throughout),
// settles what completed, and naks whatever remains with reason="worker draining".
// It returns nil once the worker shut down, or an error if the drain budget ran out.
func (w *Worker) Drain(ctx context.Context) error {
	if w.st == nil {
		return &Error{Code: "config_error", Message: "Drain on a worker that never ran", err: ErrConfig}
	}
	w.drainOnce.Do(func() { close(w.st.stopFetch) })
	timer := w.client.clk.NewTimer(w.cfg.DrainTimeout)
	defer timer.Stop()
	select {
	case <-w.finished:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C():
		return errors.New("messq: drain budget exhausted; leftover handlers were nakked and reported")
	}
}

// preflight issues ONE GetConsumer: existence (a clear not_found beats an infinite
// 404 loop) and max_ack_pending for the concurrency warning. Failures retry with
// backoff — the daemon may simply not be up yet.
func (w *Worker) preflight(ctx context.Context) (ConsumerView, error) {
	backoffs := 0
	for {
		info, err := w.client.GetConsumer(ctx, w.cfg.Stream, w.cfg.Consumer)
		if err == nil {
			e := WorkerEvent{Kind: EventStarted}
			if w.cfg.Concurrency > int(info.MaxAckPending) {
				e.Err = fmt.Errorf(
					"Concurrency %d exceeds max_ack_pending %d; raise it with `messq consumer edit` "+
						"or the worker will idle on flow_control", w.cfg.Concurrency, info.MaxAckPending)
			}
			w.emit(e)
			return info, nil
		}
		if w.cfg.FailFast && errors.Is(err, ErrNotFound) {
			return ConsumerView{}, err
		}
		d := w.cfg.Backoff.next(backoffs)
		backoffs++
		if !wait(ctx, w.client.clk, d) {
			return ConsumerView{}, &Error{Code: "unavailable", Message: "preflight cancelled", Status: 0, err: ctx.Err()}
		}
	}
}

// batchSize bounds the fetch by FREE handler slots (Decision 3): with Concurrency 1
// and Batch 64 a naive worker claims 64, works one, and lets 63 leases expire.
func (w *Worker) batchSize(freeSlots int64) int {
	want := w.cfg.Batch
	if want <= 0 {
		want = DefaultPublishBatchCap // effectively unbounded; the slot bound decides
	}
	if w.cfg.Prefetch {
		return want
	}
	return int(min(int64(want), max(freeSlots, 0)))
}

func (w *Worker) fetcherLoop(ctx context.Context, info ConsumerView, st *workerState) error {
	consecutiveFails := 0
	for {
		select {
		case <-st.stopFetch:
			return nil
		case <-ctx.Done():
			return nil
		default:
		}

		batch := w.batchSize(int64(w.cfg.Concurrency) - st.slotsUsed.Load())
		if !w.cfg.Prefetch && batch <= 0 {
			// All slots busy: wait for a release signal instead of hot-looping.
			select {
			case <-st.slotNotify:
				continue
			case <-st.stopFetch:
				return nil
			case <-ctx.Done():
				return nil
			}
		}

		res, err := w.client.Fetch(ctx, w.cfg.Stream, w.cfg.Consumer, FetchRequest{Batch: batch, Wait: w.cfg.Wait})
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			if w.cfg.FailFast && errors.Is(err, ErrNotFound) {
				return err
			}
			consecutiveFails++
			st.stats.reconnects.Add(1)
			w.emit(WorkerEvent{Kind: EventReconnect, Err: err, Attempt: consecutiveFails})
			if !wait(ctx, w.client.clk, w.cfg.Backoff.next(consecutiveFails-1)) {
				return nil
			}
			continue
		}
		consecutiveFails = 0

		if len(res.Messages) == 0 {
			if !w.holdSleep(ctx, res.Hold, res.RetryAfter, info) {
				return nil
			}
			continue
		}

		st.stats.fetched.Add(int64(len(res.Messages)))
		w.emit(WorkerEvent{Kind: EventFetched, Attempt: len(res.Messages)})
		if !w.cfg.Prefetch {
			st.slotsUsed.Add(int64(len(res.Messages)))
		}
		for i := range res.Messages {
			m := res.Messages[i]
			itemCtx, cancel := context.WithCancelCause(ctx)
			it := &workItem{msg: m, ctx: itemCtx, start: w.client.clk.Now()}
			w.trackClockSkew(&m, st)
			st.addLease <- newLease(w.cfg, it, w.client.clk, cancel)
			select {
			case st.work <- it:
			case <-ctx.Done():
				return nil
			}
		}
	}
}

// newLease anchors the deadline LOCALLY (monotonic clock at response receipt), never
// on the server wall-clock deadline_ms — that one is a skew detector only (§7.3).
func newLease(cfg WorkerConfig, it *workItem, clk Clock, cancel context.CancelCauseFunc) *lease {
	now := clk.Now()
	ackWait := time.Duration(it.msg.AckWaitMS) * time.Millisecond
	l := &lease{
		token:         it.msg.AckToken,
		item:          it,
		cancel:        cancel,
		ackWait:       ackWait,
		localDeadline: now.Add(ackWait),
	}
	margin := extendMargin(ackWait)
	next := now.Add(time.Duration(float64(ackWait) * cfg.ExtendAt))
	if cap := l.localDeadline.Add(-margin); next.After(cap) {
		next = cap
	}
	l.nextExtendAt = next
	return l
}

// extendMargin covers one round trip plus the server's 250 ms sweeper tick: an extend
// racing the sweeper is a coin flip that costs a duplicate delivery.
func extendMargin(ackWait time.Duration) time.Duration {
	switch m := ackWait / 10; {
	case m < 500*time.Millisecond:
		return 500 * time.Millisecond
	case m > 5*time.Second:
		return 5 * time.Second
	default:
		return m
	}
}

func (w *Worker) trackClockSkew(m *Delivered, st *workerState) {
	if m.DeadlineMS == 0 || m.AckWaitMS == 0 {
		return
	}
	delta := m.DeadlineMS - w.client.clk.Now().UnixMilli()
	halfWait := m.AckWaitMS / 2
	if delta > halfWait || -delta > halfWait {
		st.clockSkewOnce.Do(func() {
			w.emit(WorkerEvent{
				Kind: EventClockSkew,
				Msg:  m,
				Err: fmt.Errorf("broker deadline_ms differs from the local deadline by %d ms; "+
					"scheduling uses the LOCAL monotonic clock only", delta),
			})
		})
	}
}

// holdSleep implements the hold matrix (issue §7.4): every reason produces documented
// backoff instead of a hot loop. Returns false when ctx ended.
func (w *Worker) holdSleep(ctx context.Context, hold HoldReason, retryAfter time.Duration, info ConsumerView) bool {
	var d time.Duration
	switch hold {
	case HoldNone, HoldEmpty:
		return true // immediate re-fetch; the long poll already did the waiting
	case HoldPaused:
		// An idle worker whose consumer someone paused must be discoverable: event EVERY time.
		d = min(max(retryAfter, time.Second), 5*time.Second)
		w.emit(WorkerEvent{
			Kind: EventHold, Hold: hold,
			Err: fmt.Errorf("consumer %s/%s is paused", w.cfg.Stream, w.cfg.Consumer),
		})
	case HoldFlowControl:
		d = jitter(max(retryAfter, 100*time.Millisecond))
		w.emit(WorkerEvent{
			Kind: EventHold, Hold: hold,
			Err: fmt.Errorf("flow control (max_ack_pending %d)", info.MaxAckPending),
		})
	case HoldBackoff, HoldCatchingUp:
		// The server knows when the next row becomes visible; honour its hint exactly.
		d = retryAfter
	case HoldShuttingDown:
		d = w.cfg.Backoff.next(1) // the daemon drains; systemd brings it back (#17)
	default:
		// Unknown future hold reason: treat like backoff-with-hint, never spin.
		d = max(retryAfter, w.cfg.Backoff.Initial)
	}
	if d <= 0 {
		return true
	}
	return wait(ctx, w.client.clk, d)
}

func (w *Worker) handlerLoop(ctx context.Context, h Handler, st *workerState) {
	for it := range st.work {
		w.emit(WorkerEvent{Kind: EventHandling, Msg: &it.msg, Attempt: it.msg.Attempt})
		act := w.runHandler(ctx, it, h, st)
		select {
		case st.outcomes <- outcome{item: it, act: act}:
		case <-ctx.Done():
			// The worker shut down around us (drain deadline, external cancel);
			// the keeper already resolved this token one way or another.
			return
		}
	}
}

// runHandler calls h with panic recovery per OnPanic and maps the result onto a
// settle action — the exhaustive mapping of issue §7.2.
func (w *Worker) runHandler(ctx context.Context, it *workItem, h Handler, _ *workerState) (act settleAction) {
	started := w.client.clk.Now()
	err := func() (herr error) {
		defer func() {
			r := recover()
			if r == nil {
				return
			}
			buf := make([]byte, 4096)
			n := runtime.Stack(buf, false)
			w.emit(WorkerEvent{
				Kind:     EventPanicRecovered,
				Msg:      &it.msg,
				Err:      fmt.Errorf("panic: %v", r),
				Duration: w.client.clk.Now().Sub(started),
				Stack:    buf[:n],
			})
			switch w.cfg.OnPanic {
			case TermOnPanic:
				herr = Permanent(fmt.Errorf("panic: %v", r))
			case Repanic:
				panic(r)
			default: // NakAndContinue
				herr = fmt.Errorf("panic: %v", r)
			}
		}()
		return h(it.ctx, &it.msg)
	}()

	var actErr error
	switch {
	case err == nil:
		return settleAction{kind: 'a'}
	case errors.Is(err, ErrPermanent):
		// The reason that reaches trace/DLQ is the CAUSE's sentence, not our wrapper.
		return settleAction{kind: 't', reason: truncateReason(causeText(err))}
	case errors.Is(err, ErrLeaseLost):
		return settleAction{kind: 0} // nothing may be settled for a lost lease (§7.5)
	default:
		var ra *RetryAfterError
		if errors.As(err, &ra) {
			return settleAction{kind: 'n', delay: ra.Delay, reason: truncateReason(causeText(err))}
		}
		actErr = err
	}
	_ = actErr
	return settleAction{kind: 'n', reason: truncateReason(causeText(err))}
}

// causeText renders the INNERMOST error's message: wrappers are client machinery;
// the reason budget is spent on the user's sentence.
func causeText(err error) string {
	for {
		switch x := err.(type) {
		case interface{ Unwrap() error }:
			err = x.Unwrap()
		case interface{ Unwrap() []error }:
			un := x.Unwrap()
			err = un[len(un)-1]
		default:
			return err.Error()
		}
	}
}

func (w *Worker) settlerLoop(ctx context.Context, st *workerState) {
	var pendingAcks []string
	ackTimer := w.client.clk.NewTimer(w.ackWindowOrMax())
	defer ackTimer.Stop()

	flush := func() {
		if len(pendingAcks) == 0 {
			return
		}
		tokens := pendingAcks
		pendingAcks = nil
		w.settleAcks(ctx, tokens, st)
	}

	handle := func(oc outcome) {
		tok := oc.item.msg.AckToken
		if oc.act.kind == 0 || st.isLost(tok) {
			// Lease gone (or nothing to do): the outcome is dropped, never settled.
			if oc.act.kind != 0 {
				w.emit(WorkerEvent{Kind: EventOutcomeDiscarded, Msg: &oc.item.msg})
			}
			st.finishItem(w.cfg.Prefetch, oc.item)
			return
		}
		switch oc.act.kind {
		case 'a':
			pendingAcks = append(pendingAcks, tok)
			if w.cfg.AckWindow <= 0 || len(pendingAcks) >= maxSettleTokensPerRequest {
				flush()
			}
		case 'n':
			opts := []SettleOption{WithReason(oc.act.reason)}
			if oc.act.delay > 0 {
				opts = append(opts, WithDelay(oc.act.delay))
			}
			if _, err := w.client.Nak(ctx, tok, opts...); err == nil {
				st.stats.naked.Add(1)
				w.emit(WorkerEvent{Kind: EventNaked, Msg: &oc.item.msg})
			}
			st.finishItem(w.cfg.Prefetch, oc.item)
		case 't':
			if _, err := w.client.Term(ctx, tok, oc.act.reason); err == nil {
				st.stats.termed.Add(1)
				w.emit(WorkerEvent{Kind: EventTermed, Msg: &oc.item.msg})
			}
			st.finishItem(w.cfg.Prefetch, oc.item)
		}
	}

	for {
		select {
		case oc := <-st.outcomes:
			handle(oc)

		case fn := <-st.forceNaks:
			// A sanctioned settle for a token the WORKER gave up on: bypasses the
			// lost-set discard path deliberately (the keeper marked it lost first,
			// so the handler's own late outcome still gets discarded).
			if _, err := w.client.Nak(ctx, fn.token, WithReason(fn.reason)); err == nil {
				st.stats.naked.Add(1)
			}

		case <-ackTimer.C():
			flush()
			ackTimer.Reset(w.ackWindowOrMax())

		case <-ctx.Done():
			flush()
			close(st.settlerDone)
			return

		case <-st.outcomesClosed:
			// Handlers are done: drain whatever is buffered, flush, stop.
			for {
				select {
				case oc := <-st.outcomes:
					handle(oc)
				default:
					flush()
					close(st.settlerDone)
					return
				}
			}
		}
	}
}

func (w *Worker) ackWindowOrMax() time.Duration {
	if w.cfg.AckWindow <= 0 {
		return time.Hour // batching disabled: flush driven by size/closure only
	}
	return w.cfg.AckWindow
}

// settleAcks sends one batched POST /v1/ack, retrying transport losses up to
// AckRetries (T3a makes repeat acks idempotent successes, so retrying is always safe).
func (w *Worker) settleAcks(ctx context.Context, tokens []string, st *workerState) {
	var err error
	for attempt := 0; attempt <= w.cfg.AckRetries; attempt++ {
		_, err = w.client.Ack(ctx, tokens...)
		if err == nil {
			break
		}
		if Classify(err) != KindUnavailable {
			break // a real verdict (e.g. all stale): report, don't retry blind
		}
		if !wait(ctx, w.client.clk, w.cfg.Backoff.next(attempt)) {
			break
		}
	}
	if err != nil {
		if Classify(err) == KindUnavailable {
			for range tokens {
				w.emit(WorkerEvent{Kind: EventAckLost, Err: err})
			}
		} else if Classify(err) == KindConflict {
			st.stats.staleacks.Add(int64(len(tokens)))
		}
	} else {
		st.stats.acked.Add(int64(len(tokens)))
		for _, t := range tokens {
			w.emit(WorkerEvent{Kind: EventAcked})
			st.doneToken <- t
		}
		for range tokens {
			st.releaseSlotForPrefetch()
		}
		return
	}
	for _, t := range tokens {
		st.doneToken <- t
	}
	for range tokens {
		st.releaseSlotForPrefetch()
	}
}

func (s *workerState) releaseSlotForPrefetch() {
	// Ack batches release their slots through finishItem's counterpart: slots were
	// taken per message at dispatch, so each settled token frees exactly one.
	if s.slotsUsed.Load() > 0 {
		s.slotsUsed.Add(-1)
		select {
		case s.slotNotify <- struct{}{}:
		default:
		}
	}
}
