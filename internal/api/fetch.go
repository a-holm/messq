// SPDX-License-Identifier: Apache-2.0

package api

import (
	"math/rand/v2"
	"net/http"
	"strconv"
	"time"

	"github.com/a-holm/messq/internal/errs"
	"github.com/a-holm/messq/internal/queue"
	"github.com/a-holm/messq/internal/store"
	"github.com/a-holm/messq/internal/subject"
)

// The long-poll fetch (issue §6): one response shape always — 200 with a possibly
// empty messages[] — hold_reason straight from the store's frozen set, effective
// clamps echoed, and seven decisions that make parking correct rather than merely
// plausible. The handler never blocks without a way out: its wake channel, its
// deadline timer, the client's context and the server's closing channel.

// fetchRequest is the POST .../fetch JSON body. Query parameters are refused: two
// ways to say the same thing is the bug factory #7 already refused for subject.
type fetchRequest struct {
	Batch    int   `json:"batch"`
	MaxBytes int64 `json:"max_bytes"`
	WaitMS   int64 `json:"wait_ms"`
}

// fetchResponse is THE fetch wire shape (frozen; NDJSON negotiation is post-1.0).
type fetchResponse struct {
	Messages     []store.Delivered `json:"messages"`
	HoldReason   string            `json:"hold_reason"`
	RetryAfterMS int64             `json:"retry_after_ms"`
	Pending      int64             `json:"pending"`
	Backlog      int64             `json:"backlog"`

	Batch    int   `json:"batch"` // EFFECTIVE values echoed, never the request's
	MaxBytes int64 `json:"max_bytes"`
	WaitMS   int64 `json:"wait_ms"`
}

// handleFetchConsumer is POST /v1/streams/{stream}/consumers/{consumer}/fetch.
func (s *Server) handleFetchConsumer(w http.ResponseWriter, r *http.Request) {
	stream, consumer := r.PathValue("stream"), r.PathValue("consumer")
	if err := queue.ValidateExistingStreamName(stream); err != nil {
		s.writeError(w, err)
		return
	}
	if err := queue.ValidateConsumerName(consumer); err != nil {
		s.writeError(w, err)
		return
	}
	if r.URL.RawQuery != "" {
		s.writeError(w, errs.E(errs.ErrBadRequest, "api.fetch",
			"fetch takes parameters in the JSON body only; got query %q", r.URL.RawQuery))
		return
	}

	req, err := decodeJSON[fetchRequest](w, r, s.cfg.MaxRequestBytes)
	if err != nil {
		s.writeError(w, err)
		return
	}
	if req.WaitMS < 0 {
		s.writeError(w, errs.E(errs.ErrBadRequest, "api.fetch",
			"wait_ms is %d, want >= 0", req.WaitMS))
		return
	}

	limits := s.store.ConsumerLimits()
	effective := clampFetch(req, limits, s.cfg.MaxFetchWait)

	// The consumer must exist (404 otherwise) and its filters are the wake snapshot.
	info, err := s.store.GetConsumer(r.Context(), stream, consumer)
	if err != nil {
		// serve --dev: fetching from a consumer nobody created yet auto-creates
		// it with schema defaults (issue #26 §2).
		if !s.cfg.Dev || !isNotFound(err) {
			s.writeError(w, errs.WithNext(err,
				"messq consumer add "+stream+" <name>"))
			return
		}
		if aErr := s.devAutocreateConsumer(r.Context(), stream, consumer); aErr != nil {
			s.writeError(w, aErr)
			return
		}
		if info, err = s.store.GetConsumer(r.Context(), stream, consumer); err != nil {
			s.writeError(w, err)
			return
		}
	}
	filters, ferr := subject.ParseSet(info.Filters)
	if ferr != nil {
		s.writeError(w, errs.E(errs.ErrBadRequest, "api.fetch",
			"consumer filters do not parse: %v", ferr))
		return
	}

	wait := time.Duration(effective.WaitMS) * time.Millisecond
	deadline := s.clk.Now().Add(wait)

	sub, err := s.waiters.Subscribe(queue.ConsumerKey{Stream: stream, Consumer: consumer}, filters)
	if err != nil {
		// Over --max-waiters or --max-waiters-per-consumer: typed 503 + Retry-After.
		s.writeError(w, errs.WithCode(err, string(CodeTooManyWaiters)))
		return
	}
	defer sub.Close()

	// Decision 7: no global WriteTimeout — THIS request owns its deadline via the
	// ResponseController chain (the wrapper implements Unwrap).
	if rcErr := http.NewResponseController(w).SetWriteDeadline(deadline.Add(writeSlack)); rcErr != nil {
		s.logger.Warn("fetch: set write deadline", "err", rcErr)
	}

	for attempt := 0; ; attempt++ {
		submitCtx, cancel := s.submitCtx(r.Context())
		res, ferr := s.store.Fetch(submitCtx, store.FetchReq{
			Stream:   stream,
			Consumer: consumer,
			Batch:    effective.Batch,
			MaxBytes: effective.MaxBytes,
		})
		cancel()
		if ferr != nil {
			s.writeError(w, s.classifySubmit("api.fetch", ferr))
			return
		}

		now := s.clk.Now()
		switch {
		case len(res.Messages) > 0, // work
			res.Hold == store.HoldPaused,      // parking cannot help (#9): nothing to publish into it
			res.Hold == store.HoldFlowControl, // ditto: max_ack_pending is caller-owned
			!now.Before(deadline):             // wait budget spent — one shape, empty messages
			s.writeFetch(w, res, effective, "")
			return
		}

		sleepFor := s.parkDuration(now, deadline, res, attempt)
		timer := s.clk.NewTimer(sleepFor)
		select {
		case _, ok := <-sub.C():
			timer.Stop()
			if !ok {
				// ReleaseAll closed the registry under us: shutdown drain (#17).
				s.writeFetch(w, res, effective, holdShuttingDown)
				return
			}
		case <-timer.C():
		case <-r.Context().Done():
			timer.Stop()
			return // client gone while parked; nothing was claimed, defer releases
		case <-s.closing:
			timer.Stop()
			s.writeFetch(w, res, effective, holdShuttingDown)
			return
		}
	}
}

// holdShuttingDown is the wire hold_reason for a fetch released by shutdown. It is an
// API-level state — the store's HoldReason set has no such value because no STORE
// operation is shutting down.
const holdShuttingDown = "shutting_down"

// writeSlack is the extra time a parked fetch allows for writing the response after
// its wait budget ends (slow peer, big batch).
const writeSlack = 5 * time.Second

// clampedFetch is the effective request after server caps.
type clampedFetch struct {
	Batch    int
	MaxBytes int64
	WaitMS   int64
}

// clampFetch applies every cap and echoes what will ACTUALLY be used (G12): a clamp
// nobody sees is how wrong dashboards get built. batch omitted/0 → 1.
func clampFetch(req fetchRequest, limits queue.ConsumerLimits, maxWait time.Duration) clampedFetch {
	out := clampedFetch(req)
	if out.Batch <= 0 {
		out.Batch = 1
	}
	if out.Batch > limits.MaxFetchBatch {
		out.Batch = limits.MaxFetchBatch
	}
	if out.MaxBytes <= 0 {
		out.MaxBytes = limits.FetchMaxBytes
	}
	if out.MaxBytes > limits.FetchMaxBytes {
		out.MaxBytes = limits.FetchMaxBytes
	}
	if out.WaitMS > maxWait.Milliseconds() {
		out.WaitMS = maxWait.Milliseconds()
	}
	return out
}

// parkDuration decides decision 4 (NextVisibleAt caps the sleep) and decision 5 (the
// empty-wake damper, never on the first iteration). Jitter is ±20% so fleets of
// workers do not re-fetch in lockstep.
func (s *Server) parkDuration(now, deadline time.Time, res store.FetchResult, attempt int) time.Duration {
	d := deadline.Sub(now)
	if d <= 0 {
		return 0
	}
	if res.NextVisibleAtMS > 0 {
		if next := time.UnixMilli(res.NextVisibleAtMS); next.After(now) && next.Before(deadline) {
			d = next.Sub(now)
		}
	}
	if attempt > 0 && s.cfg.FetchEmptyDamper > 0 {
		jittered := float64(s.cfg.FetchEmptyDamper) * (0.8 + 0.4*rand.Float64())
		damp := min(time.Duration(jittered), d)
		if damp < d {
			d = damp
		}
	}
	return d
}

// writeFetch renders the one response shape and mirrors the shell-worker headers.
// holdOverride ("" = derive from res.Hold) carries API-level states like
// shutting_down that the store's closed HoldReason set deliberately lacks.
func (s *Server) writeFetch(w http.ResponseWriter, res store.FetchResult, eff clampedFetch, holdOverride string) {
	hold := holdWire(res.Hold)
	if holdOverride != "" {
		hold = holdOverride
	}
	body := fetchResponse{
		Messages:     res.Messages,
		HoldReason:   hold,
		RetryAfterMS: retryAfterMS(s.clk.Now(), res),
		Pending:      res.Pending,
		Backlog:      res.Backlog,
		Batch:        eff.Batch,
		MaxBytes:     eff.MaxBytes,
		WaitMS:       eff.WaitMS,
	}
	w.Header().Set("Messq-Pending", strconv.FormatInt(res.Pending, 10))
	w.Header().Set("Messq-Backlog", strconv.FormatInt(res.Backlog, 10))
	w.Header().Set("Messq-Hold-Reason", body.HoldReason)
	if len(body.Messages) == 0 {
		body.Messages = []store.Delivered{} // an array, never null
	}
	s.writeJSON(w, http.StatusOK, body)
}

// holdWire maps the closed store.HoldReason set onto the wire value WITHOUT a default
// arm: when the store grows a new hold reason this fails to compile until someone
// decides what it means on the wire.
func holdWire(h store.HoldReason) string {
	switch h {
	case store.HoldNone:
		return ""
	case store.HoldPaused:
		return string(store.HoldPaused)
	case store.HoldFlowControl:
		return string(store.HoldFlowControl)
	case store.HoldBackoff:
		return string(store.HoldBackoff)
	case store.HoldCatchingUp:
		return string(store.HoldCatchingUp)
	case store.HoldEmpty:
		return string(store.HoldEmpty)
	default:
		// Unreachable for every current value; kept as a plain panic-free fallback so
		// a future value surfaces as data review, not as a 500. The exhaustive linter
		// still forces the new case above because default-signifies-exhaustive is off.
		return string(h)
	}
}

// retryAfterMS gives holds where waiting again makes sense a concrete hint: when the
// next claimable instant is known (backoff/catching_up), the milliseconds until it;
// otherwise zero ("no server guidance"). No default arm: a new hold reason must be
// classified here on purpose.
func retryAfterMS(now time.Time, res store.FetchResult) int64 {
	switch res.Hold {
	case store.HoldBackoff:
		if res.NextVisibleAtMS > now.UnixMilli() {
			return res.NextVisibleAtMS - now.UnixMilli()
		}
	case store.HoldCatchingUp:
		if res.NextVisibleAtMS > now.UnixMilli() {
			return res.NextVisibleAtMS - now.UnixMilli()
		}
	case store.HoldNone, store.HoldPaused, store.HoldFlowControl, store.HoldEmpty:
		return 0
	}
	return 0
}
