// SPDX-License-Identifier: Apache-2.0

package api

import (
	"errors"
	"fmt"
	"net/http"
	"time"

	"github.com/a-holm/messq/internal/errs"
	"github.com/a-holm/messq/internal/queue"
	"github.com/a-holm/messq/internal/store"
)

// The provisional consumer routes (issue #9 §9): consumer create-or-update, list, get,
// patch, name-confirmed delete, and a single-shot fetch. #14 replaces this data plane
// with the closed error enum and long-poll; until then these routes settle the wire
// shapes #10 builds against.

// unsupportedError refuses a delivery-semantics feature that is not implemented yet.
// Accepting and ignoring one would be a correctness lie, so the request names the
// issue that owns the feature instead.
type unsupportedError struct {
	field string
	issue string
}

func (e *unsupportedError) Error() string {
	return e.field + " is not supported yet (issue " + e.issue + ")"
}
func (e *unsupportedError) Unwrap() error { return errs.ErrBadRequest }

// consumerConfigRequest is the POST /v1/streams/{s}/consumers wire shape. Durations
// travel as milliseconds. It is pre-seeded with the Go defaults so an absent field keeps
// its default; dead_policy and start default separately (both depend on the stream name
// or must round-trip through the pure parser).
type consumerConfigRequest struct {
	Name          string   `json:"name"`
	Filters       []string `json:"filters"`
	AckWaitMS     int64    `json:"ack_wait_ms"`
	MaxDeliver    int32    `json:"max_deliver"`
	MaxAckPending int64    `json:"max_ack_pending"`
	BackoffMS     []int64  `json:"backoff_ms"`
	DeadPolicy    string   `json:"dead_policy"` // "" = default (drop on a .dlq stream)
	Paused        bool     `json:"paused"`
	Start         string   `json:"start"` // "first" | "new" | "seq:N" | "time:T"
	// Ordered and MaxRate are phase-2 features (#38/#39); present values are refused.
	Ordered *bool  `json:"ordered"`
	MaxRate *int64 `json:"max_rate"`
}

// defaultConsumerConfigRequest seeds the wire shape with the same defaults the store
// hands a consumer that names nothing but itself.
func defaultConsumerConfigRequest() consumerConfigRequest {
	d := queue.DefaultConsumerConfig("")
	backoff := make([]int64, len(d.Backoff))
	for i, b := range d.Backoff {
		backoff[i] = b.Milliseconds()
	}
	return consumerConfigRequest{
		Filters:       d.Filters,
		AckWaitMS:     d.AckWait.Milliseconds(),
		MaxDeliver:    d.MaxDeliver,
		MaxAckPending: d.MaxAckPending,
		BackoffMS:     backoff,
	}
}

// config renders the wire shape as the pure layer's value type, defaulting dead_policy
// from the stream name when the request left it unset.
func (r consumerConfigRequest) config(stream string) queue.ConsumerConfig {
	dp := queue.DeadPolicy(r.DeadPolicy)
	if r.DeadPolicy == "" {
		dp = queue.DefaultDeadPolicyForStream(stream)
	}
	return queue.ConsumerConfig{
		Name:          r.Name,
		Filters:       r.Filters,
		AckWait:       time.Duration(r.AckWaitMS) * time.Millisecond,
		MaxDeliver:    r.MaxDeliver,
		MaxAckPending: r.MaxAckPending,
		Backoff:       durationsMS(r.BackoffMS),
		DeadPolicy:    dp,
		Paused:        r.Paused,
	}
}

func durationsMS(ms []int64) []time.Duration {
	out := make([]time.Duration, len(ms))
	for i, v := range ms {
		out[i] = time.Duration(v) * time.Millisecond
	}
	return out
}

// consumerPatchRequest is the sparse PATCH wire shape, mapping one-to-one onto
// store.ConsumerPatch.
type consumerPatchRequest struct {
	Filters       *[]string         `json:"filters"`
	AckWaitMS     *int64            `json:"ack_wait_ms"`
	MaxDeliver    *int32            `json:"max_deliver"`
	MaxAckPending *int64            `json:"max_ack_pending"`
	BackoffMS     *[]int64          `json:"backoff_ms"`
	DeadPolicy    *queue.DeadPolicy `json:"dead_policy"`
}

func (r consumerPatchRequest) patch() store.ConsumerPatch {
	return store.ConsumerPatch{
		Filters:       r.Filters,
		AckWaitMS:     r.AckWaitMS,
		MaxDeliver:    r.MaxDeliver,
		MaxAckPending: r.MaxAckPending,
		BackoffMS:     r.BackoffMS,
		DeadPolicy:    r.DeadPolicy,
	}
}

// createConsumerResponse wraps the created/updated consumer plus the warnings the
// validation produced, so #24 can surface them before it grows its own path. Changed
// reports whether THIS request altered stored state (declarative-upsert contract).
type createConsumerResponse struct {
	store.ConsumerInfo
	Warnings queue.Warnings `json:"warnings,omitempty"`
	Changed  bool           `json:"changed"`
}

func (s *Server) handleCreateConsumer(w http.ResponseWriter, r *http.Request) {
	stream := r.PathValue("stream")
	if err := queue.ValidateExistingStreamName(stream); err != nil {
		s.writeError(w, err)
		return
	}
	req := defaultConsumerConfigRequest()
	if err := decodeJSONInto(w, r, s.cfg.MaxRequestBytes, &req); err != nil {
		s.writeError(w, err)
		return
	}
	if req.Ordered != nil {
		s.writeError(w, &unsupportedError{field: "ordered", issue: "#38"})
		return
	}
	if req.MaxRate != nil {
		s.writeError(w, &unsupportedError{field: "max_rate", issue: "#39"})
		return
	}
	start, err := queue.ParseStartPosition(req.Start)
	if err != nil {
		s.writeError(w, err)
		return
	}
	cfg := req.config(stream)

	// Declarative upsert (issue §6): a taken name with a DIFFERENT configuration is
	// refused here, so the store never sees an update through POST. An identical
	// document still flows into the store command, whose identical-recreate fast path
	// writes nothing and emits no event — and whose immutable-start check runs even
	// when the rest of the document matches (moving a cursor belongs to seek).
	if refused := s.consumerExistsRefusal(w, r, stream, cfg); refused {
		return
	}

	res, err := s.store.CreateConsumer(r.Context(), stream, cfg, start, actorAPI)
	if err != nil {
		var imm *store.ImmutableFieldError
		if errors.As(err, &imm) {
			s.writeError(w, err, "messq seek "+stream+" "+cfg.Name+" --to <seq>")
			return
		}
		s.writeError(w, err)
		return
	}
	status := http.StatusCreated
	if !res.Created {
		status = http.StatusOK // identical re-create: no event written by the engine
	}
	s.writeJSON(w, status, createConsumerResponse{
		ConsumerInfo: res.Info,
		Warnings:     res.Warnings,
		Changed:      res.Created || res.Updated,
	})
}

func (s *Server) handleListConsumers(w http.ResponseWriter, r *http.Request) {
	stream := r.PathValue("stream")
	if err := queue.ValidateExistingStreamName(stream); err != nil {
		s.writeError(w, err)
		return
	}
	consumers, err := s.store.ListConsumers(r.Context(), stream)
	if err != nil {
		s.writeError(w, err)
		return
	}
	s.writeJSON(w, http.StatusOK, consumers)
}

func (s *Server) handleGetConsumer(w http.ResponseWriter, r *http.Request) {
	stream, consumer := r.PathValue("stream"), r.PathValue("consumer")
	if err := queue.ValidateExistingStreamName(stream); err != nil {
		s.writeError(w, err)
		return
	}
	if err := queue.ValidateConsumerName(consumer); err != nil {
		s.writeError(w, err)
		return
	}
	info, err := s.store.GetConsumer(r.Context(), stream, consumer)
	if err != nil {
		s.writeError(w, err)
		return
	}
	s.writeJSON(w, http.StatusOK, info)
}

func (s *Server) handleUpdateConsumer(w http.ResponseWriter, r *http.Request) {
	stream, consumer := r.PathValue("stream"), r.PathValue("consumer")
	if err := queue.ValidateExistingStreamName(stream); err != nil {
		s.writeError(w, err)
		return
	}
	if err := queue.ValidateConsumerName(consumer); err != nil {
		s.writeError(w, err)
		return
	}
	var req consumerPatchRequest
	if err := decodeJSONInto(w, r, s.cfg.MaxRequestBytes, &req); err != nil {
		s.writeError(w, err)
		return
	}
	// Filters gate (#15/#9): re-scoping filters strands outstanding rows against the
	// old patterns' semantics unless the caller names the permission explicitly.
	if req.Filters != nil {
		cur, gErr := s.store.GetConsumer(r.Context(), stream, consumer)
		if gErr != nil {
			s.writeError(w, gErr)
			return
		}
		if !s.checkFilterChangeRefusal(w, stream, consumer,
			cur.Filters, *req.Filters, r.URL.Query().Get("allow_filter_change") == "1") {
			return
		}
	}
	info, err := s.store.UpdateConsumer(r.Context(), stream, consumer, req.patch(), actorAPI)
	if err != nil {
		s.writeError(w, err)
		return
	}
	s.writeJSON(w, http.StatusOK, info)
}

func (s *Server) handleDeleteConsumer(w http.ResponseWriter, r *http.Request) {
	stream, consumer := r.PathValue("stream"), r.PathValue("consumer")
	if err := queue.ValidateExistingStreamName(stream); err != nil {
		s.writeError(w, err)
		return
	}
	if err := queue.ValidateConsumerName(consumer); err != nil {
		s.writeError(w, err)
		return
	}
	if !s.checkDryRunGate(w, r) {
		return
	}

	// Blast radius first: the delivery rows the delete drops, counted from memory of
	// a read — pending plus in-flight is what stops being deliverable.
	next := []string{"messq consumer rm " + stream + " " + consumer + " --confirm " + consumer}
	if confirm := r.URL.Query().Get("confirm"); confirm == "" || confirm != consumer {
		info, gErr := s.store.GetConsumer(r.Context(), stream, consumer)
		if gErr != nil {
			s.writeError(w, gErr)
			return
		}
		blast := fmt.Sprintf("%d pending and %d in-flight message%s",
			info.Pending, info.Inflight, plural(int(info.Pending+info.Inflight)))
		if !s.confirmRefusal(w, "consumer", consumer, confirm, blast, next) {
			return
		}
	}

	res, err := s.store.DeleteConsumer(r.Context(), stream, consumer, actorAPI)
	if err != nil {
		s.writeError(w, err)
		return
	}
	s.writeJSON(w, http.StatusOK, deleteConsumerResponse{Deleted: res})
}

// deleteConsumerResponse wraps the deletion receipt under a single "deleted" key.
type deleteConsumerResponse struct {
	Deleted store.ConsumerDeleteResult `json:"deleted"`
}

// handlePauseConsumer pauses one consumer idempotently; in-flight rows keep their
// claim-time deadlines (documented, NOT frozen), so the response carries a finding
// naming their count and earliest deadline when any exist.
func (s *Server) handlePauseConsumer(w http.ResponseWriter, r *http.Request) {
	s.handleSetPausedRoute(w, r, true)
}

// handleResumeConsumer resumes one paused consumer idempotently.
func (s *Server) handleResumeConsumer(w http.ResponseWriter, r *http.Request) {
	s.handleSetPausedRoute(w, r, false)
}

// pauseResumeResponse echoes the state change plus the deadline finding.
type pauseResumeResponse struct {
	Stream   string    `json:"stream"`
	Name     string    `json:"name"`
	Paused   bool      `json:"paused"`
	Changed  bool      `json:"changed"`
	Findings []finding `json:"findings,omitempty"`
}

func (s *Server) handleSetPausedRoute(w http.ResponseWriter, r *http.Request, wantPaused bool) {
	stream, consumer := r.PathValue("stream"), r.PathValue("consumer")
	if err := queue.ValidateExistingStreamName(stream); err != nil {
		s.writeError(w, err)
		return
	}
	if err := queue.ValidateConsumerName(consumer); err != nil {
		s.writeError(w, err)
		return
	}

	cur, err := s.store.GetConsumer(r.Context(), stream, consumer)
	if err != nil {
		s.writeError(w, err)
		return
	}
	if cur.Paused == wantPaused {
		// Idempotent no-op: no writer command, no event, changed:false.
		s.writeJSON(w, http.StatusOK, pauseResumeResponse{
			Stream: stream, Name: consumer, Paused: wantPaused, Changed: false,
		})
		return
	}
	var findings []finding
	if wantPaused && cur.Inflight > 0 {
		findings = s.pauseFindings(stream, consumer, cur.Inflight, 0)
	}
	info, err := s.store.SetPaused(r.Context(), stream, consumer, wantPaused, actorAPI)
	if err != nil {
		s.writeError(w, err)
		return
	}
	s.writeJSON(w, http.StatusOK, pauseResumeResponse{
		Stream: stream, Name: consumer, Paused: info.Paused, Changed: true, Findings: findings,
	})
}
