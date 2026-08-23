// SPDX-License-Identifier: Apache-2.0

package api

import (
	"encoding/json"
	"errors"
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

// fetchRequest is the POST .../fetch wire shape. WaitMS is accepted and ignored: the
// single-shot contract has no waiter; #14 adds long-poll.
type fetchRequest struct {
	Batch    int   `json:"batch"`
	MaxBytes int64 `json:"max_bytes"`
	WaitMS   int64 `json:"wait_ms"`
}

// createConsumerResponse wraps the created/updated consumer plus the warnings the
// validation produced, so #24 can surface them before it grows its own path.
type createConsumerResponse struct {
	store.ConsumerInfo
	Warnings queue.Warnings `json:"warnings,omitempty"`
}

func (s *Server) handleCreateConsumer(w http.ResponseWriter, r *http.Request) {
	stream := r.PathValue("stream")
	if err := queue.ValidateExistingStreamName(stream); err != nil {
		s.writeError(w, err)
		return
	}
	req := defaultConsumerConfigRequest()
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		s.writeError(w, errs.E(errs.ErrBadRequest, "api.createConsumer", "invalid JSON body: %v", err))
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
		status = http.StatusOK // idempotent re-create or update
	}
	s.writeJSON(w, status, createConsumerResponse{ConsumerInfo: res.Info, Warnings: res.Warnings})
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
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		s.writeError(w, errs.E(errs.ErrBadRequest, "api.updateConsumer", "invalid JSON body: %v", err))
		return
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
	confirm := r.URL.Query().Get("confirm")
	if confirm != consumer {
		s.writeError(w, errs.E(errs.ErrConflict, "api.deleteConsumer",
			"confirm parameter %q does not match consumer name %q", confirm, consumer),
			"messq consumer rm "+stream+" "+consumer+" --confirm "+consumer)
		return
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
	var req fetchRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		s.writeError(w, errs.E(errs.ErrBadRequest, "api.fetchConsumer", "invalid JSON body: %v", err))
		return
	}
	res, err := s.store.Fetch(r.Context(), store.FetchReq{
		Stream: stream, Consumer: consumer, Batch: req.Batch, MaxBytes: req.MaxBytes,
	})
	if err != nil {
		s.writeError(w, err)
		return
	}
	s.writeJSON(w, http.StatusOK, res)
}
