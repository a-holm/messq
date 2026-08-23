// SPDX-License-Identifier: Apache-2.0

package api

import (
	"crypto/rand"
	"encoding/json"
	"errors"
	"net/http"
	"time"

	"github.com/a-holm/messq/internal/errs"
	"github.com/a-holm/messq/internal/id"
	"github.com/a-holm/messq/internal/queue"
	"github.com/a-holm/messq/internal/store"
)

// actorAPI is the audit actor recorded on every stream-lifecycle event until #16 lands
// authentication and a per-request actor. It is a constant, not a header, so every
// command this slice issues writes the same honest actor.
const actorAPI = "api"

// streamConfigRequest is the POST /v1/streams wire shape. Durations travel as
// milliseconds; max_msgs/max_bytes keep their zero value to mean "unlimited". The struct
// is pre-seeded with DefaultConfig's values, so an absent field keeps its default rather
// than zeroing — subjects, max_age_ms, max_msg_size and dedup_window_ms all default to
// something other than zero.
type streamConfigRequest struct {
	Name          string   `json:"name"`
	Subjects      []string `json:"subjects"`
	Retention     string   `json:"retention"`
	MaxMsgs       int64    `json:"max_msgs"`
	MaxBytes      int64    `json:"max_bytes"`
	MaxAgeMS      int64    `json:"max_age_ms"`
	MaxMsgSize    int64    `json:"max_msg_size"`
	Discard       string   `json:"discard"`
	DedupWindowMS int64    `json:"dedup_window_ms"`
}

// defaultStreamConfigRequest seeds the wire shape with the same defaults the store hands
// a stream that names nothing but itself, so decode leaves an absent field at its default.
func defaultStreamConfigRequest() streamConfigRequest {
	d := queue.DefaultConfig("")
	return streamConfigRequest{
		Subjects:      d.Subjects,
		Retention:     string(d.Retention),
		MaxAgeMS:      d.MaxAge.Milliseconds(),
		MaxMsgSize:    d.MaxMsgSize,
		Discard:       string(d.Discard),
		DedupWindowMS: d.DedupWindow.Milliseconds(),
	}
}

// config renders the wire shape as the pure layer's value type.
func (r streamConfigRequest) config() queue.StreamConfig {
	return queue.StreamConfig{
		Name:        r.Name,
		Subjects:    r.Subjects,
		Retention:   queue.Retention(r.Retention),
		MaxMsgs:     r.MaxMsgs,
		MaxBytes:    r.MaxBytes,
		MaxAge:      time.Duration(r.MaxAgeMS) * time.Millisecond,
		MaxMsgSize:  r.MaxMsgSize,
		Discard:     queue.Discard(r.Discard),
		DedupWindow: time.Duration(r.DedupWindowMS) * time.Millisecond,
	}
}

// streamPatchRequest is the sparse PATCH /v1/streams/{stream} wire shape. Every field is
// a pointer so "absent" and "zero" stay distinguishable: max_msgs: 0 means unlimited, and
// omitting it must not mean that. The shape maps one-to-one onto store.StreamPatch.
type streamPatchRequest struct {
	Subjects      *[]string        `json:"subjects"`
	Retention     *queue.Retention `json:"retention"`
	MaxMsgs       *int64           `json:"max_msgs"`
	MaxBytes      *int64           `json:"max_bytes"`
	MaxAgeMS      *int64           `json:"max_age_ms"`
	MaxMsgSize    *int64           `json:"max_msg_size"`
	Discard       *queue.Discard   `json:"discard"`
	DedupWindowMS *int64           `json:"dedup_window_ms"`
}

func (r streamPatchRequest) patch() store.StreamPatch {
	return store.StreamPatch{
		Subjects:      r.Subjects,
		Retention:     r.Retention,
		MaxMsgs:       r.MaxMsgs,
		MaxBytes:      r.MaxBytes,
		MaxAgeMS:      r.MaxAgeMS,
		MaxMsgSize:    r.MaxMsgSize,
		Discard:       r.Discard,
		DedupWindowMS: r.DedupWindowMS,
	}
}

// streamUpdateResponse is the PATCH wire shape: the updated StreamInfo (flattened) plus
// how many stored messages a narrowed subject set left behind. NarrowedMsgs is always
// present — zero when the patch did not touch subjects — so the CLI contract is a fixed
// shape rather than a field that appears only sometimes.
type streamUpdateResponse struct {
	store.StreamInfo
	NarrowedMsgs int64 `json:"narrowed_msgs"`
}

// deleteStreamResponse wraps the deletion receipt under a single "deleted" key, the shape
// the issue pins for DELETE.
type deleteStreamResponse struct {
	Deleted store.DeleteResult `json:"deleted"`
}

// errorEnvelope is the one wire shape every error shares (issue §7 / PLAN §7): a stable
// machine code, one human sentence, the suggested next commands, and the request's
// trace id.
type errorEnvelope struct {
	Error errorBody `json:"error"`
}

type errorBody struct {
	Code    string   `json:"code"`
	Message string   `json:"message"`
	Next    []string `json:"next"`
	TraceID string   `json:"trace_id"`
}

func (s *Server) handleCreateStream(w http.ResponseWriter, r *http.Request) {
	req := defaultStreamConfigRequest()
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		s.writeError(w, errs.E(errs.ErrBadRequest, "api.createStream", "invalid JSON body: %v", err))
		return
	}
	cfg := req.config()
	if err := queue.ValidateStreamConfig(cfg, s.limits); err != nil {
		s.writeError(w, err)
		return
	}

	info, existed, err := s.store.CreateStream(r.Context(), cfg, actorAPI)
	if err != nil {
		var existsErr *store.StreamExistsError
		if errors.As(err, &existsErr) {
			s.writeError(w, err, "messq stream edit "+cfg.Name)
			return
		}
		s.writeError(w, err)
		return
	}

	status := http.StatusCreated
	if existed {
		status = http.StatusOK
	}
	s.writeJSON(w, status, info)
}

func (s *Server) handleListStreams(w http.ResponseWriter, r *http.Request) {
	streams, err := s.store.ListStreams(r.Context())
	if err != nil {
		s.writeError(w, err)
		return
	}
	s.writeJSON(w, http.StatusOK, streams)
}

func (s *Server) handleGetStream(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("stream")
	if err := queue.ValidateExistingStreamName(name); err != nil {
		s.writeError(w, err)
		return
	}
	info, err := s.store.GetStream(r.Context(), name)
	if err != nil {
		s.writeError(w, err)
		return
	}
	s.writeJSON(w, http.StatusOK, info)
}

func (s *Server) handleUpdateStream(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("stream")
	if err := queue.ValidateExistingStreamName(name); err != nil {
		s.writeError(w, err)
		return
	}

	var req streamPatchRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		s.writeError(w, errs.E(errs.ErrBadRequest, "api.updateStream", "invalid JSON body: %v", err))
		return
	}
	allowDataLoss := r.URL.Query().Get("allow_data_loss") == "1"

	res, err := s.store.UpdateStream(r.Context(), name, req.patch(), allowDataLoss, actorAPI)
	if err != nil {
		var loseErr *queue.WouldLoseDataError
		if errors.As(err, &loseErr) {
			s.writeError(w, err, "messq stream edit "+name+" --allow-data-loss")
			return
		}
		s.writeError(w, err)
		return
	}
	s.writeJSON(w, http.StatusOK, streamUpdateResponse{StreamInfo: res.Info, NarrowedMsgs: res.NarrowedMsgs})
}

func (s *Server) handleDeleteStream(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("stream")
	if err := queue.ValidateExistingStreamName(name); err != nil {
		s.writeError(w, err)
		return
	}
	confirm := r.URL.Query().Get("confirm")

	res, err := s.store.DeleteStream(r.Context(), name, confirm, actorAPI)
	if err != nil {
		if errors.Is(err, errs.ErrConflict) {
			s.writeError(w, err, "messq stream rm "+name+" --confirm "+name)
			return
		}
		s.writeError(w, err)
		return
	}
	s.writeJSON(w, http.StatusOK, deleteStreamResponse{Deleted: res})
}

// writeJSON writes v as a JSON response with the given status. Encoding a response can
// only fail after the header is already sent, so the error is logged, not surfaced.
func (s *Server) writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(v); err != nil {
		s.logger.Warn("api: write response", "err", err)
	}
}

// writeError renders err as the issue's error envelope. The wire code and status are
// derived from the sentinel; any extra next commands the caller knows about are appended
// after the ones the error already carries.
func (s *Server) writeError(w http.ResponseWriter, err error, next ...string) {
	body := errorBody{
		Code:    wireCode(err),
		Message: errMessage(err),
		Next:    append(errs.NextOf(err), next...),
		TraceID: id.NewTraceID(rand.Reader).String(),
	}
	if body.Next == nil {
		body.Next = []string{}
	}
	s.writeJSON(w, statusFor(body.Code), errorEnvelope{Error: body})
}

// wireCode maps an error to its wire code. The order matters: stream_exists, reserved_name
// and would_lose_data each wrap a broader sentinel (conflict / bad_request), so their
// typed checks run before the generic ones.
func wireCode(err error) string {
	var existsErr *store.StreamExistsError
	if errors.As(err, &existsErr) {
		return "stream_exists"
	}
	if errors.Is(err, queue.ErrReservedName) {
		return "reserved_name"
	}
	var loseErr *queue.WouldLoseDataError
	if errors.As(err, &loseErr) {
		return "would_lose_data"
	}
	switch {
	case errors.Is(err, errs.ErrNotFound):
		return "not_found"
	case errors.Is(err, errs.ErrConflict):
		return "conflict"
	case errors.Is(err, errs.ErrBadRequest):
		return "bad_request"
	case errors.Is(err, errs.ErrReadOnly):
		return "read_only"
	case errors.Is(err, errs.ErrShuttingDown):
		return "shutting_down"
	default:
		return "internal"
	}
}

// statusFor turns a wire code into its HTTP status.
func statusFor(code string) int {
	switch code {
	case "not_found":
		return http.StatusNotFound
	case "stream_exists", "would_lose_data", "conflict":
		return http.StatusConflict
	case "reserved_name", "bad_request":
		return http.StatusBadRequest
	case "read_only", "shutting_down":
		return http.StatusServiceUnavailable
	default:
		return http.StatusInternalServerError
	}
}

// errMessage extracts the human sentence to carry in the envelope: the teaching error's
// own message when the error is an errs.Error, otherwise the error's rendered text. The
// typed store/queue errors (StreamExistsError, WouldLoseDataError) are not errs.Errors, so
// their full rendered text — which names the differing fields or the at-risk counts — is
// what the envelope carries.
func errMessage(err error) string {
	var te *errs.Error
	if errors.As(err, &te) {
		return te.Msg
	}
	return err.Error()
}
