// SPDX-License-Identifier: Apache-2.0

package api

import (
	"errors"
	"fmt"
	"net/http"
	"time"

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
// the issue pins for DELETE, plus structured findings (a surviving DLQ is the one this
// route owns).
type deleteStreamResponse struct {
	Deleted  store.DeleteResult `json:"deleted"`
	Findings []finding          `json:"findings,omitempty"`
}

func (s *Server) handleCreateStream(w http.ResponseWriter, r *http.Request) {
	req := defaultStreamConfigRequest()
	if err := decodeJSONInto(w, r, s.cfg.MaxRequestBytes, &req); err != nil {
		s.writeError(w, err)
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
	if err := decodeJSONInto(w, r, s.cfg.MaxRequestBytes, &req); err != nil {
		s.writeError(w, err)
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
	if !s.checkDryRunGate(w, r) {
		return
	}

	// The blast radius comes from the read pool BEFORE the refusal: a confirm_required
	// that says "removes messages" without numbers is not a teaching error.
	next := []string{
		"messq stream rm " + name + " --confirm " + name,
		"messq stream purge " + name + " --dry-run   # if only the messages should go",
	}
	if confirm := r.URL.Query().Get("confirm"); confirm == "" || confirm != name {
		info, gErr := s.store.GetStream(r.Context(), name)
		if gErr != nil {
			s.writeError(w, gErr) // absent stream: not_found beats any confirm code
			return
		}
		consumers, cErr := s.store.ListConsumers(r.Context(), name)
		if cErr != nil {
			consumers = nil
			s.logger.Warn("delete stream: count consumers", "err", cErr)
		}
		blast := fmt.Sprintf("%d message", info.Msgs)
		if info.Msgs == 1 {
			blast = "1 message" // plural honesty down to the last unit
		} else if info.Msgs >= 0 {
			blast = fmt.Sprintf("%d messages", info.Msgs)
		}
		if info.Bytes > 0 {
			blast += fmt.Sprintf(" (%d bytes)", info.Bytes)
		}
		blast += fmt.Sprintf(" and %d consumer%s", len(consumers), plural(len(consumers)))
		if !s.confirmRefusal(w, "stream", name, confirm, blast, next) {
			return
		}
	}

	res, err := s.store.DeleteStream(r.Context(), name, name, actorAPI)
	if err != nil {
		s.writeError(w, err)
		return
	}
	resp := deleteStreamResponse{Deleted: res}
	// The evidence of failure must outlive the thing that failed: name the surviving
	// DLQ, its depth, and how to remove it when the operator means that too.
	dlq := name + ".dlq"
	if dlqInfo, dlqErr := s.store.GetStream(r.Context(), dlq); dlqErr == nil {
		resp.Findings = []finding{{
			Level: "warn",
			Code:  "surviving_dlq",
			Message: fmt.Sprintf("dead-letter stream %s survives with %d messages; "+
				"messq stream rm %s --confirm %s removes it too",
				dlq, dlqInfo.Msgs, dlq, dlq),
		}}
	}
	s.writeJSON(w, http.StatusOK, resp)
}

// finding is one structured warning attached to a success response — never prose in an
// error, because the operation DID succeed (#9's findings shape).
type finding struct {
	Level   string `json:"level"`
	Code    string `json:"code"`
	Message string `json:"message"`
}

func plural(n int) string {
	if n == 1 {
		return ""
	}
	return "s"
}
