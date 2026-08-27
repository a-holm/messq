// SPDX-License-Identifier: Apache-2.0

package api

import (
	"net/http"
	"strconv"

	"github.com/a-holm/messq/internal/errs"
	"github.com/a-holm/messq/internal/queue"
	"github.com/a-holm/messq/internal/store"
)

// GET /v1/streams/{stream}/consumers/{consumer}/pending (issue #15 §9): the stuck-set
// window. Bounded by construction — limit clamps to --pending-max-limit with the
// EFFECTIVE value echoed (silent clamping builds wrong dashboards); ack_token is
// derived ONLY for inflight rows because a scheduled row has no live lease and its
// token would fence-fail.

func (s *Server) handlePendingList(w http.ResponseWriter, r *http.Request) {
	stream, consumer := r.PathValue("stream"), r.PathValue("consumer")
	if err := queue.ValidateExistingStreamName(stream); err != nil {
		s.writeError(w, err)
		return
	}
	if err := queue.ValidateConsumerName(consumer); err != nil {
		s.writeError(w, err)
		return
	}

	q := r.URL.Query()
	limit := 50 // documented default when the caller names no explicit limit
	if raw := q.Get("limit"); raw != "" {
		n, pErr := strconv.Atoi(raw)
		if pErr != nil || n < 0 {
			s.writeError(w, errs.E(errs.ErrBadRequest, "api.pending",
				"limit %q is not a non-negative integer", raw))
			return
		}
		limit = n
	}
	effective := min(limit, s.cfg.PendingMaxLimit)

	var after int64
	if raw := q.Get("after"); raw != "" {
		v, aErr := strconv.ParseInt(raw, 10, 64)
		if aErr != nil || v < 0 {
			s.writeError(w, errs.E(errs.ErrBadRequest, "api.pending",
				"after %q is not a sequence cursor", raw))
			return
		}
		after = v
	}

	var state string
	switch q.Get("state") {
	case "":
	case "ready", "inflight":
		state = q.Get("state")
	default:
		s.writeError(w, errs.E(errs.ErrBadRequest, "api.pending",
			"state %q is unknown; valid states are ready and inflight", q.Get("state")))
		return
	}

	list, err := s.store.PendingList(r.Context(), stream, consumer, store.PendingQuery{
		Limit: effective,
		After: after,
		State: state,
	})
	if err != nil {
		s.writeError(w, err)
		return
	}
	// Echo the effective cap even when unclamped: clients parse one shape.
	s.writeJSON(w, http.StatusOK, list)
}
