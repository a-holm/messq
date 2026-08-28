// SPDX-License-Identifier: Apache-2.0

package api

import (
	"net/http"

	"github.com/a-holm/messq/internal/queue"
	"github.com/a-holm/messq/internal/store"
)

// POST /v1/streams/{stream}/purge (issue #15 §5): the destructive range delete with
// the one-code-path dry-run contract. Body {up_to_seq?, subject?, keep?}; ?dry_run=1
// answers applied:false with the identical impact and touches nothing. The confirm
// handshake does NOT apply here — previewing is the step you take before you know the
// name is right, so no ?confirm is required with or without the flag.

type purgeRequest struct {
	UpToSeq *int64 `json:"up_to_seq"`
	Subject string `json:"subject"`
	Keep    *int64 `json:"keep"`
}

// purgeResponse is the shared shape of preview and execution — identical modulo
// `applied`, which is what makes a preview the truth.
type purgeResponse struct {
	Applied bool              `json:"applied"`
	Stream  string            `json:"stream"`
	Impact  store.PurgeImpact `json:"impact"`
}

func (s *Server) handlePurgeStream(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("stream")
	if err := queue.ValidateExistingStreamName(name); err != nil {
		s.writeError(w, err)
		return
	}

	var req purgeRequest
	if err := decodeJSONInto(w, r, s.cfg.MaxRequestBytes, &req); err != nil {
		s.writeError(w, err)
		return
	}
	spec := store.PurgeSpec{UpToSeq: req.UpToSeq, Subject: req.Subject}
	if req.Keep != nil {
		spec.Keep = *req.Keep
	}

	dryRun := r.URL.Query().Get("dry_run") == "1"
	res, err := s.store.Purge(r.Context(), name, spec, dryRun, actorAPI)
	if err != nil {
		s.writeError(w, err)
		return
	}
	s.writeJSON(w, http.StatusOK, purgeResponse{
		Applied: !dryRun,
		Stream:  res.Stream,
		Impact:  res.Impact,
	})
}
