// SPDX-License-Identifier: Apache-2.0

package api

import (
	"net/http"

	"github.com/a-holm/messq/internal/queue"
	"github.com/a-holm/messq/internal/store"
)

// POST /v1/streams/{stream}/consumers/{consumer}/seek (issue #15 §7). Body
// {"to":"start"|"new"|"seq:N"|"time:<unix-ms>"} — the SAME grammar creation-time start
// uses, parsed by queue.ParseStartPosition. ?dry_run=1 previews with an identical
// impact; the real run fences tokens by bumping the generation and drops every
// delivery row of this consumer.

type seekRequest struct {
	To string `json:"to"`
}

type seekResponse struct {
	Applied  bool             `json:"applied"`
	Stream   string           `json:"stream"`
	Consumer string           `json:"consumer"`
	Impact   store.SeekImpact `json:"impact"`
}

func (s *Server) handleSeekConsumer(w http.ResponseWriter, r *http.Request) {
	stream, consumer := r.PathValue("stream"), r.PathValue("consumer")
	if err := queue.ValidateExistingStreamName(stream); err != nil {
		s.writeError(w, err)
		return
	}
	if err := queue.ValidateConsumerName(consumer); err != nil {
		s.writeError(w, err)
		return
	}

	var req seekRequest
	if err := decodeJSONInto(w, r, s.cfg.MaxRequestBytes, &req); err != nil {
		s.writeError(w, err)
		return
	}
	to, err := queue.ParseStartPosition(req.To)
	if err != nil {
		// T10's teaching shape: parse failures are bad_request naming the grammar.
		s.writeError(w, err,
			"messq seek "+stream+" "+consumer+" --to seq:1000")
		return
	}

	dryRun := r.URL.Query().Get("dry_run") == "1"
	res, err := s.store.Seek(r.Context(), stream, consumer, to, dryRun, actorAPI)
	if err != nil {
		s.writeError(w, err)
		return
	}
	s.writeJSON(w, http.StatusOK, seekResponse{
		Applied:  !dryRun,
		Stream:   res.Stream,
		Consumer: res.Consumer,
		Impact:   res.Impact,
	})
}
