// SPDX-License-Identifier: Apache-2.0

package api

import (
	"errors"
	"net/http"
	"strconv"

	"github.com/a-holm/messq/internal/errs"
	"github.com/a-holm/messq/internal/store"
)

// handleListMessages is GET /v1/streams/{stream}/messages: a bounded range listing. The
// store clamps limit and echoes the effective value; a wildcard-subject scan reports
// complete:false plus a scanned_to_seq resume point rather than scanning the stream.
func (s *Server) handleListMessages(w http.ResponseWriter, r *http.Request) {
	stream := r.PathValue("stream")
	q := r.URL.Query()

	query, err := listQuery(stream, q.Get("from_seq"), q.Get("subject"), q.Get("limit"), q.Get("order"), q.Get("include_body"))
	if err != nil {
		s.writeError(w, err)
		return
	}

	page, err := s.store.ListMessages(r.Context(), query)
	if err != nil {
		s.writeError(w, err)
		return
	}
	s.writeJSON(w, http.StatusOK, page)
}

// listQuery parses and validates the range-listing query string into a store.ListQuery.
func listQuery(stream, fromSeq, subject, limit, order, includeBody string) (store.ListQuery, error) {
	q := store.ListQuery{Stream: stream, Subject: subject}

	if fromSeq != "" {
		n, err := strconv.ParseInt(fromSeq, 10, 64)
		if err != nil || n < 0 {
			return store.ListQuery{}, errs.E(errs.ErrBadRequest, "api.listMessages",
				"from_seq %q is not a non-negative integer", fromSeq)
		}
		q.FromSeq = n
	}
	if limit != "" {
		n, err := strconv.Atoi(limit)
		if err != nil || n < 0 {
			return store.ListQuery{}, errs.E(errs.ErrBadRequest, "api.listMessages",
				"limit %q is not a non-negative integer", limit)
		}
		q.Limit = n
	}
	switch order {
	case "", "asc":
		q.Order = "asc"
	case "desc":
		q.Order = "desc"
	default:
		return store.ListQuery{}, errs.E(errs.ErrBadRequest, "api.listMessages",
			"order %q is not \"asc\" or \"desc\"", order)
	}
	switch includeBody {
	case "", "0":
		q.IncludeBody = false
	case "1":
		q.IncludeBody = true
	default:
		return store.ListQuery{}, errs.E(errs.ErrBadRequest, "api.listMessages",
			"include_body %q is not 0 or 1", includeBody)
	}
	return q, nil
}

// handlePeekMessage is GET /v1/streams/{stream}/messages/{seq}: one message's metadata.
func (s *Server) handlePeekMessage(w http.ResponseWriter, r *http.Request) {
	msg, ok := s.peekSeq(w, r)
	if !ok {
		return
	}
	s.writeJSON(w, http.StatusOK, msg)
}

// handlePeekMessageData is GET /v1/streams/{stream}/messages/{seq}/data: the raw body.
func (s *Server) handlePeekMessageData(w http.ResponseWriter, r *http.Request) {
	msg, ok := s.peekSeq(w, r)
	if !ok {
		return
	}
	w.Header().Set("Content-Type", "application/octet-stream")
	w.WriteHeader(http.StatusOK)
	if _, err := w.Write(msg.Body); err != nil {
		s.logger.Warn("api.peek: write body", "err", err)
	}
}

// peekSeq parses the {seq} path value and peeks the message, writing the error envelope
// (with the self-explaining not_found detail for a missing seq) and returning ok=false on
// any failure.
func (s *Server) peekSeq(w http.ResponseWriter, r *http.Request) (store.Message, bool) {
	stream := r.PathValue("stream")
	seq, err := parseSeq(r.PathValue("seq"))
	if err != nil {
		s.writeError(w, err)
		return store.Message{}, false
	}
	msg, err := s.store.PeekSeq(r.Context(), stream, seq)
	if err != nil {
		var miss *store.PeekMissError
		if errors.As(err, &miss) {
			s.writeError(w, peekMissError(miss, seq))
			return store.Message{}, false
		}
		s.writeError(w, err)
		return store.Message{}, false
	}
	return msg, true
}

// handlePeekMessageByID is GET /v1/messages/{id}: lookup by ULID.
func (s *Server) handlePeekMessageByID(w http.ResponseWriter, r *http.Request) {
	msg, err := s.store.PeekID(r.Context(), r.PathValue("id"))
	if err != nil {
		s.writeError(w, err)
		return
	}
	s.writeJSON(w, http.StatusOK, msg)
}

// parseSeq reads a non-negative sequence number from a path value.
func parseSeq(v string) (int64, error) {
	seq, err := strconv.ParseInt(v, 10, 64)
	if err != nil || seq < 0 {
		return 0, errs.E(errs.ErrBadRequest, "api.peek",
			"seq %q is not a non-negative integer", v)
	}
	return seq, nil
}

// peekMissError renders a store.PeekMissError as the issue's self-explaining not_found:
// the requested seq, the reason (expired vs never_published) and the boundary it carries
// (first_seq for expired, last_seq for never_published).
func peekMissError(miss *store.PeekMissError, seq int64) error {
	verb := "does not exist yet"
	boundary := "last_seq"
	if miss.Reason == "expired" {
		verb = "is gone"
		boundary = "first_seq"
	}
	return errs.E(errs.ErrNotFound, "api.peek",
		"message %d %s: %s (%s %d)", seq, verb, miss.Reason, boundary, miss.Boundary)
}
