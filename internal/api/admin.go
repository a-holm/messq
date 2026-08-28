// SPDX-License-Identifier: Apache-2.0

package api

import (
	"log/slog"
	"net/http"

	"github.com/a-holm/messq/internal/errs"
)

// The admin knobs (issue #15 §2): POST /v1/admin/log-level is the ONE runtime-mutable
// setting that is not a signal (D8 puts everything else behind SIGHUP/#17), and /metrics
// is a mount point, not an implementation — while #21 has not injected a handler it
// answers 503 not_implemented, because a 404 would claim the endpoint does not exist.

// LevelSetter is the seam this issue defines; #19 backs it with the process-wide
// slog.LevelVar so a real change is observable by every slog handler in the process.
type LevelSetter interface {
	Level() slog.Level
	SetLevel(slog.Level) error
}

// logLevelRequest is the POST /v1/admin/log-level body: exactly one level name out of
// debug|info|warn|error.
type logLevelRequest struct {
	Level string `json:"level"`
}

// legalLogLevels is both the validator's vocabulary and the teaching message's content:
// an unknown level must NAME the four that work.
var legalLogLevels = map[string]slog.Level{
	"debug": slog.LevelDebug,
	"info":  slog.LevelInfo,
	"warn":  slog.LevelWarn,
	"error": slog.LevelError,
}

// handleLogLevel applies one log-level change through the injected setter, echoes
// {level, previous, changed}, and co-commits one admin.action audit event on real
// changes only (#9's no-churn rule applied to runtime knobs). A nil setter means the
// serve command wired no seam yet — not_implemented, not a silent success.
func (s *Server) handleLogLevel(w http.ResponseWriter, r *http.Request) {
	if s.cfg.LevelSetter == nil {
		s.writeError(w, errs.WithCode(errs.E(errs.ErrBadRequest, "api.logLevel",
			"no log-level setter is configured for this process"), string(CodeNotImplemented)),
			"wired when #19 lands")
		return
	}

	var req logLevelRequest
	if err := decodeJSONInto(w, r, s.cfg.MaxRequestBytes, &req); err != nil {
		s.writeError(w, err)
		return
	}
	want, ok := legalLogLevels[req.Level]
	if !ok {
		s.writeError(w, errs.E(errs.ErrBadRequest, "api.logLevel",
			"level %q is unknown; valid levels are debug, info, warn, error", req.Level))
		return
	}

	prevName := levelName(s.cfg.LevelSetter.Level())
	if prevName == req.Level {
		s.writeJSON(w, http.StatusOK, logLevelResponse{Level: req.Level, Previous: prevName, Changed: false})
		return
	}
	if err := s.cfg.LevelSetter.SetLevel(want); err != nil {
		s.writeError(w, err)
		return
	}
	if err := s.store.RecordAdminAction(r.Context(), actorAPI, "log_level", prevName, req.Level); err != nil {
		// The change took effect but the audit row failed: say so rather than claim
		// the whole operation committed cleanly.
		s.logger.Error("log-level: record admin.action", "err", err)
		s.writeError(w, err)
		return
	}
	s.writeJSON(w, http.StatusOK, logLevelResponse{Level: req.Level, Previous: prevName, Changed: true})
}

// logLevelResponse echoes the requested level, the previous one and whether anything
// actually changed — the shape messq log-level (#24) prints verbatim.
type logLevelResponse struct {
	Level    string `json:"level"`
	Previous string `json:"previous"`
	Changed  bool   `json:"changed"`
}

// levelName bands any slog level onto the four wire names: a custom INFO+4 still
// reads as info, because that is what an operator scanning yesterday's journal needs.
func levelName(l slog.Level) string {
	switch {
	case l > slog.LevelWarn:
		return "error"
	case l > slog.LevelInfo:
		return "warn"
	case l > slog.LevelDebug:
		return "info"
	default:
		return "debug"
	}
}

// handleMetrics delegates to the injected handler or answers 503 not_implemented with
// next pointing at #21 — mounted regardless, never 404.
func (s *Server) handleMetrics(w http.ResponseWriter, r *http.Request) {
	if s.cfg.Metrics == nil {
		s.writeError(w, errs.WithCode(errs.E(errs.ErrBadRequest, "api.metrics",
			"metrics scraping arrives with issue #21; the mount point exists today"),
			string(CodeNotImplemented)),
			"messq events --follow   # watch the journal until then")
		return
	}
	s.cfg.Metrics.ServeHTTP(w, r)
}
