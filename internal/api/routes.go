// SPDX-License-Identifier: Apache-2.0

package api

import (
	"net/http"
	"regexp"
	"sort"
	"strings"

	"github.com/a-holm/messq/internal/auth"
	"github.com/a-holm/messq/internal/queue"
)

// Route is one entry of the route registry: the ONE place patterns are declared.
// #16 asserts every Mutating route declares a required role against this list, and #18
// iterates it for contract coverage; a checked-in golden (testdata/routes.golden)
// turns any add/rename/remove into a reviewed diff.
type Route struct {
	Method  string // "" on the catch-all, which answers every method
	Pattern string
	Name    string
	// Mutating is true when the route can change stored state; it gates #16's
	// role-completeness test (every mutating route declares a non-empty set).
	Mutating bool
	// Roles is the declared role set (#16 enforces). Empty ⇒ unauthenticated; the
	// consistency tests pin that to the two probes.
	Roles auth.RoleSet
	// DryRun declares that ?dry_run=1 is honoured here. Any other route answering a
	// dry_run query parameter refuses with 400 dry_run_unsupported.
	DryRun bool
	// Confirm names the path value ?confirm= must equal verbatim ("stream" |
	// "consumer"); "" means the route takes no name confirmation. Missing confirm
	// yields confirm_required, a mismatched one confirm_mismatch.
	Confirm string
}

// routes is the registry. Order is declaration order and part of the golden.
func (*Server) routes() []Route {
	return []Route{
		{http.MethodGet, "/healthz", "healthz", false, rolesNone, false, ""},
		{http.MethodGet, "/readyz", "readyz", false, rolesNone, false, ""},
		{http.MethodGet, "/metrics", "metrics", false, rolesAdmin, false, ""},
		{http.MethodGet, "/v1/info", "info", false, rolesAdmin, false, ""},
		{http.MethodPost, "/v1/admin/log-level", "admin_log_level", true, rolesAdmin, false, ""},

		{http.MethodPost, "/v1/streams", "create_stream", true, rolesAdmin, false, ""},
		{http.MethodGet, "/v1/streams", "list_streams", false, rolesAdmin, false, ""},
		{http.MethodGet, "/v1/streams/{stream}", "get_stream", false, rolesAdmin, false, ""},
		{http.MethodPatch, "/v1/streams/{stream}", "update_stream", true, rolesAdmin, false, ""}, // dry-run flips on with the narrowing Impact slice
		{http.MethodDelete, "/v1/streams/{stream}", "delete_stream", true, rolesAdmin, false, "stream"},

		{
			http.MethodPost, "/v1/streams/{stream}/messages", "publish_message", true,
			auth.RoleSet(1 << auth.RolePublish), false, "",
		},
		{
			http.MethodPost, "/v1/streams/{stream}/messages:batch", "publish_batch", true,
			auth.RoleSet(1 << auth.RolePublish), false, "",
		},
		{http.MethodGet, "/v1/streams/{stream}/messages", "list_messages", false, rolesAdmin, false, ""},
		{http.MethodGet, "/v1/streams/{stream}/messages/{seq}", "peek_message", false, rolesAdmin, false, ""},
		{http.MethodGet, "/v1/streams/{stream}/messages/{seq}/data", "peek_message_data", false, rolesAdmin, false, ""},
		{http.MethodGet, "/v1/messages/{id}", "peek_by_id", false, rolesAdmin, false, ""},

		{http.MethodPost, "/v1/streams/{stream}/consumers", "create_consumer", true, rolesAdmin, false, ""},
		{http.MethodGet, "/v1/streams/{stream}/consumers", "list_consumers", false, rolesAdmin, false, ""},
		{http.MethodGet, "/v1/streams/{stream}/consumers/{consumer}", "get_consumer", false, rolesAdmin, false, ""},
		{
			http.MethodPatch, "/v1/streams/{stream}/consumers/{consumer}", "update_consumer", true,
			rolesAdmin, false, "",
		},
		{
			http.MethodDelete, "/v1/streams/{stream}/consumers/{consumer}", "delete_consumer", true,
			rolesAdmin, false, "consumer",
		},
		{
			http.MethodPost, "/v1/streams/{stream}/consumers/{consumer}/pause", "pause_consumer", true,
			rolesAdmin, false, "",
		},
		{
			http.MethodPost, "/v1/streams/{stream}/consumers/{consumer}/resume", "resume_consumer", true,
			rolesAdmin, false, "",
		},
		{
			http.MethodPost, "/v1/streams/{stream}/consumers/{consumer}/fetch", "fetch_consumer", true,
			rolesConsumeAndAdmin, false, "",
		},

		{http.MethodPost, "/v1/ack", "ack", true, rolesConsumeAndAdmin, false, ""},
		{http.MethodPost, "/v1/nak", "nak", true, rolesConsumeAndAdmin, false, ""},
		{http.MethodPost, "/v1/term", "term", true, rolesConsumeAndAdmin, false, ""},
		{http.MethodPost, "/v1/extend", "extend", true, rolesConsumeAndAdmin, false, ""},

		{"", "/", "catch_all", false, rolesNone, false, ""},
	}
}

// newRouter builds the ServeMux from routes() and nothing else — a pattern that skips
// the registry cannot exist here, which is what makes the golden meaningful.
func (s *Server) newRouter() http.Handler {
	s.initRouteMatchers()
	mux := http.NewServeMux()
	for _, rt := range s.routes() {
		mux.HandleFunc(rt.methodPattern(), s.routeHandler(rt.Name))
	}
	return mux
}

// compiledRoute pairs a registry entry with a path matcher for the catch-all's Allow
// computation: ServeMux routes every unmatched-method request to "/" (the catch-all),
// so deciding 404-vs-405 and naming Allow is OUR job, not the mux's.
type compiledRoute struct {
	methods map[string]bool // path-matching methods of one pattern
	re      *regexp.Regexp
}

// initRouteMatchers compiles the wildcard patterns ({name} → [^/]+) once.
func (s *Server) initRouteMatchers() {
	s.routesOnce.Do(func() {
		byPattern := make(map[string]*compiledRoute)
		for _, rt := range s.routes() {
			if rt.Method == "" {
				continue // the catch-all never answers Allow
			}
			c := byPattern[rt.Pattern]
			if c == nil {
				pattern := regexp.QuoteMeta(rt.Pattern)
				pattern = wildcard.ReplaceAllString(pattern, `[^/]+`)
				c = &compiledRoute{
					methods: make(map[string]bool),
					re:      regexp.MustCompile(`^` + pattern + `$`),
				}
				byPattern[rt.Pattern] = c
				s.compiled = append(s.compiled, c)
			}
			c.methods[rt.Method] = true
		}
	})
}

var wildcard = regexp.MustCompile(`\\\{[a-z]+\\\}`)

// allowedMethods returns the sorted methods whose pattern matches path, or nil.
func (s *Server) allowedMethods(path string) []string {
	s.initRouteMatchers()
	var methods []string
	for _, c := range s.compiled {
		if c.re.MatchString(path) {
			for m := range c.methods {
				methods = append(methods, m)
			}
		}
	}
	sort.Strings(methods)
	return methods
}

func (r Route) methodPattern() string {
	if r.Method == "" {
		return r.Pattern
	}
	return r.Method + " " + r.Pattern
}

// routeHandler resolves a registry name onto its handler in ONE switch — the compile-
// time check that every registry entry has an implementation (a new name without a case
// fails to build via the default panic).
func (s *Server) routeHandler(name string) http.HandlerFunc {
	switch name {
	case "healthz":
		return s.handleHealthz
	case "readyz":
		return s.handleReadyz
	case "metrics":
		return s.handleMetrics
	case "admin_log_level":
		return s.handleLogLevel
	case "info":
		return s.handleInfo
	case "create_stream":
		return s.handleCreateStream
	case "list_streams":
		return s.handleListStreams
	case "get_stream":
		return s.handleGetStream
	case "update_stream":
		return s.handleUpdateStream
	case "delete_stream":
		return s.handleDeleteStream
	case "publish_message":
		return s.handlePublishMessage
	case "publish_batch":
		return s.handlePublishBatch
	case "list_messages":
		return s.handleListMessages
	case "peek_message":
		return s.handlePeekMessage
	case "peek_message_data":
		return s.handlePeekMessageData
	case "peek_by_id":
		return s.handlePeekMessageByID
	case "create_consumer":
		return s.handleCreateConsumer
	case "list_consumers":
		return s.handleListConsumers
	case "get_consumer":
		return s.handleGetConsumer
	case "update_consumer":
		return s.handleUpdateConsumer
	case "delete_consumer":
		return s.handleDeleteConsumer
	case "fetch_consumer":
		return s.handleFetchConsumer
	case "pause_consumer":
		return s.handlePauseConsumer
	case "resume_consumer":
		return s.handleResumeConsumer
	case "ack":
		return s.handleSettle(queue.VerbAck)
	case "nak":
		return s.handleSettle(queue.VerbNak)
	case "term":
		return s.handleSettle(queue.VerbTerm)
	case "extend":
		return s.handleSettle(queue.VerbExtend)
	case "catch_all":
		return s.handleNotFound
	default:
		panic("api: route name " + name + " has no handler")
	}
}

// handleNotFound is the catch-all. ServeMux hands every request that matches no
// method+pattern pair here — including wrong-method requests, which stdlib would
// answer with plain-text 405 — so THIS decides: a path no pattern matches is a 404
// envelope; a path matching with a different method is a 405 envelope that preserves
// Allow (G1).
func (s *Server) handleNotFound(w http.ResponseWriter, r *http.Request) {
	if allow := s.allowedMethods(r.URL.Path); len(allow) > 0 {
		w.Header().Set("Allow", strings.Join(allow, ", "))
		s.writeEnvelope(w, http.StatusMethodNotAllowed, ErrorBody{
			Code:    CodeMethodNotAllowed,
			Message: "method not allowed on this route",
		})
		return
	}
	s.writeEnvelope(w, http.StatusNotFound, ErrorBody{
		Code:    CodeNotFound,
		Message: "no such route",
	})
}
