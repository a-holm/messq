// SPDX-License-Identifier: Apache-2.0

package api

import (
	"errors"
	"fmt"
	"net/http"
	"slices"

	"github.com/a-holm/messq/internal/errs"
	"github.com/a-holm/messq/internal/queue"
	"github.com/a-holm/messq/internal/store"
)

// The consumer control plane's declarative semantics (issue #15 §6 / G8): POST is a
// full-document upsert that CONVERGES or refuses, PATCH is the sparse edit, and the
// destructive-ish filter rewrite names its permission. Shared helpers live here so
// consumers.go keeps only handler wiring.

// consumerConfigsEqual compares a desired document against the stored one across the
// patchable surface (name excluded — the caller keyed on it; cursor/generation are
// operational state, never configuration). Filter ORDER is irrelevant to matching
// semantics, so the comparison sorts copies first.
func consumerConfigsEqual(stored queue.ConsumerConfig, want queue.ConsumerConfig) bool {
	a := slices.Clone(stored.Filters)
	b := slices.Clone(want.Filters)
	slices.Sort(a)
	slices.Sort(b)
	x := slices.Clone(stored.Backoff)
	y := slices.Clone(want.Backoff)
	return slices.Equal(a, b) &&
		stored.AckWait == want.AckWait &&
		stored.MaxDeliver == want.MaxDeliver &&
		stored.MaxAckPending == want.MaxAckPending &&
		slices.Equal(x, y) &&
		stored.DeadPolicy == want.DeadPolicy
}

// configDiffNames lists the top-level fields whose stored value differs from want —
// the words inside consumer_exists's teaching message.
func configDiffNames(stored queue.ConsumerConfig, want queue.ConsumerConfig) []string {
	var out []string
	if !slices.Equal(stored.Filters, want.Filters) {
		out = append(out, "filters")
	}
	if stored.AckWait != want.AckWait {
		out = append(out, "ack_wait_ms")
	}
	if stored.MaxDeliver != want.MaxDeliver {
		out = append(out, "max_deliver")
	}
	if stored.MaxAckPending != want.MaxAckPending {
		out = append(out, "max_ack_pending")
	}
	if !slices.Equal(stored.Backoff, want.Backoff) {
		out = append(out, "backoff_ms")
	}
	if stored.DeadPolicy != want.DeadPolicy {
		out = append(out, "dead_policy")
	}
	return out
}

// consumerUpsertGuard implements the POST contract: identical ⇒ change:false without
// touching the writer at all (the no-churn guarantee lives in this early return, not
// in store-side luck); different-and-taken ⇒ 409 consumer_exists. ok=false means the
// response is already written.
// Refusal is the FIRST return pair only when the response was written here; an
// identical document returns the stored row so the handler answers changed:false.
func (s *Server) consumerExistsRefusal(w http.ResponseWriter, r *http.Request, stream string, want queue.ConsumerConfig) bool {
	cur, err := s.store.GetConsumer(r.Context(), stream, want.Name)
	switch {
	case err == nil:
	case errors.Is(err, errs.ErrNotFound):
		return false // fresh name: fall through to the create command
	default:
		s.writeError(w, err)
		return true
	}
	if consumerConfigsEqual(cur.Config(), want) {
		return false
	}
	diff := configDiffNames(cur.Config(), want)
	e := &store.ConsumerExistsError{Stream: stream, Name: want.Name, Diff: diff, Current: cur}
	s.writeError(w, e,
		"messq consumer edit "+stream+" "+want.Name+"   # apply your changes as a sparse PATCH",
		"PATCH /v1/streams/"+stream+"/consumers/"+want.Name)
	return true
}

// checkFilterChangeRefusal guards PATCH against silently rewriting filters while rows
// outstanding on them exist (#9): with ?allow_filter_change=1 absent it refuses.
func (s *Server) checkFilterChangeRefusal(w http.ResponseWriter, stream, consumer string, currentFilters []string, wantFilters []string, allowed bool) bool {
	a := slices.Clone(currentFilters)
	b := slices.Clone(wantFilters)
	slices.Sort(a)
	slices.Sort(b)
	if slices.Equal(a, b) || allowed {
		return true
	}
	e := &consumerFilterChangeError{
		stream: stream, consumer: consumer,
		from: fmt.Sprint(len(currentFilters)), to: fmt.Sprint(len(wantFilters)),
	}
	s.writeError(w, e,
		"re-send with ?allow_filter_change=1",
		"messq seek "+stream+" "+consumer+" --to first   # back-fill under the new filters")
	return false
}

// consumerFilterChangeError teaches the permission + the back-fill instead of failing
// obscurely later when old-filter rows fence.
type consumerFilterChangeError struct {
	stream   string
	consumer string
	from     string
	to       string
}

func (e *consumerFilterChangeError) Error() string {
	return fmt.Sprintf("changing filters re-scopes deliveries for %q/%q (%s -> %s patterns); "+
		"pass ?allow_filter_change=1 if you mean it", e.consumer, e.stream, e.from, e.to)
}
func (*consumerFilterChangeError) Unwrap() error { return errs.ErrConflict }

// earliestInflightDeadline reports the count of in-flight rows and the earliest ack_wait
// deadline among them, so pausing can say exactly what will time out (issue §6: pause
// does NOT freeze deadlines — sweeper #11 still collects them).
func (s *Server) pauseFindings(stream, name string, inflight int64, earliestMS int64) []finding {
	if inflight <= 0 {
		return nil
	}
	msg := fmt.Sprintf("%d in-flight message%s keep their claim-time deadline%s and will "+
		"time out during the pause%s; they will be redelivered on resume", inflight, plural(int(inflight)),
		func() string {
			if inflight > 1 {
				return "s"
			}
			return ""
		}(),
		func() string {
			if earliestMS > 0 {
				return fmt.Sprintf(" (earliest %d)", earliestMS)
			}
			return ""
		}())
	return []finding{{
		Level: "warn", Code: "inflight_will_expire",
		Message: msg,
	}}
}
