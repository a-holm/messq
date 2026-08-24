// SPDX-License-Identifier: Apache-2.0

package api

import (
	"net/http"
	"time"

	"github.com/a-holm/messq/internal/errs"
	"github.com/a-holm/messq/internal/queue"
	"github.com/a-holm/messq/internal/store"
)

// The settle endpoints (issue §8): /v1/ack|nak|term|extend wrapping #10's Settle.
// Status codes describe the REQUEST, not the tokens: a single-token request maps its
// one outcome onto the HTTP status (200 ok / 409 stale_ack / 404), because that is
// what a shell worker branches on; a BATCH always answers 200 with per-token results
// in request order and honest ok/stale/unknown counters — there is no defensible
// status for "3 ok, 1 fenced", and a strict client reads stale == 0.

// settleItemReq is one item of the batch form; delay_ms is nak-only.
type settleItemReq struct {
	Token   string `json:"token"`
	DelayMS *int64 `json:"delay_ms"`
	Reason  string `json:"reason"`
}

// settleReq accepts every legal spelling of one settle request. Exactly ONE of Token /
// Tokens / Items may be present, and which depends on the verb:
//
//	ack             {"token"} | {"tokens":[…]}
//	nak/term/extend {"token" (+delay_ms)} | {"items":[…]}
type settleReq struct {
	Token   string          `json:"token"`
	DelayMS *int64          `json:"delay_ms"`
	Tokens  []string        `json:"tokens"`
	Items   []settleItemReq `json:"items"`
}

// settleItemResult is the per-token wire outcome. Result is ok/stale/unknown; the
// token_attempt/current_attempt detail keys are pinned by the issue's transcript.
type settleItemResult struct {
	Token          string `json:"token"`
	Result         string `json:"result"`
	Reason         string `json:"reason,omitempty"`
	TokenAttempt   int32  `json:"token_attempt,omitempty"`
	CurrentAttempt int32  `json:"current_attempt,omitempty"`
}

// settleResponse is THE settle shape for single and batch forms alike — one thing to
// parse everywhere.
type settleResponse struct {
	Results []settleItemResult `json:"results"`
	OK      int                `json:"ok"`
	Stale   int                `json:"stale"`
	Unknown int                `json:"unknown"`
}

// handleSettle builds the four verb handlers off one implementation.
func (s *Server) handleSettle(verb queue.Verb) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		req, err := decodeJSON[settleReq](w, r, s.cfg.MaxRequestBytes)
		if err != nil {
			s.writeError(w, err)
			return
		}

		slots, submittable, single, perr := parseSettleReq(verb, req)
		if perr != nil {
			s.writeError(w, perr)
			return
		}
		if len(submittable) > s.store.MaxSettleBatch() {
			s.writeError(w, errs.E(errs.ErrTooLarge, "api.settle",
				"a settle batch of %d items exceeds --max-settle-batch %d",
				len(submittable), s.store.MaxSettleBatch()))
			return
		}

		submitCtx, cancel := s.submitCtx(r.Context())
		defer cancel()
		res, serr := s.store.Settle(submitCtx, store.SettleCmd{Items: submittable})
		if serr != nil {
			s.writeError(w, s.classifySubmit("api.settle", serr))
			return
		}

		out := renderSettle(slots, res)

		// A nak whose release is visible NOW wakes this consumer's parked fetches
		// immediately (issue §7 wake source C); later releases ride the sweeper and
		// the NextVisibleAt timer cap.
		if verb == queue.VerbNak {
			now := s.clk.Now()
			for i := range submittable {
				if ir := &res.Results[i]; ir.Status == queue.ItemStatusOK &&
					!ir.RetryAt.After(now) {
					s.waiters.Wake(queue.ConsumerKey{
						Stream:   submittable[i].Token.Stream,
						Consumer: submittable[i].Token.Consumer,
					})
				}
			}
		}

		// Single-token requests project their one outcome onto the STATUS; every
		// non-200 body is an envelope so clients parse one failure shape everywhere.
		if single {
			item := out.Results[0]
			switch item.Result {
			case "ok":
			case "stale":
				s.writeError(w, errs.WithCode(
					errs.E(errs.ErrStaleAck, "api.settle", "%s", item.reasonText()),
					string(CodeStaleAck)))
				return
			default:
				s.writeError(w, errs.E(errs.ErrNotFound, "api.settle", "%s", item.reasonText()))
				return
			}
		}

		s.writeJSON(w, http.StatusOK, out)
	}
}

// settleSlot is one REQUEST-order slot: a parsed token heading to the writer, or a
// placeholder carrying the parse error for a token that could not parse at all.
type settleSlot struct {
	raw      string
	parseErr error
}

// parseSettleReq validates the request shape against the verb and parses tokens.
// Malformed tokens in a BATCH become unknown placeholders; in a SINGLE request they
// are invalid_token/400 — a typo'd token deserves a typed rejection, not a silent
// counter bump.
func parseSettleReq(verb queue.Verb, req settleReq) ([]settleSlot, []store.SettleItem, bool, error) {
	badShape := func(format string, args ...any) ([]settleSlot, []store.SettleItem, bool, error) {
		return nil, nil, false, errs.E(errs.ErrBadRequest, "api.settle", format, args...)
	}

	given := 0
	if req.Token != "" {
		given++
	}
	if len(req.Tokens) > 0 {
		given++
	}
	if len(req.Items) > 0 {
		given++
	}
	switch {
	case given == 0:
		return badShape(`settle needs a token: pass {"token"} or a batch form`)
	case given > 1:
		return badShape("pass exactly ONE of token, tokens or items")
	}
	if req.DelayMS != nil {
		if *req.DelayMS < 0 {
			return badShape("delay_ms is %d, want >= 0", *req.DelayMS)
		}
		if verb != queue.VerbNak {
			return badShape("delay_ms is nak-only")
		}
	}

	var (
		items   []settleItemReq
		single  bool
		tokens  []string
		reasons = map[string]string{}
		delays  = map[string]*int64{}
	)

	switch {
	case req.Token != "":
		single = true
		tokens = []string{req.Token}
		items = []settleItemReq{{Token: req.Token, DelayMS: req.DelayMS}}
		if req.DelayMS != nil {
			delays[req.Token] = req.DelayMS
		}
	case len(req.Tokens) > 0:
		if verb != queue.VerbAck {
			return badShape("the tokens array is ack-only; use {\"items\":[…]} for %s",
				string(verb))
		}
		for _, tok := range req.Tokens {
			tokens = append(tokens, tok)
			items = append(items, settleItemReq{Token: tok})
		}
	default:
		for _, it := range req.Items {
			if it.DelayMS != nil && verb != queue.VerbNak {
				return badShape("delay_ms is nak-only")
			}
			tokens = append(tokens, it.Token)
			items = append(items, it)
		}
	}

	var (
		slots       = make([]settleSlot, len(tokens))
		submittable = make([]store.SettleItem, 0, len(tokens))
	)
	for i, raw := range tokens {
		slots[i] = settleSlot{raw: raw}
		token, terr := queue.ParseToken(raw)
		if terr != nil {
			slots[i].parseErr = terr
			if single {
				return nil, nil, false, terr // invalid_token via ErrUnknownToken default
			}
			continue
		}
		item := store.SettleItem{Token: token, Verb: verb, Reason: items[i].Reason}
		if verb == queue.VerbNak && items[i].DelayMS != nil {
			d := time.Duration(*items[i].DelayMS) * time.Millisecond
			item.Delay = &d
		}
		_ = delays
		_ = reasons
		submittable = append(submittable, item)
	}
	return slots, submittable, single, nil
}

// renderSettle walks the request-order slots and merges the writer's results into
// them, so output order ALWAYS matches input order even with unparseable tokens.
func renderSettle(slots []settleSlot, res store.SettleResult) settleResponse {
	out := settleResponse{Results: make([]settleItemResult, 0, len(slots))}
	next := 0
	for _, slot := range slots {
		if slot.parseErr != nil {
			out.Results = append(out.Results, settleItemResult{
				Token:  slot.raw,
				Result: "unknown",
				Reason: "malformed_token",
			})
			out.Unknown++
			continue
		}
		rendered := renderItem(res.Results[next])
		next++
		out.Results = append(out.Results, rendered)
		switch rendered.Result {
		case "ok":
			out.OK++
		case "stale":
			out.Stale++
		default:
			out.Unknown++
		}
	}
	return out
}

// renderItem maps one store ItemResult onto the wire. No default arm on the status
// switch: a new ItemStatus must be classified here on purpose.
func renderItem(ir store.ItemResult) settleItemResult {
	var result string
	switch ir.Status {
	case queue.ItemStatusOK:
		result = "ok"
	case queue.ItemStatusStale:
		result = "stale"
	case queue.ItemStatusStaleAck:
		result = "stale"
	case queue.ItemStatusWrongGen:
		result = "stale"
	case queue.ItemStatusUnknown:
		result = "unknown"
	}
	out := settleItemResult{
		Token:        ir.Token.String(),
		Result:       result,
		TokenAttempt: ir.Attempt,
	}
	switch ir.Status {
	case queue.ItemStatusStaleAck:
		out.Reason = "attempt_fence"
		out.CurrentAttempt = ir.CurrentAttempt
	case queue.ItemStatusWrongGen:
		out.Reason = "generation_fence"
	case queue.ItemStatusUnknown:
		if ir.Err != nil {
			out.Reason = ir.Err.Error()
		} else {
			out.Reason = "no_live_delivery"
		}
	}
	return out
}

// reasonText renders the human sentence for a single-token envelope failure.
func (r settleItemResult) reasonText() string {
	if r.Reason != "" {
		return r.Reason
	}
	return "settle outcome: " + r.Result
}
