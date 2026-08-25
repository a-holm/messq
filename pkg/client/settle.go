// SPDX-License-Identifier: Apache-2.0

package client

import (
	"context"
	"time"
)

// The settle verbs (issue §5). Per-token truth rides SettleResult even when the
// request-level verdict is an error status; single-token calls project their one
// outcome onto the HTTP status, which surfaces here as a typed *Error.

// ItemResult is one token's settle outcome.
type ItemResult = SettleItem

// Ack settles deliveries as done. Repeat acks are safe (T3a makes them idempotent).
func (c *Client) Ack(ctx context.Context, tokens ...string) (SettleResult, error) {
	for _, t := range tokens {
		if t == "" {
			return SettleResult{}, &Error{Code: "bad_request", Message: "ack token is empty", err: ErrBadRequest}
		}
	}
	return do[SettleResult](ctx, c, "POST", "/v1/ack", nil, map[string][]string{"tokens": tokens})
}

// Nak releases a delivery back to the consumer's schedule now. WithDelay overrides
// the consumer's backoff for this one nak; WithReason attaches why (truncated to the
// 4 KiB wire budget) so `messq trace` and the DLQ carry the cause.
func (c *Client) Nak(ctx context.Context, token string, opts ...SettleOption) (ItemResult, error) {
	body := settleItemWire{Token: token}
	var delay *time.Duration
	var reason string
	for _, o := range opts {
		o(&body, &delay, &reason)
	}
	if reason != "" {
		body.Reason = truncateReason(reason)
	}
	if delay != nil {
		ms := delay.Milliseconds()
		body.DelayMS = &ms
	}
	res, err := do[SettleResult](ctx, c, "POST", "/v1/nak", nil, body)
	if err != nil {
		return ItemResult{}, err
	}
	return res.Item(), nil
}

// Term dead-letters a delivery immediately (T6): straight to <stream>.dlq with the
// reason as its Messq-Last-Reason header.
func (c *Client) Term(ctx context.Context, token, reason string) (ItemResult, error) {
	item := settleItemWire{Token: token, Reason: truncateReason(reason)}
	res, err := do[SettleResult](ctx, c, "POST", "/v1/term", nil, item)
	if err != nil {
		return ItemResult{}, err
	}
	return res.Item(), nil
}

// Extend renews every token's lease by its ack_wait (T7), capped by the daemon's
// --max-ack-wait. Results are per-token: unknown/stale mean the lease is gone.
func (c *Client) Extend(ctx context.Context, tokens ...string) (SettleResult, error) {
	items := make([]settleItemWire, len(tokens))
	for i, t := range tokens {
		if t == "" {
			return SettleResult{}, &Error{Code: "bad_request", Message: "extend token is empty", err: ErrBadRequest}
		}
		items[i] = settleItemWire{Token: t}
	}
	return do[SettleResult](ctx, c, "POST", "/v1/extend", nil, map[string][]settleItemWire{"items": items})
}

// settleItemWire mirrors internal/api's settle item request shape.
type settleItemWire struct {
	Token   string `json:"token"`
	DelayMS *int64 `json:"delay_ms,omitempty"`
	Reason  string `json:"reason,omitempty"`
}

// SettleOption is one optional parameter of Nak.
type SettleOption func(body *settleItemWire, delay **time.Duration, reason *string)

// WithDelay sets an explicit visibility delay for this nak (T4), overriding the
// consumer's backoff schedule for this release.
func WithDelay(d time.Duration) SettleOption {
	return func(_ *settleItemWire, delay **time.Duration, _ *string) { *delay = &d }
}

// WithReason attaches a cause string; truncated to 4 KiB client-side so the wire
// budget of #10's --max-reason-bytes can never be exceeded from here.
func WithReason(s string) SettleOption {
	return func(_ *settleItemWire, _ **time.Duration, reason *string) { *reason = s }
}

func truncateReason(s string) string {
	if len(s) > reasonLimit {
		return s[:reasonLimit]
	}
	return s
}
