// SPDX-License-Identifier: Apache-2.0

package client

import (
	"context"
	"net/url"
)

// Fetch long-polls one batch for a consumer. Wait parks server-side; the response
// always has ONE shape: possibly-empty messages, why (Hold), and the EFFECTIVE
// clamped request values. Fetch never carries the control-plane timeout — the wait
// owns the deadline, and a cancelled context may still have claimed messages (#14
// §6.6); that is why Drain exists instead of "just cancel".
func (c *Client) Fetch(ctx context.Context, stream, consumer string, req FetchRequest) (FetchResponse, error) {
	if err := validStreamName(stream); err != nil {
		return FetchResponse{}, err
	}
	if err := validConsumerName(consumer); err != nil {
		return FetchResponse{}, err
	}
	path := "/v1/streams/" + url.PathEscape(stream) +
		"/consumers/" + url.PathEscape(consumer) + "/fetch"
	res, err := doUntimed[fetchResponseWire](ctx, c, "POST", path, nil, fetchRequestWire(req))
	if err != nil {
		return FetchResponse{}, err
	}
	return res.export(), nil
}
