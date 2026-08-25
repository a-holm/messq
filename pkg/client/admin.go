// SPDX-License-Identifier: Apache-2.0

package client

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
)

// The control plane (issue §6): one wrapper per #15 registry route, each three-ish
// lines over the do[] generic. Every path segment is url.PathEscaped AND pre-validated
// against the mirrored S11 grammar, so a bad name is a local ErrBadRequest instead of
// a round trip. AdminOption mirrors #15's query conventions and NOTHING more: the
// client never invents a confirmation and never defaults Confirm from the resource
// name — that would defeat the guard.

// StreamConfig is the create-stream form; zero fields take the server's defaults.
type StreamConfig struct {
	Name          string   `json:"name"`
	Subjects      []string `json:"subjects,omitempty"`
	Retention     string   `json:"retention,omitempty"`
	MaxMsgs       int64    `json:"max_msgs"`
	MaxBytes      int64    `json:"max_bytes"`
	MaxAgeMS      int64    `json:"max_age_ms"`
	MaxMsgSize    int64    `json:"max_msg_size"`
	Discard       string   `json:"discard,omitempty"`
	DedupWindowMS int64    `json:"dedup_window_ms"`
}

// StreamView mirrors the daemon's stream info wire shape.
type StreamView struct {
	Name          string   `json:"name"`
	Subjects      []string `json:"subjects"`
	Retention     string   `json:"retention"`
	MaxMsgs       int64    `json:"max_msgs"`
	MaxBytes      int64    `json:"max_bytes"`
	MaxAgeMS      int64    `json:"max_age_ms"`
	MaxMsgSize    int64    `json:"max_msg_size"`
	Discard       string   `json:"discard"`
	DedupWindowMS int64    `json:"dedup_window_ms"`
	CreatedAt     int64    `json:"created_at"`
	FirstSeq      int64    `json:"first_seq"`
	LastSeq       int64    `json:"last_seq"`
	Msgs          int64    `json:"msgs"`
	Bytes         int64    `json:"bytes"`
	DLQ           bool     `json:"dlq,omitempty"`
	Origin        string   `json:"origin,omitempty"`
}

// StreamPatch is the sparse update form: nil fields are left unchanged.
type StreamPatch struct {
	Subjects      *[]string `json:"subjects"`
	Retention     *string   `json:"retention"`
	MaxMsgs       *int64    `json:"max_msgs"`
	MaxBytes      *int64    `json:"max_bytes"`
	MaxAgeMS      *int64    `json:"max_age_ms"`
	MaxMsgSize    *int64    `json:"max_msg_size"`
	Discard       *string   `json:"discard"`
	DedupWindowMS *int64    `json:"dedup_window_ms"`
}

// StreamUpdateResult carries the updated view plus how many stored messages a
// narrowed subject set left behind.
type StreamUpdateResult struct {
	StreamView
	NarrowedMsgs int64 `json:"narrowed_msgs"`
}

// DeleteResult counts what a delete removed.
type DeleteResult struct {
	Messages  int64 `json:"messages"`
	Bytes     int64 `json:"bytes"`
	Consumers int64 `json:"consumers"`
}

// ConsumerConfig is the create-consumer form.
type ConsumerConfig struct {
	Name          string   `json:"name"`
	Filters       []string `json:"filters,omitempty"`
	AckWaitMS     int64    `json:"ack_wait_ms"`
	MaxDeliver    int32    `json:"max_deliver"`
	MaxAckPending int64    `json:"max_ack_pending"`
	BackoffMS     []int64  `json:"backoff_ms,omitempty"`
	DeadPolicy    string   `json:"dead_policy,omitempty"` // "" = default (drop on .dlq)
	Paused        bool     `json:"paused"`
	Start         string   `json:"start,omitempty"` // "first" | "new" | "seq:N" | "time:T"
}

// ConsumerView mirrors the daemon's consumer info wire shape, statistics included.
type ConsumerView struct {
	Stream         string   `json:"stream"`
	Name           string   `json:"name"`
	Filters        []string `json:"filters"`
	AckWaitMS      int64    `json:"ack_wait_ms"`
	MaxDeliver     int32    `json:"max_deliver"`
	MaxAckPending  int64    `json:"max_ack_pending"`
	BackoffMS      []int64  `json:"backoff_ms"`
	DeadPolicy     string   `json:"dead_policy"`
	Paused         bool     `json:"paused"`
	CursorSeq      int64    `json:"cursor_seq"`
	Generation     int64    `json:"generation"`
	CreatedAt      int64    `json:"created_at"`
	RetryHorizonMS int64    `json:"retry_horizon_ms"`
	Pending        int64    `json:"pending"`
	Inflight       int64    `json:"inflight"`
	ReadyNow       int64    `json:"ready_now"`
	InBackoff      int64    `json:"in_backoff"`
}

// ConsumerPatch is the sparse consumer update form.
type ConsumerPatch struct {
	Filters       *[]string `json:"filters"`
	AckWaitMS     *int64    `json:"ack_wait_ms"`
	MaxDeliver    *int32    `json:"max_deliver"`
	MaxAckPending *int64    `json:"max_ack_pending"`
	BackoffMS     *[]int64  `json:"backoff_ms"`
	DeadPolicy    *string   `json:"dead_policy"`
	Paused        *bool     `json:"paused,omitempty"`
}

// MessageView is one stored message as inspection returns it (no body; bodies ride
// PeekMessageData raw or Fetch decoded).
type MessageView struct {
	Stream      string            `json:"stream"`
	Seq         int64             `json:"seq"`
	ID          string            `json:"id"`
	Subject     string            `json:"subject"`
	Header      map[string]string `json:"headers,omitempty"`
	Size        int64             `json:"size"`
	PublishedAt int64             `json:"published_at"`
	TraceID     string            `json:"trace_id"`
}

// MessagePage is one bounded page of ListMessages; Complete=false comes with
// ScannedToSeq as the resume point for wildcard scans.
type MessagePage struct {
	Messages     []MessageView `json:"messages"`
	Complete     bool          `json:"complete"`
	ScannedToSeq int64         `json:"scanned_to_seq"`
	Limit        int           `json:"limit"`
}

// Info is the /v1/info shape: version, uptime, durability, db size, node id.
type Info struct {
	Version     string `json:"version"`
	UptimeMS    int64  `json:"uptime_ms"`
	Durability  string `json:"durability"`
	Synchronous int    `json:"synchronous"`
	DBBytes     int64  `json:"db_bytes"`
	NodeID      string `json:"node_id"`
}

// ListOptions narrows a message listing; zero fields take server defaults.
type ListOptions struct {
	FromSeq     int64
	Subject     string
	Limit       int
	Order       string // "" | "asc" | "desc"
	IncludeBody bool   // note: with bodies the server tightens the effective limit tenfold
}

func (o ListOptions) values() url.Values {
	q := url.Values{}
	if o.FromSeq > 0 {
		q.Set("from_seq", fmt.Sprintf("%d", o.FromSeq))
	}
	if o.Subject != "" {
		q.Set("subject", o.Subject)
	}
	if o.Limit > 0 {
		q.Set("limit", fmt.Sprintf("%d", o.Limit))
	}
	if o.Order != "" {
		q.Set("order", o.Order)
	}
	if o.IncludeBody {
		q.Set("include_body", "1")
	}
	return q
}

// AdminOption is one query-string modifier of a mutating control-plane call.
type AdminOption func(url.Values)

// DryRun asks the daemon to validate without executing (?dry_run=1).
func DryRun() AdminOption { return func(q url.Values) { q.Set("dry_run", "1") } }

// Confirm names the resource being destroyed (?confirm=<name>). The client never
// fills this in by itself.
func Confirm(name string) AdminOption { return func(q url.Values) { q.Set("confirm", name) } }

// AllowDataLoss acknowledges an operation that would drop data (?allow_data_loss=1).
func AllowDataLoss() AdminOption { return func(q url.Values) { q.Set("allow_data_loss", "1") } }

func adminQuery(opts ...AdminOption) url.Values {
	q := url.Values{}
	for _, o := range opts {
		o(q)
	}
	return q
}

// Healthz reports nil once the daemon answers 200 — the body is plain text, not JSON,
// so this rides its own status-only path.
func (c *Client) Healthz(ctx context.Context) error { return c.statusOK(ctx, "GET", "/healthz") }

// statusOK performs a request whose body is ignored, draining and closing it.
func (c *Client) statusOK(ctx context.Context, method, path string) error {
	resp, err := c.plain(ctx, method, path)
	if err != nil {
		return err
	}
	defer resp.Body.Close() //nolint:errcheck // drain already handled the reuse story
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 4096))
	if resp.StatusCode != http.StatusOK {
		return &Error{Code: "internal", Message: fmt.Sprintf("%s %s: unexpected status %d", method, path, resp.StatusCode), Status: resp.StatusCode}
	}
	return nil
}

// Info reads the daemon's self-description.
func (c *Client) Info(ctx context.Context) (Info, error) {
	return do[Info](ctx, c, "GET", "/v1/info", nil, nil)
}

func (c *Client) CreateStream(ctx context.Context, cfg StreamConfig) (StreamView, error) {
	if err := validNewStreamName(cfg.Name); err != nil {
		return StreamView{}, err
	}
	return do[StreamView](ctx, c, "POST", "/v1/streams", nil, cfg)
}

func (c *Client) ListStreams(ctx context.Context) ([]StreamView, error) {
	return do[[]StreamView](ctx, c, "GET", "/v1/streams", nil, nil)
}

func (c *Client) GetStream(ctx context.Context, name string) (StreamView, error) {
	if err := validStreamName(name); err != nil {
		return StreamView{}, err
	}
	return do[StreamView](ctx, c, "GET", "/v1/streams/"+url.PathEscape(name), nil, nil)
}

func (c *Client) UpdateStream(ctx context.Context, name string, patch StreamPatch, opts ...AdminOption) (StreamUpdateResult, error) {
	if err := validStreamName(name); err != nil {
		return StreamUpdateResult{}, err
	}
	path := "/v1/streams/" + url.PathEscape(name)
	return do[StreamUpdateResult](ctx, c, "PATCH", path, adminQuery(opts...), patch)
}

func (c *Client) DeleteStream(ctx context.Context, name string, opts ...AdminOption) (DeleteResult, error) {
	if err := validStreamName(name); err != nil {
		return DeleteResult{}, err
	}
	q := adminQuery(opts...)
	if q.Get("confirm") == "" && !q.Has("dry_run") {
		return DeleteResult{}, &Error{
			Code:    "bad_request",
			Message: fmt.Sprintf("DeleteStream %q needs Confirm(%q) — the client never invents a confirmation", name, name),
			err:     ErrBadRequest,
		}
	}
	res, err := do[struct {
		Deleted DeleteResult `json:"deleted"`
	}](ctx, c, "DELETE", "/v1/streams/"+url.PathEscape(name), q, nil)
	if err != nil {
		return DeleteResult{}, err
	}
	return res.Deleted, nil
}

func (c *Client) CreateConsumer(ctx context.Context, stream string, cfg ConsumerConfig) (ConsumerView, error) {
	if err := validStreamName(stream); err != nil {
		return ConsumerView{}, err
	}
	if err := validConsumerName(cfg.Name); err != nil {
		return ConsumerView{}, err
	}
	path := "/v1/streams/" + url.PathEscape(stream) + "/consumers"
	return do[ConsumerView](ctx, c, "POST", path, nil, cfg)
}

func (c *Client) ListConsumers(ctx context.Context, stream string) ([]ConsumerView, error) {
	if err := validStreamName(stream); err != nil {
		return nil, err
	}
	path := "/v1/streams/" + url.PathEscape(stream) + "/consumers"
	return do[[]ConsumerView](ctx, c, "GET", path, nil, nil)
}

func (c *Client) GetConsumer(ctx context.Context, stream, name string) (ConsumerView, error) {
	if err := validStreamName(stream); err != nil {
		return ConsumerView{}, err
	}
	if err := validConsumerName(name); err != nil {
		return ConsumerView{}, err
	}
	path := "/v1/streams/" + url.PathEscape(stream) + "/consumers/" + url.PathEscape(name)
	return do[ConsumerView](ctx, c, "GET", path, nil, nil)
}

func (c *Client) UpdateConsumer(ctx context.Context, stream, name string, patch ConsumerPatch, opts ...AdminOption) (ConsumerView, error) {
	if err := validStreamName(stream); err != nil {
		return ConsumerView{}, err
	}
	if err := validConsumerName(name); err != nil {
		return ConsumerView{}, err
	}
	path := "/v1/streams/" + url.PathEscape(stream) + "/consumers/" + url.PathEscape(name)
	return do[ConsumerView](ctx, c, "PATCH", path, adminQuery(opts...), patch)
}

func (c *Client) DeleteConsumer(ctx context.Context, stream, name string, opts ...AdminOption) (DeleteResult, error) {
	if err := validStreamName(stream); err != nil {
		return DeleteResult{}, err
	}
	if err := validConsumerName(name); err != nil {
		return DeleteResult{}, err
	}
	path := "/v1/streams/" + url.PathEscape(stream) + "/consumers/" + url.PathEscape(name)
	res, err := do[struct {
		Deleted DeleteResult `json:"deleted"`
	}](ctx, c, "DELETE", path, adminQuery(opts...), nil)
	if err != nil {
		return DeleteResult{}, err
	}
	return res.Deleted, nil
}

func (c *Client) ListMessages(ctx context.Context, stream string, opts ListOptions) (MessagePage, error) {
	if err := validStreamName(stream); err != nil {
		return MessagePage{}, err
	}
	path := "/v1/streams/" + url.PathEscape(stream) + "/messages"
	page, err := do[MessagePage](ctx, c, "GET", path, opts.values(), nil)
	if err != nil {
		return MessagePage{}, err
	}
	if page.Messages == nil {
		page.Messages = []MessageView{}
	}
	return page, nil
}

func (c *Client) PeekMessage(ctx context.Context, stream string, seq int64) (MessageView, error) {
	if err := validStreamName(stream); err != nil {
		return MessageView{}, err
	}
	if seq <= 0 {
		return MessageView{}, &Error{Code: "bad_request", Message: fmt.Sprintf("seq %d must be positive", seq), err: ErrBadRequest}
	}
	path := "/v1/streams/" + url.PathEscape(stream) + "/messages/" + fmt.Sprintf("%d", seq)
	return do[MessageView](ctx, c, "GET", path, nil, nil)
}

func (c *Client) PeekMessageData(ctx context.Context, stream string, seq int64) ([]byte, error) {
	if err := validStreamName(stream); err != nil {
		return nil, err
	}
	if seq <= 0 {
		return nil, &Error{Code: "bad_request", Message: fmt.Sprintf("seq %d must be positive", seq), err: ErrBadRequest}
	}
	path := "/v1/streams/" + url.PathEscape(stream) + "/messages/" + fmt.Sprintf("%d", seq) + "/data"
	resp, err := c.plain(ctx, "GET", path)
	if err != nil {
		return nil, err
	}
	data, rerr := readCapped(resp.Body, c.maxResponseBytes)
	drainErr := drain(resp.Body, 4096)
	closeErr := resp.Body.Close()
	switch {
	case rerr != nil:
		return nil, rerr
	case resp.StatusCode != http.StatusOK:
		return nil, &Error{
			Code:    "not_found",
			Message: fmt.Sprintf("GET %s: status %d", path, resp.StatusCode),
			Status:  resp.StatusCode,
			err:     ErrNotFound,
		}
	case closeErr != nil:
		return nil, unreachable("GET "+path, closeErr)
	case drainErr != nil:
		return data, nil
	}
	return data, nil
}

func (c *Client) PeekByID(ctx context.Context, id string) (MessageView, error) {
	if id == "" {
		return MessageView{}, &Error{Code: "bad_request", Message: "message id is empty", err: ErrBadRequest}
	}
	return do[MessageView](ctx, c, "GET", "/v1/messages/"+url.PathEscape(id), nil, nil)
}
