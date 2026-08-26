// SPDX-License-Identifier: Apache-2.0

package client

import (
	"encoding/json"
	"strconv"
	"time"
)

// Msg is one message to publish. Header keys ride as Messq-Header-* request headers;
// a key whose canonical form starts with "Messq-" is rejected locally (the daemon's
// reserved namespace), as are keys that cannot be MIME header names.
type Msg struct {
	Subject string
	Body    []byte
	Header  map[string]string // user headers → Messq-Header-*
	MsgID   string            // → Messq-Msg-Id; makes a publish retry-safe (§5.1)
	TraceID string            // → Messq-Trace-Id; empty ⇒ the server mints one
}

// PublishAck is the receipt of one stored (or deduplicated) message.
type PublishAck struct {
	Stream      string `json:"stream"`
	Seq         int64  `json:"seq"`
	ID          string `json:"id"` // ULID
	TraceID     string `json:"trace_id"`
	Duplicate   bool   `json:"duplicate"` // dedup hit — success, not an error
	PublishedAt int64  `json:"published_at"`
}

// PublishBatchAck reports one receipt per entry of a batch, in input order.
type PublishBatchAck struct {
	Stream  string       `json:"stream"`
	Results []PublishAck `json:"results"`
}

// HoldReason is the typed form of the fetch response's hold_reason: why a fetch
// returned without messages. The values are the closed set the wire carries today;
// unknown future values decode verbatim into the string type.
type HoldReason string

// The hold reasons, exactly as the daemon writes them.
const (
	HoldNone         HoldReason = ""
	HoldPaused       HoldReason = "paused"
	HoldFlowControl  HoldReason = "flow_control"
	HoldBackoff      HoldReason = "backoff"
	HoldCatchingUp   HoldReason = "catching_up"
	HoldEmpty        HoldReason = "empty"
	HoldShuttingDown HoldReason = "shutting_down"
)

// Delivered is one message handed to a consumer. Body arrives base64 on the wire
// (body_b64) and decodes through encoding/json's []byte support — there is no
// hand-rolled base64 path anywhere in this package.
type Delivered struct {
	Stream      string            `json:"stream"`
	Consumer    string            `json:"consumer"`
	Seq         int64             `json:"seq"`
	ID          string            `json:"id"`
	Subject     string            `json:"subject"`
	Header      map[string]string `json:"headers,omitempty"`
	Body        []byte            `json:"body_b64"`
	Size        int64             `json:"size"`
	Attempt     int               `json:"attempt"`
	MaxDeliver  int               `json:"max_deliver"` // 0 = unlimited
	AckToken    string            `json:"ack_token"`
	DeadlineMS  int64             `json:"deadline_ms"`  // server wall clock — display/skew detection ONLY
	AckWaitMS   int64             `json:"ack_wait_ms"`  // a duration: skew-free; what scheduling uses
	PublishedAt int64             `json:"published_at"` // unix ms
	TraceID     string            `json:"trace_id"`
}

// deliveredAlias keeps UnmarshalJSON from recursing.
type deliveredAlias Delivered

// UnmarshalJSON decodes the wire shape and normalises a zero-length body to a non-nil
// empty slice, so callers never nil-check.
func (d *Delivered) UnmarshalJSON(b []byte) error {
	type str = deliveredAlias
	var a str
	if err := json.Unmarshal(b, &a); err != nil {
		return err
	}
	*d = Delivered(a)
	if d.Body == nil {
		d.Body = []byte{}
	}
	return nil
}

// DedupKey is THE consumer-side deduplication key (PLAN §6): stream/seq, stable across
// every redelivery and across a dead-letter redrive — unlike msg_id, which is a
// publish-side identity.
func (d *Delivered) DedupKey() string {
	return d.Stream + "/" + strconv.FormatInt(d.Seq, 10)
}

// AttemptOf renders the attempt as "2/5", or "2/∞" when max_deliver is 0 (unlimited).
func (d *Delivered) AttemptOf() string {
	if d.MaxDeliver == 0 {
		return strconv.Itoa(d.Attempt) + "/∞"
	}
	return strconv.Itoa(d.Attempt) + "/" + strconv.Itoa(d.MaxDeliver)
}

// FetchRequest asks for up to Batch messages, parking server-side up to Wait when the
// consumer has nothing ready. Zero fields mean the server defaults (batch 1).
type FetchRequest struct {
	Batch    int           // 0 ⇒ 1 (the server's default)
	Wait     time.Duration // 0 ⇒ single-shot
	MaxBytes int64         // 0 ⇒ the server's configured cap
}

// fetchRequestWire is the POST body shape: flat integer milliseconds.
type fetchRequestWireShape struct {
	Batch    int   `json:"batch"`
	MaxBytes int64 `json:"max_bytes"`
	WaitMS   int64 `json:"wait_ms"`
}

func fetchRequestWire(r FetchRequest) fetchRequestWireShape {
	return fetchRequestWireShape{
		Batch:    r.Batch,
		MaxBytes: r.MaxBytes,
		WaitMS:   r.Wait.Milliseconds(),
	}
}

// FetchResponse is the ONE fetch answer shape: possibly-empty messages plus why
// (Hold), how long to wait before asking again (RetryAfter), and the EFFECTIVE
// clamped request values the server actually used.
type FetchResponse struct {
	Messages   []Delivered
	Hold       HoldReason // "" | paused | flow_control | backoff | catching_up | empty | shutting_down
	RetryAfter time.Duration
	Pending    int64
	Backlog    int64
	Batch      int           // EFFECTIVE batch after server clamping
	MaxBytes   int64         // EFFECTIVE byte cap
	Wait       time.Duration // EFFECTIVE wait
}

// fetchResponseWire mirrors internal/api's fetchResponse exactly.
type fetchResponseWire struct {
	Messages     []Delivered `json:"messages"`
	HoldReason   string      `json:"hold_reason"`
	RetryAfterMS int64       `json:"retry_after_ms"`
	Pending      int64       `json:"pending"`
	Backlog      int64       `json:"backlog"`

	Batch    int   `json:"batch"`
	MaxBytes int64 `json:"max_bytes"`
	WaitMS   int64 `json:"wait_ms"`
}

func (w fetchResponseWire) export() FetchResponse {
	out := FetchResponse{
		Messages:   w.Messages,
		Hold:       HoldReason(w.HoldReason),
		RetryAfter: time.Duration(w.RetryAfterMS) * time.Millisecond,
		Pending:    w.Pending,
		Backlog:    w.Backlog,
		Batch:      w.Batch,
		MaxBytes:   w.MaxBytes,
		Wait:       time.Duration(w.WaitMS) * time.Millisecond,
	}
	if out.Messages == nil {
		out.Messages = []Delivered{}
	}
	return out
}

// SettleStatus is the frozen five-value outcome vocabulary of one settle item (#10):
// ok did it; stale was nothing-to-do; stale_ack says a duplicate probably happened
// (D7); wrong_generation fences an older consumer generation; unknown means the token
// could not be resolved at all. Unknown future values survive decode verbatim.
type SettleStatus string

// The settle item statuses.
const (
	SettleOK              SettleStatus = "ok"
	SettleStale           SettleStatus = "stale"
	SettleStaleAck        SettleStatus = "stale_ack"
	SettleWrongGeneration SettleStatus = "wrong_generation"
	SettleUnknown         SettleStatus = "unknown"
)

// SettleItem is the per-token outcome. Results arrive in REQUEST order, including
// tokens that could not be parsed (those come back unknown).
type SettleItem struct {
	Token          string       `json:"token"`
	Status         SettleStatus `json:"result"`
	Reason         string       `json:"reason,omitempty"`
	TokenAttempt   int          `json:"token_attempt,omitempty"`
	CurrentAttempt int          `json:"current_attempt,omitempty"`
}

// SettleResult is the whole settle answer: per-token results plus the honest counters.
// Present even on all-failed responses, so per-token truth never depends on the HTTP
// status.
type SettleResult struct {
	Results []SettleItem `json:"results"`
	OK      int          `json:"ok"`
	Stale   int          `json:"stale"`
	Unknown int          `json:"unknown"`
}

// Item maps a token to its outcome; convenience over Results for single-token calls.
func (r SettleResult) Item() SettleItem {
	if len(r.Results) == 0 {
		return SettleItem{}
	}
	return r.Results[0]
}
