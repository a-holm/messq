// SPDX-License-Identifier: Apache-2.0

package client

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
)

// deliveredGolden is this package's provisional decode fixture for the fetch wire
// shape. Issue #18 owns the committed contract goldens; until they land, this file
// pins the shape the daemon on main emits today (internal/api/fetch.go), plus three
// injected unknown fields proving lenient decode.
const deliveredGolden = `{
  "stream": "orders",
  "consumer": "workers",
  "seq": 7,
  "id": "01J8ZC9Q2M3N4P5Q6R7S8T9UVW",
  "subject": "orders.west",
  "headers": {"tenant": "acme"},
  "body_b64": "aGVsbG8sIG1lc3Nx",
  "size": 11,
  "attempt": 2,
  "max_deliver": 5,
  "ack_token": "orders/workers/7/2/1",
  "deadline_ms": 1756100000000,
  "ack_wait_ms": 30000,
  "published_at": 1756099999000,
  "trace_id": "5af0…",
  "x_unknown_field": {"later": true},
  "y_unknown_field": [1, 2, 3],
  "z_unknown_field": "ignored"
}`

func wantDelivered() Delivered {
	return Delivered{
		Stream:      "orders",
		Consumer:    "workers",
		Seq:         7,
		ID:          "01J8ZC9Q2M3N4P5Q6R7S8T9UVW",
		Subject:     "orders.west",
		Header:      map[string]string{"tenant": "acme"},
		Body:        []byte("hello, messq"),
		Size:        11,
		Attempt:     2,
		MaxDeliver:  5,
		AckToken:    "orders/workers/7/2/1",
		DeadlineMS:  1756100000000,
		AckWaitMS:   30000,
		PublishedAt: 1756099999000,
		TraceID:     "5af0…",
	}
}

func TestDeliveredDecodesLeniently(t *testing.T) {
	t.Parallel()

	var got Delivered
	if err := json.Unmarshal([]byte(deliveredGolden), &got); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if diff := cmp.Diff(wantDelivered(), got); diff != "" {
		t.Errorf("delivered decode mismatch (-want +got):\n%s", diff)
	}
	if got.Body == nil || string(got.Body) != "hello, messq" {
		t.Errorf("Body = %v, want base64-decoded bytes", got.Body)
	}
}

func TestDeliveredZeroLengthBodyIsNotNil(t *testing.T) {
	t.Parallel()

	var got Delivered
	const in = `{"stream":"s","consumer":"c","seq":1,"body_b64":"","size":0,"attempt":1}`
	if err := json.Unmarshal([]byte(in), &got); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if got.Body == nil {
		t.Fatal("Body is nil for a zero-length body; must be a non-nil empty slice")
	}
	if len(got.Body) != 0 {
		t.Errorf("len(Body) = %d, want 0", len(got.Body))
	}
}

func TestDedupKey(t *testing.T) {
	t.Parallel()

	d := wantDelivered()
	if got := d.DedupKey(); got != "orders/7" {
		t.Errorf("DedupKey() = %q, want stream/seq", got)
	}
	// Stable across redelivery: attempt moves, key does not.
	d.Attempt = 9
	if d.DedupKey() != "orders/7" {
		t.Error("DedupKey changed with attempt")
	}
}

func TestAttemptOf(t *testing.T) {
	t.Parallel()

	d := wantDelivered()
	if got := d.AttemptOf(); got != "2/5" {
		t.Errorf("AttemptOf() = %q, want 2/5", got)
	}
	d.MaxDeliver = 0 // unlimited
	if got := d.AttemptOf(); got != "2/∞" {
		t.Errorf("AttemptOf() = %q, want 2/∞ for max_deliver=0", got)
	}
}

func TestSettleResultDecodes(t *testing.T) {
	t.Parallel()

	const in = `{
      "results": [
        {"token":"orders/w/7/2/1","result":"ok"},
        {"token":"orders/w/8/1/1","result":"stale","reason":"attempt moved on",
         "token_attempt":1,"current_attempt":2},
        {"token":"garbage","result":"unknown","reason":"unparseable token"}
      ],
      "ok": 1, "stale": 1, "unknown": 1
    }`
	var got SettleResult
	if err := json.Unmarshal([]byte(in), &got); err != nil {
		t.Fatalf("decode: %v", err)
	}
	want := SettleResult{
		Results: []SettleItem{
			{Token: "orders/w/7/2/1", Status: SettleOK},
			{
				Token: "orders/w/8/1/1", Status: SettleStale, Reason: "attempt moved on",
				TokenAttempt: 1, CurrentAttempt: 2,
			},
			{Token: "garbage", Status: SettleUnknown, Reason: "unparseable token"},
		},
		OK: 1, Stale: 1, Unknown: 1,
	}
	if diff := cmp.Diff(want, got); diff != "" {
		t.Errorf("settle decode mismatch (-want +got):\n%s", diff)
	}
}

func TestSettleItemStatusConstants(t *testing.T) {
	t.Parallel()

	// The five-value frozen vocabulary (#10). The wire renders the subset the API
	// produces today; the constants cover the enum so callers can switch over all of
	// them and unknown values still survive decode verbatim.
	for _, s := range []SettleStatus{SettleOK, SettleStale, SettleStaleAck, SettleWrongGeneration, SettleUnknown} {
		if !strings.Contains(string(s), "/") && len(s) == 0 {
			t.Errorf("empty status constant %q", s)
		}
	}
	var preserved SettleItem
	if err := json.Unmarshal([]byte(`{"token":"t","result":"brand_new"}`), &preserved); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if preserved.Status != "brand_new" {
		t.Errorf("unknown result value not preserved verbatim: %q", preserved.Status)
	}
}

func TestFetchRoundTripShapeTags(t *testing.T) {
	t.Parallel()

	// The REQUEST marshals into the wire's flat millisecond form.
	req := FetchRequest{Batch: 4, Wait: 1500 * time.Millisecond, MaxBytes: 4096}
	b, err := json.Marshal(fetchRequestWire(req))
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	want := `{"batch":4,"max_bytes":4096,"wait_ms":1500}`
	if string(b) != want {
		t.Errorf("fetch request wire = %s, want %s", b, want)
	}

	// The RESPONSE decodes hold_reason into the typed constants (through the wire
	// mirror, exactly as production does).
	const in = `{"messages":[],"hold_reason":"paused","retry_after_ms":500,
	             "pending":3,"backlog":9,"batch":1,"max_bytes":1048576,"wait_ms":100}`
	var wire fetchResponseWire
	if err := json.Unmarshal([]byte(in), &wire); err != nil {
		t.Fatalf("decode: %v", err)
	}
	got := wire.export()
	if got.Hold != HoldPaused || got.RetryAfter != 500*time.Millisecond ||
		got.Pending != 3 || got.Backlog != 9 ||
		got.Batch != 1 || got.MaxBytes != 1048576 || got.Wait != 100*time.Millisecond {
		t.Errorf("fetch response decode = %+v", got)
	}
}
