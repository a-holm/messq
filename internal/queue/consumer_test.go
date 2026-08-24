// SPDX-License-Identifier: Apache-2.0

package queue

import (
	"errors"
	"testing"
	"time"

	"github.com/a-holm/messq/internal/errs"
)

func validConsumerConfig() ConsumerConfig {
	return ConsumerConfig{
		Name:          "worker",
		Filters:       []string{"orders.>"},
		AckWait:       30 * time.Second,
		MaxDeliver:    5,
		MaxAckPending: 1000,
		Backoff:       []time.Duration{time.Second, 5 * time.Second, 30 * time.Second},
		DeadPolicy:    DeadPolicyDLQ,
	}
}

func TestValidateConsumerConfig(t *testing.T) {
	tests := []struct {
		name    string
		mutate  func(*ConsumerConfig)
		wantErr error
	}{
		{"valid", func(*ConsumerConfig) {}, nil},
		{"bad name slash", func(c *ConsumerConfig) { c.Name = "a/b" }, errs.ErrBadRequest},
		{"bad name dot", func(c *ConsumerConfig) { c.Name = "a.b" }, errs.ErrBadRequest},
		{"no filters", func(c *ConsumerConfig) { c.Filters = nil }, errs.ErrBadRequest},
		{"invalid filter", func(c *ConsumerConfig) { c.Filters = []string{"orders..x"} }, errs.ErrBadSubject},
		{"ack wait below min", func(c *ConsumerConfig) { c.AckWait = 99 * time.Millisecond }, errs.ErrBadRequest},
		{"ack wait above max", func(c *ConsumerConfig) { c.AckWait = time.Hour + time.Millisecond }, errs.ErrBadRequest},
		{"max deliver negative", func(c *ConsumerConfig) { c.MaxDeliver = -1 }, errs.ErrBadRequest},
		{"max deliver over cap", func(c *ConsumerConfig) { c.MaxDeliver = 1001 }, errs.ErrBadRequest},
		{"max ack pending zero", func(c *ConsumerConfig) { c.MaxAckPending = 0 }, errs.ErrBadRequest},
		{"max ack pending over cap", func(c *ConsumerConfig) { c.MaxAckPending = 1_000_001 }, errs.ErrBadRequest},
		{"no backoff", func(c *ConsumerConfig) { c.Backoff = nil }, errs.ErrBadRequest},
		{"backoff entry over cap", func(c *ConsumerConfig) {
			c.Backoff = []time.Duration{25 * time.Hour}
		}, errs.ErrBadRequest},
		{"dead policy closed set", func(c *ConsumerConfig) { c.DeadPolicy = "fancy" }, errs.ErrBadRequest},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := validConsumerConfig()
			tt.mutate(&cfg)
			_, err := ValidateConsumerConfig(cfg, DefaultConsumerLimits())
			switch {
			case tt.wantErr == nil && err != nil:
				t.Fatalf("ValidateConsumerConfig() = %v, want nil", err)
			case tt.wantErr != nil && !errors.Is(err, tt.wantErr):
				t.Fatalf("ValidateConsumerConfig() = %v, want %v", err, tt.wantErr)
			}
		})
	}
}

func TestValidateConsumerConfigBoundaries(t *testing.T) {
	l := DefaultConsumerLimits()
	base := validConsumerConfig()

	// ack_wait boundary: exactly the min and max are legal.
	base.AckWait = l.MinAckWait
	if _, err := ValidateConsumerConfig(base, l); err != nil {
		t.Fatalf("ack_wait at min refused: %v", err)
	}
	base.AckWait = l.MaxAckWait
	if _, err := ValidateConsumerConfig(base, l); err != nil {
		t.Fatalf("ack_wait at max refused: %v", err)
	}
	base = validConsumerConfig()

	// max_deliver boundary: 0 and 1000 legal.
	base.MaxDeliver = 0
	if _, err := ValidateConsumerConfig(base, l); err != nil {
		t.Fatalf("max_deliver 0 refused: %v", err)
	}
	base.MaxDeliver = 1000
	if _, err := ValidateConsumerConfig(base, l); err != nil {
		t.Fatalf("max_deliver 1000 refused: %v", err)
	}
	base = validConsumerConfig()

	// max_ack_pending boundary: 1 and cap legal.
	base.MaxAckPending = 1
	if _, err := ValidateConsumerConfig(base, l); err != nil {
		t.Fatalf("max_ack_pending 1 refused: %v", err)
	}
	base.MaxAckPending = l.MaxAckPendingCap
	if _, err := ValidateConsumerConfig(base, l); err != nil {
		t.Fatalf("max_ack_pending at cap refused: %v", err)
	}
	base = validConsumerConfig()

	// backoff entry boundary: 0 and 24h legal; 16 entries legal.
	base.Backoff = []time.Duration{0}
	if _, err := ValidateConsumerConfig(base, l); err != nil {
		t.Fatalf("backoff[0]=0 refused: %v", err)
	}
	base.Backoff = []time.Duration{24 * time.Hour}
	if _, err := ValidateConsumerConfig(base, l); err != nil {
		t.Fatalf("backoff[0]=24h refused: %v", err)
	}
	base.Backoff = make([]time.Duration, 16)
	for i := range base.Backoff {
		base.Backoff[i] = time.Second
	}
	if _, err := ValidateConsumerConfig(base, l); err != nil {
		t.Fatalf("16 backoff entries refused: %v", err)
	}
	base.Backoff = make([]time.Duration, 17)
	for i := range base.Backoff {
		base.Backoff[i] = time.Second
	}
	if _, err := ValidateConsumerConfig(base, l); !errors.Is(err, errs.ErrBadRequest) {
		t.Fatalf("17 backoff entries accepted: %v", err)
	}
}

func TestValidateConsumerConfigWarnings(t *testing.T) {
	l := DefaultConsumerLimits()
	cfg := validConsumerConfig()
	cfg.MaxDeliver = 0
	w, err := ValidateConsumerConfig(cfg, l)
	if err != nil {
		t.Fatalf("max_deliver 0 must warn not error: %v", err)
	}
	if !hasWarningCode(w, WarningMaxDeliverUnlimited) {
		t.Fatalf("warnings = %+v, want max_deliver_unlimited", w)
	}

	cfg = validConsumerConfig()
	cfg.Backoff = []time.Duration{30 * time.Second, time.Second} // non-monotonic
	w, err = ValidateConsumerConfig(cfg, l)
	if err != nil {
		t.Fatalf("non-monotonic backoff must warn not error: %v", err)
	}
	if !hasWarningCode(w, WarningBackoffNonMonotonic) {
		t.Fatalf("warnings = %+v, want backoff_nonmonotonic", w)
	}
}

func hasWarningCode(w Warnings, code string) bool {
	for _, x := range w {
		if x.Code == code {
			return true
		}
	}
	return false
}

func TestDefaultConsumerConfigAndLimits(t *testing.T) {
	cfg := DefaultConsumerConfig("worker")
	if len(cfg.Filters) != 1 || cfg.Filters[0] != ">" {
		t.Errorf("filters = %v, want [\">\"]", cfg.Filters)
	}
	if cfg.AckWait != 30*time.Second || cfg.MaxDeliver != 5 || cfg.MaxAckPending != 1000 {
		t.Errorf("defaults = ack %v / deliver %d / pending %d, want 30s/5/1000",
			cfg.AckWait, cfg.MaxDeliver, cfg.MaxAckPending)
	}
	if cfg.DeadPolicy != DeadPolicyDLQ || cfg.Paused {
		t.Errorf("dead_policy/paused = %q/%v, want dlq/false", cfg.DeadPolicy, cfg.Paused)
	}
	if len(cfg.Backoff) != 5 || cfg.Backoff[0] != time.Second || cfg.Backoff[4] != 10*time.Minute {
		t.Errorf("backoff = %v, want [1s,5s,30s,2m,10m]", cfg.Backoff)
	}

	l := DefaultConsumerLimits()
	if l.MinAckWait != 100*time.Millisecond || l.MaxAckWait != time.Hour ||
		l.MaxFilters != 32 || l.MaxBackoffEntries != 16 || l.MaxBackoffDelay != 24*time.Hour ||
		l.MaxAckPendingCap != 1_000_000 || l.MaxConsumers != 1024 ||
		l.ScanLimit != 4096 || l.MaxFetchBatch != 1024 ||
		l.FetchMaxBytes != 8<<20 || l.FlowBlockedInterval != 10*time.Second {
		t.Errorf("DefaultConsumerLimits = %+v, want the §10 ceilings", l)
	}
	if _, err := ValidateConsumerConfig(cfg, l); err != nil {
		t.Fatalf("DefaultConsumerConfig does not pass its own validator: %v", err)
	}
}

func TestRetryHorizon(t *testing.T) {
	cfg := DefaultConsumerConfig("worker") // backoff [1s,5s,30s,2m,10m], max_deliver 5
	if got := RetryHorizon(cfg); got != 156*time.Second {
		t.Fatalf("RetryHorizon(default, 5) = %v, want 156s", got)
	}
	cfg.MaxDeliver = 0
	if got := RetryHorizon(cfg); got != HorizonInfinite {
		t.Fatalf("RetryHorizon(max_deliver=0) = %v, want infinite", got)
	}
	cfg = DefaultConsumerConfig("worker")
	cfg.MaxDeliver = 1
	if got := RetryHorizon(cfg); got != 0 {
		t.Fatalf("RetryHorizon(max_deliver=1) = %v, want 0", got)
	}
	cfg.MaxDeliver = 7 // 6 waits, last entry repeats: 1+5+30+120+600+600 = 1356s
	if got := RetryHorizon(cfg); got != 1356*time.Second {
		t.Fatalf("RetryHorizon(max_deliver=7) = %v, want 1356s", got)
	}
	cfg.Backoff = []time.Duration{30 * time.Second}
	cfg.MaxDeliver = 5 // single entry repeats: 4 * 30s = 120s
	if got := RetryHorizon(cfg); got != 120*time.Second {
		t.Fatalf("RetryHorizon(single 30s, 5) = %v, want 120s", got)
	}
}

// TestTimeToDead pins issue #11 §5 S8.4's worked table: the default config's worst-case
// first-delivery-to-DEAD is 306 s (MaxDeliver*ack_wait + RetryHorizon), infinite when
// unlimited, and finite-ish for tight configs so the "fifth backoff entry unused at
// max_deliver=5" invariant is asserted by construction (4 waits, not 5).
func TestTimeToDead(t *testing.T) {
	cfg := DefaultConsumerConfig("worker") // ack_wait 30s, max_deliver 5, backoff 5 entries
	if got := TimeToDead(cfg); got != 306*time.Second {
		t.Fatalf("TimeToDead(default,5) = %v, want 306s", got)
	}
	cfg.MaxDeliver = 0
	if got := TimeToDead(cfg); got != HorizonInfinite {
		t.Fatalf("TimeToDead(max_deliver=0) = %v, want infinite", got)
	}
	cfg = DefaultConsumerConfig("worker")
	cfg.MaxDeliver = 1
	if got := TimeToDead(cfg); got != 30*time.Second {
		t.Fatalf("TimeToDead(max_deliver=1) = %v, want 30s (one attempt, no wait)", got)
	}
	// ack_wait = 10s, max_deliver = 3, backoff [1s,5s]: 3*10 + (1+5) = 36s.
	cfg = DefaultConsumerConfig("worker")
	cfg.AckWait = 10 * time.Second
	cfg.MaxDeliver = 3
	cfg.Backoff = []time.Duration{1 * time.Second, 5 * time.Second}
	if got := TimeToDead(cfg); got != 36*time.Second {
		t.Fatalf("TimeToDead(10s,3,[1s,5s]) = %v, want 36s", got)
	}
}

func TestParseStartPosition(t *testing.T) {
	tests := []struct {
		in   string
		want StartPosition
	}{
		{"first", StartPosition{Kind: StartFirst}},
		{"new", StartPosition{Kind: StartNew}},
		{"seq:10494", StartPosition{Kind: StartSeq, Seq: 10494}},
		{"time:1757000042114", StartPosition{Kind: StartTime, Time: 1757000042114}},
	}
	for _, tt := range tests {
		got, err := ParseStartPosition(tt.in)
		if err != nil {
			t.Fatalf("ParseStartPosition(%q) = %v", tt.in, err)
		}
		if got != tt.want {
			t.Fatalf("ParseStartPosition(%q) = %+v, want %+v", tt.in, got, tt.want)
		}
		if got.String() != tt.in {
			t.Fatalf("round trip: String() = %q, want %q", got.String(), tt.in)
		}
	}
	for _, bad := range []string{"", "now", "seq:-1", "seq:abc", "time:-5", "time:x"} {
		if _, err := ParseStartPosition(bad); !errors.Is(err, errs.ErrBadRequest) {
			t.Fatalf("ParseStartPosition(%q) = %v, want ErrBadRequest", bad, err)
		}
	}
}

func TestDeadPolicyForStream(t *testing.T) {
	if got := DefaultDeadPolicyForStream("orders"); got != DeadPolicyDLQ {
		t.Fatalf("DefaultDeadPolicyForStream(orders) = %q, want dlq", got)
	}
	if got := DefaultDeadPolicyForStream("orders.dlq"); got != DeadPolicyDrop {
		t.Fatalf("DefaultDeadPolicyForStream(orders.dlq) = %q, want drop", got)
	}
	if err := ValidateDeadPolicyForStream("orders.dlq", DeadPolicyDLQ); !errors.Is(err, errs.ErrBadRequest) {
		t.Fatalf("dlq on .dlq stream = %v, want ErrBadRequest", err)
	}
	if err := ValidateDeadPolicyForStream("orders.dlq", DeadPolicyDrop); err != nil {
		t.Fatalf("drop on .dlq stream = %v, want nil", err)
	}
	if err := ValidateDeadPolicyForStream("orders", DeadPolicyDLQ); err != nil {
		t.Fatalf("dlq on normal stream = %v, want nil", err)
	}
}
