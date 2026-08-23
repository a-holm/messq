// SPDX-License-Identifier: Apache-2.0

package queue

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/a-holm/messq/internal/errs"
	"github.com/a-holm/messq/internal/subject"
)

// The consumer half of the pure validation layer (issue #9): consumer names, consumer
// configuration, start positions, the computed retry horizon and the dead-letter
// policy. Like the stream-side types it performs no I/O, reads no wall clock and
// iterates no maps, so #13's reference model and the fuzzers drive it directly.
// Delivery state itself lives in internal/store; this package only validates and
// computes.

// DeadPolicy is the closed set of dead-letter behaviours (§4.2): "dlq" routes the
// max_deliver-th failure to <stream>.dlq; "drop" discards it. Stored and defaulted
// here only — #12 owns the actual routing.
type DeadPolicy string

// The dead-letter policies.
const (
	// DeadPolicyDLQ redelivers the terminal failure to the stream's .dlq stream.
	DeadPolicyDLQ DeadPolicy = "dlq"
	// DeadPolicyDrop discards the terminal failure.
	DeadPolicyDrop DeadPolicy = "drop"
)

// Start is the closed set of start-position kinds a consumer may be created with
// (issue §1). It is honoured only at creation: moving an existing cursor is seek (#28).
type Start string

// The start-position kinds.
const (
	// StartFirst begins at the stream's oldest live message (first_seq).
	StartFirst Start = "first"
	// StartNew begins at the stream's head (stream_seq.next): only future messages.
	StartNew Start = "new"
	// StartSeq begins at a specific sequence number, clamped to [first_seq, next].
	StartSeq Start = "seq"
	// StartTime begins at the first message published at or after a wall-clock ms.
	StartTime Start = "time"
)

// StartPosition carries the creation-time cursor anchor. Kind selects which of Seq
// and Time is meaningful. The zero value is invalid: a consumer must be created with
// an explicit start.
type StartPosition struct {
	Kind Start
	Seq  int64 // StartSeq only
	Time int64 // StartTime only, unix ms
}

// String renders the wire form ParseStartPosition accepts, so the pair round-trips.
func (s StartPosition) String() string {
	switch s.Kind {
	case StartFirst:
		return "first"
	case StartNew:
		return "new"
	case StartSeq:
		return fmt.Sprintf("seq:%d", s.Seq)
	case StartTime:
		return fmt.Sprintf("time:%d", s.Time)
	default:
		return ""
	}
}

// ParseStartPosition parses the wire spelling of a start position: "first", "new",
// "seq:N" or "time:T". Anything else is errs.ErrBadRequest.
func ParseStartPosition(s string) (StartPosition, error) {
	switch s {
	case "first":
		return StartPosition{Kind: StartFirst}, nil
	case "new":
		return StartPosition{Kind: StartNew}, nil
	case "":
		return StartPosition{}, errs.E(errs.ErrBadRequest, "",
			"start is required: one of \"first\", \"new\", \"seq:N\", \"time:T\"")
	}
	if rest, ok := strings.CutPrefix(s, "seq:"); ok {
		n, err := strconv.ParseInt(rest, 10, 64)
		if err != nil || n < 0 {
			return StartPosition{}, errs.E(errs.ErrBadRequest, "",
				"start %q is not a valid sequence number", s)
		}
		return StartPosition{Kind: StartSeq, Seq: n}, nil
	}
	if rest, ok := strings.CutPrefix(s, "time:"); ok {
		n, err := strconv.ParseInt(rest, 10, 64)
		if err != nil || n < 0 {
			return StartPosition{}, errs.E(errs.ErrBadRequest, "",
				"start %q is not a valid unix-millisecond timestamp", s)
		}
		return StartPosition{Kind: StartTime, Time: n}, nil
	}
	return StartPosition{}, errs.E(errs.ErrBadRequest, "",
		"start %q is not one of \"first\", \"new\", \"seq:N\", \"time:T\"", s)
}

// ConsumerConfig is the validated shape of a consumer's configuration, as stored in
// the consumers row and as the fetch path consumes it. Durations carry milliseconds
// on the wire; here they are time.Duration so arithmetic stays honest.
type ConsumerConfig struct {
	Name          string
	Filters       []string        // NATS subject patterns; default [">"]
	AckWait       time.Duration   // default 30s
	MaxDeliver    int32           // default 5; 0 = unlimited (warned)
	MaxAckPending int64           // default 1000
	Backoff       []time.Duration // default [1s,5s,30s,2m,10m]; last value repeats
	DeadPolicy    DeadPolicy      // default "dlq"; "drop" on a .dlq stream
	Paused        bool
}

// ConsumerLimits are the process-wide ceilings from serve flags (issue §10): they
// bound what any single consumer may ask for, and what one fetch may do.
type ConsumerLimits struct {
	MinAckWait          time.Duration // 100ms
	MaxAckWait          time.Duration // 1h (--max-ack-wait)
	MaxFilters          int           // 32
	MaxBackoffEntries   int           // 16
	MaxBackoffDelay     time.Duration // 24h
	MaxAckPendingCap    int64         // 1_000_000
	MaxConsumers        int           // --max-consumers, 1024 (process-wide)
	ScanLimit           int           // --scan-limit, 4096
	MaxFetchBatch       int           // --max-fetch-batch, 1024
	FetchMaxBytes       int64         // --fetch-max-bytes, 8 MiB
	FlowBlockedInterval time.Duration // --flow-blocked-interval, 10s
}

// maxDeliverCap is the hard upper bound on max_deliver (§1): 0 means "retry forever",
// anything above 1000 is a misconfiguration.
const maxDeliverCap int32 = 1000

// Defaults for a consumer created without an explicit field (issue §1), verbatim
// from the §4.2 column defaults so schema and Go defaults cannot drift.
var defaultConsumerBackoff = []time.Duration{
	1 * time.Second, 5 * time.Second, 30 * time.Second, 2 * time.Minute, 10 * time.Minute,
}

// DefaultConsumerConfig returns the configuration a consumer that names nothing but
// itself receives: filter [">"], 30s ack wait, 5 max deliveries, 1000 pending,
// the five-entry backoff, dead_policy dlq, not paused.
func DefaultConsumerConfig(name string) ConsumerConfig {
	return ConsumerConfig{
		Name:          name,
		Filters:       []string{">"},
		AckWait:       30 * time.Second,
		MaxDeliver:    5,
		MaxAckPending: 1000,
		Backoff:       append([]time.Duration(nil), defaultConsumerBackoff...),
		DeadPolicy:    DeadPolicyDLQ,
	}
}

// DefaultConsumerLimits returns the §10 ceilings behind the serve-flag defaults.
func DefaultConsumerLimits() ConsumerLimits {
	return ConsumerLimits{
		MinAckWait:          100 * time.Millisecond,
		MaxAckWait:          time.Hour,
		MaxFilters:          32,
		MaxBackoffEntries:   16,
		MaxBackoffDelay:     24 * time.Hour,
		MaxAckPendingCap:    1_000_000,
		MaxConsumers:        1024,
		ScanLimit:           4096,
		MaxFetchBatch:       1024,
		FetchMaxBytes:       8 << 20,
		FlowBlockedInterval: 10 * time.Second,
	}
}

// ValidateConsumerName applies rule S11 minus '.': consumer names are ASCII, 1–64
// bytes, [A-Za-z0-9_-], start and end alphanumeric, no '/', no '.'. The dependency on
// the ack-token grammar runs both ways (S3.2): the token is "stream/consumer/…", so a
// consumer name must never contain '/', and it never contains '.' because the token's
// grammar needs the separator unambiguous. See subject.ValidateConsumerName.
func ValidateConsumerName(name string) error {
	return subject.ValidateConsumerName(name)
}

// ValidateConsumerConfig checks one whole configuration against the process limits:
// the name rules, filter-pattern grammar and count via the #3 compiler, the ack_wait
// window, max_deliver's closed range, the max_ack_pending window, the backoff array's
// length and per-entry window, and the dead_policy closed set. Findings that are
// misconfigurations but not refusals are returned as Warnings.
func ValidateConsumerConfig(c ConsumerConfig, l ConsumerLimits) (Warnings, error) {
	if err := ValidateConsumerName(c.Name); err != nil {
		return nil, err
	}
	if len(c.Filters) < 1 || len(c.Filters) > l.MaxFilters {
		return nil, errs.E(errs.ErrBadRequest, "",
			"a consumer carries %d filters, want 1..%d", len(c.Filters), l.MaxFilters)
	}
	if _, err := subject.ParseSet(c.Filters); err != nil {
		return nil, err
	}
	if c.AckWait < l.MinAckWait || c.AckWait > l.MaxAckWait {
		return nil, errs.E(errs.ErrBadRequest, "",
			"ack_wait is %v, want %v..%v", c.AckWait, l.MinAckWait, l.MaxAckWait)
	}
	if c.MaxDeliver < 0 || c.MaxDeliver > maxDeliverCap {
		return nil, errs.E(errs.ErrBadRequest, "",
			"max_deliver is %d, want 0..%d (0 = unlimited)", c.MaxDeliver, maxDeliverCap)
	}
	if c.MaxAckPending < 1 || c.MaxAckPending > l.MaxAckPendingCap {
		return nil, errs.E(errs.ErrBadRequest, "",
			"max_ack_pending is %d, want 1..%d", c.MaxAckPending, l.MaxAckPendingCap)
	}
	if len(c.Backoff) < 1 || len(c.Backoff) > l.MaxBackoffEntries {
		return nil, errs.E(errs.ErrBadRequest, "",
			"backoff holds %d entries, want 1..%d", len(c.Backoff), l.MaxBackoffEntries)
	}
	for i, d := range c.Backoff {
		if d < 0 || d > l.MaxBackoffDelay {
			return nil, errs.E(errs.ErrBadRequest, "",
				"backoff[%d] is %v, want 0..%v", i, d, l.MaxBackoffDelay)
		}
	}
	switch c.DeadPolicy {
	case DeadPolicyDLQ, DeadPolicyDrop:
	default:
		return nil, errs.E(errs.ErrBadRequest, "",
			"dead_policy %q is not one of \"dlq\", \"drop\"", c.DeadPolicy)
	}

	var w Warnings
	if c.MaxDeliver == 0 {
		w = append(w, Warning{Code: WarningMaxDeliverUnlimited, Message: "max_deliver is 0: a poison message will retry forever; you probably want a DLQ alert instead"})
	}
	if !backoffMonotonic(c.Backoff) {
		w = append(w, Warning{Code: WarningBackoffNonMonotonic, Message: "backoff entries are not non-decreasing"})
	}
	if c.MaxDeliver > 1 && RetryHorizon(c) < c.AckWait {
		w = append(w, Warning{Code: WarningHorizonShorterThanAckWait, Message: "retry horizon is shorter than ack_wait: retries will pile up"})
	}
	return w, nil
}

// DefaultDeadPolicyForStream returns the dead_policy a consumer on stream defaults to:
// "drop" on a ".dlq" stream (S3.4: no DLQ of a DLQ), "dlq" everywhere else.
func DefaultDeadPolicyForStream(stream string) DeadPolicy {
	if strings.HasSuffix(stream, reservedSuffix) {
		return DeadPolicyDrop
	}
	return DeadPolicyDLQ
}

// ValidateDeadPolicyForStream applies S3.4: a consumer on a ".dlq" stream may not
// route to dead-lettering (there is no DLQ of a DLQ), so an explicit "dlq" is refused.
func ValidateDeadPolicyForStream(stream string, dp DeadPolicy) error {
	if strings.HasSuffix(stream, reservedSuffix) && dp == DeadPolicyDLQ {
		return errs.E(errs.ErrBadRequest, "",
			"stream %q is a dead-letter stream; dead_policy must be \"drop\"", stream)
	}
	return nil
}

// HorizonInfinite is RetryHorizon's value for max_deliver = 0 (retry forever). It is
// the largest representable Duration, a sentinel no real horizon reaches.
const HorizonInfinite time.Duration = time.Duration(1<<63 - 1)

// RetryHorizon computes the total redelivery window a consumer with this config can
// span before the dead path: the sum of the first max_deliver-1 backoff entries, the
// last entry repeating past the end of the array (S8.4). max_deliver = 0 (unlimited)
// returns HorizonInfinite; max_deliver = 1 returns 0 (no redelivery). The default
// array and max_deliver = 5 therefore sum 1s+5s+30s+2m = 156s.
func RetryHorizon(c ConsumerConfig) time.Duration {
	if c.MaxDeliver == 0 {
		return HorizonInfinite
	}
	if c.MaxDeliver <= 1 || len(c.Backoff) == 0 {
		return 0
	}
	waits := int(c.MaxDeliver) - 1
	var sum time.Duration
	for i := 0; i < waits; i++ {
		idx := i
		if idx >= len(c.Backoff) {
			idx = len(c.Backoff) - 1
		}
		sum += c.Backoff[idx]
	}
	return sum
}

func backoffMonotonic(backoff []time.Duration) bool {
	for i := 1; i < len(backoff); i++ {
		if backoff[i] < backoff[i-1] {
			return false
		}
	}
	return true
}

// Warning is one non-fatal validation finding. Code is the stable machine-readable
// name; Message is the human sentence the CLI (#24) surfaces.
type Warning struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

// Warning codes returned by ValidateConsumerConfig.
const (
	WarningMaxDeliverUnlimited       = "max_deliver_unlimited"
	WarningBackoffNonMonotonic       = "backoff_nonmonotonic"
	WarningHorizonShorterThanAckWait = "horizon_shorter_than_ack_wait"
)

// Warnings is the ordered set of non-fatal findings a validation produced.
type Warnings []Warning
