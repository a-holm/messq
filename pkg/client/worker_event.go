// SPDX-License-Identifier: Apache-2.0

package client

import (
	"fmt"
	"time"
)

// EventKind names one observable worker occurrence. The words are §9.2's vocabulary,
// so a worker's log line and the broker's event row use one word for one thing.
type EventKind uint8

// The worker event kinds.
const (
	EventStarted          EventKind = iota // preflight GetConsumer answered
	EventFetched                           // a fetch returned messages
	EventHandling                          // a handler started
	EventAcked                             // an ack landed
	EventNaked                             // a nak landed
	EventTermed                            // a term landed (straight to DLQ)
	EventExtended                          // one batched extend landed
	EventLeaseLost                         // the broker no longer honours a token
	EventLeaseCapped                       // T7's cap reached / extend_capped answered
	EventStaleAck                          // a settle came back stale
	EventHold                              // a fetch parked on a hold reason
	EventReconnect                         // transport failed; backing off
	EventPanicRecovered                    // a handler panicked; policy applied
	EventAckLost                           // an ack could not be delivered after retries
	EventHandlerTimeout                    // MaxLease reached with the handler still running
	EventClockSkew                         // |deadline_ms − localDeadline| > ackWait/2
	EventOutcomeDiscarded                  // a late handler result dropped: lease was lost
	EventDrained                           // the worker drained cleanly
)

func (k EventKind) String() string {
	switch k {
	case EventStarted:
		return "started"
	case EventFetched:
		return "fetched"
	case EventHandling:
		return "handling"
	case EventAcked:
		return "acked"
	case EventNaked:
		return "naked"
	case EventTermed:
		return "termed"
	case EventExtended:
		return "extended"
	case EventLeaseLost:
		return "lease_lost"
	case EventLeaseCapped:
		return "lease_capped"
	case EventStaleAck:
		return "stale_ack"
	case EventHold:
		return "hold"
	case EventReconnect:
		return "reconnect"
	case EventPanicRecovered:
		return "panic_recovered"
	case EventAckLost:
		return "ack_lost"
	case EventHandlerTimeout:
		return "handler_timeout"
	case EventClockSkew:
		return "clock_skew"
	case EventOutcomeDiscarded:
		return "outcome_discarded"
	case EventDrained:
		return "drained"
	default:
		return fmt.Sprintf("event(%d)", uint8(k))
	}
}

// WorkerEvent is the struct-typed observability callback payload: no logging
// dependency, renderable by #23/#25 however they like.
type WorkerEvent struct {
	Kind     EventKind
	Msg      *Delivered    // the message concerned, when there is one
	Err      error         // warning text (Started), the handler's error, or the cause
	Duration time.Duration // handling duration where meaningful
	Hold     HoldReason    // EventHold only
	Attempt  int           // delivery attempt, where known
	Stack    []byte        // EventPanicRecovered only
}

// WorkerStats is the counter set #31 samples between polls.
type WorkerStats struct {
	Fetched        int64 // messages claimed
	Acked          int64
	Naked          int64
	Termed         int64
	Extends        int64 // TOKENS extended, across batched requests
	LeaseLost      int64
	StaleAcks      int64
	Reconnects     int64
	LeakedHandlers int64 // handlers still running past their lease's death
}
