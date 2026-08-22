// SPDX-License-Identifier: Apache-2.0

package obs

// Event is one committed state change handed to the projection sinks after its batch
// committed. This issue (#6) fixes only the carrier: the closed vocabulary, the per-event
// field schema, the slog handlers and the follower rings are #19's. Renaming a field here is
// a breaking change once #19 lands.
//
// Events are produced inside Apply (in-transaction event rows are the source of truth,
// PLAN §D11) and published OUTSIDE the transaction, strictly after the commit that made them
// real — a dropped or rolled-back batch can never produce a projection.
type Event struct {
	// Event is the vocabulary name, e.g. "msg.publish". Closed set; see SEMANTICS S2.4.
	Event string

	// TS is the batch timestamp in Unix milliseconds: every row and every event of one
	// batch shares it.
	TS int64

	// Identity fields for trace and fan-out; empty where not applicable. No cardinality
	// rule applies to the struct itself — it never becomes a metric label.
	Stream   string
	Consumer string
	Subject  string
	MsgID    string
	TraceID  string
	Actor    string

	Seq     int64
	Attempt int64

	// Detail carries command-specific payload for the sink handlers.
	Detail map[string]any
}

// Sink receives committed events for fan-out (slog lines, metric counters, /v1/events
// followers). Publish runs on the writer's fan-out pump, off the reply path — but it MUST
// NOT block regardless: implementations buffer internally and drop loudly on overflow
// (#19 owns the ring policy).
type Sink interface {
	Publish([]Event)
}

// NopSink is the default [Sink]: events are discarded. A deployment without #19's handlers
// configured loses nothing durable — the events table already holds everything.
type NopSink struct{}

// Publish discards the events.
func (NopSink) Publish([]Event) {}
