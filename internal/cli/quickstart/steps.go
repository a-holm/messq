// SPDX-License-Identifier: Apache-2.0

package quickstart

// TourSteps is the seven-step plan as DATA (issue §1): the addition of the
// executed redrive step is one entry, and the argv/echo equality test walks
// exactly this list. Every argv runs through the real command tree — there is
// no private renderer anywhere in the tour.
//
// Step 6's deliberate ack_wait timeout and step 7's trace need the delivery
// commands (stream/pub/sub/trace) that #13/#14 own; on a tree without them the
// engine stops at the first unknown command with its teaching footer, which is
// the correct behaviour for a tour whose commands are missing — a tour that
// fakes its steps would be a lie.
func TourSteps() []Step {
	return []Step{
		{
			Title: "A stream is a durable, append-only sequence of messages.",
			Argv:  []string{"stream", "add", "demo", "--subjects", "demo.>"},
		},
		{
			Title: "Publishing returns only after the write is on disk.",
			Argv:  []string{"pub", "demo", "demo.hello", "--data", "hello world"},
		},
		{
			Title: "A consumer is a durable cursor with retry rules of its own.",
			Argv:  []string{"consumer", "add", "demo", "worker", "--ack-wait", "2s", "--max-deliver", "3", "--backoff", "500ms"},
		},
		{
			Title: "Fetch, work, ack. An ack deletes the delivery — the pending set stays small.",
			Argv:  []string{"sub", "demo", "worker", "--count", "1"},
		},
		{
			Title: "A worker that fails temporarily naks; messq retries it on the backoff schedule.",
			Argv:  []string{"pub", "demo", "demo.flaky", "--data", `{"charge":42}`},
			Note:  "the next command runs a worker whose first attempt fails on purpose",
		},
		{
			Title: "A worker that fails then succeeds is the nak path, seen live.",
			Argv:  []string{"sub", "demo", "worker", "--count", "1", "--exec", "messq quickstart-handler"},
			Note:  "attempt 1 exits 75 (nak); attempt 2 exits 0 (ack) — resolved",
		},
		{
			Title: "A worker that never answers is the interesting case. Nobody acks this one.",
			Argv:  []string{"pub", "demo", "demo.poison", "--data", "bad payload"},
		},
		{
			Title: "Every transition was written in the same transaction as the change. Replay it:",
			Argv:  []string{"trace", "LAST_PUBLISHED_ID"},
			Note:  "the trace is printed unprompted: the DLQ message, replayed from the journal",
		},
	}
}
