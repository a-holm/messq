// SPDX-License-Identifier: Apache-2.0

// Package exec turns a shell script into a messq worker ("messq sub --exec",
// issue #25): one child process per message, payload on stdin, metadata in
// MESSQ_* environment variables, and the child's exit code as the ack decision.
//
// The one architectural rule (PLAN.md section 8, D14, issue #25 §1): this package
// contains NO fetch call, NO ack/nak/term/extend call and NO retry loop. It
// receives a message, runs a process, and returns an error that client.Worker
// translates into a settle. Everything durable belongs to #22's Worker; scripts/
// layers.sh enforces that internal/cli/exec may only import pkg/client and
// internal/clock, never net/http directly and never the store/api/queue layers.
//
// Sub-surface map (issue #25 §5):
//
//	words.go   SplitWords    — --exec command splitting (pure, fuzzed)
//	reason.go  SanitizeStderr — bounded stderr → operator-safe reason (pure, fuzzed)
//	outcome.go Classify      — exit code → transition table (pure)
//	env.go     BuildEnv      — the MESSQ_* child environment (pure, fuzzed)
//	child.go                 — spawn/pump/drain/escalate lifecycle
//	runner.go                — Handle: one message, one child, one outcome
//	render.go                — per-message lines and run summary (all three faces)
//	hint.go                  — the one-time exit-code hint
package exec
