// SPDX-License-Identifier: Apache-2.0

// Package errs holds messq's closed sentinel set and the teaching-error carrier.
//
// Every classifiable failure in the tree is one of the sentinels declared here, wrapped with
// %w. Boundaries classify with errors.Is, never by matching strings, and the daemon never
// panics for an operational condition.
//
// The set is closed on purpose. The HTTP layer maps every sentinel onto the machine-readable
// code of the error envelope (PLAN.md section 7) and the CLI maps the same set onto the
// documented exit codes (PLAN.md section 8); both iterate [All], so adding a sentinel without
// adding its row breaks those tests. That is the point of the registry, not a side effect.
//
// [Error] carries the teaching format: what happened, and what to type next. The Next
// suggestions are commands, never prose, and they are what the CLI prints under the error and
// what the API returns in the envelope's next array.
package errs
