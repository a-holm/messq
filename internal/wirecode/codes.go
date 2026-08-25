// SPDX-License-Identifier: Apache-2.0

// Package wirecode is the single source of truth for messq's closed machine-code enum
// (PLAN.md section 7: "Codes are a closed, documented enum and part of the 1.0
// compatibility contract"). The API layer's code→status mapping, #14's neverOverHTTP
// set and the wire-contract gates of issue #18 all consume THIS table so they cannot
// drift apart.
//
// Adding a row is additive; changing a status or removing a member is a BREAKING wire
// change governed by docs/compatibility.md. Layering rule (scripts/layers.sh): this
// package imports nothing internal — it sits at the bottom next to internal/errs.
package wirecode

import "sort"

// Code is one machine code of the closed §7 error envelope enum.
type Code string

// The codes on the wire today, plus the members pinned by PLAN/issues that ship with
// later milestones. Declaration order here is documentation order; iteration order is
// always sorted.
const (
	// Produced by the current API surface.
	NotFound        Code = "not_found"
	StreamExists    Code = "stream_exists"
	Conflict        Code = "conflict"
	ImmutableField  Code = "immutable_field"
	WouldLoseData   Code = "would_lose_data"
	ReservedName    Code = "reserved_name"
	BadRequest      Code = "bad_request"
	BadSubject      Code = "bad_subject"
	SubjectMismatch Code = "subject_mismatch"
	HeaderTooLarge  Code = "header_too_large"
	ReservedHeader  Code = "reserved_header"
	Unsupported     Code = "unsupported"
	TooLarge        Code = "too_large"
	ReadOnly        Code = "read_only"
	ShuttingDown    Code = "shutting_down"
	Internal        Code = "internal"

	// Rebase completion (#18 × #14): these rows were seeded before #14 merged;
	// its classifier maps them from S13 sentinels and live routes emit them
	// (settle answers stale_ack, fetch enforces flow_control), so they are
	// produced on the wire today.
	Unauthorized Code = "unauthorized" // #14 bearer auth, shipped
	Forbidden    Code = "forbidden"    // #14 roles, shipped
	InvalidToken Code = "invalid_token"
	StaleAck     Code = "stale_ack"
	Paused       Code = "paused"
	FlowControl  Code = "flow_control"
	StreamFull   Code = "stream_full"
	DiskFull     Code = "disk_full" // emittable today; #17 continues with degraded-writes semantics

	// Reserved for named future issues; producing one before its owner ships fails
	// the wire freeze.
	RateLimited Code = "rate_limited" // #39: max in-flight / flow control

	// NeverOverHTTP: these sentinels exist in internal/errs but can never appear in
	// an HTTP envelope — two are startup refusals before any listener exists, and
	// one lives entirely client-side.
	Locked      Code = "locked"       // data dir held by another process
	SchemaNewer Code = "schema_newer" // data dir written by a newer binary
	Unavailable Code = "unavailable"  // client-side: daemon unreachable
)

// Kind classifies a table row.
type Kind int

const (
	// Produced: the daemon emits this code today.
	Produced Kind = iota
	// Planned: frozen now, produced by a route still being built.
	Planned
	// Reserved: held for the owning issue; emitting it early fails the freeze.
	Reserved
	// NeverOverHTTP: never appears in an HTTP envelope at all.
	NeverOverHTTP
)

// Entry is one row of the closed-code table.
type Entry struct {
	// Status is the HTTP status the envelope carries; 0 when the code never rides
	// HTTP.
	Status int
	// Kind classifies the row.
	Kind Kind
	// Owner names the issue that owns a Planned/Reserved row ("#39"); empty for
	// Produced rows.
	Owner string
}

// Table is THE source. Edit it and every consumer moves together: the API's
// codeStatus map, the freeze gates, PROTOCOL.md's code table and #14's envelope.
var Table = map[Code]Entry{
	NotFound:        {Status: 404},
	StreamExists:    {Status: 409},
	Conflict:        {Status: 409},
	ImmutableField:  {Status: 409},
	WouldLoseData:   {Status: 409},
	ReservedName:    {Status: 400},
	BadRequest:      {Status: 400},
	BadSubject:      {Status: 400},
	SubjectMismatch: {Status: 400},
	HeaderTooLarge:  {Status: 400},
	ReservedHeader:  {Status: 400},
	Unsupported:     {Status: 400},
	TooLarge:        {Status: 413},
	ReadOnly:        {Status: 503},
	ShuttingDown:    {Status: 503},
	Internal:        {Status: 500},

	Unauthorized: {Status: 401},
	Forbidden:    {Status: 403},
	InvalidToken: {Status: 400},
	StaleAck:     {Status: 409},
	Paused:       {Status: 409},
	FlowControl:  {Status: 429},
	StreamFull:   {Status: 507},
	DiskFull:     {Status: 507},

	RateLimited: {Status: 429, Kind: Reserved, Owner: "#39"},

	Locked:      {Kind: NeverOverHTTP},
	SchemaNewer: {Kind: NeverOverHTTP},
	Unavailable: {Kind: NeverOverHTTP},
}

// All returns every code in sorted order. Deterministic iteration is what makes the
// committed enum artifacts diff-stable.
func All() []Code {
	out := make([]Code, 0, len(Table))
	for c := range Table {
		out = append(out, c)
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}

// NeverOverHTTPSet returns the codes that can never appear in an HTTP envelope,
// sorted. This is the set #14's envelope refuses to emit and the auth/listener tests
// assert stays off the wire.
func NeverOverHTTPSet() []Code {
	var out []Code
	for _, c := range All() {
		if Table[c].Kind == NeverOverHTTP {
			out = append(out, c)
		}
	}
	return out
}
