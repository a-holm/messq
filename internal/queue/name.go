// SPDX-License-Identifier: Apache-2.0

// Package queue holds the pure validation layer of the publish and stream-lifecycle
// paths (issue #7) and the consumer-side configuration, token and start-position
// grammar (issue #9): stream names, stream configs, sparse updates, publish requests,
// header encoding, trace-id precedence, consumer configs and the ack token. It
// performs no I/O, reads no wall clock and iterates no maps, so the fuzzers can hammer
// it (PLAN §3.3) and #13's reference model can drive it directly.
package queue

import (
	"strings"

	"github.com/a-holm/messq/internal/errs"
	"github.com/a-holm/messq/internal/subject"
)

// ErrReservedName refuses a user-chosen stream name that ends in ".dlq": the suffix is
// how decision D3 derives a stream's dead-letter stream, so a user must never be able
// to occupy it first.
var ErrReservedName = errs.E(errs.ErrBadRequest, "",
	"stream names ending in \".dlq\" are reserved for dead-lettering")

// reservedSuffix is the dead-letter stream suffix of D3.
const reservedSuffix = ".dlq"

// ValidateStreamName applies every rule a user-supplied stream name must pass at
// creation: rule S11 syntax from internal/subject plus the ".dlq" reservation. The
// length cap is subject.MaxNewStreamNameBytes, which leaves room for the suffix so the
// derived "<name>.dlq" always has an expressible name.
//
// Case-collision with an existing stream is a storage-level check, not a pure one; see
// internal/store.
func ValidateStreamName(name string) error {
	if strings.HasSuffix(name, reservedSuffix) {
		return errs.E(ErrReservedName, "", "stream %q ends in %q", name, reservedSuffix)
	}
	return subject.ValidateNewStreamName(name)
}

// ValidateExistingStreamName applies S11 without the creation-only rules: publishing to
// an existing "<stream>.dlq" stream is allowed (that is how redrive works), and lookups
// of derived names must accept them.
func ValidateExistingStreamName(name string) error {
	return subject.ValidateStreamName(name)
}
