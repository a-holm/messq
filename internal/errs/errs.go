// SPDX-License-Identifier: Apache-2.0

package errs

import "errors"

// The sentinel set. Messages are lower case and carry no trailing punctuation because they
// are wrapped into longer sentences by every caller.
var (
	// ErrNotFound is a named stream, consumer or message that does not exist.
	ErrNotFound = errors.New("not found")
	// ErrConflict is a create that collides with an existing name.
	ErrConflict = errors.New("already exists")
	// ErrBadRequest is a malformed or out-of-range argument.
	ErrBadRequest = errors.New("invalid request")
	// ErrBadSubject is a subject or pattern that the grammar rejects, or one the stream
	// does not accept.
	ErrBadSubject = errors.New("subject is not valid or not accepted by this stream")
	// ErrTooLarge is a body above the stream's max_msg_size.
	ErrTooLarge = errors.New("message exceeds max_msg_size")
	// ErrStreamFull is a publish into a stream at its limit with discard=new.
	ErrStreamFull = errors.New("stream is at its limit and discard=new")
	// ErrFlowControl is a fetch blocked by max_ack_pending.
	ErrFlowControl = errors.New("max_ack_pending reached")
	// ErrStaleAck is a settle for a delivery that was already redelivered.
	ErrStaleAck = errors.New("stale ack: the message was already redelivered")
	// ErrUnknownToken is an ack token that does not parse or names nothing.
	ErrUnknownToken = errors.New("unknown or malformed ack token")
	// ErrWrongGen is an ack token from before the consumer was reset.
	ErrWrongGen = errors.New("token generation is stale; the consumer was reset")
	// ErrPaused is a fetch against a paused consumer.
	ErrPaused = errors.New("consumer is paused")
	// ErrDiskFull is the degraded-writes state: not enough free space to accept writes.
	ErrDiskFull = errors.New("insufficient free disk space")
	// ErrReadOnly is the latched read-only state after a write fault.
	ErrReadOnly = errors.New("storage is latched read-only")
	// ErrShuttingDown is a request that arrived during graceful shutdown.
	ErrShuttingDown = errors.New("shutting down")
	// ErrUnauthorized is a request with no credentials where credentials are required.
	ErrUnauthorized = errors.New("authentication required")
	// ErrForbidden is a request whose credentials do not cover the operation.
	ErrForbidden = errors.New("not permitted for this token")
	// ErrLocked is a data directory held by another process.
	ErrLocked = errors.New("data directory is locked by another process")
	// ErrSchemaNewer is a data directory written by a newer binary.
	ErrSchemaNewer = errors.New("data directory schema is newer than this binary")
	// ErrUnavailable is a client-side failure to reach the daemon.
	ErrUnavailable = errors.New("daemon unreachable")
)

// registry is the declaration order All hands out. It is a package-level slice rather than a
// literal inside All so that a copy is all All has to build.
var registry = []error{
	ErrNotFound,
	ErrConflict,
	ErrBadRequest,
	ErrBadSubject,
	ErrTooLarge,
	ErrStreamFull,
	ErrFlowControl,
	ErrStaleAck,
	ErrUnknownToken,
	ErrWrongGen,
	ErrPaused,
	ErrDiskFull,
	ErrReadOnly,
	ErrShuttingDown,
	ErrUnauthorized,
	ErrForbidden,
	ErrLocked,
	ErrSchemaNewer,
	ErrUnavailable,
}

// All returns every sentinel in declaration order. The order is stable across runs and across
// calls, and the returned slice is a copy: the API and CLI mapping tests iterate it, and a
// caller that sorts it must not disturb the next caller.
func All() []error {
	out := make([]error, len(registry))
	copy(out, registry)
	return out
}
