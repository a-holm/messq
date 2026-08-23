// SPDX-License-Identifier: Apache-2.0

// Package queue is the pure message-lifecycle validation layer: stream and consumer
// names, stream and consumer configuration, sparse updates, publish requests, header
// encoding, trace-id precedence, ack-token minting and the start-position grammar. It
// performs no I/O, reads no wall clock and iterates no maps, so the fuzzers can hammer
// it (PLAN §3.3) and #13's reference model can drive it directly.
package queue
