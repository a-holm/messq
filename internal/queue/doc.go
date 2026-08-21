// SPDX-License-Identifier: Apache-2.0

// Package queue is the pure message-lifecycle state machine: Apply(state, cmd, now) returns mutations and events, with no I/O, no time.Now and no map-iteration-order dependence.
package queue
