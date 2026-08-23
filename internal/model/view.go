// SPDX-License-Identifier: Apache-2.0

package model

// Event is one observable transition emitted by the model. The shape is filled in by the
// slices that define the transitions; this slice only needs the type so DrainEvents has
// something to name.
type Event struct{}

// View is the comparable state snapshot the harness diffs after every action (PLAN.md §5.2).
// The full field set follows the transitions; this slice pins only the clock the model owns.
type View struct {
	Now int64 // the model's current time, unix milliseconds
}
