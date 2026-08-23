// SPDX-License-Identifier: Apache-2.0

package model

// Model is the naive, spec-derived in-memory broker under test. This slice carries the
// skeleton only: the state machine, the View snapshot and the event stream arrive with the
// slices that exercise them. Everything here has a matching test, so the shape stays honest
// as the package grows (the `unused` linter rejects fields declared ahead of their use).
type Model struct {
	now int64 // unix milliseconds; monotone within a process (PLAN.md E4)
}

// New returns an empty Model at time zero.
func New() *Model { return &Model{} }

// View returns a snapshot of the current observable state.
func (m *Model) View() View { return View{Now: m.now} }

// DrainEvents returns the events accumulated since the last call. The stream is drained
// after every action, which is how the harness compares what each side emitted.
func (m *Model) DrainEvents() []Event { return nil }
