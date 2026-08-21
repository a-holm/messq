// SPDX-License-Identifier: Apache-2.0

package queue

// SabotageState is a closed enum, the shape the delivery state will have.
type SabotageState int

const (
	// SabotagePending is the initial state.
	SabotagePending SabotageState = iota
	// SabotageInFlight is the claimed state.
	SabotageInFlight
	// SabotageDead is the terminal state.
	SabotageDead
)

// SabotageSwitch handles two of the three states.
func SabotageSwitch(s SabotageState) string {
	switch s {
	case SabotagePending:
		return "pending"
	case SabotageInFlight:
		return "in flight"
	}
	return ""
}
