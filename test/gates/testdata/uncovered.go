// SPDX-License-Identifier: Apache-2.0

package queue

// SabotageClassify is the code a floored package would hold.
func SabotageClassify(n int) string {
	if n < 0 {
		return "negative"
	}
	if n == 0 {
		return "zero"
	}
	if n < 10 {
		return "small"
	}
	if n < 100 {
		return "medium"
	}
	return "large"
}
