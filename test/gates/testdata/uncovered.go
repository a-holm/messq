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

// The helpers below are more of the same: plausible state-machine code the sabotage's single
// test never calls, so the package's measured coverage falls well under its 90.0% floor. Each
// is deliberately small, making the uncovered statements easy to count and hard to exercise by
// accident. internal/queue now carries real code (~116 statements), so a lone five-branch
// classifier no longer moves the number; this body does.

func sabotageGrade(score int) string {
	if score >= 90 {
		return "A"
	}
	if score >= 80 {
		return "B"
	}
	return "C"
}

func sabotageBucket(size int) string {
	if size < 16 {
		return "tiny"
	}
	if size < 64 {
		return "small"
	}
	if size < 256 {
		return "medium"
	}
	return "huge"
}

func sabotageSign(n int) int {
	if n > 0 {
		return 1
	}
	if n < 0 {
		return -1
	}
	return 0
}

func sabotageAbs(n int) int {
	if n < 0 {
		return -n
	}
	return n
}

func sabotageClamp(v, lo, hi int) int {
	if v < lo {
		return lo
	}
	if v > hi {
		return hi
	}
	return v
}

func sabotageLevel(n int) string {
	if n < 5 {
		return "debug"
	}
	if n < 20 {
		return "info"
	}
	if n < 100 {
		return "warn"
	}
	return "error"
}

func sabotageEven(n int) bool {
	return n%2 == 0
}

func sabotageParity(n int) string {
	if n%2 == 0 {
		return "even"
	}
	return "odd"
}

func sabotageRange(n int) string {
	if n < 0 {
		return "below"
	}
	if n == 0 {
		return "zero"
	}
	return "above"
}

func sabotageTier(n int) string {
	if n < 3 {
		return "bronze"
	}
	if n < 7 {
		return "silver"
	}
	if n < 12 {
		return "gold"
	}
	return "platinum"
}

func sabotageFactorial(n int) int {
	out := 1
	for i := 2; i <= n; i++ {
		out *= i
	}
	return out
}

func sabotageCollatz(n int) int {
	if n%2 == 0 {
		return n / 2
	}
	return 3*n + 1
}

func sabotageStep(n int) int {
	if n < 10 {
		return n + 1
	}
	if n < 100 {
		return n + 10
	}
	return n + 100
}
