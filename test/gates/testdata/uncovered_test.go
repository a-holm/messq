// SPDX-License-Identifier: Apache-2.0

package queue

import "testing"

// TestSabotageClassify exercises one branch of five, which is what a package that lost most of
// its tests looks like.
func TestSabotageClassify(t *testing.T) {
	if got := SabotageClassify(-1); got != "negative" {
		t.Errorf("SabotageClassify(-1) = %q, want %q", got, "negative")
	}
}
