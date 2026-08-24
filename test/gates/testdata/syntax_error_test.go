// SPDX-License-Identifier: Apache-2.0

package queue_test

import (
	"testing"
)

// TestSabotageBodySyntaxError carries a syntax error inside a function body only. go list
// parses package clauses and imports without bodies, so this once slipped past `layers` in a
// production file; in the test binary the body parse is on the dependency edge `layers`
// walks, so the tree must fail to load and the gate must refuse it.
func TestSabotageBodySyntaxError(t *testing.T) {
	if true {
	t.Log("the malformed body is the sabotage")
}