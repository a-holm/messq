// SPDX-License-Identifier: Apache-2.0

package subject_test

import (
	"testing"

	_ "github.com/a-holm/messq/internal/store"
)

// TestSabotageLeaf exists only for its import: internal/subject is a leaf, and a test that
// reaches up into the store makes it one no longer.
func TestSabotageLeaf(t *testing.T) {
	t.Log("the import is the sabotage")
}
