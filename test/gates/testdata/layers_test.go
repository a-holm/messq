// SPDX-License-Identifier: Apache-2.0

package queue

import (
	"testing"

	_ "github.com/a-holm/messq/internal/store"
)

// TestSabotageLayers exists only for its import: a test in package queue reaching into the
// store is invisible to a guard that walks the production build alone.
func TestSabotageLayers(t *testing.T) {
	t.Log("the import is the sabotage")
}
