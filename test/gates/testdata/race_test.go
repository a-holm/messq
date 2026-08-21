// SPDX-License-Identifier: Apache-2.0

package cli

import (
	"sync"
	"testing"
)

// TestSabotageRace increments a shared counter from two goroutines with no synchronisation.
func TestSabotageRace(t *testing.T) {
	counter := 0

	var wg sync.WaitGroup
	for range 2 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range 1000 {
				counter++
			}
		}()
	}
	wg.Wait()

	if counter == 0 {
		t.Error("counter never advanced")
	}
}
