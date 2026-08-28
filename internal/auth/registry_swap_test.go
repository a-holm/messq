// SPDX-License-Identifier: Apache-2.0

package auth

import (
	"crypto/sha256"
	"encoding/hex"
	"testing"
)

// hashOfCredential computes the stored-digest form for a fixture credential.
func hashOfCredential(cred string) [32]byte {
	return sha256.Sum256([]byte(cred))
}

// TestRegistrySwapBattery drives 1000 swap iterations against a concurrent
// reader under -race. The invariants:
//
//  1. immediately after SwapTokens(s), the registry verifies EXACTLY the
//     credentials of s and nothing else — retained-entry mutants fail this;
//  2. no read ever observes a torn snapshot (the race detector fails an
//     implementation that mutates a shared map while verifiers read it).
func TestRegistrySwapRaceBattery(t *testing.T) {
	credA := "msq1_swap-a_credential-A-msq1_shape_ok"
	credB := "msq1_swap-b_credential-B-msq1_shape_ok"
	tokA := Token{ID: "swap-a", Hash: hashOfCredential(credA), Roles: allRoles, Patterns: mustPatternsForTest("*")}
	tokB := Token{ID: "swap-b", Hash: hashOfCredential(credB), Roles: allRoles, Patterns: mustPatternsForTest("*")}

	reg := NewRegistry([]Token{tokA})

	// The concurrent reader never asserts logical outcomes — it exists so the
	// race detector sees simultaneous Verify traffic during every swap.
	stop := make(chan struct{})
	readerDone := make(chan struct{})
	go func() {
		defer close(readerDone)
		for i := 0; ; i++ {
			select {
			case <-stop:
				return
			default:
			}
			if _, vErr := reg.Verify(credA); vErr != nil {
				// The reader asserts nothing about WHICH snapshot is live; both
				// credentials are invalid exactly when the other one is live.
				continue
			}
			if i%16 == 0 {
				if _, vErrB := reg.Verify(credB); vErrB != nil {
					continue
				}
			}
		}
	}()

	for i := 0; i < 1000; i++ {
		reg.SwapTokens([]Token{tokB})
		if _, err := reg.Verify(credB); err != nil {
			t.Fatalf("iteration %d: snapshot B live but credential B rejected: %v", i, err)
		}
		if p, err := reg.Verify(credA); err == nil {
			t.Fatalf("iteration %d: stale credential A still verified as %q after swap", i, p.ID)
		}

		reg.SwapTokens([]Token{tokA})
		if _, err := reg.Verify(credA); err != nil {
			t.Fatalf("iteration %d: snapshot A live but credential A rejected: %v", i, err)
		}
		if _, err := reg.Verify(credB); err == nil {
			t.Fatalf("iteration %d: stale credential B still verified after swap back to A", i)
		}
	}
	close(stop)
	<-readerDone
}

func mustPatternsForTest(raw string) []Pattern {
	p, err := ParsePattern(raw)
	if err != nil {
		panic(err)
	}
	return []Pattern{p}
}

var _ = hex.EncodeToString // keep encoding import if assertions grow digests
