// SPDX-License-Identifier: Apache-2.0

package loadgen

import (
	"bytes"
	"testing"
)

// TestPayloadDeterministic proves the deterministic keyed fill: the same (key, size) yields
// byte-identical bodies, and different keys (or sizes) differ, so the reconciler can
// byte-compare a recovered body without a ledger the size of the workload.
func TestPayloadDeterministic(t *testing.T) {
	a := Payload("key-1", 1024)
	b := Payload("key-1", 1024)
	if !bytes.Equal(a, b) {
		t.Fatal("Payload is not deterministic for the same key and size")
	}
	if len(a) != 1024 {
		t.Fatalf("Payload size = %d, want 1024", len(a))
	}

	c := Payload("key-2", 1024)
	if bytes.Equal(a, c) {
		t.Fatal("Payload collides across different keys")
	}
	d := Payload("key-1", 512)
	if bytes.Equal(a, d) {
		t.Fatal("Payload of a different size must not match")
	}
}
