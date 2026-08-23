// SPDX-License-Identifier: Apache-2.0

package queue

import (
	_ "embed"
	"strconv"
	"strings"
	"testing"
)

// tokenCorpus is the committed ack-token corpus (testdata/tokens/valid.txt), embedded so the
// package stays free of os (layers.sh: internal/queue is a pure state machine, no I/O). #10's
// fuzz target seeds from the same file on disk.
//
//go:embed testdata/tokens/valid.txt
var tokenCorpus string

func TestTokenString(t *testing.T) {
	tests := []struct {
		tok  Token
		want string
	}{
		{Token{"orders", "worker", 10494, 1, 1}, "orders/worker/10494/1/1"},
		{Token{"orders.eu", "refund-worker", 1000000, 5, 3}, "orders.eu/refund-worker/1000000/5/3"},
		{Token{"a", "b", 0, 0, 0}, "a/b/0/0/0"},
		{Token{"stream.with.dots", "worker", 42, 7, 2}, "stream.with.dots/worker/42/7/2"},
	}
	for _, tt := range tests {
		if got := tt.tok.String(); got != tt.want {
			t.Fatalf("Token.String() = %q, want %q", got, tt.want)
		}
	}
}

// TestTokenCorpusWellFormed pins the shape of the committed corpus (testdata/tokens/valid.txt)
// that #10's fuzz target seeds from: every line must be five slash-separated fields with
// numeric seq/attempt/generation, so the parser #10 writes has a well-formed seed set and
// this test is the guard that keeps it that way.
func TestTokenCorpusWellFormed(t *testing.T) {
	seen := 0
	for _, line := range strings.Split(tokenCorpus, "\n") {
		if strings.TrimSpace(line) == "" || strings.HasPrefix(strings.TrimSpace(line), "#") {
			continue
		}
		seen++
		parts := strings.Split(line, "/")
		if len(parts) != 5 {
			t.Fatalf("corpus line %q has %d fields, want 5", line, len(parts))
		}
		if parts[0] == "" || parts[1] == "" {
			t.Fatalf("corpus line %q has an empty stream or consumer name", line)
		}
		for _, i := range []int{2, 3, 4} {
			if _, err := strconv.ParseInt(parts[i], 10, 64); err != nil {
				t.Fatalf("corpus line %q field %d is not an integer: %v", line, i, err)
			}
		}
	}
	if seen == 0 {
		t.Fatal("corpus is empty")
	}
}
