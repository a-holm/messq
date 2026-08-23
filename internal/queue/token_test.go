// SPDX-License-Identifier: Apache-2.0

package queue

import (
	"bufio"
	"os"
	"strconv"
	"strings"
	"testing"
)

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
	f, err := os.Open("testdata/tokens/valid.txt")
	if err != nil {
		t.Fatalf("open corpus: %v", err)
	}
	defer func() {
		if cerr := f.Close(); cerr != nil {
			t.Fatalf("close corpus: %v", cerr)
		}
	}()
	sc := bufio.NewScanner(f)
	seen := 0
	for sc.Scan() {
		line := sc.Text()
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
	if err := sc.Err(); err != nil {
		t.Fatalf("scan corpus: %v", err)
	}
	if seen == 0 {
		t.Fatal("corpus is empty")
	}
}
