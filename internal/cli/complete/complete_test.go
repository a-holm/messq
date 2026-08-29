// SPDX-License-Identifier: Apache-2.0

package complete

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/a-holm/messq/internal/clock"
	"github.com/a-holm/messq/pkg/client"
	"pgregory.net/rapid"
)

// slowDaemon is the fixture for the never-hang rule: a unix socket that
// accepts and answers N seconds late — slower than the budget by design.
func newSlowDaemon(t *testing.T, dir string, d time.Duration) string {
	t.Helper()
	sock := filepath.Join(dir, "slow.sock")
	ln, err := (&net.ListenConfig{}).Listen(context.Background(), "unix", sock)
	if err != nil {
		t.Fatalf("slow daemon listen: %v", err)
	}
	t.Cleanup(func() { sinkTestErr(ln.Close()) })
	go func() {
		for {
			conn, aErr := ln.Accept()
			if aErr != nil {
				return
			}
			go func(c net.Conn) {
				defer sinkTestErr(c.Close())
				if sleepErr := (clock.System{}).Sleep(context.Background(), d); sleepErr != nil {
					return
				}
				// A minimal HTTP response; the client may already be gone.
				if _, wErr := c.Write([]byte("HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: 2\r\n\r\n[]")); wErr != nil {
					sinkTestErr(wErr)
				}
			}(conn)
		}
	}()
	return sock
}

// sinkTestErr consumes a cleanup error whose only handler could be a log line;
// the connection is being torn down with the test either way.
func sinkTestErr(err error) { _ = err }

// TestCompletionBudgetHoldsOnSlowDaemon pins the 200ms rule: against a daemon
// that answers in 5 seconds, completion returns EMPTY within a small multiple
// of the budget — the deadline cuts the operation, not the shell.
func TestCompletionBudgetHoldsOnSlowDaemon(t *testing.T) {
	sock := newSlowDaemon(t, t.TempDir(), 5*time.Second)
	r := &Resolver{
		Addr: "unix://" + sock,
		Dial: func(context.Context) (*client.Client, error) { return client.New("unix://" + sock) },
	}
	start := (clock.System{}).Now()
	got, directive := r.Streams(context.Background(), "")
	elapsed := (clock.System{}).Since(start)
	if len(got) != 0 {
		t.Errorf("slow daemon produced candidates: %v", got)
	}
	if directive != directiveNoFileComp {
		t.Errorf("directive = %v, want NoFileComp", directive)
	}
	if elapsed > 2*time.Second {
		t.Errorf("completion took %s; the 200ms budget did not cut the operation", elapsed)
	}
}

// TestCompletionDeadDaemonSilentEmpty pins the dead-daemon face: empty, NoFileComp,
// and the whole point — nothing on stderr, exit 0. The no-stderr half is
// structural: this package never writes to stderr (no Writer field exists, no
// fmt.Fprintln(os.Stderr) in the file — the txtar completion script asserts it
// end to end).
func TestCompletionDeadDaemonSilentEmpty(t *testing.T) {
	missing := filepath.Join(t.TempDir(), "nope.sock")
	r := &Resolver{
		Addr: "unix://" + missing,
		Dial: func(context.Context) (*client.Client, error) { return client.New("unix://" + missing) },
	}
	got, directive := r.Streams(context.Background(), "")
	if len(got) != 0 || directive != directiveNoFileComp {
		t.Errorf("dead daemon: (%v, %v), want (empty, NoFileComp)", got, directive)
	}
	got, directive = r.Consumers(context.Background(), "orders", "")
	if len(got) != 0 || directive != directiveNoFileComp {
		t.Errorf("dead daemon consumers: (%v, %v)", got, directive)
	}
}

// TestCompletionUnknownStreamIsEmpty pins the scoping rule: a consumer
// completion against a stream that does not exist (or was not typed) is empty,
// never an error.
func TestCompletionUnknownStreamIsEmpty(t *testing.T) {
	r := &Resolver{Addr: "unix:///nope", Dial: func(context.Context) (*client.Client, error) {
		return nil, errors.New("no daemon")
	}}
	got, _ := r.Consumers(context.Background(), "", "")
	if len(got) != 0 {
		t.Errorf("empty stream name produced candidates: %v", got)
	}
}

// TestCompletionCacheTTLAndStaleness drives the 5s/60s cache rules on a fake
// clock: fresh entries serve, entries past 5s refetch, and a failed refresh
// still serves up to 60s.
func TestCompletionCacheTTLAndStaleness(t *testing.T) {
	dir := t.TempDir()
	clk := clock.NewFake(time.UnixMilli(1_700_000_000_000))
	cache := &DiskCache{Dir: dir, Now: clk}

	requests := 0
	r := &Resolver{
		Addr: "unix:///dev/null",
		Dial: func(context.Context) (*client.Client, error) {
			return nil, errors.New("disabled in this test")
		},
		Cache: cache,
	}
	fetch := func(context.Context) ([]Completion, error) {
		requests++
		return []Completion{{Value: "orders"}}, nil
	}

	// First call fetches.
	if got, _ := r.cached(context.Background(), "streams", "", fetch); len(got) != 1 {
		t.Fatalf("first call: %v", got)
	}
	// Second call inside the TTL: cached, no new request.
	if got, _ := r.cached(context.Background(), "streams", "", fetch); len(got) != 1 || requests != 1 {
		t.Fatalf("cached call: got %v after %d requests, want cache hit", got, requests)
	}
	// Past 5s: refetch.
	clk.Advance(6 * time.Second)
	if _, _ = r.cached(context.Background(), "streams", "", fetch); requests != 2 {
		t.Fatalf("stale entry served as fresh: %d requests, want 2", requests)
	}
	// Now the daemon dies (fetch errors): a stale entry still serves until 60s.
	clk.Advance(1 * time.Second)
	dying := func(context.Context) ([]Completion, error) { return nil, errors.New("daemon gone") }
	if got, _ := r.cached(context.Background(), "streams", "", dying); len(got) != 1 {
		t.Fatalf("stale-serve after refresh failure: %v", got)
	}
	// Past 60s since the LAST successful store: the stale answer is gone.
	clk.Advance(70 * time.Second)
	if got, _ := r.cached(context.Background(), "streams", "", dying); len(got) != 0 {
		t.Fatalf("stale entry served past 60s: %v", got)
	}
}

// TestCompletionCacheCorruptIgnored pins the corruption rule: a truncated or
// garbage cache file is indistinguishable from a miss, never an error.
func TestCompletionCacheCorruptIgnored(t *testing.T) {
	dir := t.TempDir()
	cache := &DiskCache{Dir: dir}
	id := CacheID("unix:///x", "")
	corrupt := []cacheEntryCorruptCase{
		{name: "garbage", body: "definitely not json"},
		{name: "truncated", body: `{"stored_at_ms":1,"items":[{"val`},
		{name: "wrong shape", body: `{"stored_at_ms":"soon","items":[]}`},
	}
	for _, tc := range corrupt {
		if err := os.WriteFile(filepath.Join(dir, "complete-"+id+"-streams.json"), []byte(tc.body), 0o600); err != nil {
			t.Fatal(err)
		}
		if got, ok := cache.Load(id + "-streams"); ok || len(got) != 0 {
			t.Errorf("%s: cache returned (%v, %v), want miss", tc.name, got, ok)
		}
	}
}

type cacheEntryCorruptCase struct {
	name string
	body string
}

// TestCompletionCachePerm pins the 0600 rule on a written cache file.
func TestCompletionCachePerm(t *testing.T) {
	dir := t.TempDir()
	cache := &DiskCache{Dir: dir}
	id := CacheID("unix:///x", "")
	cache.Store(id+"-streams", []Completion{{Value: "orders"}})
	info, err := os.Stat(filepath.Join(dir, "complete-"+id+"-streams.json"))
	if err != nil {
		t.Fatalf("cache file missing: %v", err)
	}
	if perm := info.Mode().Perm(); perm != 0o600 {
		t.Errorf("cache file mode = %o, want 600", perm)
	}
}

// TestCacheIDSeparatesTokens pins the key derivation: the same address with a
// different token file is a different cache world.
func TestCacheIDSeparatesTokens(t *testing.T) {
	if CacheID("unix:///run/messq.sock", "") == CacheID("unix:///run/messq.sock", "/etc/messq/tokens") {
		t.Error("CacheID ignored the token file")
	}
	if CacheID("unix:///a", "") == CacheID("unix:///b", "") {
		t.Error("CacheID ignored the address")
	}
}

// TestLiteralSubjectPrefix pins the wildcard rule: `orders.>` completes as
// `orders.` (NoSpace), concrete patterns complete whole.
func TestLiteralSubjectPrefix(t *testing.T) {
	for _, tc := range []struct {
		pattern, want string
	}{
		{"orders.>", "orders."},
		{"orders.*", "orders."},
		{"orders", "orders"},
		{">", ""},
	} {
		got, _ := literalSubjectPrefix(tc.pattern)
		if got != tc.want {
			t.Errorf("literalSubjectPrefix(%q) = %q, want %q", tc.pattern, got, tc.want)
		}
	}
}

// TestFilterCandidatesProperty is the rapid property: for random candidate sets
// and prefixes the result is exactly the prefix-matching subset — sorted,
// deduplicated, capped — never containing a tab or newline (which would
// corrupt the __complete protocol).
func TestFilterCandidatesProperty(t *testing.T) {
	rapid.Check(t, func(rt *rapid.T) {
		n := rapid.IntRange(0, 60).Draw(rt, "n")
		raw := make([]Completion, n)
		for i := range raw {
			raw[i] = Completion{
				Value: rapid.StringMatching(`[a-z.*]{0,8}`).Draw(rt, "v"),
				Desc:  rapid.StringMatching(`[a-z\t\n ]{0,5}`).Draw(rt, "d"),
			}
		}
		prefix := rapid.StringMatching(`[a-z]{0,3}`).Draw(rt, "prefix")
		got := filterCandidates(raw, prefix)

		seen := map[string]bool{}
		for i, c := range got {
			if strings.ContainsAny(c.Value+c.Desc, "\t\n") {
				rt.Fatalf("candidate %q carries a protocol byte", c.Value)
			}
			if !strings.HasPrefix(c.Value, prefix) {
				rt.Fatalf("candidate %q lacks prefix %q", c.Value, prefix)
			}
			if seen[c.Value] {
				rt.Fatalf("duplicate candidate %q", c.Value)
			}
			seen[c.Value] = true
			if i > 0 && got[i-1].Value > c.Value {
				rt.Fatalf("result unsorted at %d: %q > %q", i, got[i-1].Value, c.Value)
			}
		}
		if len(got) > maxCandidates {
			rt.Fatalf("result over cap: %d", len(got))
		}
	})
}

// TestOneRequestPerInvocation pins the never-expensive rule at the client seam:
// completing N candidates issues exactly one ListStreams request, whatever the
// result size.
func TestOneRequestPerInvocation(t *testing.T) {
	var requests int
	r := &Resolver{
		Addr: "unix:///fake",
		Dial: func(context.Context) (*client.Client, error) {
			return nil, errors.New("unused: fetcher is injected")
		},
	}
	fetch := func(ctx context.Context) ([]Completion, error) {
		requests++
		out := make([]Completion, 0, 50)
		for i := range 50 {
			out = append(out, Completion{Value: fmt.Sprintf("s%03d", i)})
		}
		return out, nil
	}
	got, _ := r.cached(context.Background(), "streams", "", fetch)
	if requests != 1 {
		t.Fatalf("%d requests for one completion, want exactly 1 (never N+1)", requests)
	}
	if len(got) != 50 {
		t.Fatalf("got %d candidates, want 50", len(got))
	}
}
