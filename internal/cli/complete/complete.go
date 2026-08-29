// SPDX-License-Identifier: Apache-2.0

// Package complete is the dynamic shell-completion resolver (issue #26 §3):
// live stream/consumer/subject completion over the daemon with three hard
// rules, all testable:
//
//   - NEVER HANG: one Budget covers dial + request + decode. On timeout,
//     refusal, or any error: no candidates, no stderr, exit 0,
//     ShellCompDirectiveNoFileComp — deliberately NOT ShellCompDirectiveError,
//     which makes several shells fall back to filename completion or beep, so a
//     dead daemon would make the shell look broken. "No suggestions" is the
//     honest and quiet answer.
//   - NEVER EXPENSIVE: one request per invocation; results cached for 5 s on
//     disk (0600, temp+rename, keyed by the address+token pair) and served up
//     to 60 s stale across a daemon restart.
//   - NEVER REVEALS: a 403 is the same silent empty as a dead daemon. A
//     completion must not tell you what you cannot see.
package complete

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/a-holm/messq/internal/clock"
	"github.com/a-holm/messq/pkg/client"
	"github.com/spf13/cobra"
)

// cobraDirective mirrors cobra.ShellCompDirective so the package's signatures
// read without importing cobra at every call site.
type cobraDirective = cobra.ShellCompDirective

// directiveNoFileComp is THE failure answer: no candidates, no file fallback.
// Deliberately not ShellCompDirectiveError — see the package comment.
const directiveNoFileComp = cobra.ShellCompDirectiveNoFileComp

// Budget bounds one whole completion operation, dial included (ADR-0010's
// "must never hang" is part of the contract, not a tuning knob).
const Budget = 200 * time.Millisecond

// cacheTTL is how long a fresh entry is trusted; staleTTL is how long a failed
// refresh may still serve the previous answer (completion keeps working across
// a daemon restart).
const (
	cacheTTL  = 5 * time.Second
	staleTTL  = 60 * time.Second
	cachePerm = 0o600
)

// maxCandidates caps every result: truncation drops candidates silently rather
// than hanging a shell on a 10k-stream daemon.
const maxCandidates = 500

// Completion is one candidate: the value shell inserts plus the short
// description shown next to it. The __complete protocol splits on \t, so a
// value or description carrying a tab or newline would corrupt the protocol —
// filterCandidates refuses both (property-tested).
type Completion struct {
	Value string
	Desc  string
}

// Resolver computes live completions against one daemon.
type Resolver struct {
	// Addr and TokenFile identify the daemon target (cache key + dialling).
	Addr      string
	TokenFile string
	// Dial builds the client; production resolves it from Addr/TokenFile,
	// tests inject fixtures.
	Dial func(ctx context.Context) (*client.Client, error)
	// Cache is the on-disk cache; nil disables caching (tests that probe
	// request counts construct their own).
	Cache *DiskCache
	// Budget bounds the operation. Zero means the package default.
	Budget time.Duration
	// Clock drives the cache's freshness decisions; nil means the system clock.
	Clock clock.Clock
}

// Streams returns the daemon's stream names matching prefix, one request.
func (r *Resolver) Streams(ctx context.Context, prefix string) ([]Completion, cobraDirective) {
	return r.cached(ctx, "streams", prefix, func(ctx context.Context) ([]Completion, error) {
		cl, err := r.dial(ctx)
		if err != nil {
			return nil, err
		}
		streams, err := cl.ListStreams(ctx)
		if err != nil {
			return nil, err
		}
		out := make([]Completion, 0, len(streams))
		for _, s := range streams {
			out = append(out, Completion{
				Value: s.Name,
				Desc:  streamDesc(s.Msgs),
			})
		}
		return out, nil
	})
}

// Consumers returns the consumer names of one stream matching prefix. An
// unknown or absent stream is an EMPTY result, never an error — completing a
// consumer of a stream you have not typed yet must not teach the shell to beep.
func (r *Resolver) Consumers(ctx context.Context, stream, prefix string) ([]Completion, cobraDirective) {
	if stream == "" {
		return nil, directiveNoFileComp
	}
	return r.cached(ctx, "consumers:"+stream, prefix, func(ctx context.Context) ([]Completion, error) {
		cl, err := r.dial(ctx)
		if err != nil {
			return nil, err
		}
		consumers, err := cl.ListConsumers(ctx, stream)
		if err != nil {
			return nil, err
		}
		out := make([]Completion, 0, len(consumers))
		for _, c := range consumers {
			out = append(out, Completion{
				Value: c.Name,
				Desc:  consumerDesc(c),
			})
		}
		return out, nil
	})
}

// Subjects offers the stream's configured subject patterns. Concrete patterns
// are offered whole; wildcard patterns are offered as their LITERAL PREFIX
// ("orders." for "orders.>") with NoSpace — you cannot publish to `orders.>`,
// and suggesting it would teach the wrong thing.
func (r *Resolver) Subjects(ctx context.Context, stream, prefix string) ([]Completion, cobraDirective) {
	if stream == "" {
		return nil, directiveNoFileComp
	}
	return r.cached(ctx, "subjects:"+stream, prefix, func(ctx context.Context) ([]Completion, error) {
		cl, err := r.dial(ctx)
		if err != nil {
			return nil, err
		}
		info, err := cl.GetStream(ctx, stream)
		if err != nil {
			return nil, err
		}
		out := make([]Completion, 0, len(info.Subjects))
		for _, p := range info.Subjects {
			v, noSpace := literalSubjectPrefix(p)
			out = append(out, Completion{Value: v})
			_ = noSpace
		}
		return out, nil
	})
}

// literalSubjectPrefix strips a trailing wildcard so the shell can continue
// typing: "orders.>" completes as "orders.", "orders.*" as "orders.". A bare
// ">" (accept-everything) completes as "" — there is no subject to type, the
// shell should just stop. A concrete pattern completes whole.
func literalSubjectPrefix(pattern string) (string, bool) {
	switch {
	case pattern == ">":
		return "", true
	case strings.HasSuffix(pattern, ".>"):
		return strings.TrimSuffix(pattern, ">"), true
	case strings.HasSuffix(pattern, ".*"):
		return strings.TrimSuffix(pattern, "*"), true
	case strings.HasSuffix(pattern, "*"):
		return strings.TrimSuffix(pattern, "*"), true
	default:
		return pattern, false
	}
}

// cached is the one-request/silent-failure envelope around any fetcher: the
// budget covers dial+request+decode, errors are silent-empty, and the disk
// cache serves fresh entries, then stale ones.
func (r *Resolver) cached(ctx context.Context, key, prefix string, fetch func(context.Context) ([]Completion, error)) ([]Completion, cobraDirective) {
	budget := r.Budget
	if budget <= 0 {
		budget = Budget
	}
	ctx, cancel := context.WithTimeout(ctx, budget)
	defer cancel()

	if r.Cache != nil {
		id := r.cacheID(key)
		if cached, ok := r.Cache.Load(id); ok {
			return filterCandidates(cached, prefix), directiveNoFileComp
		}
	}
	got, err := fetch(ctx)
	if err != nil {
		// The stale-serve path: a failed refresh may still hand back the
		// previous answer for staleTTL — completion outlives daemon restarts.
		if r.Cache != nil {
			if stale, ok := r.Cache.LoadStale(r.cacheID(key), staleTTL); ok {
				return filterCandidates(stale, prefix), directiveNoFileComp
			}
		}
		return nil, directiveNoFileComp
	}
	if r.Cache != nil {
		r.Cache.Store(r.cacheID(key), got)
	}
	return filterCandidates(got, prefix), directiveNoFileComp
}

// cacheID combines the daemon identity with the completion kind.
func (r *Resolver) cacheID(kind string) string {
	return CacheID(r.Addr, r.TokenFile) + "-" + kind
}

func (r *Resolver) dial(ctx context.Context) (*client.Client, error) {
	if r.Dial == nil {
		return nil, errors.New("complete: no dial function")
	}
	return r.Dial(ctx)
}

// addrID is the cache's identity of the daemon target: sha256 of the address
// and token file (issue §3: keyed addr+token, never the address alone — two
// daemons behind one address with different tokens are different worlds).
func CacheID(addr, tokenFile string) string {
	h := sha256.Sum256([]byte(addr + "\x00" + tokenFile))
	return hex.EncodeToString(h[:16])
}

func streamDesc(msgs int64) string {
	switch msgs {
	case 0:
		return "empty"
	case 1:
		return "1 message"
	default:
		return fmt.Sprintf("%d messages", msgs)
	}
}

func consumerDesc(c client.ConsumerView) string {
	return fmt.Sprintf("pending %d, inflight %d", c.Pending, c.Inflight)
}

// filterCandidates applies the prefix filter and the protocol hygiene rules:
// sorted, deduplicated, capped, and free of tabs or newlines (which would
// corrupt the __complete protocol). Property-tested over random inputs.
func filterCandidates(in []Completion, prefix string) []Completion {
	out := make([]Completion, 0, len(in))
	seen := map[string]bool{}
	for _, c := range in {
		v := sanitize(c.Value)
		if v == "" || !strings.HasPrefix(v, prefix) || seen[v] {
			continue
		}
		seen[v] = true
		out = append(out, Completion{Value: v, Desc: sanitize(c.Desc)})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Value < out[j].Value })
	if len(out) > maxCandidates {
		out = out[:maxCandidates]
	}
	return out
}

// sanitize removes the two protocol-breaking bytes; a candidate that collapses
// to nothing is dropped by the caller.
func sanitize(s string) string {
	s = strings.ReplaceAll(s, "	", " ")
	s = strings.ReplaceAll(s, "\n", " ")
	s = strings.ReplaceAll(s, "\r", " ")
	return s
}

// ---- disk cache ----

// DiskCache is the 5-second on-disk completion cache under
// $XDG_CACHE_HOME/messq/: 0600, written temp+rename, keyed by CacheID. A
// corrupt, truncated or foreign file is ignored and refetched — a cache error
// is never an error path the user sees.
type DiskCache struct {
	// Dir is the cache root; production resolves it from XDG_CACHE_HOME.
	Dir string
	// Now is the freshness clock; nil means the system clock. It flows through
	// the Clock seam like every other wall-clock read in the tree.
	Now clock.Clock
}

// clk resolves the cache's clock.
func (c *DiskCache) clk() clock.Clock {
	if c == nil || c.Now == nil {
		return clock.System{}
	}
	return c.Now
}

// NewDiskCache resolves the production cache dir, creating it if missing. An
// uncreatable dir disables caching (nil, nil) — caching is an optimisation.
func NewDiskCache() *DiskCache {
	base := os.Getenv("XDG_CACHE_HOME")
	if base == "" {
		home, err := os.UserHomeDir()
		if err != nil {
			return nil
		}
		base = filepath.Join(home, ".cache")
	}
	dir := filepath.Join(base, "messq")
	//nolint:gosec // the dir comes from XDG_CACHE_HOME or the operator's home — configuration, not request data
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return nil
	}
	return &DiskCache{Dir: dir}
}

type cacheEntry struct {
	StoredAtMS int64        `json:"stored_at_ms"`
	Items      []Completion `json:"items"`
}

func (c *DiskCache) path(id string) string {
	return filepath.Join(c.Dir, "complete-"+id+".json")
}

// safeID guards against a cache id carrying path separators: production ids are
// CacheID hex plus a fixed kind, but the guard keeps the file name honest even
// for a foreign id.
func safeID(id string) bool {
	return !strings.ContainsAny(id, `/\`) && !strings.Contains(id, "..")
}

// Load returns a fresh (≤ cacheTTL) entry.
func (c *DiskCache) Load(id string) ([]Completion, bool) {
	return c.load(id, cacheTTL)
}

// LoadStale returns an entry no older than ttl, however old it would otherwise
// be considered — the refresh-failed path.
func (c *DiskCache) LoadStale(id string, ttl time.Duration) ([]Completion, bool) {
	return c.load(id, ttl)
}

func (c *DiskCache) load(id string, ttl time.Duration) ([]Completion, bool) {
	if c == nil || !safeID(id) {
		return nil, false
	}
	raw, err := os.ReadFile(c.path(id))
	if err != nil {
		return nil, false
	}
	var e cacheEntry
	if err := json.Unmarshal(raw, &e); err != nil {
		return nil, false // corrupt: ignored, never surfaced
	}
	ageMS := c.clk().Now().UnixMilli() - e.StoredAtMS
	if ageMS > ttl.Milliseconds() {
		return nil, false
	}
	return e.Items, true
}

// Store writes an entry atomically (temp + rename): two shells completing at
// once both see a valid file, last writer wins.
func (c *DiskCache) Store(id string, items []Completion) {
	if c == nil || !safeID(id) || len(items) == 0 {
		return
	}
	raw, err := json.Marshal(cacheEntry{StoredAtMS: c.clk().Now().UnixMilli(), Items: items})
	if err != nil {
		return
	}
	tmp, err := os.CreateTemp(c.Dir, ".complete-*")
	if err != nil {
		return
	}
	name := tmp.Name()
	if _, wErr := tmp.Write(raw); wErr == nil {
		if cErr := tmp.Close(); cErr == nil {
			if chErr := os.Chmod(name, cachePerm); chErr == nil {
				// Last writer wins; a lost race changes nothing.
				_ = os.Rename(name, c.path(id)) //nolint:errcheck // see above
				return
			}
		}
	}
	sinkErr(tmp.Close())
	sinkErr(os.Remove(name))
}

// sinkErr consumes a best-effort cache-write error: caching is an optimisation
// and its failures have no surface to report on.
func sinkErr(err error) {
	_ = err
}
