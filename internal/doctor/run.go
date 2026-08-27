// SPDX-License-Identifier: Apache-2.0

package doctor

import (
	"context"
	"fmt"
	"strings"

	"github.com/a-holm/messq/internal/clock"
)

// RunOptions configures one doctor run's source selection and collection
// facts. Check thresholds (idle-after, since, backup limits) ride the Snapshot
// in later slices; Run only owns WHERE state comes from.
type RunOptions struct {
	// Addr is the daemon address for live collection. Unused when DataDir set.
	Addr string
	// DataDir selects the offline source. Empty means live.
	DataDir string
	// Clock stamps the snapshot's Now; nil means clock.System{}.
	Clock clock.Clock
}

// Collect fills a Snapshot from the configured source. Offline refusals come
// back as errors — the CLI renders them at exit 7, because "no source is
// readable" is exactly that case. Live failures become snapshot state instead,
// per §10.
func Collect(ctx context.Context, o RunOptions) (*Snapshot, error) {
	var clk clock.Clock = clock.System{}
	if o.Clock != nil {
		clk = o.Clock
	}
	if o.DataDir != "" {
		snap, err := OfflineCollector{DataDir: o.DataDir}.Collect(ctx)
		if err == nil {
			snap.Now = clk.Now()
		}
		return snap, err
	}
	addr := o.Addr
	if addr == "" {
		return nil, fmt.Errorf("no source: give --data-dir or a reachable --addr")
	}
	snap, err := LiveCollector{Addr: addr}.Collect(ctx)
	if err == nil {
		snap.Now = clk.Now()
	}
	return snap, err
}

// FilterRegistry copies r keeping only checks selected by --only patterns and
// not excluded by --skip. A pattern is an exact ID or a family prefix ending
// in `*` (`consumer.*`). Unmatched explicit IDs are returned so the caller can
// refuse them as usage: silencing a typo must never look like passing.
func FilterRegistry(r *Registry, only, skip []string) (*Registry, []string) {
	matches := func(patterns []string) func(string) bool {
		if len(patterns) == 0 {
			return nil
		}
		pats := append([]string(nil), patterns...)
		return func(id string) bool {
			for _, p := range pats {
				if p == id || (len(p) > 1 && strings.HasSuffix(p, "*") &&
					strings.HasPrefix(id, p[:len(p)-1])) {
					return true
				}
			}
			return false
		}
	}
	kept := NewRegistry()
	unknown := []string{}
	for _, pattern := range append(append([]string(nil), only...), skip...) {
		if !registryHasMatch(r, pattern) {
			unknown = append(unknown, pattern)
		}
	}
	include := matches(only)
	exclude := matches(skip)
	for _, id := range r.List() {
		if include != nil && !include(id) {
			continue
		}
		if exclude != nil && exclude(id) {
			continue
		}
		check := *r.mustGet(id)
		kept.Register(check)
	}
	return kept, unknown
}

func registryHasMatch(r *Registry, pattern string) bool {
	for _, id := range r.List() {
		if id == pattern || (len(pattern) > 1 && strings.HasSuffix(pattern, "*") &&
			strings.HasPrefix(id, pattern[:len(pattern)-1])) {
			return true
		}
	}
	return false
}
