// SPDX-License-Identifier: Apache-2.0

package cli

import (
	"context"
	"fmt"
	"sort"
	"strings"

	"github.com/a-holm/messq/internal/auth"
	"github.com/a-holm/messq/internal/lifecycle"
)

// authFileReloader implements [lifecycle.Reloader] for the --auth-file token set
// (issue #16 slice 12; issue #17 will register it with the SIGHUP loop).
//
// Two phases exactly as the registry contract demands: Validate does ALL the
// fallible work — reading, parsing, diffing — while touching nothing alive, and
// Apply cannot fail because it only swaps an already-built immutable snapshot
// into place ([auth.Registry.SwapTokens]). A malformed or unreadable file during
// a reload therefore leaves the PREVIOUS set live and the daemon serving.
//
// The proposed Change carries IDS ONLY joined with commas and marks itself
// Secret so RenderDiff shows [redacted]: ids belong in logs, digests never do.
type authFileReloader struct {
	path    string
	reg     *auth.Registry
	pending *auth.File
}

// newAuthFileReloader binds one token file path to one live registry.
func newAuthFileReloader(path string, reg *auth.Registry) *authFileReloader {
	return &authFileReloader{path: path, reg: reg}
}

// Name is the reloader identity in server.reload events.
func (r *authFileReloader) Name() string { return "authfile" }

// Validate parses the current file content side-effect-free and proposes the
// id-level diff. The parsed snapshot parks on pending until Apply consumes it;
// a later failed validation simply overwrites or abandons it without harm.
func (r *authFileReloader) Validate(_ context.Context) ([]lifecycle.Change, error) {
	file, err := readAuthTokens(r.path)
	if err != nil {
		return nil, fmt.Errorf("cannot reload %s: %w", r.path, err)
	}
	r.pending = file

	from := r.reg.LiveIDs()
	to := make([]string, 0, len(file.Tokens))
	for _, t := range file.Tokens {
		to = append(to, t.ID)
	}
	sort.Strings(from)
	sort.Strings(to)

	return []lifecycle.Change{{
		Subject: "tokens",
		From:    strings.Join(from, ","),
		To:      strings.Join(to, ","),
		Secret:  true,
	}}, nil
}

// Apply swaps the validated snapshot into the live registry. It is infallible by
// construction: everything fallible happened in Validate.
func (r *authFileReloader) Apply(_ context.Context, _ lifecycle.Change) error {
	if r.pending != nil {
		r.reg.SwapTokens(r.pending.Tokens)
		r.pending = nil
	}
	return nil
}
