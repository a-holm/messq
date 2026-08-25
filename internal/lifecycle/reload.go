// SPDX-License-Identifier: Apache-2.0

package lifecycle

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"strings"
)

// redactedPlaceholder stands in for a secret in every rendered diff. The golden
// tests assert the placeholder, so a secret can never leak through a log line.
const redactedPlaceholder = "[redacted]"

// Change is one proposed setting move, e.g. loglevel info→debug or an authfile's
// token rotation. Secret changes render as [redacted] on both sides — G7's rule is
// that a secret never appears in Change.To, because the diff is logged verbatim.
type Change struct {
	Subject string // what moved: "loglevel", "tokens", …
	From    string
	To      string
	Secret  bool
}

// LogValue makes Change safe to drop straight into a slog record.
func (c Change) LogValue() slog.Value {
	from, to := c.From, c.To
	if c.Secret {
		from, to = redactedPlaceholder, redactedPlaceholder
	}
	return slog.GroupValue(
		slog.String("subject", c.Subject),
		slog.String("from", from),
		slog.String("to", to),
	)
}

// RenderDiff renders the human-readable line carried by server.reload.
func RenderDiff(changes []Change) string {
	parts := make([]string, 0, len(changes))
	for _, c := range changes {
		from, to := c.From, c.To
		if c.Secret {
			from, to = redactedPlaceholder, redactedPlaceholder
		}
		parts = append(parts, fmt.Sprintf("%s %s->%s", c.Subject, from, to))
	}
	return strings.Join(parts, "; ")
}

// Reloader is one SIGHUP-mutable setting source (#16 registers authfile here, #19
// the log level). Validate reads the current source and proposes changes WITHOUT
// touching live state; Apply commits them. The registry never calls Apply unless
// every reloader validated — settings move all-or-nothing (G7).
type Reloader interface {
	Name() string
	Validate(ctx context.Context) ([]Change, error)
	Apply(ctx context.Context, change Change) error
}

// reloadEvent is the server.reload audit event. Local constant until #19's event
// vocabulary lands (same stubbing rule as server.start); failed=1 marks a refused
// pass, and the daemon keeps serving on the previous values.
const reloadEvent = "server.reload"

// Registry is the two-phase reloader set the signal loop's SIGHUP branch drives.
type Registry struct {
	logger    *slog.Logger
	reloaders []Reloader
}

// NewRegistry returns a registry over reloaders in registration order; both phases
// preserve that order so a diff reads the same way every time.
func NewRegistry(logger *slog.Logger, reloaders ...Reloader) *Registry {
	if logger == nil {
		logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	}
	return &Registry{logger: logger, reloaders: append([]Reloader(nil), reloaders...)}
}

// Reload runs validate-all-then-apply-all. A single failing Validate applies
// nothing, is logged with failed=1, and is returned; the daemon keeps serving on
// its previous configuration either way — a corrupt file at reload must never
// take the broker down.
func (r *Registry) Reload(ctx context.Context) error {
	type phase2 struct {
		rl      Reloader
		changes []Change
	}
	validated := make([]phase2, 0, len(r.reloaders))
	var proposed []Change
	for _, rl := range r.reloaders {
		changes, err := rl.Validate(ctx)
		if err != nil {
			r.logger.Warn(reloadEvent, "failed", 1, "reloader", rl.Name(), "err", err)
			return fmt.Errorf("lifecycle: reloader %q failed validation: %w", rl.Name(), err)
		}
		validated = append(validated, phase2{rl: rl, changes: changes})
		proposed = append(proposed, changes...)
	}
	for _, p := range validated {
		for _, c := range p.changes {
			if err := p.rl.Apply(ctx, c); err != nil {
				r.logger.Warn(reloadEvent, "failed", 1, "reloader", p.rl.Name(), "subject", c.Subject, "err", err)
				return fmt.Errorf("lifecycle: reloader %q failed to apply %q: %w", p.rl.Name(), c.Subject, err)
			}
		}
	}
	r.logger.Info(reloadEvent, "failed", 0, "changed", len(proposed), "diff", RenderDiff(proposed))
	return nil
}

// Serve consumes reload requests until ctx ends, collapsing everything queued into
// one pass. A request arriving mid-pass survives as exactly one trailing pass — a
// 100-SIGHUP burst costs at most two passes, never a hundred sequential reloads.
// The composition root wires the signal loop's SIGHUP branch to send on requests.
func (r *Registry) Serve(ctx context.Context, requests <-chan struct{}) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-requests:
		drain:
			for {
				select {
				case <-requests:
				default:
					break drain
				}
			}
			// A failed pass is already logged as server.reload failed=1 inside
			// Reload; the daemon keeps serving on the previous values.
			_ = r.Reload(ctx) //nolint:errcheck // failures are logged inside Reload
		}
	}
}
