// SPDX-License-Identifier: Apache-2.0

// Package doctor is the diagnostic engine behind `messq doctor` (issue #30):
// a registry of PURE checks over a collected Snapshot, prose findings with
// exact fix commands, and two collection sources — a running daemon (live) and
// an offline read-only open of a data directory.
//
// The architectural rule: a check may not perform I/O. Collection happens
// once, up front, into a Snapshot; every check is func(ctx, *Snapshot)
// []Finding, unit-tested against hand-written snapshot literals. An HTTP call
// or a db.Query inside the checks is a design violation.
package doctor

import (
	"context"
	"fmt"
	"sort"
	"time"
)

// Severity ranks one finding. Zero value is SevSkipped so a half-filled
// Finding literal degrades to the honest "could not judge", never to OK.
type Severity uint8

const (
	SevSkipped Severity = iota // data missing / budget expired / internal error
	SevOK                      // checked, healthy
	SevInfo                    // informational: no action demanded
	SevWarn                    // operator should look at this
	SevFail                    // broken or dangerous; cron must page
)

func (s Severity) String() string {
	switch s {
	case SevSkipped:
		return "skip"
	case SevOK:
		return "ok"
	case SevInfo:
		return "info"
	case SevWarn:
		return "warn"
	case SevFail:
		return "fail"
	default:
		return "skip"
	}
}

// MarshalJSON freezes the machine spelling of a severity as its lowercase name
// ("skip"/"ok"/"info"/"warn"/"fail") — numeric tags would leak enum ordering
// into documents operators already diff and alert on.
func (s Severity) MarshalJSON() ([]byte, error) {
	return []byte(`"` + s.String() + `"`), nil
}

// Source declares which collector a Check needs.
type Source uint8

const (
	SourceLive    Source = iota // needs a running daemon (/v1/info, /metrics)
	SourceDataDir               // needs a readable data directory
	SourceEither                // either source satisfies it
)

// Subject names WHAT a finding is about — never a message body, never a token.
type Subject struct {
	Stream   string `json:"stream,omitempty"`
	Consumer string `json:"consumer,omitempty"`
	Path     string `json:"path,omitempty"`
}

// Finding is one doctor verdict. IDs are stable and greppable; every finding
// carries either Fix commands in order or NoFix explaining why there is nothing
// to run.
type Finding struct {
	ID       string         `json:"id"`
	Severity Severity       `json:"severity"`
	Subject  Subject        `json:"subject,omitempty"`
	Title    string         `json:"title"`
	Detail   string         `json:"detail,omitempty"`
	Fix      []string       `json:"fix,omitempty"`
	NoFix    string         `json:"nofix,omitempty"`
	Evidence map[string]any `json:"evidence,omitempty"`
	Docs     string         `json:"docs"` // docs/doctor.md#<id>
}

// defaultBudget bounds one check's Eval when Check.Budget is unset.
const defaultBudget = 2 * time.Second

// Check is one registered diagnosis. Eval MUST be pure over the Snapshot.
type Check struct {
	ID      string // stable, greppable, documented ("consumer.max_deliver_unlimited")
	Summary string // one line for --list
	Explain string // teaching paragraph for --explain <id>
	Needs   Source
	Budget  time.Duration // <= 0 means defaultBudget; exceeded ⇒ SevSkipped
	Eval    func(context.Context, *Snapshot) []Finding
}

// RunChecks evaluates every registered check against one collected snapshot,
// enforcing each check's budget. A budget expiry (or panic) becomes a
// SevSkipped finding — one broken check must not cost the operator the other
// thirty during an incident.
func RunChecks(ctx context.Context, r *Registry, snap *Snapshot) []Finding {
	var out []Finding
	for _, id := range r.List() {
		check := r.mustGet(id)
		budget := check.Budget
		if budget <= 0 {
			budget = defaultBudget
		}
		cctx, cancel := context.WithTimeout(ctx, budget)
		var findings []Finding
		func() {
			defer func() {
				if rec := recover(); rec != nil {
					findings = append(findings, Finding{
						ID:       check.ID,
						Severity: SevSkipped,
						Title:    "internal error while checking",
						Detail:   fmt.Sprintf("internal error (%v) — please file a bug with --output json", rec),
						NoFix:    "this is a messq bug, not an operator problem",
						Docs:     docsAnchor(check.ID),
					})
				}
			}()
			findings = check.Eval(cctx, snap)
		}()
		cancel()
		if cctx.Err() != nil && len(findings) == 0 {
			findings = append(findings, Finding{
				ID:       check.ID,
				Severity: SevSkipped,
				Title:    "check did not finish within its budget",
				Detail:   "timed out after " + budget.String(),
				NoFix:    "this is informational; rerun with a larger --timeout if it persists",
				Docs:     docsAnchor(check.ID),
			})
		}
		out = append(out, findings...)
		if ctx.Err() != nil {
			break // whole-run budget exhausted; remaining checks are skipped by the CLI narration
		}
	}
	return out
}

// docsAnchor renders the canonical documentation link for an ID.
func docsAnchor(id string) string { return "docs/doctor.md#" + id }

// Registry holds the checks. Registration panics on duplicate IDs: the ID set
// is a contract consumed by alerts and runbooks, and a silent duplicate would
// make --list lie.
type Registry struct {
	byID map[string]*Check
}

// NewRegistry returns an empty registry.
func NewRegistry() *Registry { return &Registry{byID: map[string]*Check{}} }

// Register adds a check; duplicate IDs panic.
func (r *Registry) Register(c Check) {
	if _, dup := r.byID[c.ID]; dup {
		panic(fmt.Sprintf("doctor: duplicate check ID %q", c.ID))
	}
	r.byID[c.ID] = &c
}

// List returns all registered IDs sorted — deterministic for goldens.
func (r *Registry) List() []string {
	ids := make([]string, 0, len(r.byID))
	for id := range r.byID {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	return ids
}

// Explain returns the teaching paragraph for one ID.
func (r *Registry) Explain(id string) (string, bool) {
	c, ok := r.byID[id]
	if !ok {
		return "", false
	}
	return c.Explain, true
}

func (r *Registry) mustGet(id string) *Check {
	c, ok := r.byID[id]
	if !ok {
		panic(fmt.Sprintf("doctor: unknown check %q", id))
	}
	return c
}

// defaultRegistry is what `messq doctor` runs. Checks register into it via
// init() from the checks files.
var defaultRegistry = NewRegistry()
