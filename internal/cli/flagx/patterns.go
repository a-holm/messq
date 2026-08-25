// SPDX-License-Identifier: Apache-2.0

package flagx

import (
	"strings"

	"github.com/a-holm/messq/internal/errs"
	"github.com/a-holm/messq/internal/subject"
)

// Patterns is the repeatable --subject/--filter flag value (issue #24 slice 1). Every
// pattern is validated by internal/subject's grammar (issue #3) before it is stored, so
// a malformed pattern costs exit 2 client-side without a round trip.
type Patterns []string

// Set validates s against the subject pattern grammar and appends the raw text;
// patterns render on the wire exactly as the user typed them.
func (p *Patterns) Set(s string) error {
	if _, err := subject.ParsePattern(s); err != nil {
		return errs.E(errs.ErrBadRequest, "flagx.Patterns.Set", "%v", err)
	}
	*p = append(*p, s)
	return nil
}

// String renders display-grade comma-joined raw patterns; not canonical, because a
// literal token may legally contain a comma.
func (p Patterns) String() string { return strings.Join(p, ",") }

// Type names the value for pflag's default display.
func (p Patterns) Type() string { return "patterns" }
