// SPDX-License-Identifier: Apache-2.0

package flagx

import (
	"github.com/a-holm/messq/internal/errs"
	"github.com/a-holm/messq/internal/queue"
)

// Position is the --start flag value (issue #24 slice 1). It reuses queue's
// ParseStartPosition verbatim (D14: one parser per concept), so the CLI accepts exactly
// the grammar the daemon stores: "first", "new", "seq:N" or "time:T" with T in unix
// milliseconds. #28 reuses it for --to.
type Position struct {
	queue.StartPosition
}

// Set parses s into the embedded queue.StartPosition.
func (p *Position) Set(s string) error {
	sp, err := queue.ParseStartPosition(s)
	if err != nil {
		return errs.E(errs.ErrBadRequest, "flagx.Position.Set", "%v", err)
	}
	p.StartPosition = sp
	return nil
}

// String renders the wire form, which Set parses back to the same position.
func (p Position) String() string { return p.StartPosition.String() }

// Type names the value for pflag's default display.
func (p Position) Type() string { return "position" }
