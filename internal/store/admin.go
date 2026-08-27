// SPDX-License-Identifier: Apache-2.0

package store

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/a-holm/messq/internal/obs"
)

// Runtime admin actions that are not queue-state changes (issue #15 §2): the audit
// trail records them so raising the log level mid-incident lands in the timeline an
// operator reads afterwards. The event vocabulary (S2.4) carries admin.action; this
// file owns the one command that writes it.

// kindAdminAction labels the writer command in logs and the observer.
const kindAdminAction CmdKind = "admin.action"

// adminActionCmd co-commits one admin.action row with its batch.
type adminActionCmd struct {
	setting string
	from    string
	to      string
	actor   string
}

type adminActionResult struct{}

func (c adminActionCmd) Kind() CmdKind { return kindAdminAction }
func (c adminActionCmd) Bytes() int    { return 0 }

func (c adminActionCmd) Apply(ctx context.Context, tx *sql.Tx, now time.Time) (Result, []obs.Event, error) {
	ev, evErr := commitEvent(ctx, tx, event{
		ts:     now.UnixMilli(),
		name:   "admin.action",
		actor:  nullStr(c.actor),
		detail: nullStr(fmt.Sprintf(`{"setting":%q,"from":%q,"to":%q}`, c.setting, c.from, c.to)),
	})
	if evErr != nil {
		return nil, nil, evErr
	}
	return adminActionResult{}, []obs.Event{ev}, nil
}

// RecordAdminAction appends one admin.action event (detail {"setting","from","to"}).
// from and to describe the setting's transition as the caller rendered it — for the
// #15 log-level endpoint, the slog level names before and after the change.
func (s *Store) RecordAdminAction(ctx context.Context, actor, setting, from, to string) error {
	res, err := s.enqueue(ctx, "store.RecordAdminAction", adminActionCmd{
		setting: setting, from: from, to: to, actor: actor,
	})
	if err != nil {
		return err
	}
	if _, ok := res.(adminActionResult); !ok {
		return fmt.Errorf(
			"store.RecordAdminAction: engine returned %T, want adminActionResult", res)
	}
	return nil
}
