// SPDX-License-Identifier: Apache-2.0

package store

import (
	"context"
	"database/sql"
)

// SabotageRowsErr iterates a result set and never checks rows.Err(), so a read that fails
// halfway looks like a short result.
func SabotageRowsErr(ctx context.Context, db *sql.DB) (out []string, err error) {
	rows, err := db.QueryContext(ctx, "select subject from messages")
	if err != nil {
		return nil, err
	}
	defer func() {
		if cerr := rows.Close(); err == nil {
			err = cerr
		}
	}()

	for rows.Next() {
		var subject string
		if err := rows.Scan(&subject); err != nil {
			return nil, err
		}
		out = append(out, subject)
	}
	return out, nil
}
