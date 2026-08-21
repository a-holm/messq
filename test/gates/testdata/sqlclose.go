// SPDX-License-Identifier: Apache-2.0

package store

import (
	"context"
	"database/sql"
)

// SabotageRowsClose leaks the result set: the connection never goes back to the pool.
func SabotageRowsClose(ctx context.Context, db *sql.DB) error {
	rows, err := db.QueryContext(ctx, "select subject from messages")
	if err != nil {
		return err
	}
	for rows.Next() {
		var subject string
		if err := rows.Scan(&subject); err != nil {
			return err
		}
	}
	return rows.Err()
}
