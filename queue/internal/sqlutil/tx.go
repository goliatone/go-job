package sqlutil

import "database/sql"

// Rollback safely rolls back a transaction and ignores rollback errors.
func Rollback(tx *sql.Tx) {
	if tx == nil {
		return
	}
	_ = tx.Rollback()
}
