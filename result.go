package ep

import (
	"database/sql/driver"
)

// Result implements driver.Result with provided lastID and rowsAffected values
func Result(lastID, rowsAffected int64) driver.Result {
	return &result{lastID, rowsAffected}
}

type result struct {
	lastID       int64
	rowsAffected int64
}

func (r *result) LastInsertId() (int64, error) {
	return r.lastID, nil
}

func (r *result) RowsAffected() (int64, error) {
	return r.rowsAffected, nil
}
