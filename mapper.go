package pgxx

import (
	"github.com/jackc/pgx/v5"
	"github.com/jmoiron/sqlx"
)

// String type for SQL literals.
// Having this be a separate type instead of string helps prevent accidental SQL injection.
type SQLQuery string

func ExtractNamedQuery(query SQLQuery, argsStruct any) (SQLQuery, []any, error) {
	questionQuery, args, err := sqlx.Named(string(query), argsStruct)
	if err != nil {
		return "", nil, err
	}
	posQuery := sqlx.Rebind(sqlx.DOLLAR, questionQuery)
	return SQLQuery(posQuery), args, nil
}

// Wrapper for pgx.Rows implementing sqlx.rowsi
// This allows us to bypass the default databsse/sql adapter and use pgx types and transaction support
type rowsAdapter struct {
	rows pgx.Rows
}

func (r *rowsAdapter) Close() error {
	r.rows.Close()
	return nil
}

func (r *rowsAdapter) Columns() ([]string, error) {
	fields := r.rows.FieldDescriptions()
	cols := make([]string, len(fields))
	for i, fd := range fields {
		cols[i] = string(fd.Name)
	}
	return cols, nil
}

func (r *rowsAdapter) Err() error {
	return r.rows.Err()
}

func (r *rowsAdapter) Next() bool {
	return r.rows.Next()
}

func (r *rowsAdapter) Scan(dst ...any) error {
	return r.rows.Scan(dst...)
}

func ScanRowsUntyped[T any](rows pgx.Rows, dst any) error {
	rowsi := rowsAdapter{rows: rows}
	return sqlx.StructScan(&rowsi, dst)
}

// Scan rows to a slice of structs using sqlx mapping.
func ScanRows[T any](rows pgx.Rows, dst *[]T) error {
	rowsi := rowsAdapter{rows: rows}
	return sqlx.StructScan(&rowsi, dst)
}

// Helper function to return the first item of a list, or nil if empty
func Head[T any](xs []T) *T {
	if len(xs) == 0 {
		return nil
	} else {
		return &xs[0]
	}
}