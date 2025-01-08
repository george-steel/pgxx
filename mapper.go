// Copyright 2024-2025 George Steel
// SPDX-License-Identifier: MIT

package pgxx

import (
	"database/sql"
	"fmt"
	"reflect"

	"github.com/jackc/pgx/v5"
	"github.com/jmoiron/sqlx"
)

// Bidirectional mapping between structs, cursors, and queries.
//
// This currently calls the relevant functions in sqlx internally,
// but there is an option to fork the mapping code for more optimized access.

// Converts a query with named parameters to one using positional parameters.
// Panics if a query does not match the type of struct given, to simplify use with hardcoded queries.
func ExtractNamedQuery(query SQL, argsStruct any) (SQL, []any) {
	questionQuery, args, err := sqlx.Named(string(query), argsStruct)
	if err != nil {
		panic(err)
	}
	posQuery := sqlx.Rebind(sqlx.DOLLAR, questionQuery)
	return SQL(posQuery), args
}

// Error-tolerant version of ExtractNamedQuery for use with dynamic query strings
func MaybeExtractNamedQuery(query SQL, argsStruct any) (SQL, []any, error) {
	questionQuery, args, err := sqlx.Named(string(query), argsStruct)
	if err != nil {
		return "", nil, err
	}
	posQuery := sqlx.Rebind(sqlx.DOLLAR, questionQuery)
	return SQL(posQuery), args, nil
}

// Extracts fields from a slice of structs for a CopyFrom (bulk insert) query
func ExtractCopyParams[T any](fields []FieldName, records []T) [][]any {
	pseudoQuery := ListNamedFieldParams(fields)
	var out [][]any
	for _, r := range records {
		_, args, err := sqlx.Named(string(pseudoQuery), r)
		if err != nil {
			panic(fmt.Errorf("error extracting fields for copy: %w", err))
		}
		out = append(out, args)
	}
	return out
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

// Scan rows to a slice of structs using sqlx mapping.
func ScanRows[T any](rows pgx.Rows, dst *[]T) error {
	defer rows.Close()
	t := reflect.TypeFor[T]()
	if t.Kind() == reflect.Struct && !reflect.PointerTo(t).Implements(reflect.TypeFor[sql.Scanner]()) {
		// scanning into a struct that is meant to hold multiple columns
		rowsi := rowsAdapter{rows: rows}
		return sqlx.StructScan(&rowsi, dst)
	} else {
		// scanning a single column into a primitive type or a Scanner struct
		if len(rows.FieldDescriptions()) != 1 {
			return fmt.Errorf("expected a single column, got %v", rows.FieldDescriptions())
		}
		*dst = (*dst)[:0]
		for rows.Next() {
			var r T
			err := rows.Scan(&r)
			if err != nil {
				return err
			}
			*dst = append(*dst, r)
		}
		return rows.Err()
	}
}
