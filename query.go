// Copyright 2024-2025 George Steel
// SPDX-License-Identifier: MIT

package pgxx

import (
	"context"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// String type for SQL literals.
// Having this be a separate type instead of string helps prevent accidental SQL injection.
type SQL string

// Context in which to do database operations. Can be a Pool, Conn, or Tx
type PoolOrTx interface {
	Exec(ctx context.Context, sql string, arguments ...any) (commandTag pgconn.CommandTag, err error)
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
	SendBatch(ctx context.Context, b *pgx.Batch) pgx.BatchResults
	CopyFrom(ctx context.Context, tableName pgx.Identifier, columnNames []string, rowSrc pgx.CopyFromSource) (int64, error)
}

// Run a statement with positional parameters and return the number of rows affected.
func Exec(ctx context.Context, conn PoolOrTx, query SQL, args ...any) (int, error) {
	tag, err := conn.Exec(ctx, string(query), args...)
	if err != nil {
		return 0, err
	}
	return int(tag.RowsAffected()), nil
}

// Run a statement with named parameters (pulling them out of a struct) and return the number of rows affected.
func NamedExec(ctx context.Context, conn PoolOrTx, namedQuery SQL, argsStruct any) (int, error) {
	query, args := ExtractNamedQuery(namedQuery, argsStruct)
	tag, err := conn.Exec(ctx, string(query), args...)
	if err != nil {
		return 0, err
	}
	return int(tag.RowsAffected()), nil
}

// Run a query with positional parameters and read out the results as a slice of
// either structs (for multiple-column queries) or primitives (for single-column queries only).
func Query[T any](ctx context.Context, conn PoolOrTx, query SQL, args ...any) ([]T, error) {
	cursor, err := conn.Query(ctx, string(query), args...)
	if err != nil {
		return nil, err
	}
	var out []T
	err = ScanRows(cursor, &out)
	if err != nil {
		return nil, err
	}
	return out, nil
}

// Run a query with named parameters parameters (pulling them out of a struct) and
// read out the results as a slice of either structs (for multiple-column queries)
// or primitives (for single-column queries only).
func NamedQuery[T any](ctx context.Context, conn PoolOrTx, namedQuery SQL, argsStruct any) ([]T, error) {
	query, args := ExtractNamedQuery(namedQuery, argsStruct)
	cursor, err := conn.Query(ctx, string(query), args...)
	if err != nil {
		return nil, err
	}
	var out []T
	err = ScanRows(cursor, &out)
	if err != nil {
		return nil, err
	}
	return out, nil
}

// Run a query with positional parameters that returns at most one row and
// read out the results as a struct (for multiple-column queries) or a primitive (for single-column queries only).
// Returns nil if the query produces no rows. Discards if multiple rows are produced.
func QuerySingle[T any](ctx context.Context, conn PoolOrTx, query SQL, args ...any) (*T, error) {
	cursor, err := conn.Query(ctx, string(query), args...)
	if err != nil {
		return nil, err
	}
	var out []T
	err = ScanRows(cursor, &out)
	if err != nil {
		return nil, err
	}
	return Head(out), nil
}

// un a query with named parameters parameters (pulling them out of a struct) that returns at most one row and
// read out the results as a struct (for multiple-column queries) or a primitive (for single-column queries only).
// Returns nil if the query produces no rows. Discards if multiple rows are produced.
func NamedQuerySingle[T any](ctx context.Context, conn PoolOrTx, namedQuery SQL, argsStruct any) (*T, error) {
	query, args := ExtractNamedQuery(namedQuery, argsStruct)
	cursor, err := conn.Query(ctx, string(query), args...)
	if err != nil {
		return nil, err
	}
	var out []T
	err = ScanRows(cursor, &out)
	if err != nil {
		return nil, err
	}
	return Head(out), nil
}

// Runs a COPY FROM STDIN query for bulk insertion, with the records to insert passed in as structs.
func NamedCopyFrom[T any](ctx context.Context, conn PoolOrTx, tableName SQL, fields []FieldName, records []T) (int, error) {
	var pgxFields []string
	for _, f := range fields {
		pgxFields = append(pgxFields, string(f))
	}
	pgxTable := pgx.Identifier(strings.Split(string(tableName), "."))

	rows := ExtractCopyParams(fields, records)
	nrows, err := conn.CopyFrom(ctx, pgxTable, pgxFields, pgx.CopyFromRows(rows))
	return int(nrows), err
}
