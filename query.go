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

func Exec(ctx context.Context, conn PoolOrTx, query SQL, args ...any) (int, error) {
	tag, err := conn.Exec(ctx, string(query), args...)
	if err != nil {
		return 0, err
	}
	return int(tag.RowsAffected()), nil
}

func NamedExec(ctx context.Context, conn PoolOrTx, namedQuery SQL, argsStruct any) (int, error) {
	query, args := ExtractNamedQuery(namedQuery, argsStruct)
	tag, err := conn.Exec(ctx, string(query), args...)
	if err != nil {
		return 0, err
	}
	return int(tag.RowsAffected()), nil
}

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
