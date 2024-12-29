package pgxx

import (
	"context"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
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

// Context in which to do database operations. Can be a Pool, Conn, or Tx
type PoolOrTx interface {
	Exec(ctx context.Context, sql string, arguments ...any) (commandTag pgconn.CommandTag, err error)
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
	SendBatch(ctx context.Context, b *pgx.Batch) pgx.BatchResults
}

func Exec(ctx context.Context, conn PoolOrTx, query SQLQuery, args ...any) (int, error) {
	tag, err := conn.Exec(ctx, string(query), args...)
	if err != nil {
		return 0, err
	}
	return int(tag.RowsAffected()), nil
}

func NamedExec(ctx context.Context, conn PoolOrTx, namedQuery SQLQuery, argsStruct any) (int, error) {
	query, args, err := ExtractNamedQuery(namedQuery, argsStruct)
	if err != nil {
		return 0, err
	}
	tag, err := conn.Exec(ctx, string(query), args...)
	if err != nil {
		return 0, err
	}
	return int(tag.RowsAffected()), nil
}

func Query[T any](ctx context.Context, conn PoolOrTx, query SQLQuery, args ...any) ([]T, error) {
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

func NamedQuery[T any](ctx context.Context, conn PoolOrTx, namedQuery SQLQuery, argsStruct any) ([]T, error) {
	query, args, err := ExtractNamedQuery(namedQuery, argsStruct)
	if err != nil {
		return nil, err
	}
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

func QuerySingle[T any](ctx context.Context, conn PoolOrTx, query SQLQuery, args ...any) (*T, error) {
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

func NamedQuerySinge[T any](ctx context.Context, conn PoolOrTx, namedQuery SQLQuery, argsStruct any) (*T, error) {
	query, args, err := ExtractNamedQuery(namedQuery, argsStruct)
	if err != nil {
		return nil, err
	}
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
