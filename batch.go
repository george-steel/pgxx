package pgxx

import (
	"context"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

func NewBatch() *pgx.Batch {
	return &pgx.Batch{}
}

func RunBatch(ctx context.Context, conn PoolOrTx, batch *pgx.Batch) error {
	return conn.SendBatch(ctx, batch).Close()
}

func QueueExec(batch *pgx.Batch, out *int, query SQLQuery, args ...any) {
	batch.Queue(string(query), args...).Exec(func(tag pgconn.CommandTag) error {
		if out != nil {
			*out = int(tag.RowsAffected())
		}
		return nil
	})
}

func QueueNamedExec(batch *pgx.Batch, out *int, namedQuery SQLQuery, argsStruct any) {
	query, args := ExtractNamedQuery(namedQuery, argsStruct)
	QueueExec(batch, out, query, args)
}

func QueueQuery[T any](batch *pgx.Batch, out *[]T, query SQLQuery, args ...any) {
	batch.Queue(string(query), args...).Query(func(cursor pgx.Rows) error {
		return ScanRows(cursor, out)
	})
}

func QueueNamedQuery[T any](batch *pgx.Batch, out *[]T, namedQuery SQLQuery, argsStruct any) {
	query, args := ExtractNamedQuery(namedQuery, argsStruct)
	batch.Queue(string(query), args...).Query(func(cursor pgx.Rows) error {
		return ScanRows(cursor, out)
	})
}
