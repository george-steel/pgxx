// Copyright 2024-2025 George Steel
// SPDX-License-Identifier: MIT

package pgxx

import (
	"context"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// Allocates a new Batch
func NewBatch() *pgx.Batch {
	return &pgx.Batch{}
}

// Runs a batch of state,emts in a single operation,
// which can significantly reduce the number of network roundtrips required.
// Can take a pool or connection to use an implicit transaction
// (BEGIN and COMMIT may be added to the batch to add options, but are not necessary).
func RunBatch(ctx context.Context, conn PoolOrTx, batch *pgx.Batch) error {
	return conn.SendBatch(ctx, batch).Close()
}

// Version of Exec which queues to a batch.
// If out is not nil, writes the number of rows affected there when the batch is run.
func QueueExec(batch *pgx.Batch, out *int, query SQL, args ...any) {
	batch.Queue(string(query), args...).Exec(func(tag pgconn.CommandTag) error {
		if out != nil {
			*out = int(tag.RowsAffected())
		}
		return nil
	})
}

// Version of NamedExec which queues to a batch.
// If out is not nil, writes the number of rows affected there when the batch is run.
func QueueNamedExec(batch *pgx.Batch, out *int, namedQuery SQL, argsStruct any) {
	query, args := ExtractNamedQuery(namedQuery, argsStruct)
	QueueExec(batch, out, query, args...)
}

// Version of Query which queues to a batch.
// Writes results into *out (which must not be nil) when the batch is run.
func QueueQuery[T any](batch *pgx.Batch, out *[]T, query SQL, args ...any) {
	batch.Queue(string(query), args...).Query(func(cursor pgx.Rows) error {
		return ScanRows(cursor, out)
	})
}

// Version of QueryOne which queues to a batch.
// Writes result into *out (which must not be nil) when the batch is run.
func QueueQueryOne[T any](batch *pgx.Batch, out *T, query SQL, args ...any) {
	batch.Queue(string(query), args...).Query(func(cursor pgx.Rows) error {
		return ScanSingleRow(cursor, out, false)
	})
}

// Version of Query which queues to a batch.
// Writes results into *out (which must not be nil) when the batch is run.
func QueueNamedQuery[T any](batch *pgx.Batch, out *[]T, query SQL, args ...any) {
	batch.Queue(string(query), args...).Query(func(cursor pgx.Rows) error {
		return ScanRows(cursor, out)
	})
}

// Version of NamedQueryOne which queues to a batch.
// Writes results into *out (which must not be nil) when the batch is run.
func QueueNamedQueryOne[T any](batch *pgx.Batch, out *T, namedQuery SQL, argsStruct any) {
	query, args := ExtractNamedQuery(namedQuery, argsStruct)
	batch.Queue(string(query), args...).Query(func(cursor pgx.Rows) error {
		return ScanSingleRow(cursor, out, false)
	})
}
