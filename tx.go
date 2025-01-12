// Copyright 2024-2025 George Steel
// SPDX-License-Identifier: MIT

package pgxx

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// re-export for convienence
type Tx = pgx.Tx

// Returns if a (possibly wrapped) error is due to a transaction being clobbered by other dbactivity.
// If this returns true, the transaction should be retried.
func IsTxCollisionError(err error) bool {
	if err == nil {
		return false
	}
	var pgerr *pgconn.PgError
	if errors.As(err, &pgerr) {
		return pgerr.Code == "40001" || pgerr.Code == "40P01"
	}
	return false
}

// Transactions inserting with random keys generated within the transaction might want to retry on a unique constraint failure.
func IsUniqueViolationError(err error) bool {
	if err == nil {
		return false
	}
	var pgerr *pgconn.PgError
	if errors.As(err, &pgerr) {
		return pgerr.Code == "23505" || pgerr.Code == "23P01"
	}
	return false
}

// A pool or single connection.
type TxContext interface {
	BeginTx(ctx context.Context, txOptions pgx.TxOptions) (pgx.Tx, error)
}

// Maximum number of times to retry a transaction on collision before erroring out. Changeable.
var MaxTxRetries int = 10

// Runs a transaction in a client-side retry loop to handle collisions.
// Safe to use in serializable/ACID mode.
// retryableAction must be idempotent in its non-db side-effects as it will be run multiple times if the transaction retries.
func RunInTxWithOptions(ctx context.Context, conn TxContext, txOptions pgx.TxOptions, retryOnUniqueViolation bool, retryableAction func(pgx.Tx) error) error {
	var tx pgx.Tx
	var err error
	defer func() {
		if tx != nil {
			tx.Rollback(ctx)
		}
	}()

	for range MaxTxRetries {
		var err error
		tx, err := conn.BeginTx(ctx, txOptions)
		if err != nil {
			return err
		}
		err = retryableAction(tx)
		if err == nil {
			err = tx.Commit(ctx)
			if err == nil {
				return nil
			}
		}

		if IsTxCollisionError(err) || (retryOnUniqueViolation && IsUniqueViolationError(err)) {
			rollbackerr := tx.Rollback(ctx)
			if rollbackerr != nil {
				return rollbackerr
			}
			tx = nil
			continue
		} else {
			return err
		}
	}
	return fmt.Errorf("maximum transaction retries exceeded: %w", err)
}

// SERIALIZABLE transaction options for use with client-side retry.
var DefaultTxOptions = pgx.TxOptions{
	IsoLevel: pgx.Serializable,
}

// Runs a action inside a SERIALIZABLE transaction with client-side retry.
func RunInTx(ctx context.Context, conn TxContext, retryableAction func(pgx.Tx) error) error {
	return RunInTxWithOptions(ctx, conn, DefaultTxOptions, false, retryableAction)
}
