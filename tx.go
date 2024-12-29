package pgxx

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// Returns if a (possibly wrapped) error is due to a transaction being clobbered by other dbactivity.
// If this returns true, the transaction should be retried.
func IsTxCollisionError(err error) bool {
	var pgerr *pgconn.PgError
	if errors.As(err, &pgerr) {
		return pgerr.Code == "40001"
	}
	return false
}

// A pool or single connection.
type TxContext interface {
	BeginTx(ctx context.Context, txOptions pgx.TxOptions) (pgx.Tx, error)
}

var MaxTxRetries int = 10

// Runs a transaction in a client-side retry loop to handle collisions.
// Safe to use in serializable/ACID mode.
// retryableAction must be idempotent in its non-db side-effects as it will be run multiple times if the transaction retries.
func RunInTxWithOptions(conn TxContext, ctx context.Context, txOptions pgx.TxOptions, retryableAction func(pgx.Tx) error) error {
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

		if IsTxCollisionError((err)) {
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

func RunInTx(conn TxContext, ctx context.Context, txOptions pgx.TxOptions, retryableAction func(pgx.Tx) error) error {
	options := pgx.TxOptions{
		IsoLevel: pgx.Serializable,
	}
	return RunInTxWithOptions(conn, ctx, options, retryableAction)
}
