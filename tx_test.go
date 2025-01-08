// Copyright 2024-2025 George Steel
// SPDX-License-Identifier: MIT

package pgxx

import (
	"fmt"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"
)

func TestRaceErr(t *testing.T) {
	pgerr := pgconn.PgError{
		Code: "40001",
	}
	err := fmt.Errorf("Wrapped error: %w", &pgerr)

	if !IsTxCollisionError(err) {
		t.Errorf("Wrapped transaction race error not identified")
	}
}
