// Copyright 2024-2025 George Steel
// SPDX-License-Identifier: MIT

package pgxx_test

import (
	"context"
	"testing"

	"github.com/bitcomplete/sqltestutil"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/george-steel/pgxx"
)

const TEST_SCHEMA = `
CREATE TABLE users (
    user_id SERIAL PRIMARY KEY,
    name VARCHAR
);

CREATE TABLE accounts (
    account_id SERIAL PRIMARY KEY,
    user_id INT NOT NULL REFERENCES users (user_id),
    name VARCHAR NOT NULL,
    balance INT NOT NULL
);`

type User struct {
	UserID int    `db:"user_id"`
	Name   string `db:"name"`
}

type Account struct {
	AccountId int    `db:"account_id"`
	UserId    int    `db:"user_id"`
	Name      string `db:"name"`
	Balance   int    `db:"balance"`
}

func TestWithDatabase(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}

	ctx := context.Background()
	//ctx, cancelTimeout := context.WithTimeout(context.Background(), 20*time.Second)
	//defer cancelTimeout()

	pg, err := sqltestutil.StartPostgresContainer(ctx, "17")
	defer pg.Shutdown(context.Background())
	require.NoError(t, err, "unable to create database")

	pool, err := pgxpool.New(ctx, pg.ConnectionString())
	require.NoError(t, err, "error connecting to database")

	// set a schema
	_, err = pgxx.Exec(ctx, pool, TEST_SCHEMA)
	require.NoError(t, err, "error creating schema")

	// basic insertion snd scans
	alice := User{Name: "Alice"}
	bob := User{Name: "Bob"}

	batch := pgxx.NewBatch()
	insertUserQuery := pgxx.NamedInsertQuery("users", []pgxx.FieldName{"name"}) + " RETURNING user_id"
	pgxx.QueueNamedQueryOne(batch, &alice.UserID, insertUserQuery, &alice)
	pgxx.QueueNamedQueryOne(batch, &bob.UserID, insertUserQuery, &bob)
	err = pgxx.RunBatch(ctx, pool, batch)
	assert.NoError(t, err)
	assert.NotZero(t, alice.UserID)
	assert.NotZero(t, bob.UserID)

	selectUsersQuery := "SELECT " + pgxx.ListFields(pgxx.DBFields[User]()) + " FROM USERS ORDER BY user_id"
	users, err := pgxx.Query[User](ctx, pool, selectUsersQuery)
	assert.NoError(t, err)
	assert.Len(t, users, 2)
	assert.Equal(t, alice, users[0])
	assert.Equal(t, bob, users[1])

	// CopyFrom
	accounts := []Account{
		{UserId: alice.UserID, Name: "chequing", Balance: 100},
		{UserId: bob.UserID, Name: "chequing", Balance: 200},
	}
	nrows, err := pgxx.NamedCopyFrom(ctx, pool, "accounts", []pgxx.FieldName{"user_id", "name", "balance"}, accounts)
	assert.NoError(t, err)
	assert.Equal(t, len(accounts), nrows)

	// single selects
	selectAccountQuery := "SELECT " + pgxx.ListFields(pgxx.DBFields[Account]()) + " FROM accounts WHERE user_id = $1 and name = $2"
	// can return either the struct itself or a pointer
	accountA, err := pgxx.QueryOne[*Account](ctx, pool, selectAccountQuery, alice.UserID, "chequing")
	assert.NoError(t, err)
	assert.NotNil(t, accountA)
	assert.NotZero(t, accountA.AccountId)
	assert.Equal(t, 100, accountA.Balance)

	accountB, err := pgxx.QueryExactlyOne[Account](ctx, pool, selectAccountQuery, bob.UserID, "chequing")
	assert.NoError(t, err)
	assert.NotZero(t, accountB.AccountId)
	assert.Equal(t, 200, accountB.Balance)

	// join tables with colliding field names using prefixes
	type AccountWithUser struct {
		User    User    `db_prefix:"u_"`
		Account Account `db_prefix:"a_"`
	}
	queryWithJoin := "SELECT " +
		pgxx.ListFieldsWithPrefix(pgxx.DBFields[User](), "u.", "u_") + ", " +
		pgxx.ListFieldsWithPrefix(pgxx.DBFields[Account](), "a.", "a_") +
		" FROM users u INNER JOIN accounts a ON u.user_id = a.user_id " +
		"WHERE u.name = $1"
	joinedResult, err := pgxx.QueryExactlyOne[AccountWithUser](ctx, pool, queryWithJoin, "Alice")
	assert.NoError(t, err)
	expectedJoinedResult := AccountWithUser{
		User:    alice,
		Account: *accountA,
	}
	assert.Equal(t, expectedJoinedResult, joinedResult)

	// test colliding transactions
	tx1Retries := 0
	err = pgxx.RunInTx(ctx, pool, func(tx1 pgxx.Tx) error {
		tx1Retries += 1
		accountA1, err1 := pgxx.QueryExactlyOne[Account](ctx, tx1, selectAccountQuery, alice.UserID, "chequing")
		if err1 != nil {
			return err1
		}

		const setBalanceQuery = "UPDATE accounts SET balance = @balance WHERE account_id = @account_id"

		// launch a conflicting transaction if this is the first time
		// doing both transactions in a single thread makes it possible to guarantee a conflict
		if tx1Retries == 1 {
			err1 = pgxx.RunInTx(ctx, pool, func(tx2 pgxx.Tx) error {
				accountA2, err2 := pgxx.QueryExactlyOne[Account](ctx, tx2, selectAccountQuery, alice.UserID, "chequing")
				if err2 != nil {
					return err2
				}

				accountA2.Balance += 75
				err2 = pgxx.NamedExecExactlyOne(ctx, tx2, setBalanceQuery, accountA2)
				if err2 != nil {
					return err2
				}
				return nil
			})
			assert.NoError(t, err1, "error in second transaction")
		}

		accountA1.Balance -= 50
		err1 = pgxx.NamedExecExactlyOne(ctx, tx1, setBalanceQuery, accountA1)
		if err1 != nil {
			return err1
		}

		return nil
	})
	assert.NoError(t, err)
	// first transaction must retry due to decond transaction
	assert.Equal(t, 2, tx1Retries)
	// integrity check: neither transaction clobbered the other.
	accountA, err = pgxx.QueryExactlyOne[*Account](ctx, pool, selectAccountQuery, alice.UserID, "chequing")
	assert.NoError(t, err)
	assert.Equal(t, 125, accountA.Balance)
}
