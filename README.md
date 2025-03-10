pgxx
=====

A high-level helper for [pgx](https://pkg.go.dev/github.com/jackc/pgx/v5)
where most operations are done with a single function call that takes and returns standard go and pgx types.
Functionality is heavily inspired by [sqlx](https://pkg.go.dev/github.com/jmoiron/sqlx),
but uses the raw pgx interface instead of database/sql for increased efficiency
and to support ACID transactions in both batched and retry-loop modes (neither of which arre supported by database/sql).

For example usage, see `integration_test.go`.

When scanning results and resolving named parameters,
columns are mapped to struct fields tagged with `db:"column_name"` (fields without this tag are ignored).
Additionally, to support composite fields and ad-hoc joins, a struct field can instead be tagged with `db_prefix`,
to embed its tagged fields into the parent's mapping with a custom prefix.

In order to keep the API simple, functions based on reflection will panic on type errors (if the `any` parameter is not a struct or pointer-to-struct, or if it is missing the requested named parameters) unless otherwise indicated.
