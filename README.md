# pgxx

A high-level helper for `pgx` where most operations are done with a single function call that takes and returns standard go types.

This uses uses a mapping simillar to that of [sqlx](https://pkg.go.dev/github.com/jmoiron/sqlx),
where database columns are mapped to struct fields tagged with `db:"column_name"`.
Additionally, to support composite fields and ad-hoc join, a struct field can instead be tagged with `db_prefix`, to embed its tagged fields into the parent's mapping with a custom prefix.

Unlike `sqlx`, by directly using `pgx` types in its interface instead of `database/sql`, this library allows for the use of serializable/ACID transactions (which are not supported by `database/sql`)
using either client-side retries or batch mode (for fewer network roundtrips and better performance if queries are independent).

