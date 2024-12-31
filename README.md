# pgxx

A high-level helper for pgx where most operations are done with a single function call that takes and returns standard go types.

Currently, cursor-to-slice-of-structs and struct-to-query-parameters mapping is performed using the mapping layer of [sqlx](https://pkg.go.dev/github.com/jmoiron/sqlx).
The sqlx wrapper types (and database/sql interface) are not used, with all functions using pgx types directly.
This allows for the use of serializable/ACID transactions, which are not supported by database/sql,
using either client-side retries or batch mode (for fewer network roundtrips and better performance).

