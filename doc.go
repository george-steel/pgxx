// Pgxx is high-level client providing cursor-struct mapping for Postgres using pgx.
//
// Basic functionality is provided by [Exec], [Query], [QueryOne], and [QueryExactlyOne] (which use queries with positional parameters) and their Named counterparts (which use named parameters extracted from a struct).
//
// In order to prevent accidental injection, all queries use the [SQL] type (compatible with standard string literals).
// In order to more easily list fields in queries, this package contains
// a number of helper functions to format lists of fields in various contexts (such as [ListFields])
// as well as the [DBFields] function to get the lase of mapable fields for a given go type.
//
// For ACID transactions use RunInTx, which provides collision detection and a client-side retry loop.
// If all queries areindependent of each other, the entire transaction may be run in a single round-trip using the batch API,
// accessed through [NewBatch], [RunBatch] and the various Queue functions (such as [QueueQuery]).
package pgxx
