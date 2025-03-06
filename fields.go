// Copyright 2024-2025 George Steel
// SPDX-License-Identifier: MIT

package pgxx

type FieldName string

// Produces a list of fields comma-separated for use in queries
func ListFields(fields []FieldName) SQL {
	if len(fields) == 0 {
		return ""
	}
	out := SQL(fields[0])
	for _, f := range fields[1:] {
		out += ", " + SQL(f)
	}
	return out
}

// Produces a list of fields as named parameters
func ListNamedFieldParams(fields []FieldName) SQL {
	if len(fields) == 0 {
		return ""
	}
	out := "@" + SQL(fields[0])
	for _, f := range fields[1:] {
		out += ", @" + SQL(f)
	}
	return out
}

// Used to prefix a list of fields, using sepatate prefixes on wither side of the AS.
// When used with the db_prefix tag of the result set, this is quite useful for joins with conflicting field names
// when used in the form `ListFieldsWithPrefix(AllAFields, "a.", "a_")`.
func ListFieldsWithPrefix(fields []FieldName, queryPrefix SQL, resultPrefix SQL) SQL {
	if len(fields) == 0 {
		return ""
	}
	out := queryPrefix + SQL(fields[0]) + " AS " + resultPrefix + SQL(fields[0])
	for _, f := range fields[1:] {
		out += ", " + queryPrefix + SQL(f) + " AS " + resultPrefix + SQL(f)
	}
	return out
}

// Produces an INSERT query from a list of fields which uses named parameters matching the field names
func NamedInsertQuery(tableName SQL, fields []FieldName) SQL {
	return "INSERT INTO " + tableName + " (" + ListFields(fields) + ") VALUES (" + ListNamedFieldParams(fields) + ")"
}
