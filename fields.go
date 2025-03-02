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

// Produces an INSERT query from a list of fields which uses named parameters matching the field names
func NamedInsertQuery(tableName SQL, fields []FieldName) SQL {
	return "INSERT INTO " + tableName + " (" + ListFields(fields) + ") VALUES (" + ListNamedFieldParams(fields) + ")"
}
