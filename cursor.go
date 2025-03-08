// Copyright 2024-2025 George Steel
// SPDX-License-Identifier: MIT

package pgxx

import (
	"fmt"
	"reflect"

	"github.com/jackc/pgx/v5"
)

// Bidirectional mapping between structs, cursors, and queries.
//
// Converts a query with named parameters (using the @param syntak of pgx.NamedArgs)
// to one using positional parameters.
// Panics if a query does not match the type of struct given, to simplify use with hardcoded queries.
func ExtractNamedQuery(query SQL, argsStruct any) (SQL, []any) {
	posQuery, fields := RewriteNamedQuery(query)
	val := reflect.Indirect(reflect.ValueOf(argsStruct))
	mapping := structMappingOf(val.Type())

	args, err := mapping.extractNamedArgs(fields, val)
	if err != nil {
		panic(err)
	}
	return posQuery, args
}

// Error-tolerant version of ExtractNamedQuery for use with dynamic query strings
func MaybeExtractNamedQuery(query SQL, argsStruct any) (SQL, []any, error) {
	posQuery, fields := RewriteNamedQuery(query)
	val := reflect.Indirect(reflect.ValueOf(argsStruct))
	mapping := structMappingOf(val.Type())

	args, err := mapping.extractNamedArgs(fields, val)
	if err != nil {
		return "", nil, err
	}
	return posQuery, args, nil
}

// Extracts fields from a slice of structs for a CopyFrom (bulk insert) query
func ExtractCopyParams[T any](fields []FieldName, records []T) [][]any {
	mapper := structMappingFor[T]()
	var out [][]any
	for i := range records {
		val := reflect.ValueOf(&records[i]).Elem()
		args, err := mapper.extractNamedArgs(fields, val)
		if err != nil {
			panic(fmt.Errorf("error extracting fields for copy: %w", err))
		}
		out = append(out, args)
	}
	return out
}

// Scan rows to a slice of either structs (using the mapping defined by db and db_prefix tags)
// or single values (for queries returning a single column).
func ScanRows[T any](rows pgx.Rows, dst *[]T) error {
	defer rows.Close()
	t := reflect.TypeFor[T]()
	if isMappable(t) {
		// scanning into a struct that is meant to hold multiple columns
		mapping := structMappingOf(t)
		fields := rows.FieldDescriptions()
		cols := make([]FieldName, len(fields))
		for i, fd := range fields {
			cols[i] = FieldName(fd.Name)
		}

		*dst = (*dst)[:0]
		for rows.Next() {
			var record T
			ptrs, err := mapping.extractScanPointers(cols, reflect.ValueOf(&record))
			if err != nil {
				panic(err)
			}
			err = rows.Scan(ptrs...)
			if err != nil {
				return err
			}
			*dst = append(*dst, record)
		}
		return rows.Err()
	} else {
		// scanning a single column into a primitive type or a Scanner struct
		if len(rows.FieldDescriptions()) != 1 {
			panic(fmt.Errorf("expected a single column with return type %v, got %v", t, rows.FieldDescriptions()))
		}
		*dst = (*dst)[:0]
		for rows.Next() {
			var record T
			err := rows.Scan(&record)
			if err != nil {
				return err
			}
			*dst = append(*dst, record)
		}
		return rows.Err()
	}
}

// Scan a single row from a query result.
// If requireExact is true, errors on an empty result set or too many rows,
// if it is false, discard extra rows and leave dst unchanged if empty.
func ScanSingleRow[T any](rows pgx.Rows, dst *T, requireExact bool) error {
	defer rows.Close()
	t := reflect.TypeFor[T]()
	if isMappable(t) {
		// scanning into a struct that is meant to hold multiple columns
		mapping := structMappingOf(t)
		fields := rows.FieldDescriptions()
		cols := make([]FieldName, len(fields))
		for i, fd := range fields {
			cols[i] = FieldName(fd.Name)
		}

		if rows.Next() {
			ptrs, err := mapping.extractScanPointers(cols, reflect.ValueOf(dst))
			if err != nil {
				panic(err)
			}

			err = rows.Scan(ptrs...)
			if err != nil {
				return err
			}
			if requireExact {
				if rows.Next() {
					return pgx.ErrTooManyRows
				}
			}
			rows.Close()
			return rows.Err()
		} else {
			if rows.Err() != nil {
				return rows.Err()
			} else if requireExact {
				return pgx.ErrNoRows
			} else {
				return nil
			}
		}
	} else {
		// scanning a single column into a primitive type or a Scanner struct
		if len(rows.FieldDescriptions()) != 1 {
			panic(fmt.Errorf("expected a single column with return type %v, got %v", t, rows.FieldDescriptions()))
		}
		if rows.Next() {
			err := rows.Scan(dst)
			if err != nil {
				return err
			}
			if requireExact {
				if rows.Next() {
					return pgx.ErrTooManyRows
				}
			}
			rows.Close()
			return rows.Err()
		} else {
			if rows.Err() != nil {
				return rows.Err()
			} else if requireExact {
				return pgx.ErrNoRows
			} else {
				return nil
			}
		}
	}
}
