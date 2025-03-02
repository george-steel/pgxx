// Copyright 2024-2025 George Steel
// SPDX-License-Identifier: MIT

package pgxx

import (
	"slices"
	"testing"

	"github.com/stretchr/testify/assert"
)

type Foo struct {
	A int    `db:"a"`
	B string `db:"b"`
	Bar
}

type Foo2 struct {
	A    int    `db:"a"`
	B    string `db:"b"`
	Bar2 *Bar   `db_prefix:"bar_"`
}

type Bar struct {
	C float64 `db:"c"`
}

func TestExtractCopyParams(t *testing.T) {
	fooFields := DBFields[Foo]()
	if !slices.Equal(fooFields, []FieldName{"a", "b", "c"}) {
		t.Errorf("unexpected fields for Foo, got %v", fooFields)
	}

	foos := []Foo{
		{A: 1, B: "a", Bar: Bar{C: 1.0}},
		{A: 2, B: "b", Bar: Bar{C: 2.0}},
	}

	expectedRows := [][]any{
		{1, "a", 1.0},
		{2, "b", 2.0},
	}

	rows := ExtractCopyParams(fooFields, foos)
	assert.Equal(t, expectedRows, rows)
}

func TestExtractCopyParamsWithPrefix(t *testing.T) {
	fooFields := DBFields[Foo2]()
	if !slices.Equal(fooFields, []FieldName{"a", "b", "bar_c"}) {
		t.Errorf("unexpected fields for Foo, got %v", fooFields)
	}

	foos := []Foo2{
		{A: 1, B: "a", Bar2: &Bar{C: 1.0}},
		{A: 2, B: "b", Bar2: &Bar{C: 2.0}},
	}

	expectedRows := [][]any{
		{1, "a", 1.0},
		{2, "b", 2.0},
	}

	rows := ExtractCopyParams(fooFields, foos)
	assert.Equal(t, expectedRows, rows)
}

func TestNamedQuery(t *testing.T) {
	foo := Foo{A: 1, B: "a", Bar: Bar{C: 1.0}}

	const namedQuery SQL = `INSERT INTO foo (a, b, c) VALUES (@a, @b, @c)`
	query, args := ExtractNamedQuery(namedQuery, &foo)
	const expectedQuery SQL = `INSERT INTO foo (a, b, c) VALUES ($1, $2, $3)`
	expectedArgs := []any{1, "a", 1.0}
	assert.Equal(t, expectedQuery, query)
	assert.Equal(t, expectedArgs, args)
}
