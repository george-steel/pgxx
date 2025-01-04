package pgxx

import (
	"slices"
	"testing"
)

type Foo struct {
	A int    `db:"a"`
	B string `db:"b"`
	Bar
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
		Foo{A: 1, B: "a", Bar: Bar{C: 1.0}},
		Foo{A: 2, B: "b", Bar: Bar{C: 2.0}},
	}

	expectedRows := [][]any{
		{1, "a", 1.0},
		{2, "b", 2.0},
	}

	rows := ExtractCopyParams(fooFields, foos)
	if !slices.EqualFunc(rows, expectedRows, slices.Equal) {
		t.Errorf("unexpected rows, got %v", rows)
	}
}
