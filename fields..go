package pgxx

import "reflect"

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
	out := ":" + SQL(fields[0])
	for _, f := range fields[1:] {
		out += ", :" + SQL(f)
	}
	return out
}

// Produces an INSERT query from a list of fields which uses named parameters matching the field names
func NamedInsertQuery(tableName SQL, fields []FieldName) SQL {
	return "INSERT INTO " + tableName + " (" + ListFields(fields) + ") VALUES (" + ListNamedFieldParams(fields) + ")"
}

func listDBFields(t reflect.Type) []FieldName {
	switch t.Kind() {
	case reflect.Pointer:
		return listDBFields(t.Elem())
	case reflect.Struct:
		var out []FieldName
		for i := range t.NumField() {
			f := t.Field(i)
			dbtag := f.Tag.Get("db")
			if dbtag != "" {
				out = append(out, FieldName(dbtag))
			} else if f.Anonymous {
				out = append(out, listDBFields(f.Type)...)
			}
		}
		return out
	default:
		return nil
	}
}

// Returns the tags of all struct fields tagged with `db`, indlusing those inside embedded structs.
// Does not deduplicate.
func DBFields[T any]() []FieldName {
	return listDBFields(reflect.TypeFor[T]())
}
