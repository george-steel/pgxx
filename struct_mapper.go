package pgxx

import (
	"database/sql"
	"fmt"
	"reflect"
	"slices"
	"sync"
)

func isMappable(t reflect.Type) bool {
	switch t.Kind() {
	case reflect.Pointer:
		return isMappable(t.Elem())
	case reflect.Struct:
		return !reflect.PointerTo(t).Implements(reflect.TypeFor[sql.Scanner]())
	default:
		return false
	}
}

type structMapping struct {
	StructType    reflect.Type
	FieldList     []FieldName
	FieldMappings map[FieldName][]int
}

func makeStructMapping(t reflect.Type) (structMapping, error) {
	m := structMapping{
		StructType:    t,
		FieldList:     nil,
		FieldMappings: make(map[FieldName][]int),
	}
	err := extendStructMapping(&m, t, "", nil)
	return m, err
}

func extendStructMapping(m *structMapping, t reflect.Type, field_prefix string, path_prefix []int) error {
	switch t.Kind() {
	case reflect.Pointer:
		return extendStructMapping(m, t.Elem(), field_prefix, path_prefix)
	case reflect.Struct:
		for i := range t.NumField() {
			f := t.Field(i)
			dbtag := f.Tag.Get("db")
			prefixtag, isprefix := f.Tag.Lookup("db_prefix")
			if !f.IsExported() {
				continue
			}
			if dbtag != "" {
				name := FieldName(field_prefix + dbtag)
				m.FieldList = append(m.FieldList, name)
				if _, tagCollision := m.FieldMappings[name]; tagCollision {
					return fmt.Errorf("duplicate database field defined: %s", name)
				}
				if len(path_prefix) == 0 {
					m.FieldMappings[name] = []int{i}
				} else {
					m.FieldMappings[name] = slices.Concat(path_prefix, []int{i})
				}
			} else if f.Anonymous || isprefix {
				err := extendStructMapping(m, f.Type, field_prefix+prefixtag, slices.Concat(path_prefix, []int{i}))
				if err != nil {
					return err
				}
			}
		}
		return nil
	default:
		return fmt.Errorf("cannot get database fields of non-struct %s", t.Name())
	}
}

var structMappingsCache = make(map[reflect.Type]structMapping)
var structMappingsLock sync.RWMutex

func structMappingOf(t reflect.Type) structMapping {
	structMappingsLock.RLock()
	m, ok := structMappingsCache[t]
	structMappingsLock.RUnlock()
	if ok {
		return m
	}

	structMappingsLock.Lock()
	defer structMappingsLock.Unlock()
	m, ok = structMappingsCache[t]
	if ok {
		return m
	}
	m, err := makeStructMapping(t)
	if err != nil {
		panic(err)
	}
	structMappingsCache[t] = m
	return m
}

func structMappingFor[T any]() structMapping {
	return structMappingOf(reflect.TypeFor[T]())
}

// Returns the tags of all struct fields tagged with `db`, including those inside embedded structs.
// Does not deduplicate.
func DBFields[T any]() []FieldName {
	return structMappingFor[T]().FieldList
}

func (m structMapping) extractNamedArgs(fields []FieldName, val reflect.Value) ([]any, error) {
	args := make([]any, len(fields))

	if val.Kind() == reflect.Pointer {
		val = val.Elem()
	}

	for i, f := range fields {
		idx, found := m.FieldMappings[f]
		if !found {
			return nil, fmt.Errorf("missing database field %s in struct %s", f, m.StructType.Name())
		}
		fval := val.FieldByIndex(idx)
		args[i] = fval.Interface()
	}
	return args, nil
}

func (m structMapping) extractScanPointers(fields []FieldName, ptr reflect.Value) ([]any, error) {
	args := make([]any, len(fields))

	val := ptr.Elem()

	for i, f := range fields {
		idx, found := m.FieldMappings[f]
		if !found {
			return nil, fmt.Errorf("missing database field %s in struct %s", f, m.StructType.Name())
		}
		fval := val
		for _, j := range idx {
			// allocate if an embedded pointer is nil
			for fval.Kind() == reflect.Pointer {
				if fval.IsNil() {
					fval.Set(reflect.New(fval.Type().Elem()))
				}
				fval = fval.Elem()
			}
			fval = fval.Field(j)
		}
		args[i] = fval.Addr().Interface()
	}
	return args, nil
}
