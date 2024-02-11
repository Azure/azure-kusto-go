package query

import (
	kustoErrors "github.com/Azure/azure-kusto-go/azkustodata/errors"
	"github.com/Azure/azure-kusto-go/azkustodata/value"
	"reflect"
	"strings"
)

type fieldMap struct {
	colNameToFieldName map[string]string
}

var typeMapper = map[reflect.Type]fieldMap{}

// decodeToStruct takes a list of columns and a row to decode into "p" which will be a pointer
// to a struct (enforce in the decoder).
func decodeToStruct(cols []Column, row value.Values, p interface{}) error {
	t := reflect.TypeOf(p)
	v := reflect.ValueOf(p)
	fields := newFields(t)

	for i, col := range cols {
		if err := fields.convert(col, row[i], v); err != nil {
			return err
		}
	}
	return nil
}

// newFields takes in the Columns from our row and the reflect.Type of our *struct.
func newFields(ptr reflect.Type) fieldMap {
	if f, ok := typeMapper[ptr]; ok {
		return f
	} else {
		nFields := fieldMap{colNameToFieldName: make(map[string]string, ptr.Elem().NumField())}
		for i := 0; i < ptr.Elem().NumField(); i++ {
			field := ptr.Elem().Field(i)
			if tag := field.Tag.Get("kusto"); strings.TrimSpace(tag) != "" {
				nFields.colNameToFieldName[tag] = field.Name
			} else {
				nFields.colNameToFieldName[field.Name] = field.Name
			}
		}
		typeMapper[ptr] = nFields
		return nFields
	}
}

// convert converts a KustoValue that is for Column col into "v" reflect.Value with reflect.Type "t".
func (f fieldMap) convert(col Column, k value.Kusto, v reflect.Value) error {
	fieldName, ok := f.colNameToFieldName[col.Name()]
	if !ok {
		return nil
	}

	if fieldName == "-" {
		return nil
	}

	err := k.Convert(v.Elem().FieldByName(fieldName))
	if err != nil {
		return kustoErrors.ES(kustoErrors.OpTableAccess, kustoErrors.KWrongColumnType, "column %s could not store in struct.%s: %s", col.Name(), fieldName, err.Error())
	}

	return nil
}
