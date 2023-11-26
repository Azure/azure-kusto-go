package query

import (
	"encoding/csv"
	"github.com/Azure/azure-kusto-go/azkustodata/errors"
	"github.com/Azure/azure-kusto-go/azkustodata/value"
	"reflect"
	"strings"
)

func (r *Row) Table() Table {
	return r.table
}

func (r *Row) Values() value.Values {
	return r.values
}

func (r *Row) Value(i int) value.Kusto {
	return r.values[i]
}

func (r *Row) ValueByColumn(c Column) value.Kusto {
	return r.values[c.Ordinal]
}

// ExtractValues fetches all values in the row at once.
// The value of the kth column will be decoded into the kth argument to ExtractValues.
// The number of arguments must be equal to the number of columns.
// Pass nil to specify that a column should be ignored.
// ptrs should be compatible with column types. An error in decoding may leave
// some ptrs set and others not.
func (r *Row) ExtractValues(ptrs ...interface{}) error {
	if len(ptrs) != len(r.table.Columns()) {
		return errors.ES(errors.OpUnknown, errors.KClientArgs, ".Columns() requires %d arguments for this row, had %d", len(r.table.Columns()), len(ptrs))
	}

	for i, val := range r.Values() {
		if ptrs[i] == nil {
			continue
		}
		if err := val.Convert(reflect.ValueOf(ptrs[i]).Elem()); err != nil {
			return err
		}
	}

	return nil
}

// ToStruct fetches the columns in a row into the fields of a struct. p must be a pointer to struct.
// The rules for mapping a row's columns into a struct's exported fields are:
//
//  1. If a field has a `kusto: "column_name"` tag, then decode column
//     'column_name' into the field. A special case is the `column_name: "-"`
//     tag, which instructs ToStruct to ignore the field during decoding.
//
//  2. Otherwise, if the name of a field matches the name of a column (ignoring case),
//     decode the column into the field.
//
// Slice and pointer fields will be set to nil if the source column is a null value, and a
// non-nil value if the column is not NULL. To decode NULL values of other types, use
// one of the kusto types (Int, Long, Dynamic, ...) as the type of the destination field.
// You can check the .Valid field of those types to see if the value was set.
func (r *Row) ToStruct(p interface{}) error {
	// Check if p is a pointer to a struct
	if t := reflect.TypeOf(p); t == nil || t.Kind() != reflect.Ptr || t.Elem().Kind() != reflect.Struct {
		//todo - op in ctx?
		return errors.ES(errors.OpUnknown, errors.KClientArgs, "type %T is not a pointer to a struct", p)
	}
	if len(r.table.Columns()) != len(r.Values()) {
		return errors.ES(errors.OpUnknown, errors.KClientArgs, "row does not have the correct number of values(%d) for the number of columns(%d)", len(r.Values()), len(r.table.Columns()))
	}

	return decodeToStruct(r.table.Columns(), r.Values(), p)
}

// String implements fmt.Stringer for a Row. This simply outputs a CSV version of the row.
func (r *Row) String() string {
	line := []string{}
	for _, v := range r.Values() {
		line = append(line, v.String())
	}
	b := &strings.Builder{}
	w := csv.NewWriter(b)
	err := w.Write(line)
	if err != nil {
		return ""
	}
	w.Flush()
	return b.String()
}
