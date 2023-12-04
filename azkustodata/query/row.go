package query

import (
	"github.com/Azure/azure-kusto-go/azkustodata/value"
)

// Row is an interface that represents a row in a table.
// It provides methods to access and manipulate the data in the row.
type Row interface {
	// Ordinal returns the ordinal of the row.
	Ordinal() int

	// Table returns the table that the row belongs to.
	Table() Table

	// Values returns all the values in the row.
	Values() value.Values

	// Value returns the value at the specified index.
	Value(i int) value.Kusto

	// ValueByColumn returns the value in the specified column.
	ValueByColumn(c Column) value.Kusto

	// ValueByName returns the value with the specified column name.
	ValueByName(name string) value.Kusto

	// ExtractValues extracts the values from the row and assigns them to the provided pointers.
	// It returns an error if the extraction fails.
	ExtractValues(ptrs ...interface{}) error

	// ToStruct converts the row into a struct and assigns it to the provided pointer.
	// It returns an error if the conversion fails.
	ToStruct(p interface{}) error

	// String returns a string representation of the row.
	String() string
}

// ToStructs converts the rows into a slice of structs.
func ToStructs[T any](rows []Row) ([]T, error) {
	if len(rows) == 0 {
		return nil, nil
	}
	out := make([]T, len(rows))
	for i, r := range rows {
		if err := r.ToStruct(&out[i]); err != nil {
			return nil, err
		}
	}
	return out, nil
}
