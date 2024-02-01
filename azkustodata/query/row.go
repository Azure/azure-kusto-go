package query

import (
	"github.com/Azure/azure-kusto-go/azkustodata/value"
	"github.com/shopspring/decimal"
	"time"
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

	BoolByIndex(i int) (*bool, error)
	IntByIndex(i int) (*int32, error)
	LongByIndex(i int) (*int64, error)
	RealByIndex(i int) (*float64, error)
	DecimalByIndex(i int) (*decimal.Decimal, error)
	StringByIndex(i int) (string, error)
	DynamicByIndex(i int) (interface{}, error)
	DateTimeByIndex(i int) (*time.Time, error)
	TimespanByIndex(i int) (*time.Duration, error)

	BoolByName(name string) (*bool, error)
	IntByName(name string) (*int32, error)
	LongByName(name string) (*int64, error)
	RealByName(name string) (*float64, error)
	DecimalByName(name string) (*decimal.Decimal, error)
	StringByName(name string) (string, error)
	DynamicByName(name string) (interface{}, error)
	DateTimeByName(name string) (*time.Time, error)
	TimespanByName(name string) (*time.Duration, error)
}
