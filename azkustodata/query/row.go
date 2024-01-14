package query

import (
	"github.com/Azure/azure-kusto-go/azkustodata/errors"
	"github.com/Azure/azure-kusto-go/azkustodata/types"
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

	BoolByOrdinal(i int) (*bool, error)
	IntByOrdinal(i int) (*int32, error)
	LongByOrdinal(i int) (*int64, error)
	RealByOrdinal(i int) (*float64, error)
	DecimalByOrdinal(i int) (*decimal.Decimal, error)
	StringByOrdinal(i int) (string, error)
	DynamicByOrdinal(i int) (interface{}, error)
	DateTimeByOrdinal(i int) (*time.Time, error)
	TimespanByOrdinal(i int) (*time.Duration, error)

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

func conversionError(r *row, from string, to string) error {
	return errors.ES(r.table.Op(), errors.KOther, "cannot convert %s to %s", from, to)
}

func columnNotFoundError(r *row, name string) error {
	return errors.ES(r.table.Op(), errors.KOther, "column %s not found", name)
}

func (r *row) BoolByOrdinal(i int) (*bool, error) {
	val := r.Value(i)
	if val.GetType() != types.Bool {
		return nil, conversionError(r, string(val.GetType()), string(types.Bool))
	}

	return val.GetValue().(*bool), nil
}

func (r *row) IntByOrdinal(i int) (*int32, error) {
	val := r.Value(i)
	if val.GetType() != types.Int {
		return nil, conversionError(r, string(val.GetType()), string(types.Int))
	}

	return val.GetValue().(*int32), nil
}

func (r *row) LongByOrdinal(i int) (*int64, error) {
	val := r.Value(i)
	if val.GetType() != types.Long {
		return nil, conversionError(r, string(val.GetType()), string(types.Long))
	}

	return val.GetValue().(*int64), nil
}

func (r *row) RealByOrdinal(i int) (*float64, error) {
	val := r.Value(i)
	if val.GetType() != types.Real {
		return nil, conversionError(r, string(val.GetType()), string(types.Real))
	}

	return val.GetValue().(*float64), nil
}

func (r *row) DecimalByOrdinal(i int) (*decimal.Decimal, error) {
	val := r.Value(i)
	if val.GetType() != types.Decimal {
		return nil, conversionError(r, string(val.GetType()), string(types.Decimal))
	}

	return val.GetValue().(*decimal.Decimal), nil
}

func (r *row) StringByOrdinal(i int) (string, error) {
	val := r.Value(i)
	if val.GetType() != types.String {
		return "", conversionError(r, string(val.GetType()), string(types.String))
	}

	return val.GetValue().(string), nil
}

func (r *row) DynamicByOrdinal(i int) (interface{}, error) {
	val := r.Value(i)
	if val.GetType() != types.Dynamic {
		return nil, conversionError(r, string(val.GetType()), string(types.Dynamic))
	}

	return val.GetValue(), nil
}

func (r *row) DateTimeByOrdinal(i int) (*time.Time, error) {
	val := r.Value(i)
	if val.GetType() != types.DateTime {
		return nil, conversionError(r, string(val.GetType()), string(types.DateTime))
	}

	return val.GetValue().(*time.Time), nil
}

func (r *row) TimespanByOrdinal(i int) (*time.Duration, error) {
	val := r.Value(i)
	if val.GetType() != types.Timespan {
		return nil, conversionError(r, string(val.GetType()), string(types.Timespan))
	}

	return val.GetValue().(*time.Duration), nil
}

func (r *row) BoolByName(name string) (*bool, error) {
	col := r.table.ColumnByName(name)
	if col == nil {
		return nil, columnNotFoundError(r, name)
	}
	return r.BoolByOrdinal(col.Ordinal())
}

func (r *row) IntByName(name string) (*int32, error) {
	col := r.table.ColumnByName(name)
	if col == nil {
		return nil, columnNotFoundError(r, name)
	}
	return r.IntByOrdinal(col.Ordinal())
}

func (r *row) LongByName(name string) (*int64, error) {
	col := r.table.ColumnByName(name)
	if col == nil {
		return nil, columnNotFoundError(r, name)
	}
	return r.LongByOrdinal(col.Ordinal())
}

func (r *row) RealByName(name string) (*float64, error) {
	col := r.table.ColumnByName(name)
	if col == nil {
		return nil, columnNotFoundError(r, name)
	}
	return r.RealByOrdinal(col.Ordinal())
}

func (r *row) DecimalByName(name string) (*decimal.Decimal, error) {
	col := r.table.ColumnByName(name)
	if col == nil {
		return nil, columnNotFoundError(r, name)
	}
	return r.DecimalByOrdinal(col.Ordinal())
}

func (r *row) StringByName(name string) (string, error) {
	col := r.table.ColumnByName(name)
	if col == nil {
		return "", columnNotFoundError(r, name)
	}
	return r.StringByOrdinal(col.Ordinal())
}

func (r *row) DynamicByName(name string) (interface{}, error) {
	col := r.table.ColumnByName(name)
	if col == nil {
		return nil, columnNotFoundError(r, name)
	}
	return r.DynamicByOrdinal(col.Ordinal())
}

func (r *row) DateTimeByName(name string) (*time.Time, error) {
	col := r.table.ColumnByName(name)
	if col == nil {
		return nil, columnNotFoundError(r, name)
	}
	return r.DateTimeByOrdinal(col.Ordinal())
}

func (r *row) TimespanByName(name string) (*time.Duration, error) {
	col := r.table.ColumnByName(name)
	if col == nil {
		return nil, columnNotFoundError(r, name)
	}
	return r.TimespanByOrdinal(col.Ordinal())
}

// ToStructs converts a table or a slice of rows into a slice of structs.
func ToStructs[T any](data interface{}) ([]T, error) {
	var rows []Row
	var errs error

	if t, ok := data.(Table); ok {
		rows, errs = t.GetAllRows()
	} else if r, ok := data.([]Row); ok {
		rows = r
	} else if r, ok := data.(Row); ok {
		rows = []Row{r}
	} else {
		return nil, errors.ES(errors.OpUnknown, errors.KInternal, "invalid data type - expected Table or []Row")
	}

	if rows == nil || len(rows) == 0 {
		return nil, errs
	}
	out := make([]T, len(rows))
	for i, r := range rows {
		if err := r.ToStruct(&out[i]); err != nil {
			out = out[:i]
			if len(out) == 0 {
				out = nil
			}
			return out, err
		}
	}

	return out, errs
}

type StructResult[T any] struct {
	Out T
	Err error
}

func ToStructsIterative[T any](tb IterativeTable) chan StructResult[T] {
	out := make(chan StructResult[T])

	go func() {
		defer close(out)
		for rowResult := range tb.Rows() {
			if rowResult.Err() != nil {
				out <- StructResult[T]{Err: rowResult.Err()}
			} else {
				var s T
				if err := rowResult.Row().ToStruct(&s); err != nil {
					out <- StructResult[T]{Err: err}
				} else {
					out <- StructResult[T]{Out: s}
				}
			}
		}
	}()

	return out
}
