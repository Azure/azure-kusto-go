package table

import (
	"testing"
	"time"

	"github.com/Azure/azure-kusto-go/kusto/data/types"
	"github.com/Azure/azure-kusto-go/kusto/data/value"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
)

func TestRowColumns(t *testing.T) {
	t.Parallel()

	str := ""
	strPtr := new(string)
	colPtr := new(Column)

	tests := []struct {
		desc    string
		columns Columns
		ptrs    []interface{}
		err     bool
	}{
		{
			desc:    "len(ptrs) != len(columns)",
			columns: Columns{Column{}, Column{}},
			ptrs:    []interface{}{strPtr},
			err:     true,
		},
		{
			desc:    "non-*string/*Column arg",
			columns: Columns{Column{}, Column{}},
			ptrs:    []interface{}{str, strPtr},
			err:     true,
		},
		{
			desc:    "Success",
			columns: Columns{Column{Name: "hello"}, Column{Name: "world"}},
			ptrs:    []interface{}{strPtr, colPtr},
		},
	}

	for _, test := range tests {
		row := &Row{ColumnTypes: test.columns}
		err := row.Columns(test.ptrs...)
		if test.err {
			assert.Error(t, err)
			continue
		} else {
			assert.NoError(t, err)
		}

		assert.Equal(t, "hello", *strPtr)
		assert.Equal(t, "world", colPtr.Name)

	}
}

func TestRowToStruct(t *testing.T) {
	t.Parallel()

	firstName := new(string)
	*firstName = "John"

	tests := []struct {
		desc    string
		columns Columns
		row     value.Values
		got     interface{}
		want    interface{}
		err     bool
	}{
		{
			desc: "Non pointer to struct",
			columns: Columns{
				{Name: "Id", Type: types.Long},
			},
			row: value.Values{
				value.Long{Value: 1, Valid: true},
			},
			got: struct {
				ID int64 `kusto:"Id"`
			}{},
			err: true,
		},
		{
			desc: "Pointer, but not to struct",
			columns: Columns{
				{Name: "Id", Type: types.Long},
			},
			row: value.Values{
				value.Long{Value: 1, Valid: true},
			},
			got: firstName,
			err: true,
		},
		{
			desc: "len(columns) != len(rows)",
			columns: Columns{
				{Name: "Id", Type: types.Long},
			},
			row: value.Values{
				value.Long{Value: 1, Valid: true},
				value.Long{Value: 1, Valid: true},
			},
			err: true,
		},
		{
			desc: "Success",
			columns: Columns{
				{Name: "Id", Type: types.Long},
				{Name: "FirstName", Type: types.String},
				{Name: "LastName", Type: types.String},
				{Name: "NotInStruct", Type: types.DateTime},
				{Name: "NullReal", Type: types.Real},
				{Name: "NullString", Type: types.String},
			},
			row: value.Values{
				value.Long{Value: 1, Valid: true},
				value.String{Value: "John", Valid: true},
				value.String{Value: "Doak", Valid: true},
				value.DateTime{Value: time.Now(), Valid: true},
				value.Real{Valid: false},
				value.String{Valid: false},
			},
			got: &struct {
				ID         int64 `kusto:"Id"`
				FirstName  *string
				LastName   string
				NullReal   float64
				NullString *string
			}{},
			want: &struct {
				ID         int64 `kusto:"Id"`
				FirstName  *string
				LastName   string
				NullReal   float64
				NullString *string
			}{ID: 1, FirstName: firstName, LastName: "Doak", NullReal: 0.0, NullString: nil},
		},
	}

	for _, test := range tests {
		row := &Row{ColumnTypes: test.columns, Values: test.row}
		err := row.ToStruct(test.got)

		if test.err {
			assert.Error(t, err)
			continue
		} else {
			assert.NoError(t, err)
		}

		assert.Equal(t, test.want, test.got)
	}
}

func TestExtractValuePartial(t *testing.T) {
	t.Parallel()
	columns := Columns{
		{Name: "Id", Type: types.Long},
		{Name: "FirstName", Type: types.String},
		{Name: "LastName", Type: types.String},
		{Name: "NotInStruct", Type: types.DateTime},
		{Name: "NullReal", Type: types.Real},
		{Name: "NullString", Type: types.String},
	}
	row := &Row{
		ColumnTypes: columns,
		Values: value.Values{
			value.Long{Value: 1, Valid: true},
			value.String{Value: "John", Valid: true},
			value.String{Value: "Doak", Valid: true},
			value.DateTime{Value: time.Now(), Valid: true},
			value.Real{Valid: false},
			value.String{Valid: false},
		},
	}
	var id int64
	var firstName string
	var lastName string
	var nullReal float64
	var nullString string
	assert.NoError(t, row.ExtractValues(&id, &firstName, &lastName, nil, &nullReal, &nullString))

	assert.Equal(t, firstName, "John")
	assert.Equal(t, lastName, "Doak")
	assert.Equal(t, nullReal, 0.0)
	assert.Equal(t, nullString, "")
}

func TestExtractValueAll(t *testing.T) {
	t.Parallel()
	columns := Columns{
		{Name: "Bool", Type: types.Bool},
		{Name: "DateTime", Type: types.DateTime},
		{Name: "Dynamic", Type: types.Dynamic},
		{Name: "GUID", Type: types.GUID},
		{Name: "Int", Type: types.Int},
		{Name: "Long", Type: types.Long},
		{Name: "Real", Type: types.Real},
		{Name: "String", Type: types.String},
		{Name: "Timespan", Type: types.Timespan},
		{Name: "Decimal", Type: types.Decimal},
	}
	row := &Row{
		ColumnTypes: columns,
		Values: value.Values{
			value.Bool{Value: true, Valid: true},
			value.DateTime{Value: time.Time{}, Valid: true},
			value.Dynamic{Value: make([]byte, 0), Valid: true},
			value.GUID{Value: uuid.UUID{}, Valid: true},
			value.Int{Value: 1, Valid: true},
			value.Long{Value: 2, Valid: true},
			value.Real{Value: 3.4, Valid: true},
			value.String{Value: "test", Valid: true},
			value.Timespan{Value: 10, Valid: true},
			value.Decimal{Value: "5.6", Valid: true},
		},
	}
	var boolVar bool
	var datetimeVar time.Time
	var dynamicVar []byte
	var guidVar uuid.UUID
	var intVar int32
	var longVar int64
	var realVar float64
	var stringVar string
	var timespanVar time.Duration
	var decimalVar string
	assert.NoError(t, row.ExtractValues(&boolVar, &datetimeVar, &dynamicVar, &guidVar, &intVar, &longVar, &realVar, &stringVar, &timespanVar, &decimalVar))

	assert.Equal(t, true, boolVar)
	assert.Equal(t, time.Time{}, datetimeVar)
	assert.Equal(t, []byte{}, dynamicVar)
	assert.Equal(t, uuid.UUID{}, guidVar)
	assert.Equal(t, int32(1), intVar)
	assert.Equal(t, int64(2), longVar)
	assert.Equal(t, 3.4, realVar)
	assert.Equal(t, "test", stringVar)
	assert.Equal(t, time.Duration(10), timespanVar)
	assert.Equal(t, "5.6", decimalVar)
}
