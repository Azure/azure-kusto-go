package table

import (
	"testing"
	"time"

	"github.com/Azure/azure-kusto-go/kusto/data/types"
	"github.com/Azure/azure-kusto-go/kusto/data/value"

	"github.com/kylelemons/godebug/pretty"
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
		switch {
		case err == nil && test.err:
			t.Errorf("TestRowColumns(%s): got err == nil, want err != nil", test.desc)
			continue
		case err != nil && !test.err:
			t.Errorf("TestRowColumns(%s): got err == %s, want err == nil", test.desc, err)
			continue
		case err != nil:
			continue
		}

		if *strPtr != "hello" {
			t.Errorf("TestRowColumns(%s): *strPtr: got %s, want 'hello'", test.desc, *strPtr)
		}
		if colPtr.Name != "world" {
			t.Errorf("TestRowColumns(%s): colPtr.ColumnName: got %s, want 'world'", test.desc, colPtr.Name)
		}
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
		switch {
		case err == nil && test.err:
			t.Errorf("TestRowToStruct(%s): got err == nil, want err != nil", test.desc)
			continue
		case err != nil && !test.err:
			t.Errorf("TestRowToStruct(%s): got err == %s, want err == nil", test.desc, err)
			continue
		case err != nil:
			continue
		}

		if diff := pretty.Compare(test.want, test.got); diff != "" {
			t.Errorf("TestRowToStruct(%s): -want/+got:\n%s", test.desc, diff)
		}
	}
}
