package kusto

import (
	"testing"
	"time"

	"github.com/Azure/azure-kusto-go/kusto/types"

	"github.com/kylelemons/godebug/pretty"
)

func TestRowColumns(t *testing.T) {
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
		row := &Row{columns: test.columns}
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
	firstName := new(string)
	*firstName = "John"

	tests := []struct {
		desc    string
		columns Columns
		row     types.KustoValues
		got     interface{}
		want    interface{}
		err     bool
	}{
		{
			desc: "Non pointer to struct",
			columns: Columns{
				{Name: "Id", Type: CTLong},
			},
			row: types.KustoValues{
				types.Long{Value: 1, Valid: true},
			},
			got: struct {
				ID int64 `kusto:"Id"`
			}{},
			err: true,
		},
		{
			desc: "Pointer, but not to struct",
			columns: Columns{
				{Name: "Id", Type: CTLong},
			},
			row: types.KustoValues{
				types.Long{Value: 1, Valid: true},
			},
			got: firstName,
			err: true,
		},
		{
			desc: "len(columns) != len(rows)",
			columns: Columns{
				{Name: "Id", Type: CTLong},
			},
			row: types.KustoValues{
				types.Long{Value: 1, Valid: true},
				types.Long{Value: 1, Valid: true},
			},
			err: true,
		},
		{
			desc: "Success",
			columns: Columns{
				{Name: "Id", Type: CTLong},
				{Name: "FirstName", Type: CTString},
				{Name: "LastName", Type: CTString},
				{Name: "NotInStruct", Type: CTDateTime},
				{Name: "NullReal", Type: CTReal},
				{Name: "NullString", Type: CTString},
			},
			row: types.KustoValues{
				types.Long{Value: 1, Valid: true},
				types.String{Value: "John", Valid: true},
				types.String{Value: "Doak", Valid: true},
				types.DateTime{Value: time.Now(), Valid: true},
				types.Real{Valid: false},
				types.String{Valid: false},
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
		row := &Row{columns: test.columns, row: test.row}
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
