package kusto

import (
	"fmt"
	"io"
	"testing"

	"github.com/Azure/azure-kusto-go/kusto/types"

	"github.com/kylelemons/godebug/pretty"
)

func TestFromStruct(t *testing.T) {
	// Note: we have covered everything else used here in other tests, so this is just input validation.
	tests := []struct {
		desc  string
		input interface{}
		err   bool
	}{
		{
			desc:  "Input non-struct",
			input: 1,
			err:   true,
		},
		{
			desc:  "Input struct",
			input: struct{ Int int32 }{1},
			err:   true,
		},
		{
			desc:  "Input non-struct",
			input: new(int),
			err:   true,
		},
		{
			desc:  "Input *struct",
			input: &struct{ Int int32 }{1},
		},
	}

	for _, test := range tests {
		m, err := NewMockRows(Columns{{Name: "Int", Type: CTInt}})
		if err != nil {
			panic(err)
		}

		err = m.Struct(test.input)
		switch {
		case err == nil && test.err:
			t.Errorf("TestFromStruct(%s): got err == nil, want err != nil", test.desc)
			continue
		case err != nil && !test.err:
			t.Errorf("TestFromStruct(%s): got err == %s, want err == nil", test.desc, err)
			continue
		case err != nil:
			continue
		}
	}
}

func TestRow(t *testing.T) {
	tests := []struct {
		desc       string
		columns    Columns
		input      []interface{}
		want       []types.KustoValues
		err        bool
		nextRowErr bool
	}{
		{
			desc: "Row has length 0 ",
			columns: Columns{
				{Name: "Int", Type: CTInt},
				{Name: "String", Type: CTString},
				{Name: "Long", Type: CTLong},
			},
			input: []interface{}{
				types.KustoValues{types.Int{Value: 2, Valid: true}, types.String{}, types.Long{}},
				types.KustoValues{},
			},
			err: true,
		},
		{
			desc: "Columns and Rows don't match up",
			columns: Columns{
				{Name: "Int", Type: CTInt},
				{Name: "String", Type: CTReal}, // CTReal won't match the types.String{} in input
				{Name: "Long", Type: CTLong},
			},
			input: []interface{}{
				types.KustoValues{types.Int{Value: 2, Valid: true}, types.String{}, types.Long{}},
				types.KustoValues{types.Int{Value: 2, Valid: true}, types.String{}, types.Long{}},
			},
			err: true,
		},
		{
			desc: "Non io.EOF error",
			columns: Columns{
				{Name: "Int", Type: CTInt},
				{Name: "String", Type: CTString},
				{Name: "Long", Type: CTLong},
			},
			input: []interface{}{
				types.KustoValues{types.Int{Value: 2, Valid: true}, types.String{}, types.Long{}},
				types.KustoValues{types.Int{Value: 1, Valid: true}, types.String{}, types.Long{}},
				fmt.Errorf("non io.EOF error"),
			},
			nextRowErr: true,
		},
		{
			desc: "Success",
			columns: Columns{
				{Name: "Int", Type: CTInt},
				{Name: "String", Type: CTString},
				{Name: "Long", Type: CTLong},
			},
			input: []interface{}{
				types.KustoValues{types.Int{Value: 2, Valid: true}, types.String{}, types.Long{}},
				types.KustoValues{types.Int{Value: 1, Valid: true}, types.String{}, types.Long{}},
			},
			want: []types.KustoValues{
				types.KustoValues{types.Int{Value: 2, Valid: true}, types.String{}, types.Long{}},
				types.KustoValues{types.Int{Value: 1, Valid: true}, types.String{}, types.Long{}},
			},
		},
	}

	for _, test := range tests {
		m, err := NewMockRows(test.columns)
		if err != nil {
			panic(err)
		}

		for _, in := range test.input {
			switch v := in.(type) {
			case types.KustoValues:
				err = m.Row(v)
			case error:
				m.Error(v)
			default:
				panic("unsupported type")
			}
		}
		switch {
		case err == nil && test.err:
			t.Errorf("TestRow(%s): got err == nil, want err != nil", test.desc)
			continue
		case err != nil && !test.err:
			t.Errorf("TestRow(%s): got err == %s, want err == nil", test.desc, err)
			continue
		case err != nil:
			continue
		}

		var got []types.KustoValues
		var nextRowErr error
		for {
			r, err := m.nextRow()
			if err != nil {
				if err != io.EOF {
					nextRowErr = err
				}
				break
			}
			got = append(got, r.Values())
		}

		switch {
		case nextRowErr == nil && test.nextRowErr:
			t.Errorf("TestRow(%s): nextRow() got err == nil, want err != nil", test.desc)
			continue
		case nextRowErr != nil && !test.nextRowErr:
			t.Errorf("TestRow(%s): nextRow() got err == %s, want err == nil", test.desc, err)
			continue
		case nextRowErr != nil:
			continue
		}

		if diff := pretty.Compare(test.want, got); diff != "" {
			t.Errorf("TestRow(%s): -want/+got:\n%s", test.desc, diff)
		}
	}
}
