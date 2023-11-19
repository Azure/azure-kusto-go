package azkustodata

import (
	"fmt"
	"io"
	"testing"

	"github.com/Azure/azure-kusto-go/azkustodata/table"
	"github.com/Azure/azure-kusto-go/azkustodata/types"
	"github.com/Azure/azure-kusto-go/azkustodata/value"

	"github.com/kylelemons/godebug/pretty"
)

func TestFromStruct(t *testing.T) {
	t.Parallel()

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
		m, err := NewMockRows(table.Columns{{Name: "Int", Type: types.Int}})
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
	t.Parallel()

	tests := []struct {
		desc       string
		columns    table.Columns
		input      []interface{}
		want       []value.Values
		err        bool
		nextRowErr bool
	}{
		{
			desc: "Row has length 0 ",
			columns: table.Columns{
				{Name: "Int", Type: types.Int},
				{Name: "String", Type: types.String},
				{Name: "Long", Type: types.Long},
			},
			input: []interface{}{
				value.Values{value.NewInt(2), value.NewNullString(), value.NewNullLong()},
				value.Values{},
			},
			err: true,
		},
		{
			desc: "Columns and Rows don't match up",
			columns: table.Columns{
				{Name: "Int", Type: types.Int},
				{Name: "String", Type: types.Real}, // CTReal won't match the value.NewNullString() in input
				{Name: "Long", Type: types.Long},
			},
			input: []interface{}{
				value.Values{value.NewInt(2), value.NewNullString(), value.NewNullLong()},
				value.Values{value.NewInt(2), value.NewNullString(), value.NewNullLong()},
			},
			err: true,
		},
		{
			desc: "Non io.EOF error",
			columns: table.Columns{
				{Name: "Int", Type: types.Int},
				{Name: "String", Type: types.String},
				{Name: "Long", Type: types.Long},
			},
			input: []interface{}{
				value.Values{value.NewInt(2), value.NewNullString(), value.NewNullLong()},
				value.Values{value.NewInt(1), value.NewNullString(), value.NewNullLong()},
				fmt.Errorf("non io.EOF error"),
			},
			nextRowErr: true,
		},
		{
			desc: "Success",
			columns: table.Columns{
				{Name: "Int", Type: types.Int},
				{Name: "String", Type: types.String},
				{Name: "Long", Type: types.Long},
			},
			input: []interface{}{
				value.Values{value.NewInt(2), value.NewNullString(), value.NewNullLong()},
				value.Values{value.NewInt(1), value.NewNullString(), value.NewNullLong()},
			},
			want: []value.Values{
				{value.NewInt(2), value.NewNullString(), value.NewNullLong()},
				{value.NewInt(1), value.NewNullString(), value.NewNullLong()},
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
			case value.Values:
				err = m.Row(v)
			case error:
				_ = m.Error(v)
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

		var got []value.Values
		var nextRowErr error
		for {
			r, err := m.nextRow()
			if err != nil {
				if err != io.EOF {
					nextRowErr = err
				}
				break
			}
			got = append(got, r.Values)
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
