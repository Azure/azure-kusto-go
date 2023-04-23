package v1

import (
	"testing"

	"github.com/Azure/azure-kusto-go/azkustodata/table"
	"github.com/Azure/azure-kusto-go/azkustodata/types"
	"github.com/stretchr/testify/require"
)

// TestDataType tests for https://github.com/Azure/azure-kusto-go/issues/116
func TestDataType(t *testing.T) {
	tests := []struct {
		desc string
		dt   DataType
		want table.Column
		err  bool
	}{
		{
			desc: "Error: ColumnType is incorrect",
			dt:   DataType{ColumnType: "incorrect"},
			err:  true,
		},
		{
			desc: "Error: ColumnType and DataType were not set",
			dt:   DataType{},
			err:  true,
		},
		{
			desc: "Error: DataType.DataType is incorrect",
			dt:   DataType{DataType: "incorrect"},
			err:  true,
		},
		{
			desc: "Success: ColumnType is correct",
			dt:   DataType{ColumnName: "someString", ColumnType: "System.String"},
			want: table.Column{Name: "someString", Type: types.String},
		},
		{
			desc: "Success: DataType is correct",
			dt:   DataType{ColumnName: "someString", DataType: "System.String"},
			want: table.Column{Name: "someString", Type: types.String},
		},
	}

	for _, test := range tests {
		got, err := test.dt.toColumn()
		switch {
		case err == nil && test.err:
			t.Errorf("TestDataType(%s): got err == nil, want err != nil", test.desc)
			continue
		case err != nil && !test.err:
			t.Errorf("TestDataType(%s): got err == %s, want err == nil", test.desc, err)
			continue
		case err != nil:
			continue
		}
		require.EqualValues(t, test.want, got)
	}
}
