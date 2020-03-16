package v1

import (
	"testing"

	"github.com/Azure/azure-kusto-go/kusto/data/table"
	"github.com/Azure/azure-kusto-go/kusto/data/value"

	"github.com/kylelemons/godebug/pretty"
)

func TestDataTableUnmarshal(t *testing.T) {
	tests := []struct {
		desc string
		m    map[string]interface{}
		want DataTable
		err  bool
	}{
		{
			desc: "TableName does not exist",
			m: map[string]interface{}{
				"Columns": []interface{}{
					map[string]interface{}{
						"ColumnName": "TableId",
						"ColumnType": "int",
					},
					map[string]interface{}{
						"ColumnName": "Key",
						"ColumnType": "string",
					},
					map[string]interface{}{
						"ColumnName": "Value",
						"ColumnType": "dynamic",
					},
				},
				"Rows": []interface{}{
					[]interface{}{
						1,
						"Visualization",
						"{\"Visualization\":null}",
					},
				},
			},
			err: true,
		},
		{
			desc: "TableName is not a string",
			m: map[string]interface{}{
				"TableName": 82,
				"Columns": []interface{}{
					map[string]interface{}{
						"ColumnName": "TableId",
						"ColumnType": "int",
					},
					map[string]interface{}{
						"ColumnName": "Key",
						"ColumnType": "string",
					},
					map[string]interface{}{
						"ColumnName": "Value",
						"ColumnType": "dynamic",
					},
				},
				"Rows": []interface{}{
					[]interface{}{
						1,
						"Visualization",
						"{\"Visualization\":null}",
					},
				},
			},
			err: true,
		},
		{
			desc: "Columns does not exist",
			m: map[string]interface{}{
				"TableName": "@ExtendedProperties",
				"Rows": []interface{}{
					[]interface{}{
						1,
						"Visualization",
						"{\"Visualization\":null}",
					},
				},
			},
			err: true,
		},
		{
			desc: "Columns is not a []interface{}",
			m: map[string]interface{}{
				"TableName": "@ExtendedProperties",
				"Columns":   interface{}("hello"),
				"Rows": []interface{}{
					[]interface{}{
						1,
						"Visualization",
						"{\"Visualization\":null}",
					},
				},
			},
			err: true,
		},
		{
			desc: "Rows does not exist",
			m: map[string]interface{}{
				"TableName": "@ExtendedProperties",
				"Columns": []interface{}{
					map[string]interface{}{
						"ColumnName": "TableId",
						"ColumnType": "int",
					},
					map[string]interface{}{
						"ColumnName": "Key",
						"ColumnType": "string",
					},
					map[string]interface{}{
						"ColumnName": "Value",
						"ColumnType": "dynamic",
					},
				},
			},
			err: true,
		},
		{
			desc: "Rows is not an []interface{}",
			m: map[string]interface{}{
				"TableName": "@ExtendedProperties",
				"Columns": []interface{}{
					map[string]interface{}{
						"ColumnName": "TableId",
						"ColumnType": "int",
					},
					map[string]interface{}{
						"ColumnName": "Key",
						"ColumnType": "string",
					},
					map[string]interface{}{
						"ColumnName": "Value",
						"ColumnType": "dynamic",
					},
				},
				"Rows": interface{}("crap"),
			},
			err: true,
		},
		{
			desc: "Column has missing ColumnName",
			m: map[string]interface{}{
				"TableName": "@ExtendedProperties",
				"Columns": []interface{}{
					map[string]interface{}{
						"ColumnName": "TableId",
						"ColumnType": "int",
					},
					map[string]interface{}{
						"ColumnType": "string",
					},
					map[string]interface{}{
						"ColumnName": "Value",
						"ColumnType": "dynamic",
					},
				},
				"Rows": []interface{}{
					[]interface{}{
						1,
						"Visualization",
						"{\"Visualization\":null}",
					},
				},
			},
			err: true,
		},
		{
			desc: "Column has missing ColumnType",
			m: map[string]interface{}{
				"TableName": "@ExtendedProperties",
				"Columns": []interface{}{
					map[string]interface{}{
						"ColumnName": "TableId",
						"ColumnType": "int",
					},
					map[string]interface{}{
						"ColumnName": "Key",
					},
					map[string]interface{}{
						"ColumnName": "Value",
						"ColumnType": "dynamic",
					},
				},
				"Rows": []interface{}{
					[]interface{}{
						1,
						"Visualization",
						"{\"Visualization\":null}",
					},
				},
			},
			err: true,
		},
		{
			desc: "Success",
			m: map[string]interface{}{
				"TableName": "@ExtendedProperties",
				"Columns": []interface{}{
					map[string]interface{}{
						"ColumnName": "TableId",
						"ColumnType": "int",
					},
					map[string]interface{}{
						"ColumnName": "Key",
						"ColumnType": "string",
					},
					map[string]interface{}{
						"ColumnName": "Value",
						"ColumnType": "dynamic",
					},
				},
				"Rows": []interface{}{
					[]interface{}{
						1,
						"Visualization",
						"{\"Visualization\":null}",
					},
				},
			},
			want: DataTable{
				TableName: "@ExtendedProperties",
				Columns: []table.Column{
					{
						Name: "TableId",
						Type: "int",
					},
					{
						Name: "Key",
						Type: "string",
					},
					{
						Name: "Value",
						Type: "dynamic",
					},
				},
				Rows: []value.Values{
					{
						value.Int{Value: 1, Valid: true},
						value.String{Value: "Visualization", Valid: true},
						value.Dynamic{Value: map[string]interface{}{"Visualization": nil}, Valid: true},
					},
				},
			},
		},
		{
			desc: "Success with DataType instead of ColumnType",
			m: map[string]interface{}{
				"TableName": "@ExtendedProperties",
				"Columns": []interface{}{
					map[string]interface{}{
						"ColumnName": "TableId",
						"DataType":   "Int32", // here is where I did it.
					},
					map[string]interface{}{
						"ColumnName": "Key",
						"ColumnType": "string",
					},
					map[string]interface{}{
						"ColumnName": "Value",
						"ColumnType": "dynamic",
					},
				},
				"Rows": []interface{}{
					[]interface{}{
						1,
						"Visualization",
						"{\"Visualization\":null}",
					},
				},
			},
			want: DataTable{
				TableName: "@ExtendedProperties",
				Columns: []table.Column{
					{
						Name: "TableId",
						Type: "int",
					},
					{
						Name: "Key",
						Type: "string",
					},
					{
						Name: "Value",
						Type: "dynamic",
					},
				},
				Rows: []value.Values{
					{
						value.Int{Value: 1, Valid: true},
						value.String{Value: "Visualization", Valid: true},
						value.Dynamic{Value: map[string]interface{}{"Visualization": nil}, Valid: true},
					},
				},
			},
		},
	}

	for _, test := range tests {
		got := DataTable{}
		err := got.Unmarshal(test.m)
		switch {
		case err == nil && test.err:
			t.Errorf("TestDataTableUnmarshal(%s): err == nil, want err != nil", test.desc)
			continue
		case err != nil && !test.err:
			t.Errorf("TestDataTableUnmarshal(%s): err == %s, want err == nil", test.desc, err)
			continue
		case err != nil:
			continue
		}

		if diff := pretty.Compare(test.want, got); diff != "" {
			t.Errorf("TestDataTableUnmarshal(%s): -want/+got:\n%s", test.desc, diff)
		}
	}
}
