package data

import (
	"encoding/json"
	"testing"

	"github.com/Azure/azure-kusto-go/data/types"

	"github.com/kylelemons/godebug/pretty"
)

func TestDataTableUnmarshal(t *testing.T) {
	tests := []struct {
		desc string
		m    map[string]interface{}
		want dataTable
		err  bool
	}{
		{
			desc: "FrameType field does not exist",
			m: map[string]interface{}{
				"TableId":   json.Number("0"),
				"TableKind": "QueryProperties",
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
			err: true,
		},
		{
			desc: "FrameType is not a string type",
			m: map[string]interface{}{
				"FrameType": 2,
				"TableId":   json.Number("0"),
				"TableKind": "QueryProperties",
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
			err: true,
		},
		{
			desc: "FrameType is not dataTable",
			m: map[string]interface{}{
				"FrameType": "DataSetHeader",
				"TableId":   json.Number("0"),
				"TableKind": "QueryProperties",
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
			err: true,
		},
		{
			desc: "TableId does not exist",
			m: map[string]interface{}{
				"FrameType": "DataTable",
				"TableKind": "QueryProperties",
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
			err: true,
		},
		{
			desc: "TableId is not a json.Number or int",
			m: map[string]interface{}{
				"FrameType": "DataTable",
				"TableId":   "hello world",
				"TableKind": "QueryProperties",
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
			err: true,
		},
		{
			desc: "TableKind does not exist",
			m: map[string]interface{}{
				"FrameType": "DataTable",
				"TableId":   json.Number("0"),
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
			err: true,
		},
		{
			desc: "TableKind is not a string",
			m: map[string]interface{}{
				"FrameType": "DataTable",
				"TableId":   json.Number("0"),
				"TableKind": 34,
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
			err: true,
		},
		{
			desc: "TableName does not exist",
			m: map[string]interface{}{
				"FrameType": "DataTable",
				"TableId":   json.Number("0"),
				"TableKind": "QueryProperties",
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
				"FrameType": "DataTable",
				"TableId":   json.Number("0"),
				"TableKind": "QueryProperties",
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
				"FrameType": "DataTable",
				"TableId":   json.Number("0"),
				"TableKind": "QueryProperties",
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
				"FrameType": "DataTable",
				"TableId":   json.Number("0"),
				"TableKind": "QueryProperties",
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
				"FrameType": "DataTable",
				"TableId":   json.Number("0"),
				"TableKind": "QueryProperties",
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
				"FrameType": "DataTable",
				"TableId":   json.Number("0"),
				"TableKind": "QueryProperties",
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
				"FrameType": "DataTable",
				"TableId":   json.Number("0"),
				"TableKind": "QueryProperties",
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
				"FrameType": "DataTable",
				"TableId":   json.Number("0"),
				"TableKind": "QueryProperties",
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
				"FrameType": "DataTable",
				"TableId":   json.Number("0"),
				"TableKind": "QueryProperties",
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
			want: dataTable{
				baseFrame: baseFrame{FrameType: "DataTable"},
				TableID:   0,
				TableKind: "QueryProperties",
				TableName: "@ExtendedProperties",
				Columns: []Column{
					{
						ColumnName: "TableId",
						ColumnType: "int",
					},
					{
						ColumnName: "Key",
						ColumnType: "string",
					},
					{
						ColumnName: "Value",
						ColumnType: "dynamic",
					},
				},
				Rows: []types.KustoValues{
					{
						types.Int{Value: 1, Valid: true},
						types.String{Value: "Visualization", Valid: true},
						types.Dynamic{Value: "{\"Visualization\":null}", Valid: true},
					},
				},
			},
		},
	}

	for _, test := range tests {
		got := dataTable{}
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

func TestDataSetCompletionUnmarshal(t *testing.T) {
	tests := []struct {
		desc string
		m    map[string]interface{}
		want dataSetCompletion
		err  bool
	}{
		{
			desc: "FrameType field does not exist",
			m: map[string]interface{}{
				"HasErrors":    true,
				"Cancelled":    true,
				"OneApiErrors": []interface{}{"error"},
			},
			err: true,
		},
		{
			desc: "FrameType is not a string type",
			m: map[string]interface{}{
				"FrameType":    2,
				"HasErrors":    true,
				"Cancelled":    true,
				"OneApiErrors": []interface{}{"error"},
			},
			err: true,
		},
		{
			desc: "FrameType is not DataSetCompletion",
			m: map[string]interface{}{
				"FrameType":    ftDataSetHeader,
				"HasErrors":    true,
				"Cancelled":    true,
				"OneApiErrors": []interface{}{"error"},
			},
			err: true,
		},
		{
			desc: "HasErrors does not exist",
			m: map[string]interface{}{
				"FrameType":    ftDataSetCompletion,
				"Cancelled":    true,
				"OneApiErrors": []interface{}{"error"},
			},
			err: true,
		},
		{
			desc: "HasErrors is not a bool",
			m: map[string]interface{}{
				"FrameType":    ftDataSetCompletion,
				"HasErrors":    "hello",
				"Cancelled":    true,
				"OneApiErrors": []interface{}{"error"},
			},
			err: true,
		},
		{
			desc: "Cancelled does not exist",
			m: map[string]interface{}{
				"FrameType":    ftDataSetCompletion,
				"HasErrors":    true,
				"OneApiErrors": []interface{}{"error"},
			},
			err: true,
		},
		{
			desc: "Cancelled is not a bool",
			m: map[string]interface{}{
				"FrameType":    ftDataSetCompletion,
				"HasErrors":    true,
				"Cancelled":    "world",
				"OneApiErrors": []interface{}{"error"},
			},
			err: true,
		},
		{
			desc: "OneAPIErrors is not a []interface{}",
			m: map[string]interface{}{
				"FrameType":    ftDataSetCompletion,
				"HasErrors":    true,
				"Cancelled":    true,
				"OneApiErrors": interface{}("error"),
			},
			err: true,
		},
		{
			desc: "OneAPIErrors has an entry that is not a string",
			m: map[string]interface{}{
				"FrameType":    ftDataSetCompletion,
				"HasErrors":    true,
				"Cancelled":    true,
				"OneApiErrors": []interface{}{"error", 2},
			},
			err: true,
		},
		{
			desc: "Success WITHOUT OneAPIErrors existing",
			m: map[string]interface{}{
				"FrameType": ftDataSetCompletion,
				"HasErrors": true,
				"Cancelled": true,
			},
			want: dataSetCompletion{
				baseFrame: baseFrame{FrameType: ftDataSetCompletion},
				HasErrors: true,
				Cancelled: true,
			},
		},
		{
			desc: "Success WITH OneAPIErrors existing",
			m: map[string]interface{}{
				"FrameType":    ftDataSetCompletion,
				"HasErrors":    true,
				"Cancelled":    true,
				"OneApiErrors": []interface{}{"error"},
			},
			want: dataSetCompletion{
				baseFrame:    baseFrame{FrameType: ftDataSetCompletion},
				HasErrors:    true,
				Cancelled:    true,
				OneAPIErrors: []string{"error"},
			},
		},
	}

	for _, test := range tests {
		got := dataSetCompletion{}
		err := got.Unmarshal(test.m)
		switch {
		case err == nil && test.err:
			t.Errorf("TestDataSetCompletionUnmarshal(%s): err == nil, want err != nil", test.desc)
			continue
		case err != nil && !test.err:
			t.Errorf("TestDataSetCompletionUnmarshal(%s): err == %s, want err == nil", test.desc, err)
			continue
		case err != nil:
			continue
		}

		if diff := pretty.Compare(test.want, got); diff != "" {
			t.Errorf("TestDataSetCompletionUnmarshal(%s): -want/+got:\n%s", test.desc, diff)
		}
	}
}
