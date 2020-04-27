package v1

import (
	"context"
	"encoding/json"
	"io/ioutil"
	"strings"
	"testing"
	"time"

	"github.com/Azure/azure-kusto-go/kusto/data/errors"
	"github.com/Azure/azure-kusto-go/kusto/data/value"

	"github.com/google/uuid"
	"github.com/kylelemons/godebug/pretty"
)

func TestNormalDecode(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	jsonStr := `{
		"Tables": [
			{   
				"TableName":"QueryCompletionInformation",
				"Columns":[  
					{  
						"ColumnName":"Timestamp",
						"ColumnType":"datetime"
					},
					{  
						"ColumnName":"ClientRequestId",
						"ColumnType":"string"
					},
					{  
						"ColumnName":"ActivityId",
						"ColumnType":"guid"
					}
				],
				"Rows":[  
					[  
						"2019-08-27T04:14:55.302919Z",
						"KPC.execute;752dd747-5f6a-45c6-9ee2-e6662530ecc3",
						"011e7e1b-3c8f-4e91-a04b-0fa5f7be6100"
					],
					[  
						"2020-08-27T04:14:55.302919Z",
						"KPE.execute;752dd747-5f6a-45c6-9ee2-e6662530ecc3",
						"211e7e1b-3c8f-4e91-a04b-0fa5f7be6100"
					]
				]
			},
			{   
				"TableName":"QueryCompletionInformation",
				"Columns":[  
					{  
						"ColumnName":"Timestamp",
						"ColumnType":"datetime"
					},
					{  
						"ColumnName":"ClientRequestId",
						"ColumnType":"string"
					},
					{  
						"ColumnName":"ActivityId",
						"ColumnType":"guid"
					}
				],
				"Rows":[  
					[  
						"2021-08-27T04:14:55.302919Z",
						"KPF.execute;752dd747-5f6a-45c6-9ee2-e6662530ecc3",
						"311e7e1b-3c8f-4e91-a04b-0fa5f7be6100"
					],
					[  
						"2022-08-27T04:14:55.302919Z",
						"KPG.execute;752dd747-5f6a-45c6-9ee2-e6662530ecc3",
						"411e7e1b-3c8f-4e91-a04b-0fa5f7be6100"
					]
				]
			}
		]
	}
	`

	if !json.Valid([]byte(jsonStr)) {
		panic("the json string isn't valid")
	}

	frame1Want := DataTable{
		TableName: "QueryCompletionInformation",
		DataTypes: DataTypes{
			{
				ColumnName: "Timestamp",
				ColumnType: "datetime",
			},
			{
				ColumnName: "ClientRequestId",
				ColumnType: "string",
			},
			{
				ColumnName: "ActivityId",
				ColumnType: "guid",
			},
		},
		KustoRows: []value.Values{
			{
				value.DateTime{Value: timeMustParse(time.RFC3339Nano, "2019-08-27T04:14:55.302919Z"), Valid: true},
				value.String{Value: "KPC.execute;752dd747-5f6a-45c6-9ee2-e6662530ecc3", Valid: true},
				value.GUID{Value: uuid.MustParse("011e7e1b-3c8f-4e91-a04b-0fa5f7be6100"), Valid: true},
			},
			{
				value.DateTime{Value: timeMustParse(time.RFC3339Nano, "2020-08-27T04:14:55.302919Z"), Valid: true},
				value.String{Value: "KPE.execute;752dd747-5f6a-45c6-9ee2-e6662530ecc3", Valid: true},
				value.GUID{Value: uuid.MustParse("211e7e1b-3c8f-4e91-a04b-0fa5f7be6100"), Valid: true},
			},
		},
		Op: errors.OpQuery,
	}

	frame2Want := DataTable{
		TableName: "QueryCompletionInformation",
		DataTypes: DataTypes{
			{
				ColumnName: "Timestamp",
				ColumnType: "datetime",
			},
			{
				ColumnName: "ClientRequestId",
				ColumnType: "string",
			},
			{
				ColumnName: "ActivityId",
				ColumnType: "guid",
			},
		},
		KustoRows: []value.Values{
			{
				value.DateTime{Value: timeMustParse(time.RFC3339Nano, "2021-08-27T04:14:55.302919Z"), Valid: true},
				value.String{Value: "KPF.execute;752dd747-5f6a-45c6-9ee2-e6662530ecc3", Valid: true},
				value.GUID{Value: uuid.MustParse("311e7e1b-3c8f-4e91-a04b-0fa5f7be6100"), Valid: true},
			},
			{
				value.DateTime{Value: timeMustParse(time.RFC3339Nano, "2022-08-27T04:14:55.302919Z"), Valid: true},
				value.String{Value: "KPG.execute;752dd747-5f6a-45c6-9ee2-e6662530ecc3", Valid: true},
				value.GUID{Value: uuid.MustParse("411e7e1b-3c8f-4e91-a04b-0fa5f7be6100"), Valid: true},
			},
		},
		Op: errors.OpQuery,
	}

	dec := Decoder{}
	ch := dec.Decode(ctx, ioutil.NopCloser(strings.NewReader(jsonStr)), errors.OpQuery)

	fr := <-ch
	frame1Got, ok := fr.(DataTable)
	if !ok {
		t.Fatalf("TestNormalDecode: first frame was not a DataTable, was %T: %s", fr, fr)
	}

	if diff := pretty.Compare(frame1Want, frame1Got); diff != "" {
		t.Fatalf("TestNormalDecode: first frame: -want/+got:\n%s", diff)
	}

	fr = <-ch
	frame2Got, ok := fr.(DataTable)
	if !ok {
		t.Fatalf("TestNormalDecode: second frame was not a DataTable, was %T: %s", fr, fr)
	}

	if diff := pretty.Compare(frame2Want, frame2Got); diff != "" {
		t.Fatalf("TestNormalDecode: second frame: -want/+got:\n%s", diff)
	}
}

func timeMustParse(layout string, p string) time.Time {
	t, err := time.Parse(layout, p)
	if err != nil {
		panic(err)
	}
	return t
}
