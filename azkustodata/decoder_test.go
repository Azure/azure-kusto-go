package azkustodata

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/Azure/azure-kusto-go/azkustodata/errors"
	"github.com/Azure/azure-kusto-go/azkustodata/types"

	"github.com/google/uuid"
	"github.com/kylelemons/godebug/pretty"
)

func TestNormalDecode(t *testing.T) {
	ctx := context.Background()

	jsonStr := `[
		{
			"FrameType":"dataSetHeader",
			"IsProgressive":false,
			"Version":"v2.0"
		},
		{  
			"FrameType":"DataTable",
			"TableId":0,
			"TableKind":"QueryProperties",
			"TableName":"@ExtendedProperties",
			"Columns":[  
			   {  
				  "ColumnName":"TableId",
				  "ColumnType":"int"
			   },
			   {  
				  "ColumnName":"Key",
				  "ColumnType":"string"
			   },
			   {  
				  "ColumnName":"Value",
				  "ColumnType":"dynamic"
			   }
			],
			"Rows":[  
			   [  
				  1,
				  "Visualization",
				  "{\"Visualization\":null,\"Title\":null,\"XColumn\":null,\"Series\":null,\"YColumns\":null,\"AnomalyColumns\":null,\"XTitle\":null,\"YTitle\":null,\"XAxis\":null,\"YAxis\":null,\"Legend\":null,\"YSplit\":null,\"Accumulate\":false,\"IsQuerySorted\":false,\"Kind\":null,\"Ymin\":\"NaN\",\"Ymax\":\"NaN\"}"
			   ]
			]
		 },
		 {  
			"FrameType":"DataTable",
			"TableId":1,
			"TableKind":"QueryCompletionInformation",
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
			   ]
			]
		},
		{
			"FrameType":"DataSetCompletion",
			"HasErrors":false,
			"Cancelled":false
		}
	]
	`

	frame1Want := dataSetHeader{
		baseFrame:     baseFrame{FrameType: "dataSetHeader"},
		IsProgressive: false,
		Version:       "v2.0",
		op:            errors.OpQuery,
	}

	frame2Want := dataTable{
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
				types.Dynamic{
					Value: "{\"Visualization\":null,\"Title\":null,\"XColumn\":null,\"Series\":null,\"YColumns\":null,\"AnomalyColumns\":null,\"XTitle\":null,\"YTitle\":null,\"XAxis\":null,\"YAxis\":null,\"Legend\":null,\"YSplit\":null,\"Accumulate\":false,\"IsQuerySorted\":false,\"Kind\":null,\"Ymin\":\"NaN\",\"Ymax\":\"NaN\"}",
					Valid: true,
				},
			},
		},
		op: errors.OpQuery,
	}

	frame3Want := dataTable{
		baseFrame: baseFrame{FrameType: "DataTable"},
		TableID:   1,
		TableKind: "QueryCompletionInformation",
		TableName: "QueryCompletionInformation",
		Columns: []Column{
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
		Rows: []types.KustoValues{
			{
				types.DateTime{Value: timeMustParse(time.RFC3339Nano, "2019-08-27T04:14:55.302919Z"), Valid: true},
				types.String{Value: "KPC.execute;752dd747-5f6a-45c6-9ee2-e6662530ecc3", Valid: true},
				types.GUID{Value: uuid.MustParse("011e7e1b-3c8f-4e91-a04b-0fa5f7be6100"), Valid: true},
			},
		},
		op: errors.OpQuery,
	}

	frame4Want := dataSetCompletion{
		baseFrame: baseFrame{FrameType: "DataSetCompletion"},
		HasErrors: false,
		Cancelled: false,
		op:        errors.OpQuery,
	}

	dec := newDecoder(strings.NewReader(jsonStr), errors.OpQuery)
	ch := dec.decodeV2(ctx)

	fr := <-ch
	frame1Got, ok := fr.(dataSetHeader)
	if !ok {
		t.Fatalf("TestNormalDecode: first frame was not a dataSetHeader, was %T: %s", fr, fr)
	}

	if diff := pretty.Compare(frame1Want, frame1Got); diff != "" {
		t.Fatalf("TestNormalDecode: first frame: -want/+got:\n%s", diff)
	}

	fr = <-ch
	frame2Got, ok := fr.(dataTable)
	if !ok {
		t.Fatalf("TestNormalDecode: second frame was not a dataTable, was %T: %s", fr, fr)
	}

	if diff := pretty.Compare(frame2Want, frame2Got); diff != "" {
		t.Fatalf("TestNormalDecode: second frame: -want/+got:\n%s", diff)
	}

	fr = <-ch
	frame3Got, ok := fr.(dataTable)
	if !ok {
		t.Fatalf("TestNormalDecode: third frame was not a dataTable, was %T: %s", fr, fr)
	}

	if diff := pretty.Compare(frame3Want, frame3Got); diff != "" {
		t.Fatalf("TestNormalDecode: third frame: -want/+got:\n%s", diff)
	}

	fr = <-ch
	frame4Got, ok := fr.(dataSetCompletion)
	if !ok {
		t.Fatalf("TestNormalDecode: fourth frame was not a dataSetCompletion, was %T: %s", fr, fr)
	}
	if diff := pretty.Compare(frame4Want, frame4Got); diff != "" {
		t.Fatalf("TestNormalDecode: fourth frame: -want/+got:\n%s", diff)
	}
}

func timeMustParse(layout string, p string) time.Time {
	t, err := time.Parse(layout, p)
	if err != nil {
		panic(err)
	}
	return t
}
