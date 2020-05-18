package v2

import (
	"context"
	"encoding/json"
	"io/ioutil"
	"strings"
	"testing"
	"time"

	"github.com/Azure/azure-kusto-go/kusto/data/errors"
	"github.com/Azure/azure-kusto-go/kusto/data/table"
	"github.com/Azure/azure-kusto-go/kusto/data/value"

	"github.com/google/uuid"
	"github.com/kylelemons/godebug/pretty"
)

func TestNormalDecode(t *testing.T) {
	t.Parallel()

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

	frame1Want := DataSetHeader{
		Base:          Base{FrameType: "dataSetHeader"},
		IsProgressive: false,
		Version:       "v2.0",
		Op:            errors.OpQuery,
	}

	frame2Want := DataTable{
		Base:      Base{FrameType: "DataTable"},
		TableID:   0,
		TableKind: "QueryProperties",
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
		KustoRows: []value.Values{
			{
				value.Int{Value: 1, Valid: true},
				value.String{Value: "Visualization", Valid: true},
				value.Dynamic{
					Value: []byte("{\"Visualization\":null,\"Title\":null,\"XColumn\":null,\"Series\":null,\"YColumns\":null,\"AnomalyColumns\":null,\"XTitle\":null,\"YTitle\":null,\"XAxis\":null,\"YAxis\":null,\"Legend\":null,\"YSplit\":null,\"Accumulate\":false,\"IsQuerySorted\":false,\"Kind\":null,\"Ymin\":\"NaN\",\"Ymax\":\"NaN\"}"),
					Valid: true,
				},
			},
		},
		Op: errors.OpQuery,
	}

	frame3Want := DataTable{
		Base:      Base{FrameType: "DataTable"},
		TableID:   1,
		TableKind: "QueryCompletionInformation",
		TableName: "QueryCompletionInformation",
		Columns: []table.Column{
			{
				Name: "Timestamp",
				Type: "datetime",
			},
			{
				Name: "ClientRequestId",
				Type: "string",
			},
			{
				Name: "ActivityId",
				Type: "guid",
			},
		},
		KustoRows: []value.Values{
			{
				value.DateTime{Value: timeMustParse(time.RFC3339Nano, "2019-08-27T04:14:55.302919Z"), Valid: true},
				value.String{Value: "KPC.execute;752dd747-5f6a-45c6-9ee2-e6662530ecc3", Valid: true},
				value.GUID{Value: uuid.MustParse("011e7e1b-3c8f-4e91-a04b-0fa5f7be6100"), Valid: true},
			},
		},
		Op: errors.OpQuery,
	}

	frame4Want := DataSetCompletion{
		Base:      Base{FrameType: "DataSetCompletion"},
		HasErrors: false,
		Cancelled: false,
		Op:        errors.OpQuery,
	}

	dec := Decoder{}
	ch := dec.Decode(ctx, ioutil.NopCloser(strings.NewReader(jsonStr)), errors.OpQuery)

	fr := <-ch
	frame1Got, ok := fr.(DataSetHeader)
	if !ok {
		t.Fatalf("TestNormalDecode: first frame was not a DataSetHeader, was %T: %s", fr, fr)
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

	fr = <-ch
	frame3Got, ok := fr.(DataTable)
	if !ok {
		t.Fatalf("TestNormalDecode: third frame was not a DataTable, was %T: %s", fr, fr)
	}

	if diff := pretty.Compare(frame3Want, frame3Got); diff != "" {
		t.Fatalf("TestNormalDecode: third frame: -want/+got:\n%s", diff)
	}

	fr = <-ch
	frame4Got, ok := fr.(DataSetCompletion)
	if !ok {
		t.Fatalf("TestNormalDecode: fourth frame was not a DataSetCompletion, was %T: %s", fr, fr)
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

func mustMapInter(i interface{}) map[string]interface{} {
	if v, ok := i.(map[string]interface{}); ok {
		return v
	}

	var b []byte
	var err error
	switch v := i.(type) {
	case string:
		b = []byte(v)
	case []byte:
		b = v
	default:
		b, err = json.Marshal(i)
		if err != nil {
			panic(err)
		}
	}

	m := map[string]interface{}{}
	if err := json.Unmarshal(b, &m); err != nil {
		panic(err)
	}
	return m
}
