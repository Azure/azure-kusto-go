package v1

import (
	"context"
	"encoding/json"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/Azure/azure-kusto-go/data/errors"
	"github.com/Azure/azure-kusto-go/data/value"
	"github.com/stretchr/testify/require"

	"github.com/google/uuid"
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

	wantFrames := []interface{}{
		DataTable{
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
		},
		DataTable{
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
		},
	}

	dec := Decoder{}
	ch := dec.Decode(ctx, io.NopCloser(strings.NewReader(jsonStr)), errors.OpQuery)

	for _, want := range wantFrames {
		got := <-ch
		require.EqualValues(t, want, got)
	}
}

func TestErrorDecode(t *testing.T) {
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
					],
					{
						"OneApiErrors": [{
							"error": {
								"code": "LimitsExceeded",
								"message": "Request is invalid and cannot be executed.",
								"@type": "Kusto.Data.Exceptions.KustoServicePartialQueryFailureLimitsExceededException",
								"@message": "Query execution has exceeded the allowed limits (80DA0003): .",
								"@context": {
									"timestamp": "2018-12-10T15:10:48.8352222Z",
									"machineName": "RD0003FFBEDEB9",
									"processName": "Kusto.Azure.Svc",
									"processId": 4328,
									"threadId": 7284,
									"appDomainName": "RdRuntime",
									"clientRequestd": "KPC.execute;d3a43e37-0d7f-47a9-b6cd-a889b2aee3d3",
									"activityId": "a57ec272-8846-49e6-b458-460b841ed47d",
									"subActivityId": "a57ec272-8846-49e6-b458-460b841ed47d",
									"activityType": "PO-OWIN-CallContext",
									"parentActivityId": "a57ec272-8846-49e6-b458-460b841ed47d",
									"activityStack": "(Activity stack: CRID=KPC.execute;d3a43e37-0d7f-47a9-b6cd-a889b2aee3d3 ARID=a57ec272-8846-49e6-b458-460b841ed47d > PO-OWIN-CallContext/a57ec272-8846-49e6-b458-460b841ed47d)"
								},
								"@permanent": false
							}
						}]
					}
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

	wantFrames := []interface{}{
		DataTable{
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
			RowErrors: []errors.Error{
				*errors.ES(errors.OpQuery, errors.KLimitsExceeded, "Request is invalid and cannot be executed.;See https://docs.microsoft."+
					"com/en-us/azure/kusto/concepts/querylimits"),
			},
			Op: errors.OpQuery,
		},
		DataTable{
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
		},
	}

	dec := Decoder{}
	ch := dec.Decode(ctx, io.NopCloser(strings.NewReader(jsonStr)), errors.OpQuery)

	for _, want := range wantFrames {
		got := <-ch
		require.EqualValues(t, want, got)
	}
}

func timeMustParse(layout string, p string) time.Time {
	t, err := time.Parse(layout, p)
	if err != nil {
		panic(err)
	}
	return t
}
