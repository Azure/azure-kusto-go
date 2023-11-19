package v2

import (
	"context"
	"encoding/json"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/Azure/azure-kusto-go/azkustodata/errors"
	"github.com/Azure/azure-kusto-go/azkustodata/table"
	"github.com/Azure/azure-kusto-go/azkustodata/value"
	"github.com/stretchr/testify/require"

	"github.com/google/uuid"
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
    "TableKind":"PrimaryResult",
    "TableName":"PrimaryResult",
    "Columns":[
      {
        "ColumnName":"x",
        "ColumnType":"long"
      }
    ],
    "Rows":[
      [
        1
      ],
      [
        2
      ],
      [
        3
      ],
      [
        4
      ],
      [
        5
      ]
    ]
  },
  {
    "FrameType":"DataTable",
    "TableId":2,
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

	wantFrames := []interface{}{
		DataSetHeader{
			Base:          Base{FrameType: "dataSetHeader"},
			IsProgressive: false,
			Version:       "v2.0",
			Op:            errors.OpQuery,
		},
		DataTable{
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
					value.NewInt(1),
					value.NewString("Visualization"),
					value.NewDynamic(
						[]byte("{\"Visualization\":null,\"Title\":null,\"XColumn\":null,\"Series\":null,\"YColumns\":null,\"AnomalyColumns\":null,\"XTitle\":null,\"YTitle\":null,\"XAxis\":null,\"YAxis\":null,\"Legend\":null,\"YSplit\":null,\"Accumulate\":false,\"IsQuerySorted\":false,\"Kind\":null,\"Ymin\":\"NaN\",\"Ymax\":\"NaN\"}"),
					),
				},
			},
			RowErrors: nil,
			Op:        errors.OpQuery,
		},
		DataTable{
			Base:      Base{FrameType: "DataTable"},
			TableID:   1,
			TableKind: "PrimaryResult",
			TableName: "PrimaryResult",
			Columns: []table.Column{
				{
					Name: "x",
					Type: "long",
				},
			},
			KustoRows: []value.Values{
				{value.NewLong(1)},
				{value.NewLong(2)},
				{value.NewLong(3)},
				{value.NewLong(4)},
				{value.NewLong(5)},
			},
			Op: errors.OpQuery,
		},
		DataTable{
			Base:      Base{FrameType: "DataTable"},
			TableID:   2,
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
					value.NewDateTime(timeMustParse(time.RFC3339Nano, "2019-08-27T04:14:55.302919Z")),
					value.NewString("KPC.execute;752dd747-5f6a-45c6-9ee2-e6662530ecc3"),
					value.NewGUID(uuid.MustParse("011e7e1b-3c8f-4e91-a04b-0fa5f7be6100")),
				},
			},
			RowErrors: nil,
			Op:        errors.OpQuery,
		},
		DataSetCompletion{
			Base:      Base{FrameType: "DataSetCompletion"},
			HasErrors: false,
			Cancelled: false,
			Op:        errors.OpQuery,
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
    "TableKind":"PrimaryResult",
    "TableName":"PrimaryResult",
    "Columns":[
      {
        "ColumnName":"x",
        "ColumnType":"long"
      }
    ],
    "Rows":[
      [
        1
      ],
      [
        2
      ],
      [
        3
      ],
      [
        4
      ],
      [
        5
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
    "FrameType":"DataTable",
    "TableId":2,
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
    "HasErrors":true,
    "Cancelled":false,
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
					"clientRequestId": "KPC.execute;d3a43e37-0d7f-47a9-b6cd-a889b2aee3d3",
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
	`

	wantFrames := []interface{}{
		DataSetHeader{
			Base:          Base{FrameType: "dataSetHeader"},
			IsProgressive: false,
			Version:       "v2.0",
			Op:            errors.OpQuery,
		},
		DataTable{
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
					value.NewInt(1),
					value.NewString("Visualization"),
					value.NewDynamic(
						[]byte("{\"Visualization\":null,\"Title\":null,\"XColumn\":null,\"Series\":null,\"YColumns\":null,\"AnomalyColumns\":null,\"XTitle\":null,\"YTitle\":null,\"XAxis\":null,\"YAxis\":null,\"Legend\":null,\"YSplit\":null,\"Accumulate\":false,\"IsQuerySorted\":false,\"Kind\":null,\"Ymin\":\"NaN\",\"Ymax\":\"NaN\"}"),
					),
				},
			},
			Op: errors.OpQuery,
		},
		DataTable{
			Base:      Base{FrameType: "DataTable"},
			TableID:   1,
			TableKind: "PrimaryResult",
			TableName: "PrimaryResult",
			Columns: []table.Column{
				{
					Name: "x",
					Type: "long",
				},
			},
			KustoRows: []value.Values{
				{value.NewLong(1)},
				{value.NewLong(2)},
				{value.NewLong(3)},
				{value.NewLong(4)},
				{value.NewLong(5)},
			},
			RowErrors: []errors.Error{
				*errors.ES(errors.OpUnknown, errors.KLimitsExceeded, "Request is invalid and cannot be executed.;See https://docs.microsoft."+
					"com/en-us/azure/kusto/concepts/querylimits"),
			},
			Op: errors.OpQuery,
		},
		DataTable{
			Base:      Base{FrameType: "DataTable"},
			TableID:   2,
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
					value.NewDateTime(timeMustParse(time.RFC3339Nano, "2019-08-27T04:14:55.302919Z")),
					value.NewString("KPC.execute;752dd747-5f6a-45c6-9ee2-e6662530ecc3"),
					value.NewGUID(uuid.MustParse("011e7e1b-3c8f-4e91-a04b-0fa5f7be6100")),
				},
			},
			Op: errors.OpQuery,
		},
		DataSetCompletion{
			Base:      Base{FrameType: "DataSetCompletion"},
			HasErrors: true,
			Cancelled: false,
			OneAPIErrors: []interface{}{
				map[string]interface{}{
					"error": map[string]interface{}{
						"@context": map[string]interface{}{
							"activityId":       "a57ec272-8846-49e6-b458-460b841ed47d",
							"activityStack":    "(Activity stack: CRID=KPC.execute;d3a43e37-0d7f-47a9-b6cd-a889b2aee3d3 ARID=a57ec272-8846-49e6-b458-460b841ed47d > PO-OWIN-CallContext/a57ec272-8846-49e6-b458-460b841ed47d)",
							"activityType":     "PO-OWIN-CallContext",
							"appDomainName":    "RdRuntime",
							"clientRequestId":  "KPC.execute;d3a43e37-0d7f-47a9-b6cd-a889b2aee3d3",
							"machineName":      "RD0003FFBEDEB9",
							"parentActivityId": "a57ec272-8846-49e6-b458-460b841ed47d",
							"processId":        json.Number("4328"),
							"processName":      "Kusto.Azure.Svc",
							"subActivityId":    "a57ec272-8846-49e6-b458-460b841ed47d",
							"threadId":         json.Number("7284"),
							"timestamp":        "2018-12-10T15:10:48.8352222Z",
						},
						"@message":   "Query execution has exceeded the allowed limits (80DA0003): .",
						"@permanent": false,
						"@type":      "Kusto.Data.Exceptions.KustoServicePartialQueryFailureLimitsExceededException",
						"code":       "LimitsExceeded",
						"message":    "Request is invalid and cannot be executed.",
					},
				},
			},
			Error: *errors.ES(errors.OpUnknown, errors.KLimitsExceeded, "Request is invalid and cannot be executed.;See https://docs.microsoft."+
				"com/en-us/azure/kusto/concepts/querylimits"),
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
