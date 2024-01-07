package v2

import (
	_ "embed"
	"encoding/json"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

//go:embed testData/validFrames.json
var validFrames string

//go:embed testData/partialError.json
var partialErrors string

//go:embed testData/twoTables.json
var twoTables string

//go:embed testData/error.txt
var errorText string

//go:embed testData/partialErrorFullDataset.json
var partialErrorFullDataset string

func readAndDecodeFrames(src string, ch chan Frame) error {
	br, err := prepareReadBuffer(strings.NewReader(src))
	if err != nil {
		return err
	}
	err = readFramesIterative(br, ch)
	if err != nil {
		return err
	}

	return nil
}

func TestReadFramesWithValidInput(t *testing.T) {
	t.Parallel()
	ch := make(chan Frame)

	// err channel
	errChan := make(chan error)

	go func() {
		err := readAndDecodeFrames(validFrames, ch)
		errChan <- err
	}()

	dataSetHeader := <-ch
	assert.Equal(t, &DataSetHeader{
		IsProgressive:           false,
		Version:                 "v2.0",
		IsFragmented:            true,
		ErrorReportingPlacement: "EndOfTable",
	}, dataSetHeader)

	dataTable := (<-ch).(*DataTable)
	assert.Equal(t, dataTable.TableId, 0)
	assert.Equal(t, dataTable.TableKind, "QueryProperties")
	assert.Equal(t, dataTable.TableName, "@ExtendedProperties")
	assert.Equal(t, dataTable.Columns, []FrameColumn{
		{"TableId", "int"},
		{"Key", "string"},
		{"Value", "dynamic"},
	})
	assert.Equal(t, dataTable.Rows, RawRows{
		NewRawRow(json.Number("1"),
			"Visualization",
			`{"Visualization":null,"Title":null,"XColumn":null,"Series":null,"YColumns":null,"AnomalyColumns":null,"XTitle":null,"YTitle":null,"XAxis":null,"YAxis":null,"Legend":null,"YSplit":null,"Accumulate":false,"IsQuerySorted":false,"Kind":null,"Ymin":"NaN","Ymax":"NaN","Xmin":null,"Xmax":null}`),
	})

	tableHeader := (<-ch).(*TableHeader)
	assert.Equal(t, tableHeader.TableId, 1)
	assert.Equal(t, tableHeader.TableKind, "PrimaryResult")
	assert.Equal(t, tableHeader.TableName, "AllDataTypes")
	assert.Equal(t, tableHeader.Columns, []FrameColumn{
		{"vnum", "int"},
		{"vdec", "decimal"},
		{"vdate", "datetime"},
		{"vspan", "timespan"},
		{"vobj", "dynamic"},
		{"vb", "bool"},
		{"vreal", "real"},
		{"vstr", "string"},
		{"vlong", "long"},
		{"vguid", "guid"},
	})

	tableFragment := (<-ch).(*TableFragment)
	assert.Equal(t, tableFragment.TableFragmentType, "DataAppend")
	assert.Equal(t, tableFragment.TableId, 1)
	assert.Equal(t, tableFragment.Rows, RawRows{
		NewRawRow(json.Number("1"),
			"2.00000000000001",
			"2020-03-04T14:05:01.3109965Z",
			"01:23:45.6789000",
			map[string]interface{}{"moshe": "value"},
			true,
			json.Number("0.01"),
			"asdf",
			json.Number("9223372036854775807"),
			"123e27de-1e4e-49d9-b579-fe0b331d3642"),
	})

	tableCompletion := (<-ch).(*TableCompletion)
	assert.Equal(t, tableCompletion.TableId, 1)
	assert.Equal(t, tableCompletion.RowCount, 1)
	assert.Equal(t, tableCompletion.OneApiErrors, []OneApiError(nil))

	dataTable = (<-ch).(*DataTable)
	assert.Equal(t, dataTable.TableId, 2)
	assert.Equal(t, dataTable.TableKind, "QueryCompletionInformation")
	assert.Equal(t, dataTable.TableName, "QueryCompletionInformation")

	dataSetCompletion := (<-ch).(*DataSetCompletion)
	assert.Equal(t, dataSetCompletion.HasErrors, false)
	assert.Equal(t, dataSetCompletion.Cancelled, false)
	assert.Equal(t, dataSetCompletion.OneApiErrors, []OneApiError(nil))

	assert.Nil(t, <-ch)

	err := <-errChan
	require.NoError(t, err)
}

func TestReadFramesWithErrors(t *testing.T) {
	t.Parallel()
	ch := make(chan Frame)

	// err channel
	errChan := make(chan error)

	go func() {
		err := readAndDecodeFrames(partialErrors, ch)
		errChan <- err
		require.NoError(t, err)
	}()

	dataSetHeader := <-ch
	assert.Equal(t, &DataSetHeader{
		IsProgressive:           false,
		Version:                 "v2.0",
		IsFragmented:            true,
		ErrorReportingPlacement: "EndOfTable",
	}, dataSetHeader)

	dataTable := (<-ch).(*DataTable)
	assert.Equal(t, dataTable.TableId, 0)
	assert.Equal(t, dataTable.TableKind, "QueryProperties")
	assert.Equal(t, dataTable.TableName, "@ExtendedProperties")
	assert.Equal(t, dataTable.Columns, []FrameColumn{
		{"TableId", "int"},
		{"Key", "string"},
		{"Value", "dynamic"},
	})
	assert.Equal(t, dataTable.Rows, RawRows{
		NewRawRow(json.Number("1"),
			"Visualization",
			`{"Visualization":null,"Title":null,"XColumn":null,"Series":null,"YColumns":null,"AnomalyColumns":null,"XTitle":null,"YTitle":null,"XAxis":null,"YAxis":null,"Legend":null,"YSplit":null,"Accumulate":false,"IsQuerySorted":false,"Kind":null,"Ymin":"NaN","Ymax":"NaN","Xmin":null,"Xmax":null}`),
	})

	tableHeader := (<-ch).(*TableHeader)
	assert.Equal(t, tableHeader.TableId, 1)
	assert.Equal(t, tableHeader.TableKind, "PrimaryResult")
	assert.Equal(t, tableHeader.TableName, "PrimaryResult")
	assert.Equal(t, tableHeader.Columns, []FrameColumn{
		{"A", "int"},
	})

	tableFragment := (<-ch).(*TableFragment)
	assert.Equal(t, tableFragment.TableFragmentType, "DataAppend")
	assert.Equal(t, tableFragment.TableId, 1)
	assert.Equal(t, tableFragment.Rows, RawRows{
		NewRawRow(json.Number("1")),
	})

	tableCompletion := (<-ch).(*TableCompletion)
	assert.Equal(t, tableCompletion.TableId, 1)
	assert.Equal(t, tableCompletion.RowCount, 1)
	assert.Equal(t, tableCompletion.OneApiErrors, []OneApiError{
		{
			ErrorMessage: ErrorMessage{
				Code:        "LimitsExceeded",
				Message:     "Request is invalid and cannot be executed.",
				Type:        "Kusto.Data.Exceptions.KustoServicePartialQueryFailureLimitsExceededException",
				Description: "Query execution has exceeded the allowed limits (80DA0003): The results of this query exceed the set limit of 1 records, so not all records were returned (E_QUERY_RESULT_SET_TOO_LARGE, 0x80DA0003). See https://aka.ms/kustoquerylimits for more information and possible solutions..",
				Context: ErrorContext{
					Timestamp:        "2023-11-28T08:30:06.4085369Z",
					ServiceAlias:     "<censored>",
					MachineName:      "KSEngine000000",
					ProcessName:      "Kusto.WinSvc.Svc",
					ProcessId:        4900,
					ThreadId:         6828,
					ClientRequestId:  "blab6",
					ActivityId:       "123e27de-1e4e-49d9-b579-fe0b331d3642",
					SubActivityId:    "123e27de-1e4e-49d9-b579-fe0b331d3642",
					ActivityType:     "GW.Http.CallContext",
					ParentActivityId: "123e27de-1e4e-49d9-b579-fe0b331d3642",
					ActivityStack:    "(Activity stack: CRID=blab6 ARID=123e27de-1e4e-49d9-b579-fe0b331d3642 > GW.Http.CallContext/123e27de-1e4e-49d9-b579-fe0b331d3642)",
				},
				IsPermanent: false,
			},
		},
	})

	dataSetCompletion := (<-ch).(*DataSetCompletion)
	assert.Equal(t, dataSetCompletion.HasErrors, true)
	assert.Equal(t, dataSetCompletion.Cancelled, false)
	assert.Equal(t, dataSetCompletion.OneApiErrors, []OneApiError{
		{
			ErrorMessage: ErrorMessage{
				Code:        "LimitsExceeded",
				Message:     "Request is invalid and cannot be executed.",
				Type:        "Kusto.Data.Exceptions.KustoServicePartialQueryFailureLimitsExceededException",
				Description: "Query execution has exceeded the allowed limits (80DA0003): The results of this query exceed the set limit of 1 records, so not all records were returned (E_QUERY_RESULT_SET_TOO_LARGE, 0x80DA0003). See https://aka.ms/kustoquerylimits for more information and possible solutions..",
				Context: ErrorContext{
					Timestamp:        "2023-11-28T08:30:06.4085369Z",
					ServiceAlias:     "<censored>",
					MachineName:      "KSEngine000000",
					ProcessName:      "Kusto.WinSvc.Svc",
					ProcessId:        4900,
					ThreadId:         6828,
					ClientRequestId:  "blab6",
					ActivityId:       "123e27de-1e4e-49d9-b579-fe0b331d3642",
					SubActivityId:    "123e27de-1e4e-49d9-b579-fe0b331d3642",
					ActivityType:     "GW.Http.CallContext",
					ParentActivityId: "123e27de-1e4e-49d9-b579-fe0b331d3642",
					ActivityStack:    "(Activity stack: CRID=blab6 ARID=123e27de-1e4e-49d9-b579-fe0b331d3642 > GW.Http.CallContext/123e27de-1e4e-49d9-b579-fe0b331d3642)",
				},
				IsPermanent: false,
			},
		},
	})

	assert.Nil(t, <-ch)

	err := <-errChan
	require.NoError(t, err)
}

func TestReadFramesWithInvalidInput(t *testing.T) {
	t.Parallel()
	src := `[{]`

	ch := make(chan Frame)
	// err channel
	errChan := make(chan error)

	go func() {
		err := readAndDecodeFrames(src, ch)
		errChan <- err
	}()

	for range ch {
		assert.Fail(t, "should not receive any frames")
	}

	err := <-errChan
	require.ErrorContains(t, err, "invalid character ']'")
}

func TestReadFramesWithInvalidFrameType(t *testing.T) {
	t.Parallel()
	src := `[{"FrameType": "InvalidFrameType"}
]`

	ch := make(chan Frame)
	// err channel
	errChan := make(chan error)

	go func() {
		err := readAndDecodeFrames(src, ch)
		errChan <- err
	}()

	invalid := <-ch
	assert.Nil(t, invalid)

	err := <-errChan
	require.ErrorContains(t, err, "unknown frame type: InvalidFrameType")
}

func TestReadFramesWithInvalidFrame(t *testing.T) {
	t.Parallel()
	src := `[{"FrameType": "DataSetHeader", "IsProgressive": "invalid"}
]`

	ch := make(chan Frame)
	// err channel
	errChan := make(chan error)

	go func() {
		err := readAndDecodeFrames(src, ch)
		errChan <- err
	}()

	invalid := <-ch
	assert.Nil(t, invalid)

	err := <-errChan
	require.ErrorContains(t, err, "json: cannot unmarshal string")
}
