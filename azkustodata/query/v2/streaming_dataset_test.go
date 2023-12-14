package v2

import (
	"context"
	"github.com/Azure/azure-kusto-go/azkustodata/errors"
	"github.com/Azure/azure-kusto-go/azkustodata/query"
	"github.com/Azure/azure-kusto-go/azkustodata/value"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"io"
	"strings"
	"testing"
	"time"
)

type table1 struct {
	A int
}
type table2 struct {
	A string
	B int
}

func TestStreamingDataSet_ReadFrames_WithError(t *testing.T) {
	t.Parallel()
	reader := strings.NewReader("invalid")
	d := &streamingDataset{
		baseDataset:  *newBaseDataset(query.NewDataset(context.Background(), errors.OpQuery), false),
		reader:       io.NopCloser(reader),
		frames:       make(chan Frame, DefaultFrameCapacity),
		errorChannel: make(chan error, 1),
		results:      make(chan query.TableResult, 1),
	}

	br, err := prepareReadBuffer(d.reader)
	require.ErrorContains(t, err, "invalid")
	require.Nil(t, br)
}

func TestStreamingDataSet_DecodeTables_WithInvalidFrame(t *testing.T) {
	t.Parallel()
	reader := strings.NewReader(`[{"FrameType": "InvalidFrameType"}
]`)
	d, err := NewStreamingDataSet(context.Background(), io.NopCloser(reader), DefaultFrameCapacity)
	assert.NoError(t, err)

	tableResult := <-d.Results()
	if tableResult != nil {
		assert.Nil(t, tableResult.Table())
		assert.Error(t, tableResult.Err(), "invalid frame type: InvalidFrameType")
	}
}

func TestStreamingDataSet_DecodeTables_Skip(t *testing.T) {
	t.Parallel()
	reader := strings.NewReader(validFrames)
	d, err := NewStreamingDataSet(context.Background(), io.NopCloser(reader), DefaultFrameCapacity)
	assert.NoError(t, err)

	for tableResult := range d.Results() {
		require.NoError(t, tableResult.Err())
		tableResult.Table().SkipToEnd()
	}
}

func TestStreamingDataSet_DecodeTables_GetRows(t *testing.T) {
	t.Parallel()
	reader := strings.NewReader(validFrames)
	d, err := NewStreamingDataSet(context.Background(), io.NopCloser(reader), DefaultFrameCapacity)
	assert.NoError(t, err)
	ts, err := value.TimespanFromString("01:23:45.6789000")
	assert.NoError(t, err)
	u, err := uuid.Parse("123e27de-1e4e-49d9-b579-fe0b331d3642")
	assert.NoError(t, err)

	tables := []struct {
		rows    []query.Row
		id      int64
		name    string
		kind    string
		columns []query.Column
	}{
		{

			id:   0,
			name: "@ExtendedProperties",
			kind: "QueryProperties",
			columns: []query.Column{
				query.NewColumn(0, "TableId", "int"),
				query.NewColumn(1, "Key", "string"),
				query.NewColumn(2, "Value", "dynamic"),
			},
			rows: []query.Row{
				query.NewRow(nil, 0, value.Values{
					value.NewInt(1),
					value.NewString("Visualization"),
					value.NewDynamic([]byte("{\"Visualization\":null,\"Title\":null,\"XColumn\":null,\"Series\":null,\"YColumns\":null,\"AnomalyColumns\":null,\"XTitle\":null,\"YTitle\":null,\"XAxis\":null,\"YAxis\":null,\"Legend\":null,\"YSplit\":null,\"Accumulate\":false,\"IsQuerySorted\":false,\"Kind\":null,\"Ymin\":\"NaN\",\"Ymax\":\"NaN\",\"Xmin\":null,\"Xmax\":null}")),
				}),
			},
		},
		{

			id:   1,
			name: "AllDataTypes",
			kind: "PrimaryResult",
			columns: []query.Column{
				query.NewColumn(0, "vnum", "int"),
				query.NewColumn(1, "vdec", "decimal"),
				query.NewColumn(2, "vdate", "datetime"),
				query.NewColumn(3, "vspan", "timespan"),
				query.NewColumn(4, "vobj", "dynamic"),
				query.NewColumn(5, "vb", "bool"),
				query.NewColumn(6, "vreal", "real"),
				query.NewColumn(7, "vstr", "string"),
				query.NewColumn(8, "vlong", "long"),
				query.NewColumn(9, "vguid", "guid"),
			},
			rows: []query.Row{
				query.NewRow(nil, 0, value.Values{
					value.NewInt(1),
					value.DecimalFromString("2.00000000000001"),
					value.NewDateTime(time.Date(2020, 3, 4, 14, 5, 1, 310996500, time.UTC)),
					ts,
					value.NewDynamic([]byte("{\"moshe\":\"value\"}")),
					value.NewBool(true),
					value.NewReal(0.01),
					value.NewString("asdf"),
					value.NewLong(9223372036854775807),
					value.NewGUID(u),
				})},
		},
		{

			id:   2,
			name: "QueryCompletionInformation",
			kind: "QueryCompletionInformation",
			columns: []query.Column{
				query.NewColumn(0, "Timestamp", "datetime"),
				query.NewColumn(1, "ClientRequestId", "string"),
				query.NewColumn(2, "ActivityId", "guid"),
				query.NewColumn(3, "SubActivityId", "guid"),
				query.NewColumn(4, "ParentActivityId", "guid"),
				query.NewColumn(5, "Level", "int"),
				query.NewColumn(6, "LevelName", "string"),
				query.NewColumn(7, "StatusCode", "int"),
				query.NewColumn(8, "StatusCodeName", "string"),
				query.NewColumn(9, "EventType", "int"),
				query.NewColumn(10, "EventTypeName", "string"),
				query.NewColumn(11, "Payload", "string"),
			},
			rows: []query.Row{
				query.NewRow(nil, 0, value.Values{
					value.NewDateTime(time.Date(2023, 11, 26, 13, 34, 17, 73147800, time.UTC)),
					value.NewString("blab6"),
					value.NewGUID(u),
					value.NewGUID(u),
					value.NewGUID(u),
					value.NewInt(4),
					value.NewString("Info"),
					value.NewInt(0),
					value.NewString("S_OK (0)"),
					value.NewInt(4),
					value.NewString("QueryInfo"),
					value.NewString("{\"Count\":1,\"Text\":\"Query completed successfully\"}"),
				}),
				query.NewRow(nil, 1, value.Values{
					value.NewDateTime(time.Date(2023, 11, 26, 13, 34, 17, 73147800, time.UTC)),
					value.NewString("blab6"),
					value.NewGUID(u),
					value.NewGUID(u),
					value.NewGUID(u),
					value.NewInt(4),
					value.NewString("Info"),
					value.NewInt(0),
					value.NewString("S_OK (0)"),
					value.NewInt(5),
					value.NewString("WorkloadGroup"),
					value.NewString("{\"Count\":1,\"Text\":\"default\"}"),
				}),
			},
		},
	}

	for tableResult := range d.Results() {
		assert.NoError(t, tableResult.Err())
		if tableResult.Table() != nil {
			tb := tableResult.Table()
			expectedTable := tables[tb.Ordinal()]
			assert.Equal(t, expectedTable.id, tb.Ordinal())
			assert.Equal(t, expectedTable.name, tb.Name())
			assert.Equal(t, expectedTable.kind, tb.Kind())
			assert.Equal(t, expectedTable.columns, tb.Columns())

			i := 0
			for rowResult := range tb.Rows() {
				assert.NoError(t, rowResult.Err())
				rows := expectedTable.rows
				expectedRow := rows[i]
				for j, val := range rowResult.Row().Values() {
					assert.Equal(t, expectedRow.Values()[j].GetValue(), val.GetValue())
				}
				i++
			}
		}
	}

	expectedQueryCompletionInformation := []QueryCompletionInformation{
		{
			Timestamp:        time.Date(2023, 11, 26, 13, 34, 17, 73147800, time.UTC),
			ClientRequestId:  "blab6",
			ActivityId:       u,
			SubActivityId:    u,
			ParentActivityId: u,
			Level:            4,
			LevelName:        "Info",
			StatusCode:       0,
			StatusCodeName:   "S_OK (0)",
			EventType:        4,
			EventTypeName:    "QueryInfo",
			Payload:          "{\"Count\":1,\"Text\":\"Query completed successfully\"}",
		},
		{
			Timestamp:        time.Date(2023, 11, 26, 13, 34, 17, 73147800, time.UTC),
			ClientRequestId:  "blab6",
			ActivityId:       u,
			SubActivityId:    u,
			ParentActivityId: u,
			Level:            4,
			LevelName:        "Info",
			StatusCode:       0,
			StatusCodeName:   "S_OK (0)",
			EventType:        5,
			EventTypeName:    "WorkloadGroup",
			Payload:          "{\"Count\":1,\"Text\":\"default\"}",
		},
	}

	information := d.QueryCompletionInformation()
	assert.NotNil(t, information)
	assert.Equal(t, expectedQueryCompletionInformation, information)

	expectedQueryProperties := []QueryProperties{
		{
			TableId: 1,
			Key:     "Visualization",
			Value:   map[string]interface{}{"Visualization": nil, "Title": nil, "XColumn": nil, "Series": nil, "YColumns": nil, "AnomalyColumns": nil, "XTitle": nil, "YTitle": nil, "XAxis": nil, "YAxis": nil, "Legend": nil, "YSplit": nil, "Accumulate": false, "IsQuerySorted": false, "Kind": nil, "Ymin": "NaN", "Ymax": "NaN", "Xmin": nil, "Xmax": nil},
		},
	}

	properties := d.QueryProperties()
	assert.NotNil(t, properties)
	assert.Equal(t, expectedQueryProperties, properties)
}

func TestStreamingDataSet_MultiplePrimaryTables(t *testing.T) {
	t.Parallel()
	reader := strings.NewReader(twoTables)
	d, err := NewStreamingDataSet(context.Background(), io.NopCloser(reader), DefaultFrameCapacity)
	assert.NoError(t, err)

	table1Expected := []table1{
		{A: 1},
		{A: 2},
		{A: 3},
	}

	table2Expected := []table2{
		{A: "a", B: 1},
		{A: "b", B: 2},
		{A: "c", B: 3},
	}

	for tableResult := range d.Results() {
		assert.NoError(t, tableResult.Err())
		tb := tableResult.Table()
		id := tb.Ordinal()
		for rowResult := range tb.Rows() {
			assert.NoError(t, rowResult.Err())
			if id == 1 {
				var row table1
				err := rowResult.Row().ToStruct(&row)
				assert.NoError(t, err)
				assert.Equal(t, table1Expected[rowResult.Row().Ordinal()], row)
			}
			if id == 2 {
				var row table2
				err := rowResult.Row().ToStruct(&row)
				assert.NoError(t, err)
				assert.Equal(t, table2Expected[rowResult.Row().Ordinal()], row)
			}
		}
	}
}

func TestStreamingDataSet_DecodeTables_WithInvalidDataSetHeader(t *testing.T) {
	t.Parallel()
	reader := strings.NewReader(`[{"FrameType": "DataSetHeader", "Version": "V1"}
]`)
	d, err := NewStreamingDataSet(context.Background(), io.NopCloser(reader), DefaultFrameCapacity)
	assert.NoError(t, err)

	tableResult := <-d.Results()
	assert.Error(t, tableResult.Err())
	assert.Contains(t, tableResult.Err().Error(), "received a DataSetHeader frame that is not version 2")
}

func TestStreamingDataSet_DecodeTables_WithInvalidTableFragment(t *testing.T) {
	t.Parallel()
	reader := strings.NewReader(`[{"FrameType": "TableFragment", "TableId": 1}
]`)
	d, err := NewStreamingDataSet(context.Background(), io.NopCloser(reader), DefaultFrameCapacity)
	assert.NoError(t, err)

	tableResult := <-d.Results()
	assert.Error(t, tableResult.Err())
	assert.Contains(t, tableResult.Err().Error(), "received a TableFragment frame while no streaming table was open")
}

func TestStreamingDataSet_DecodeTables_WithInvalidTableCompletion(t *testing.T) {
	t.Parallel()
	reader := strings.NewReader(`[{"FrameType": "TableCompletion", "TableId": 1}
]`)
	d, err := NewStreamingDataSet(context.Background(), io.NopCloser(reader), DefaultFrameCapacity)
	assert.NoError(t, err)

	tableResult := <-d.Results()
	assert.Error(t, tableResult.Err())
	assert.Contains(t, tableResult.Err().Error(), "received a TableCompletion frame while no streaming table was open")
}

func TestStreamingDataSet_DecodeTables_StreamingTable_WithInvalidColumnType(t *testing.T) {
	t.Parallel()
	reader := strings.NewReader(`[{"FrameType": "TableHeader", "TableId": 1, "TableName": "TestTable", "TableKind": "PrimaryResult", "Columns": [{"ColumnName": "TestColumn", "ColumnType": "invalid"}]}
]`)
	d, err := NewStreamingDataSet(context.Background(), io.NopCloser(reader), DefaultFrameCapacity)
	assert.NoError(t, err)

	tableResult := <-d.Results()
	assert.Error(t, tableResult.Err())
	assert.Contains(t, tableResult.Err().Error(), "not valid")
}

func TestStreamingDataSet_DecodeTables_DataTable_WithInvalidColumnType(t *testing.T) {
	t.Parallel()
	reader := strings.NewReader(`[{"FrameType": "DataTable", "TableId": 1, "TableName": "TestTable", "TableKind": "PrimaryResult", "Columns": [{"ColumnName": "TestColumn", "ColumnType": "invalid"}], "Rows": [["TestValue"]]}
]`)
	d, err := NewStreamingDataSet(context.Background(), io.NopCloser(reader), DefaultFrameCapacity)
	assert.NoError(t, err)

	tableResult := <-d.Results()
	assert.Error(t, tableResult.Err())
	assert.Contains(t, tableResult.Err().Error(), "not valid")
}

func TestStreamingDataSet_PartialErrors_Streaming(t *testing.T) {
	t.Parallel()
	reader := strings.NewReader(partialErrors)
	d, err := NewStreamingDataSet(context.Background(), io.NopCloser(reader), DefaultFrameCapacity)
	assert.NoError(t, err)

	for result := range d.Results() {
		if result.Table() != nil {
			tb := result.Table()
			rows, errs := tb.GetAllRows()

			assert.ErrorContains(t, errs[0], "LimitsExceeded")

			assert.Equal(t, 1, len(rows))
			ft := &table1{}
			assert.NoError(t, rows[0].ToStruct(ft))
			assert.Equal(t, int(1), ft.A)
		}
		if result.Err() != nil {
			assert.ErrorContains(t, result.Err(), "LimitsExceeded")
		}
	}
}

func TestStreamingDataSet_PartialErrors_GetAll(t *testing.T) {
	t.Parallel()
	reader := strings.NewReader(partialErrors)
	d, err := NewStreamingDataSet(context.Background(), io.NopCloser(reader), DefaultFrameCapacity)
	results, errs := d.GetAllTables()
	assert.NoError(t, err)
	assert.Len(t, errs, 1)
	assert.ErrorContains(t, errs[0], "LimitsExceeded")

	for _, tb := range results {
		rows, errs := tb.GetAllRows()

		assert.ErrorContains(t, errs[0], "LimitsExceeded")

		assert.Equal(t, 1, len(rows))
		ft := &table1{}
		assert.NoError(t, rows[0].ToStruct(ft))
		assert.Equal(t, int(1), ft.A)

	}
}

func TestStreamingDataSet_FullError(t *testing.T) {
	t.Parallel()
	reader := strings.NewReader(errorText)
	d, err := NewStreamingDataSet(context.Background(), io.NopCloser(reader), DefaultFrameCapacity)
	assert.ErrorContains(t, err, "Bad request")
	assert.Nil(t, d)
}
