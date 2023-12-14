package v2

import (
	"context"
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

func TestFullDataSet_DecodeTables_WithInvalidFrame(t *testing.T) {
	t.Parallel()
	reader := strings.NewReader(`[{"FrameType": "InvalidFrameType"}
]`)
	d, err := NewFullDataSet(context.Background(), io.NopCloser(reader))
	assert.Error(t, err, "invalid frame type: InvalidFrameType")
	assert.Nil(t, d)
}

func TestFullDataSet_DecodeTables_GetRows(t *testing.T) {
	t.Parallel()
	reader := strings.NewReader(validFrames)
	d, err := NewFullDataSet(context.Background(), io.NopCloser(reader))
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

	tbs, errors := d.GetAllTables()
	require.Nil(t, errors)

	for _, tb := range tbs {
		expectedTable := tables[tb.Ordinal()]
		assert.Equal(t, expectedTable.id, tb.Ordinal())
		assert.Equal(t, expectedTable.name, tb.Name())
		assert.Equal(t, expectedTable.kind, tb.Kind())
		assert.Equal(t, expectedTable.columns, tb.Columns())

		i := 0
		tbRows, errs := tb.GetAllRows()
		assert.Nil(t, errs)
		for _, row := range tbRows {
			rows := expectedTable.rows
			expectedRow := rows[i]
			for j, val := range row.Values() {
				assert.Equal(t, expectedRow.Values()[j].GetValue(), val.GetValue())
			}
			i++
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

func TestFullDataSet_MultiplePrimaryTables(t *testing.T) {
	t.Parallel()
	reader := strings.NewReader(twoTables)
	d, err := NewFullDataSet(context.Background(), io.NopCloser(reader))
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

	tables, errors := d.GetAllTables()
	require.Nil(t, errors)

	for _, tb := range tables {
		id := tb.Ordinal()
		rows, errs := tb.GetAllRows()
		assert.Nil(t, errs)
		for _, tableRow := range rows {
			if id == 1 {
				var row table1
				err := tableRow.ToStruct(&row)
				assert.NoError(t, err)
				assert.Equal(t, table1Expected[tableRow.Ordinal()], row)
			}
			if id == 2 {
				var row table2
				err := tableRow.ToStruct(&row)
				assert.NoError(t, err)
				assert.Equal(t, table2Expected[tableRow.Ordinal()], row)
			}
		}
	}
}

func TestFullDataSet_DecodeTables_WithInvalidDataSetHeader(t *testing.T) {
	t.Parallel()
	reader := strings.NewReader(`[{"FrameType": "DataSetHeader", "Version": "V1"}
]`)
	d, err := NewFullDataSet(context.Background(), io.NopCloser(reader))
	assert.NoError(t, err)
	assert.Nil(t, d)
}

func TestFullDataSet_DecodeTables_WithInvalidTableFragment(t *testing.T) {
	t.Parallel()
	reader := strings.NewReader(`[{"FrameType": "TableFragment", "TableId": 1}
]`)
	d, err := NewFullDataSet(context.Background(), io.NopCloser(reader))
	assert.NoError(t, err)
	assert.Nil(t, d)
}

func TestFullDataSet_DecodeTables_WithInvalidTableCompletion(t *testing.T) {
	t.Parallel()
	reader := strings.NewReader(`[{"FrameType": "TableCompletion", "TableId": 1}
]`)
	d, err := NewFullDataSet(context.Background(), io.NopCloser(reader))
	assert.NoError(t, err)
	assert.Nil(t, d)
}

func TestFullDataSet_DecodeTables_StreamingTable_WithInvalidColumnType(t *testing.T) {
	t.Parallel()
	reader := strings.NewReader(`[{"FrameType": "TableHeader", "TableId": 1, "TableName": "TestTable", "TableKind": "PrimaryResult", "Columns": [{"ColumnName": "TestColumn", "ColumnType": "invalid"}]}
]`)
	d, err := NewFullDataSet(context.Background(), io.NopCloser(reader))
	assert.NoError(t, err)
	assert.Nil(t, d)
}

func TestFullDataSet_DecodeTables_DataTable_WithInvalidColumnType(t *testing.T) {
	t.Parallel()
	reader := strings.NewReader(`[{"FrameType": "DataTable", "TableId": 1, "TableName": "TestTable", "TableKind": "PrimaryResult", "Columns": [{"ColumnName": "TestColumn", "ColumnType": "invalid"}], "Rows": [["TestValue"]]}
]`)
	d, err := NewFullDataSet(context.Background(), io.NopCloser(reader))
	assert.NoError(t, err)
	assert.Nil(t, d)
}
