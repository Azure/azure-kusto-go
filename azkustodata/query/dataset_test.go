package query

import (
	"context"
	"github.com/Azure/azure-kusto-go/azkustodata/value"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"strings"
	"testing"
	"time"
)

func TestDataSet_ReadFrames_WithError(t *testing.T) {
	reader := strings.NewReader("invalid")
	d := &DataSet{
		reader:       reader,
		frames:       make(chan Frame, DefaultFrameCapacity),
		errorChannel: make(chan error, 1),
		tables:       make(chan TableResult, 1),
		ctx:          context.Background(),
	}
	go d.ReadFrames()

	err := <-d.errorChannel
	assert.Error(t, err)
}

func TestDataSet_DecodeTables_WithInvalidFrame(t *testing.T) {
	reader := strings.NewReader(`[{"FrameType": "InvalidFrameType"}]`)
	d := NewDataSet(context.Background(), reader, DefaultFrameCapacity)

	tableResult := <-d.tables
	assert.Nil(t, tableResult.Table)
}

func TestDataSet_DecodeTables_Skip(t *testing.T) {
	reader := strings.NewReader(strings.TrimSpace(validFrames))
	d := NewDataSet(context.Background(), reader, DefaultFrameCapacity)

	for tableResult := range d.tables {
		assert.NoError(t, tableResult.Err)
		if tableResult.Table != nil {
			if t, ok := tableResult.Table.(StreamingTable); ok {
				t.SkipToEnd()
			}
		}
	}
}

func TestDataSet_DecodeTables_GetRows(t *testing.T) {
	reader := strings.NewReader(strings.TrimSpace(validFrames))
	d := NewDataSet(context.Background(), reader, DefaultFrameCapacity)
	ts, err := value.TimespanFromString("01:23:45.6789000")
	assert.NoError(t, err)
	u, err := uuid.Parse("123e27de-1e4e-49d9-b579-fe0b331d3642")
	assert.NoError(t, err)

	tables := []fullTable{
		{
			baseTable: baseTable{
				id:   0,
				name: "@ExtendedProperties",
				kind: "QueryProperties",
				columns: []Column{
					{Ordinal: 0, Name: "TableId", Type: "int"},
					{Ordinal: 1, Name: "Key", Type: "string"},
					{Ordinal: 2, Name: "Value", Type: "dynamic"},
				},
			},
			rows: []Row{
				{table: nil, values: value.Values{
					value.NewInt(1),
					value.NewString("Visualization"),
					value.NewDynamic([]byte("{\"Visualization\":null,\"Title\":null,\"XColumn\":null,\"Series\":null,\"YColumns\":null,\"AnomalyColumns\":null,\"XTitle\":null,\"YTitle\":null,\"XAxis\":null,\"YAxis\":null,\"Legend\":null,\"YSplit\":null,\"Accumulate\":false,\"IsQuerySorted\":false,\"Kind\":null,\"Ymin\":\"NaN\",\"Ymax\":\"NaN\",\"Xmin\":null,\"Xmax\":null}")),
				}},
			},
		},
		{
			baseTable: baseTable{
				id:   1,
				name: "AllDataTypes",
				kind: "PrimaryResult",
				columns: []Column{
					{Ordinal: 0, Name: "vnum", Type: "int"},
					{Ordinal: 1, Name: "vdec", Type: "decimal"},
					{Ordinal: 2, Name: "vdate", Type: "datetime"},
					{Ordinal: 3, Name: "vspan", Type: "timespan"},
					{Ordinal: 4, Name: "vobj", Type: "dynamic"},
					{Ordinal: 5, Name: "vb", Type: "bool"},
					{Ordinal: 6, Name: "vreal", Type: "real"},
					{Ordinal: 7, Name: "vstr", Type: "string"},
					{Ordinal: 8, Name: "vlong", Type: "long"},
					{Ordinal: 9, Name: "vguid", Type: "guid"},
				},
			},
			rows: []Row{
				{table: nil, values: value.Values{
					value.NewInt(1),
					value.DecimalFromString("2.00000000000001"),
					value.DecimalFromString("2020-03-04T14:05:01.3109965Z"),
					ts,
					value.NewDynamic([]byte("{\"moshe\":\"value\"}")),
					value.NewBool(true),
					value.NewReal(0.01),
					value.NewString("asdf"),
					value.NewLong(9223372036854775807),
					value.NewGUID(u),
				}}},
		},
		{
			baseTable: baseTable{
				id:   2,
				name: "QueryCompletionInformation",
				kind: "QueryCompletionInformation",
				columns: []Column{
					{Ordinal: 0, Name: "Timestamp", Type: "datetime"},
					{Ordinal: 1, Name: "ClientRequestId", Type: "string"},
					{Ordinal: 2, Name: "ActivityId", Type: "guid"},
					{Ordinal: 3, Name: "SubActivityId", Type: "guid"},
					{Ordinal: 4, Name: "ParentActivityId", Type: "guid"},
					{Ordinal: 5, Name: "Level", Type: "int"},
					{Ordinal: 6, Name: "LevelName", Type: "string"},
					{Ordinal: 7, Name: "StatusCode", Type: "int"},
					{Ordinal: 8, Name: "StatusCodeName", Type: "string"},
					{Ordinal: 9, Name: "EventType", Type: "int"},
					{Ordinal: 10, Name: "EventTypeName", Type: "string"},
					{Ordinal: 11, Name: "Payload", Type: "string"},
				},
			},
			rows: []Row{
				{table: nil, values: value.Values{
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
				}},
				{table: nil, values: value.Values{
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
				}},
			},
		},
	}

	for tableResult := range d.tables {
		assert.NoError(t, tableResult.Err)
		if tableResult.Table != nil {
			if tb, ok := tableResult.Table.(StreamingTable); ok {
				tb.SkipToEnd()
			}

			if tb, ok := tableResult.Table.(FullTable); ok {
				expectedTable := tables[tb.Id()]
				assert.Equal(t, expectedTable.Id(), tb.Id())
				assert.Equal(t, expectedTable.Name(), tb.Name())
				assert.Equal(t, expectedTable.Kind(), tb.Kind())
				assert.Equal(t, expectedTable.Columns(), tb.Columns())
				for i, row := range tb.Rows() {
					expectedRow := expectedTable.Rows()[i]
					assert.Equal(t, expectedRow.Values(), row.Values())
				}
			}
		}
	}
}

func TestDataSet_DecodeTables_WithInvalidDataSetHeader(t *testing.T) {
	reader := strings.NewReader(`[{"FrameType": "DataSetHeader", "Version": "V1"}]`)
	d := NewDataSet(context.Background(), reader, DefaultFrameCapacity)

	tableResult := <-d.tables
	assert.Error(t, tableResult.Err)
	assert.Contains(t, tableResult.Err.Error(), "received a DataSetHeader frame that is not version 2")
}

func TestDataSet_DecodeTables_WithInvalidTableFragment(t *testing.T) {
	reader := strings.NewReader(`[{"FrameType": "TableFragment", "TableId": 1}]`)
	d := NewDataSet(context.Background(), reader, DefaultFrameCapacity)

	tableResult := <-d.tables
	assert.Error(t, tableResult.Err)
	assert.Contains(t, tableResult.Err.Error(), "received a TableFragment frame while no streaming table was open")
}

func TestDataSet_DecodeTables_WithInvalidTableCompletion(t *testing.T) {
	reader := strings.NewReader(`[{"FrameType": "TableCompletion", "TableId": 1}]`)
	d := NewDataSet(context.Background(), reader, DefaultFrameCapacity)

	tableResult := <-d.tables
	assert.Error(t, tableResult.Err)
	assert.Contains(t, tableResult.Err.Error(), "received a TableCompletion frame while no streaming table was open")
}

func TestDataSet_DecodeTables_StreamingTable_WithInvalidColumnType(t *testing.T) {
	reader := strings.NewReader(`[{"FrameType": "TableHeader", "TableId": 1, "TableName": "TestTable", "Columns": [{"ColumnName": "TestColumn", "ColumnType": "invalid"}]}
]`)
	d := NewDataSet(context.Background(), reader, DefaultFrameCapacity)

	tableResult := <-d.tables
	assert.Error(t, tableResult.Err)
	assert.Contains(t, tableResult.Err.Error(), "not valid")
}

func TestDataSet_DecodeTables_DataTable_WithInvalidColumnType(t *testing.T) {
	reader := strings.NewReader(`[{"FrameType": "DataTable", "TableId": 1, "TableName": "TestTable", "Columns": [{"ColumnName": "TestColumn", "ColumnType": "invalid"}], "Rows": [["TestValue"]]}]`)
	d := NewDataSet(context.Background(), reader, DefaultFrameCapacity)

	tableResult := <-d.tables
	assert.Error(t, tableResult.Err)
	assert.Contains(t, tableResult.Err.Error(), "not valid")
}
