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

type table1 struct {
	A int
}
type table2 struct {
	A string
	B int
}

func defaultDataset(reader io.Reader) (query.IterativeDataset, error) {
	return NewIterativeDataset(context.Background(), io.NopCloser(reader), DefaultFrameCapacity, DefaultRowCapacity, DefaultFragmentCapacity)
}

func TestStreamingDataSet_ReadFrames_WithError(t *testing.T) {
	t.Parallel()
	reader := strings.NewReader("invalid")
	dataset, err := defaultDataset(reader)
	require.ErrorContains(t, err, "invalid")
	require.Nil(t, dataset)
}

func TestStreamingDataSet_DecodeTables_WithInvalidFrame(t *testing.T) {
	t.Parallel()
	reader := strings.NewReader(`[{"FrameType":"InvalidFrameType"}
]`)
	d, err := defaultDataset(reader)
	assert.NoError(t, err)

	tableResult := <-d.Tables()
	if tableResult != nil {
		assert.Nil(t, tableResult.Table())
		assert.Error(t, tableResult.Err(), "invalid frame type: InvalidFrameType")
	}
}

func TestStreamingDataSet_DecodeTables_Skip(t *testing.T) {
	t.Parallel()
	reader := strings.NewReader(validFrames)
	d, err := defaultDataset(reader)
	assert.NoError(t, err)

	for tableResult := range d.Tables() {
		require.NoError(t, tableResult.Err())
		tableResult.Table().SkipToEnd()
	}
}

func TestStreamingDataSet_DecodeTables_GetRows(t *testing.T) {
	t.Parallel()
	for _, tt := range []struct {
		name   string
		reader io.Reader
	}{{name: "validFrames", reader: strings.NewReader(validFrames)},
		{name: "aliases", reader: strings.NewReader(aliases)},
	} {
		t.Run(tt.name, func(t *testing.T) {
			reader := tt.reader
			d, err := defaultDataset(reader)
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
						query.NewRowFromParts(nil, nil, 0, value.Values{
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
						query.NewRowFromParts(nil, nil, 0, value.Values{
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
						}),
						query.NewRowFromParts(nil, nil, 0, value.Values{
							value.NewNullInt(),
							value.NewNullDecimal(),
							value.NewNullDateTime(),
							value.NewNullTimespan(),
							value.NewNullDynamic(),
							value.NewNullBool(),
							value.NewNullReal(),
							value.NewString(""),
							value.NewNullLong(),
							value.NewNullGUID(),
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
						query.NewRowFromParts(nil, nil, 0, value.Values{
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
						query.NewRowFromParts(nil, nil, 1, value.Values{
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

			for tableResult := range d.Tables() {
				assert.NoError(t, tableResult.Err())
				if tableResult.Table() != nil {
					tb := tableResult.Table()
					expectedTable := tables[tb.Index()]
					assert.Equal(t, expectedTable.id, tb.Index())
					assert.Equal(t, expectedTable.name, tb.Name())
					assert.Equal(t, expectedTable.kind, tb.Kind())
					for i, col := range tb.Columns() {
						assert.Equal(t, expectedTable.columns[i].Name(), col.Name())
						assert.Equal(t, expectedTable.columns[i].Type(), col.Type())
					}

					i := 0
					for rowResult := range tb.Rows() {
						assert.NoError(t, rowResult.Err())
						rows := expectedTable.rows
						expectedRow := rows[i]
						for j, val := range rowResult.Row().Values() {
							assert.Equal(t, expectedRow.Values()[j], val)
						}
						i++
					}
				}
			}
		})
	}
}

func TestStreamingDataSet_MultiplePrimaryTables(t *testing.T) {
	t.Parallel()
	reader := strings.NewReader(twoTables)
	d, err := defaultDataset(reader)
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

	for tableResult := range d.Tables() {
		assert.NoError(t, tableResult.Err())
		tb := tableResult.Table()
		id := tb.Index()
		for rowResult := range tb.Rows() {
			assert.NoError(t, rowResult.Err())
			if id == 1 {
				var row table1
				err := rowResult.Row().ToStruct(&row)
				assert.NoError(t, err)
				assert.Equal(t, table1Expected[rowResult.Row().Index()], row)
			}
			if id == 2 {
				var row table2
				err := rowResult.Row().ToStruct(&row)
				assert.NoError(t, err)
				assert.Equal(t, table2Expected[rowResult.Row().Index()], row)
			}
		}
	}
}

func TestStreamingDataSet_DecodeTables_WithInvalidDataSetHeader(t *testing.T) {
	t.Parallel()
	s := twoTables
	s = strings.Replace(s, "\"Version\":\"v2.0\"", "\"Version\":\"invalid\"", 1)
	reader := strings.NewReader(s)
	d, err := defaultDataset(reader)
	assert.NoError(t, err)

	tableResult := <-d.Tables()
	if tableResult == nil {
		t.Fatal("tableResult is nil")
	}

	assert.Error(t, tableResult.Err())
	assert.Contains(t, tableResult.Err().Error(), "Expected v2.0, got invalid")
}

func TestStreamingDataSet_DecodeTables_WithInvalidTableFragment(t *testing.T) {
	t.Parallel()
	reader := strings.NewReader(`[{"FrameType":"TableFragment", "TableId": 1}
]`)
	d, err := defaultDataset(reader)
	assert.NoError(t, err)

	tableResult := <-d.Tables()
	assert.Error(t, tableResult.Err())
	assert.Contains(t, tableResult.Err().Error(), "Expected DataSetHeader, got TableFragment")
}

func TestStreamingDataSet_DecodeTables_WithInvalidTableCompletion(t *testing.T) {
	t.Parallel()
	reader := strings.NewReader(`[{"FrameType":"TableCompletion", "TableId": 1}
]`)
	d, err := defaultDataset(reader)
	assert.NoError(t, err)

	tableResult := <-d.Tables()

	assert.Error(t, tableResult.Err())
	assert.Contains(t, tableResult.Err().Error(), "Expected DataSetHeader, got TableCompletion")
}

func TestStreamingDataSet_DecodeTables_StreamingTable_WithInvalidColumnType(t *testing.T) {
	t.Parallel()
	s := twoTables
	s = strings.Replace(s, "{\"ColumnName\":\"A\",\"ColumnType\":\"int\"}", "{\"ColumnName\":\"A\",\"ColumnType\":\"invalid\"}", 1)
	reader := strings.NewReader(s)
	d, err := defaultDataset(reader)
	assert.NoError(t, err)

	tableResult := <-d.Tables()
	assert.Error(t, tableResult.Err())
	assert.Contains(t, tableResult.Err().Error(), "not valid")
}

func TestStreamingDataSet_DecodeTables_DataTable_WithInvalidColumnType(t *testing.T) {
	t.Parallel()
	s := twoTables
	s = strings.Replace(s, "{\"ColumnName\":\"TableId\",\"ColumnType\":\"int\"}", "{\"ColumnName\":\"TableId\",\"ColumnType\":\"invalid\"}", 1)
	reader := strings.NewReader(s)
	d, err := defaultDataset(reader)
	assert.NoError(t, err)

	tableResult := <-d.Tables()
	assert.Error(t, tableResult.Err())
	assert.Contains(t, tableResult.Err().Error(), "not valid")
}

func TestStreamingDataSet_PartialErrors_Streaming(t *testing.T) {
	t.Parallel()
	reader := strings.NewReader(partialErrors)
	d, err := defaultDataset(reader)
	assert.NoError(t, err)

	for result := range d.Tables() {
		if result.Table() != nil {
			tb := result.Table()
			_, err := tb.ToTable()
			assert.ErrorContains(t, err, "LimitsExceeded")
		} else if result.Err() != nil {
			assert.ErrorContains(t, result.Err(), "LimitsExceeded")
		}
	}
}

func TestStreamingDataSet_PartialErrors_GetAll(t *testing.T) {
	t.Parallel()
	reader := strings.NewReader(partialErrors)
	d, err := defaultDataset(reader)
	assert.NoError(t, err)
	_, err = d.ToDataset()
	assert.ErrorContains(t, err, "LimitsExceeded")
}

func TestStreamingDataSet_FullError(t *testing.T) {
	t.Parallel()
	reader := strings.NewReader(errorText)
	d, err := defaultDataset(reader)
	assert.ErrorContains(t, err, "Bad request")
	assert.Nil(t, d)
}

func TestStreamingDataSet_Context_Canceled(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(context.Background())
	reader := strings.NewReader(validFrames)
	_, err := NewIterativeDataset(ctx, io.NopCloser(reader), 1, 1, 1)
	assert.NoError(t, err)
	cancel()
}
