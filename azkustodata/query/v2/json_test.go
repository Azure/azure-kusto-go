package v2

import (
	"bytes"
	_ "embed"
	"github.com/google/uuid"
	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

//go:embed testData/validFrames.json
var validFrames string

//go:embed testData/aliases.json
var aliases string

//go:embed testData/partialError.json
var partialErrors string

//go:embed testData/twoTables.json
var twoTables string

//go:embed testData/error.txt
var errorText string

func TestDecodeValidFrames(t *testing.T) {
	for i := 0; i < 10000; i++ {
		reader := bytes.NewReader([]byte(validFrames))
		f, err := newFrameReader(reader)
		if err != nil {
			return
		}

		require.NotNil(t, f)

		require.NoError(t, f.advance())

		require.NoError(t, f.validateDataSetHeader())

		require.NoError(t, f.advance())

		properties, err := f.readQueryProperties()
		require.NoError(t, err)

		require.Equal(t, 0, properties.TableId)
		require.Equal(t, "QueryProperties", properties.TableKind)
		require.Equal(t, "@ExtendedProperties", properties.TableName)
		require.Equal(t, 1, properties.Rows[0].TableId)
		require.Equal(t, "Visualization", properties.Rows[0].Key)
		require.Equal(t, map[string]interface{}{"Accumulate": false, "AnomalyColumns": interface{}(nil), "IsQuerySorted": false, "Kind": interface{}(nil), "Legend": interface{}(nil), "Series": interface{}(nil), "Title": interface{}(nil), "Visualization": interface{}(nil), "XAxis": interface{}(nil), "XColumn": interface{}(nil), "XTitle": interface{}(nil), "Xmax": interface{}(nil), "Xmin": interface{}(nil), "YAxis": interface{}(nil), "YColumns": interface{}(nil), "YSplit": interface{}(nil), "YTitle": interface{}(nil), "Ymax": "NaN", "Ymin": "NaN"}, properties.Rows[0].Value)

		/*,{"FrameType":"TableHeader","TableId":1,"TableKind":"PrimaryResult","TableName":"AllDataTypes","Columns":[{"ColumnName":"vnum","ColumnType":"int"},{"ColumnName":"vdec","ColumnType":"decimal"},{"ColumnName":"vdate","ColumnType":"datetime"},{"ColumnName":"vspan","ColumnType":"timespan"},{"ColumnName":"vobj","ColumnType":"dynamic"},{"ColumnName":"vb","ColumnType":"bool"},{"ColumnName":"vreal","ColumnType":"real"},{"ColumnName":"vstr","ColumnType":"string"},{"ColumnName":"vlong","ColumnType":"long"},{"ColumnName":"vguid","ColumnType":"guid"}]}
		,{"FrameType":"TableFragment","TableFragmentType":"DataAppend","TableId":1,"Rows":[[1,"2.00000000000001","2020-03-04T14:05:01.3109965Z","01:23:45.6789000",{"moshe":"value"},true,0.01,"asdf",9223372036854775807,"123e27de-1e4e-49d9-b579-fe0b331d3642"],[null,null,null,null,null,null,null,"",null,null]]}
		,{"FrameType":"TableCompletion","TableId":1,"RowCount":1} */

		require.NoError(t, f.advance())

		tableHeader := TableHeader{}
		require.NoError(t, f.unmarshal(&tableHeader))
		require.Equal(t, 1, tableHeader.TableId)
		require.Equal(t, "PrimaryResult", tableHeader.TableKind)
		require.Equal(t, "AllDataTypes", tableHeader.TableName)
		require.Equal(t, 10, len(tableHeader.Columns))
		require.Equal(t, "vnum", tableHeader.Columns[0].ColumnName)
		require.Equal(t, "int", tableHeader.Columns[0].ColumnType)
		require.Equal(t, "vdec", tableHeader.Columns[1].ColumnName)
		require.Equal(t, "decimal", tableHeader.Columns[1].ColumnType)
		require.Equal(t, "vdate", tableHeader.Columns[2].ColumnName)
		require.Equal(t, "datetime", tableHeader.Columns[2].ColumnType)
		require.Equal(t, "vspan", tableHeader.Columns[3].ColumnName)
		require.Equal(t, "timespan", tableHeader.Columns[3].ColumnType)
		require.Equal(t, "vobj", tableHeader.Columns[4].ColumnName)
		require.Equal(t, "dynamic", tableHeader.Columns[4].ColumnType)
		require.Equal(t, "vb", tableHeader.Columns[5].ColumnName)
		require.Equal(t, "bool", tableHeader.Columns[5].ColumnType)
		require.Equal(t, "vreal", tableHeader.Columns[6].ColumnName)
		require.Equal(t, "real", tableHeader.Columns[6].ColumnType)
		require.Equal(t, "vstr", tableHeader.Columns[7].ColumnName)
		require.Equal(t, "string", tableHeader.Columns[7].ColumnType)
		require.Equal(t, "vlong", tableHeader.Columns[8].ColumnName)
		require.Equal(t, "long", tableHeader.Columns[8].ColumnType)
		require.Equal(t, "vguid", tableHeader.Columns[9].ColumnName)
		require.Equal(t, "guid", tableHeader.Columns[9].ColumnType)

		require.NoError(t, f.advance())

		tableFragment := TableFragment{Columns: tableHeader.Columns}
		require.NoError(t, f.unmarshal(&tableFragment))

		require.Equal(t, int32(1), *tableFragment.Rows[0][0].GetValue().(*int32))
		require.Equal(t, decimal.RequireFromString("2.00000000000001"), *tableFragment.Rows[0][1].GetValue().(*decimal.Decimal))
		require.Equal(t, "2020-03-04T14:05:01.3109965Z", tableFragment.Rows[0][2].GetValue().(*time.Time).Format(time.RFC3339Nano))
		require.Equal(t, "1h23m45.6789s", tableFragment.Rows[0][3].GetValue().(*time.Duration).String())
		require.Equal(t, []byte("{\"moshe\":\"value\"}"), tableFragment.Rows[0][4].GetValue().([]byte))
		require.Equal(t, true, *tableFragment.Rows[0][5].GetValue().(*bool))
		require.Equal(t, 0.01, *tableFragment.Rows[0][6].GetValue().(*float64))
		require.Equal(t, "asdf", tableFragment.Rows[0][7].GetValue().(string))
		require.Equal(t, int64(9223372036854775807), *tableFragment.Rows[0][8].GetValue().(*int64))
		require.Equal(t, "123e27de-1e4e-49d9-b579-fe0b331d3642", tableFragment.Rows[0][9].GetValue().(*uuid.UUID).String())

		require.Equal(t, (*int32)(nil), tableFragment.Rows[1][0].GetValue())
		require.Equal(t, (*decimal.Decimal)(nil), tableFragment.Rows[1][1].GetValue())
		require.Equal(t, (*time.Time)(nil), tableFragment.Rows[1][2].GetValue())
		require.Equal(t, (*time.Duration)(nil), tableFragment.Rows[1][3].GetValue())
		require.Equal(t, ([]byte)(nil), tableFragment.Rows[1][4].GetValue())
		require.Equal(t, (*bool)(nil), tableFragment.Rows[1][5].GetValue())
		require.Equal(t, (*float64)(nil), tableFragment.Rows[1][6].GetValue())
		require.Equal(t, "", tableFragment.Rows[1][7].GetValue().(string))
		require.Equal(t, (*int64)(nil), tableFragment.Rows[1][8].GetValue())
		require.Equal(t, (*uuid.UUID)(nil), tableFragment.Rows[1][9].GetValue())

		require.NoError(t, f.advance())

		tableCompletion := TableCompletion{}
		require.NoError(t, f.unmarshal(&tableCompletion))
		require.Equal(t, 1, tableCompletion.TableId)
		require.Equal(t, 2, tableCompletion.RowCount)

		require.NoError(t, f.advance())

		queryCompletionInformation, err := f.readQueryCompletionInformation()

		require.NoError(t, err)
		//{"FrameType":"DataTable","TableId":2,"TableKind":"QueryCompletionInformation","TableName":"QueryCompletionInformation","Columns":[{"ColumnName":"Timestamp","ColumnType":"datetime"},{"ColumnName":"ClientRequestId","ColumnType":"string"},{"ColumnName":"ActivityId","ColumnType":"guid"},{"ColumnName":"SubActivityId","ColumnType":"guid"},{"ColumnName":"ParentActivityId","ColumnType":"guid"},{"ColumnName":"Level","ColumnType":"int"},{"ColumnName":"LevelName","ColumnType":"string"},{"ColumnName":"StatusCode","ColumnType":"int"},{"ColumnName":"StatusCodeName","ColumnType":"string"},{"ColumnName":"EventType","ColumnType":"int"},{"ColumnName":"EventTypeName","ColumnType":"string"},{"ColumnName":"Payload","ColumnType":"string"}],"Rows":[["2023-11-26T13:34:17.0731478Z","blab6","123e27de-1e4e-49d9-b579-fe0b331d3642","123e27de-1e4e-49d9-b579-fe0b331d3642","123e27de-1e4e-49d9-b579-fe0b331d3642",4,"Info",0,"S_OK (0)",4,"QueryInfo","{\"Count\":1,\"Text\":\"Query completed successfully\"}"],["2023-11-26T13:34:17.0731478Z","blab6","123e27de-1e4e-49d9-b579-fe0b331d3642","123e27de-1e4e-49d9-b579-fe0b331d3642","123e27de-1e4e-49d9-b579-fe0b331d3642",4,"Info",0,"S_OK (0)",5,"WorkloadGroup","{\"Count\":1,\"Text\":\"default\"}"]]}

		require.Equal(t, "QueryCompletionInformation", queryCompletionInformation.TableKind)
		require.Equal(t, "QueryCompletionInformation", queryCompletionInformation.TableName)
		require.Equal(t, 2, queryCompletionInformation.TableId)
		require.Equal(t, 2, len(queryCompletionInformation.Rows))
		require.Equal(t, "2023-11-26T13:34:17.0731478Z", queryCompletionInformation.Rows[0].Timestamp.Format(time.RFC3339Nano))
		require.Equal(t, "blab6", queryCompletionInformation.Rows[0].ClientRequestId)
		require.Equal(t, "123e27de-1e4e-49d9-b579-fe0b331d3642", queryCompletionInformation.Rows[0].ActivityId.String())
		require.Equal(t, "123e27de-1e4e-49d9-b579-fe0b331d3642", queryCompletionInformation.Rows[0].SubActivityId.String())
		require.Equal(t, "123e27de-1e4e-49d9-b579-fe0b331d3642", queryCompletionInformation.Rows[0].ParentActivityId.String())
		require.Equal(t, 4, queryCompletionInformation.Rows[0].Level)
		require.Equal(t, "Info", queryCompletionInformation.Rows[0].LevelName)
		require.Equal(t, 0, queryCompletionInformation.Rows[0].StatusCode)
		require.Equal(t, "S_OK (0)", queryCompletionInformation.Rows[0].StatusCodeName)
		require.Equal(t, 4, queryCompletionInformation.Rows[0].EventType)
		require.Equal(t, "QueryInfo", queryCompletionInformation.Rows[0].EventTypeName)
		require.Equal(t, "{\"Count\":1,\"Text\":\"Query completed successfully\"}", queryCompletionInformation.Rows[0].Payload)

		require.Equal(t, "2023-11-26T13:34:17.0731478Z", queryCompletionInformation.Rows[1].Timestamp.Format(time.RFC3339Nano))
		require.Equal(t, "blab6", queryCompletionInformation.Rows[1].ClientRequestId)
		require.Equal(t, "123e27de-1e4e-49d9-b579-fe0b331d3642", queryCompletionInformation.Rows[1].ActivityId.String())
		require.Equal(t, "123e27de-1e4e-49d9-b579-fe0b331d3642", queryCompletionInformation.Rows[1].SubActivityId.String())
		require.Equal(t, "123e27de-1e4e-49d9-b579-fe0b331d3642", queryCompletionInformation.Rows[1].ParentActivityId.String())
		require.Equal(t, 4, queryCompletionInformation.Rows[1].Level)
		require.Equal(t, "Info", queryCompletionInformation.Rows[1].LevelName)
		require.Equal(t, 0, queryCompletionInformation.Rows[1].StatusCode)
		require.Equal(t, "S_OK (0)", queryCompletionInformation.Rows[1].StatusCodeName)
		require.Equal(t, 5, queryCompletionInformation.Rows[1].EventType)
		require.Equal(t, "WorkloadGroup", queryCompletionInformation.Rows[1].EventTypeName)
		require.Equal(t, "{\"Count\":1,\"Text\":\"default\"}", queryCompletionInformation.Rows[1].Payload)

		require.NoError(t, f.advance())
		dataSetCompletion := DataSetCompletion{}
		require.NoError(t, f.unmarshal(&dataSetCompletion))

		require.Equal(t, false, dataSetCompletion.HasErrors)
		require.Equal(t, false, dataSetCompletion.Cancelled)
	}
}
