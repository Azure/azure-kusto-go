package v2

import (
	"bytes"
	_ "embed"
	"github.com/Azure/azure-kusto-go/azkustodata/query"
	"github.com/Azure/azure-kusto-go/azkustodata/types"
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
		require.NoError(t, err)

		require.NotNil(t, f)

		require.NoError(t, f.advance())

		require.NoError(t, f.validateDataSetHeader())

		require.NoError(t, f.advance())

		propertiesTable, err := f.readQueryProperties()
		require.NoError(t, err)

		properties, err := query.ToStructs[QueryProperties](propertiesTable.Rows)
		require.NoError(t, err)

		require.Equal(t, 0, propertiesTable.TableId)
		require.Equal(t, "QueryProperties", propertiesTable.TableKind)
		require.Equal(t, "@ExtendedProperties", propertiesTable.TableName)
		require.Equal(t, 1, properties[0].TableId)
		require.Equal(t, "Visualization", properties[0].Key)
		require.Equal(t, map[string]interface{}{"Accumulate": false, "AnomalyColumns": interface{}(nil), "IsQuerySorted": false, "Kind": interface{}(nil), "Legend": interface{}(nil), "Series": interface{}(nil), "Title": interface{}(nil), "Visualization": interface{}(nil), "XAxis": interface{}(nil), "XColumn": interface{}(nil), "XTitle": interface{}(nil), "Xmax": interface{}(nil), "Xmin": interface{}(nil), "YAxis": interface{}(nil), "YColumns": interface{}(nil), "YSplit": interface{}(nil), "YTitle": interface{}(nil), "Ymax": "NaN", "Ymin": "NaN"}, properties[0].Value)

		require.NoError(t, f.advance())

		tableHeader := TableHeader{}
		require.NoError(t, f.unmarshal(&tableHeader))
		require.Equal(t, 1, tableHeader.TableId)
		require.Equal(t, "PrimaryResult", tableHeader.TableKind)
		require.Equal(t, "AllDataTypes", tableHeader.TableName)
		require.Equal(t, 10, len(tableHeader.Columns))
		require.Equal(t, "vnum", tableHeader.Columns[0].Name())
		require.Equal(t, types.Column("int"), tableHeader.Columns[0].Type())
		require.Equal(t, "vdec", tableHeader.Columns[1].Name())
		require.Equal(t, types.Column("decimal"), tableHeader.Columns[1].Type())
		require.Equal(t, "vdate", tableHeader.Columns[2].Name())
		require.Equal(t, types.Column("datetime"), tableHeader.Columns[2].Type())
		require.Equal(t, "vspan", tableHeader.Columns[3].Name())
		require.Equal(t, types.Column("timespan"), tableHeader.Columns[3].Type())
		require.Equal(t, "vobj", tableHeader.Columns[4].Name())
		require.Equal(t, types.Column("dynamic"), tableHeader.Columns[4].Type())
		require.Equal(t, "vb", tableHeader.Columns[5].Name())
		require.Equal(t, types.Column("bool"), tableHeader.Columns[5].Type())
		require.Equal(t, "vreal", tableHeader.Columns[6].Name())
		require.Equal(t, types.Column("real"), tableHeader.Columns[6].Type())
		require.Equal(t, "vstr", tableHeader.Columns[7].Name())
		require.Equal(t, types.Column("string"), tableHeader.Columns[7].Type())
		require.Equal(t, "vlong", tableHeader.Columns[8].Name())
		require.Equal(t, types.Column("long"), tableHeader.Columns[8].Type())
		require.Equal(t, "vguid", tableHeader.Columns[9].Name())
		require.Equal(t, types.Column("guid"), tableHeader.Columns[9].Type())

		require.NoError(t, f.advance())

		tableFragment := TableFragment{Columns: tableHeader.Columns}
		require.NoError(t, f.unmarshal(&tableFragment))

		type AllDataType struct {
			Vnum  *int32                 `kusto:"vnum"`
			Vdec  *decimal.Decimal       `kusto:"vdec"`
			Vdate *time.Time             `kusto:"vdate"`
			Vspan *time.Duration         `kusto:"vspan"`
			Vobj  map[string]interface{} `kusto:"vobj"`
			Vb    *bool                  `kusto:"vb"`
			Vreal *float64               `kusto:"vreal"`
			Vstr  string                 `kusto:"vstr"`
			Vlong *int64                 `kusto:"vlong"`
			Vguid *uuid.UUID             `kusto:"vguid"`
		}

		data, err := query.ToStructs[AllDataType](tableFragment.Rows)
		require.NoError(t, err)

		require.Equal(t, int32(1), *data[0].Vnum)
		require.Equal(t, decimal.RequireFromString("2.00000000000001"), *data[0].Vdec)
		require.Equal(t, "2020-03-04T14:05:01.3109965Z", data[0].Vdate.Format(time.RFC3339Nano))
		require.Equal(t, "1h23m45.6789s", data[0].Vspan.String())
		require.Equal(t, map[string]interface{}{"moshe": "value"}, data[0].Vobj)
		require.Equal(t, true, *data[0].Vb)
		require.Equal(t, 0.01, *data[0].Vreal)
		require.Equal(t, "asdf", data[0].Vstr)
		require.Equal(t, int64(9223372036854775807), *data[0].Vlong)
		require.Equal(t, "123e27de-1e4e-49d9-b579-fe0b331d3642", data[0].Vguid.String())

		require.Equal(t, (*int32)(nil), data[1].Vnum)
		require.Equal(t, (*decimal.Decimal)(nil), data[1].Vdec)
		require.Equal(t, (*time.Time)(nil), data[1].Vdate)
		require.Equal(t, (*time.Duration)(nil), data[1].Vspan)
		require.Equal(t, (map[string]interface{})(nil), data[1].Vobj)
		require.Equal(t, (*bool)(nil), data[1].Vb)
		require.Equal(t, (*float64)(nil), data[1].Vreal)
		require.Equal(t, "", data[1].Vstr)
		require.Equal(t, (*int64)(nil), data[1].Vlong)
		require.Equal(t, (*uuid.UUID)(nil), data[1].Vguid)

		require.NoError(t, f.advance())

		tableCompletion := TableCompletion{}
		require.NoError(t, f.unmarshal(&tableCompletion))
		require.Equal(t, 1, tableCompletion.TableId)
		require.Equal(t, 2, tableCompletion.RowCount)

		require.NoError(t, f.advance())

		queryCompletionInformationTable, err := f.readQueryCompletionInformation()

		require.NoError(t, err)

		queryCompletionInformation, err := query.ToStructs[QueryCompletionInformation](queryCompletionInformationTable.Rows)
		require.NoError(t, err)

		require.Equal(t, "QueryCompletionInformation", queryCompletionInformationTable.TableKind)
		require.Equal(t, "QueryCompletionInformation", queryCompletionInformationTable.TableName)
		require.Equal(t, 2, queryCompletionInformationTable.TableId)
		require.Equal(t, 2, len(queryCompletionInformationTable.Rows))
		require.Equal(t, "2023-11-26T13:34:17.0731478Z", queryCompletionInformation[0].Timestamp.Format(time.RFC3339Nano))
		require.Equal(t, "blab6", queryCompletionInformation[0].ClientRequestId)
		require.Equal(t, "123e27de-1e4e-49d9-b579-fe0b331d3642", queryCompletionInformation[0].ActivityId.String())
		require.Equal(t, "123e27de-1e4e-49d9-b579-fe0b331d3642", queryCompletionInformation[0].SubActivityId.String())
		require.Equal(t, "123e27de-1e4e-49d9-b579-fe0b331d3642", queryCompletionInformation[0].ParentActivityId.String())
		require.Equal(t, 4, queryCompletionInformation[0].Level)
		require.Equal(t, "Info", queryCompletionInformation[0].LevelName)
		require.Equal(t, 0, queryCompletionInformation[0].StatusCode)
		require.Equal(t, "S_OK (0)", queryCompletionInformation[0].StatusCodeName)
		require.Equal(t, 4, queryCompletionInformation[0].EventType)
		require.Equal(t, "QueryInfo", queryCompletionInformation[0].EventTypeName)
		require.Equal(t, "{\"Count\":1,\"Text\":\"Query completed successfully\"}", queryCompletionInformation[0].Payload)

		require.Equal(t, "2023-11-26T13:34:17.0731478Z", queryCompletionInformation[1].Timestamp.Format(time.RFC3339Nano))
		require.Equal(t, "blab6", queryCompletionInformation[1].ClientRequestId)
		require.Equal(t, "123e27de-1e4e-49d9-b579-fe0b331d3642", queryCompletionInformation[1].ActivityId.String())
		require.Equal(t, "123e27de-1e4e-49d9-b579-fe0b331d3642", queryCompletionInformation[1].SubActivityId.String())
		require.Equal(t, "123e27de-1e4e-49d9-b579-fe0b331d3642", queryCompletionInformation[1].ParentActivityId.String())
		require.Equal(t, 4, queryCompletionInformation[1].Level)
		require.Equal(t, "Info", queryCompletionInformation[1].LevelName)
		require.Equal(t, 0, queryCompletionInformation[1].StatusCode)
		require.Equal(t, "S_OK (0)", queryCompletionInformation[1].StatusCodeName)
		require.Equal(t, 5, queryCompletionInformation[1].EventType)
		require.Equal(t, "WorkloadGroup", queryCompletionInformation[1].EventTypeName)
		require.Equal(t, "{\"Count\":1,\"Text\":\"default\"}", queryCompletionInformation[1].Payload)

		require.NoError(t, f.advance())
		dataSetCompletion := DataSetCompletion{}
		require.NoError(t, f.unmarshal(&dataSetCompletion))

		require.Equal(t, false, dataSetCompletion.HasErrors)
		require.Equal(t, false, dataSetCompletion.Cancelled)
	}
}
