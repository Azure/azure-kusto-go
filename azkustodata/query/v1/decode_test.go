package v1

import (
	_ "embed"
	"github.com/stretchr/testify/assert"
	"io"
	"strings"
	"testing"
)

//go:embed testData/success.json
var successFile string

//go:embed testData/partialError.json
var partialErrorFile string

//go:embed testData/error.txt
var errorFile string

func TestDecodeSuccess(t *testing.T) {
	t.Parallel()

	reader := io.NopCloser(strings.NewReader(successFile))
	v1, err := decodeV1(reader)
	assert.NoError(t, err)
	assert.NotNil(t, v1)
	assert.Nil(t, v1.Exceptions)

	expectedTables := []struct {
		name    string
		columns []struct {
			name       string
			dataType   string
			columnType string
		}
		rows [][]interface{}
	}{
		{
			name: "Table_0",
			columns: []struct {
				name       string
				dataType   string
				columnType string
			}{
				{
					name:       "a",
					dataType:   "Int32",
					columnType: "int",
				},
			},
			rows: [][]interface{}{
				{float64(1)},
				{float64(2)},
				{float64(3)},
			},
		},
		{
			name: "Table_1",
			columns: []struct {
				name       string
				dataType   string
				columnType string
			}{
				{
					name:       "a",
					dataType:   "String",
					columnType: "string",
				},
				{
					name:       "b",
					dataType:   "Int32",
					columnType: "int",
				},
			},
			rows: [][]interface{}{
				{"a", float64(1)},
				{"b", float64(2)},
				{"c", float64(3)},
			},
		},
		{
			name: "Table_2",
			columns: []struct {
				name       string
				dataType   string
				columnType string
			}{
				{
					name:       "Value",
					dataType:   "String",
					columnType: "string",
				},
			},
			rows: [][]interface{}{
				{`{"Visualization":null,"Title":null,"XColumn":null,"Series":null,"YColumns":null,"AnomalyColumns":null,"XTitle":null,"YTitle":null,"XAxis":null,"YAxis":null,"Legend":null,"YSplit":null,"Accumulate":false,"IsQuerySorted":false,"Kind":null,"Ymin":"NaN","Ymax":"NaN","Xmin":null,"Xmax":null}`},
				{`{"Visualization":null,"Title":null,"XColumn":null,"Series":null,"YColumns":null,"AnomalyColumns":null,"XTitle":null,"YTitle":null,"XAxis":null,"YAxis":null,"Legend":null,"YSplit":null,"Accumulate":false,"IsQuerySorted":false,"Kind":null,"Ymin":"NaN","Ymax":"NaN","Xmin":null,"Xmax":null}`},
			},
		},
		{
			name: "Table_3",
			columns: []struct {
				name       string
				dataType   string
				columnType string
			}{
				{
					name:       "Timestamp",
					dataType:   "DateTime",
					columnType: "datetime",
				},
				{
					name:       "Severity",
					dataType:   "Int32",
					columnType: "int",
				},
				{
					name:       "SeverityName",
					dataType:   "String",
					columnType: "string",
				},
				{
					name:       "StatusCode",
					dataType:   "Int32",
					columnType: "int",
				},
				{
					name:       "StatusDescription",
					dataType:   "String",
					columnType: "string",
				},
				{
					name:       "Count",
					dataType:   "Int32",
					columnType: "int",
				},
				{
					name:       "RequestId",
					dataType:   "Guid",
					columnType: "guid",
				},
				{
					name:       "ActivityId",
					dataType:   "Guid",
					columnType: "guid",
				},
				{
					name:       "SubActivityId",
					dataType:   "Guid",
					columnType: "guid",
				},
				{
					name:       "ClientActivityId",
					dataType:   "String",
					columnType: "string",
				},
			},
			rows: [][]interface{}{
				{"2023-12-03T13:17:49.4832956Z", float64(4), "Info", float64(0), "Query completed successfully", float64(1), "6b4c0ab2-180e-46d8-b97e-593e6aea1e7a", "6b4c0ab2-180e-46d8-b97e-593e6aea1e7a", "2a41ff99-6429-418e-8bae-5cf703c5138a", "blab6"},
				{"2023-12-03T13:17:49.4832956Z", float64(6), "Stats", float64(0), `{"ExecutionTime":0.0,"resource_usage":{"cache":{"memory":{"hits":0,"misses":0,"total":0},"disk":{"hits":0,"misses":0,"total":0},"shards":{"hot":{"hitbytes":0,"missbytes":0,"retrievebytes":0},"cold":{"hitbytes":0,"missbytes":0,"retrievebytes":0},"bypassbytes":0}},"cpu":{"user":"00:00:00","kernel":"00:00:00","total cpu":"00:00:00"},"memory":{"peak_per_node":524384},"network":{"inter_cluster_total_bytes":962,"cross_cluster_total_bytes":0}},"input_dataset_statistics":{"extents":{"total":0,"scanned":0,"scanned_min_datetime":"0001-01-01T00:00:00.0000000Z","scanned_max_datetime":"0001-01-01T00:00:00.0000000Z"},"rows":{"total":0,"scanned":0},"rowstores":{"scanned_rows":0,"scanned_values_size":0},"shards":{"queries_generic":0,"queries_specialized":0}},"dataset_statistics":[{"table_row_count":3,"table_size":15},{"table_row_count":3,"table_size":43}],"cross_cluster_resource_usage":{}}`, float64(1),
					"6b4c0ab2-180e-46d8-b97e-593e6aea1e7a",
					"6b4c0ab2-180e-46d8-b97e-593e6aea1e7a",
					"2a41ff99-6429-418e-8bae-5cf703c5138a",
					"blab6"},
			},
		},
		{
			name: "Table_4",
			columns: []struct {
				name       string
				dataType   string
				columnType string
			}{
				{
					name:       "Ordinal",
					dataType:   "Int64",
					columnType: "long",
				},
				{
					name:       "Kind",
					dataType:   "String",
					columnType: "string",
				},
				{
					name:       "Name",
					dataType:   "String",
					columnType: "string",
				},
				{
					name:       "Id",
					dataType:   "String",
					columnType: "string",
				},
				{
					name:       "PrettyName",
					dataType:   "String",
					columnType: "string",
				},
			},
			rows: [][]interface{}{
				{float64(0), "QueryResult", "PrimaryResult", "e43f725a-26fd-4219-8869-30c21e1b139c", ""},
				{float64(1), "QueryResult", "PrimaryResult", "0f66e92a-8d0e-43da-8a66-ddb6bf84c49d", ""},
				{float64(2), "QueryProperties", "@ExtendedProperties", "d52bc55b-fc74-4a63-adb9-b72ff939e4c2", ""},
				{float64(3), "QueryStatus", "QueryStatus", "00000000-0000-0000-0000-000000000000", ""},
			},
		},
	}

	assert.Equal(t, len(expectedTables), len(v1.Tables))
	for i, expectedTable := range expectedTables {
		i := i
		expectedTable := expectedTable
		t.Run(expectedTable.name, func(t *testing.T) {
			t.Parallel()

			tb := v1.Tables[i]
			assert.Equal(t, expectedTable.name, tb.TableName)
			assert.Equal(t, len(expectedTable.columns), len(tb.Columns))
			for j, expectedColumn := range expectedTable.columns {
				column := tb.Columns[j]
				assert.Equal(t, expectedColumn.name, column.ColumnName)
				assert.Equal(t, expectedColumn.dataType, column.DataType)
				assert.Equal(t, expectedColumn.columnType, column.ColumnType)
			}
			assert.Equal(t, len(expectedTable.rows), len(tb.Rows))
			for j, expectedRow := range expectedTable.rows {
				row := tb.Rows[j]
				assert.Equal(t, len(expectedRow), len(row.Row))
				for k, expectedValue := range expectedRow {
					value := row.Row[k]
					assert.Equal(t, expectedValue, value)
				}
			}
		})
	}
}

func TestDecodeError(t *testing.T) {
	t.Parallel()

	reader := io.NopCloser(strings.NewReader(partialErrorFile))
	v1, err := decodeV1(reader)
	assert.NoError(t, err)
	assert.NotNil(t, v1)

	assert.Equal(t, 1, len(v1.Tables))
	tb := v1.Tables[0]
	assert.Equal(t, "Table_0", tb.TableName)
	assert.Equal(t, 1, len(tb.Columns))
	column := tb.Columns[0]
	assert.Equal(t, "a", column.ColumnName)
	assert.Equal(t, "Int32", column.DataType)
	assert.Equal(t, "int", column.ColumnType)
	assert.Equal(t, 2, len(tb.Rows))
	row := tb.Rows[0]
	assert.Equal(t, 1, len(row.Row))
	value := row.Row[0]
	assert.Equal(t, float64(1), value)
	row = tb.Rows[1]
	assert.Equal(t, 1, len(row.Errors))
	assert.Equal(t, "Query execution has exceeded the allowed limits (80DA0003): The results of this query exceed the set limit of 1 records, so not all records were returned (E_QUERY_RESULT_SET_TOO_LARGE, 0x80DA0003). See https://aka.ms/kustoquerylimits for more information and possible solutions..\r\n[0]Kusto.Data.Exceptions.KustoServicePartialQueryFailureLimitsExceededException: Query execution has exceeded the allowed limits (80DA0003): The results of this query exceed the set limit of 1 records, so not all records were returned (E_QUERY_RESULT_SET_TOO_LARGE, 0x80DA0003). See https://aka.ms/kustoquerylimits for more information and possible solutions..\r\nTimestamp=2023-12-03T13:12:01.8751538Z\r\nClientRequestId=blab6\r\nActivityId=123e27de-1e4e-49d9-b579-fe0b331d3642\r\nActivityType=GW.Http.CallContext\r\nServiceAlias=REDACTED\r\nMachineName=KSEngine000001\r\nProcessName=Kusto.WinSvc.Svc\r\nProcessId=1604\r\nThreadId=9752\r\nActivityStack=(Activity stack: CRID=blab6 ARID=123e27de-1e4e-49d9-b579-fe0b331d3642 > GW.Http.CallContext/123e27de-1e4e-49d9-b579-fe0b331d3642)\r\nMonitoredActivityContext=(ActivityType=GW.Http.CallContext, Timestamp=2023-12-03T13:12:01.8751538Z, ParentActivityId=123e27de-1e4e-49d9-b579-fe0b331d3642, TimeSinceStarted=0 [ms])ErrorCode=\r\nErrorReason=\r\nErrorMessage=\r\nDataSource=\r\nDatabaseName=\r\nClientRequestId=\r\nActivityId=123e27de-1e4e-49d9-b579-fe0b331d3642\r\nUnderlyingErrorCode=80DA0003\r\nUnderlyingErrorMessage=The results of this query exceed the set limit of 1 records, so not all records were returned (E_QUERY_RESULT_SET_TOO_LARGE, 0x80DA0003). See https://aka.ms/kustoquerylimits for more information and possible solutions.\r\n\r\n", row.Errors[0])

	assert.Equal(t, 1, len(v1.Exceptions))
	exception := v1.Exceptions[0]
	assert.Equal(t, "Query execution has exceeded the allowed limits (80DA0003): The results of this query exceed the set limit of 1 records, so not all records were returned (E_QUERY_RESULT_SET_TOO_LARGE, 0x80DA0003). See https://aka.ms/kustoquerylimits for more information and possible solutions..\r\n[0]Kusto.Data.Exceptions.KustoServicePartialQueryFailureLimitsExceededException: Query execution has exceeded the allowed limits (80DA0003): The results of this query exceed the set limit of 1 records, so not all records were returned (E_QUERY_RESULT_SET_TOO_LARGE, 0x80DA0003). See https://aka.ms/kustoquerylimits for more information and possible solutions..\r\nTimestamp=2023-12-03T13:12:01.8751538Z\r\nClientRequestId=blab6\r\nActivityId=123e27de-1e4e-49d9-b579-fe0b331d3642\r\nActivityType=GW.Http.CallContext\r\nServiceAlias=REDACTED\r\nMachineName=KSEngine000001\r\nProcessName=Kusto.WinSvc.Svc\r\nProcessId=1604\r\nThreadId=9752\r\nActivityStack=(Activity stack: CRID=blab6 ARID=123e27de-1e4e-49d9-b579-fe0b331d3642 > GW.Http.CallContext/123e27de-1e4e-49d9-b579-fe0b331d3642)\r\nMonitoredActivityContext=(ActivityType=GW.Http.CallContext, Timestamp=2023-12-03T13:12:01.8751538Z, ParentActivityId=123e27de-1e4e-49d9-b579-fe0b331d3642, TimeSinceStarted=0 [ms])ErrorCode=\r\nErrorReason=\r\nErrorMessage=\r\nDataSource=\r\nDatabaseName=\r\nClientRequestId=\r\nActivityId=123e27de-1e4e-49d9-b579-fe0b331d3642\r\nUnderlyingErrorCode=80DA0003\r\nUnderlyingErrorMessage=The results of this query exceed the set limit of 1 records, so not all records were returned (E_QUERY_RESULT_SET_TOO_LARGE, 0x80DA0003). See https://aka.ms/kustoquerylimits for more information and possible solutions.\r\n\r\n", exception)
}

func TestDecodeErrorText(t *testing.T) {
	t.Parallel()

	reader := io.NopCloser(strings.NewReader(errorFile))
	v1, err := decodeV1(reader)
	assert.ErrorContains(t, err, "General_BadRequest")
	assert.Nil(t, v1)
}
