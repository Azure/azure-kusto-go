package etoe

import (
	"context"
	"encoding/csv"
	"fmt"
	"github.com/shopspring/decimal"
	"go.uber.org/goleak"
	"io"
	"math/rand"
	"os"
	"regexp"
	"strings"
	"sync"
	"testing"
	"time"
	"unicode"

	"github.com/Azure/azure-kusto-go/kusto"
	"github.com/Azure/azure-kusto-go/kusto/data/errors"
	"github.com/Azure/azure-kusto-go/kusto/data/table"
	"github.com/Azure/azure-kusto-go/kusto/data/types"
	"github.com/Azure/azure-kusto-go/kusto/data/value"
	"github.com/Azure/azure-kusto-go/kusto/ingest"
	"github.com/Azure/azure-kusto-go/kusto/internal/frames"
	"github.com/Azure/azure-kusto-go/kusto/kql"
	"github.com/Azure/azure-kusto-go/kusto/unsafe"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var (
	pCountStmt = kusto.NewStmt("table(tableName) | count").MustDefinitions(
		kusto.NewDefinitions().Must(
			kusto.ParamTypes{
				"tableName": kusto.ParamType{Type: types.String},
			},
		),
	)
	pTableStmt = kusto.NewStmt("table(tableName)").MustDefinitions(
		kusto.NewDefinitions().Must(
			kusto.ParamTypes{
				"tableName": kusto.ParamType{Type: types.String},
			},
		),
	)

	// This is needed because of a bug in the backend that sometimes causes the tables not to drop and get stuck.
	clearStreamingCacheStatement = kusto.NewStmt(".clear database cache streamingingestion schema")
)

type CountResult struct {
	Count int64
}

type MgmtProjectionResult struct {
	A string
}

type AllDataType struct {
	Vnum  int32                  `kusto:"vnum"`
	Vdec  value.Decimal          `kusto:"vdec"`
	Vdate time.Time              `kusto:"vdate"`
	Vspan value.Timespan         `kusto:"vspan"`
	Vobj  map[string]interface{} `kusto:"vobj"`
	Vb    bool                   `kusto:"vb"`
	Vreal float64                `kusto:"vreal"`
	Vstr  string                 `kusto:"vstr"`
	Vlong int64                  `kusto:"vlong"`
	Vguid value.GUID             `kusto:"vguid"`
}

type DynamicTypeVariations struct {
	PlainValue value.Dynamic
	PlainArray value.Dynamic
	PlainJson  value.Dynamic
	JsonArray  value.Dynamic
}

type LogRow struct {
	HeaderTime       value.DateTime `kusto:"header_time"`
	HeaderId         value.GUID     `kusto:"header_id"`
	HeaderApiVersion value.String   `kusto:"header_api_version"`
	PayloadData      value.String   `kusto:"payload_data"`
	PayloadUser      value.String   `kusto:"payload_user"`
}

func (lr LogRow) CSVMarshal() []string {
	return []string{
		lr.HeaderTime.String(),
		lr.HeaderId.String(),
		lr.HeaderApiVersion.String(),
		lr.PayloadData.String(),
		lr.PayloadUser.String(),
	}
}

type queryFunc func(ctx context.Context, db string, query kusto.Statement, options ...kusto.QueryOption) (*kusto.RowIterator, error)

type mgmtFunc func(ctx context.Context, db string, query kusto.Stmt, options ...kusto.MgmtOption) (*kusto.RowIterator, error)

type queryJsonFunc func(ctx context.Context, db string, query kusto.Statement, options ...kusto.QueryOption) (string, error)

func TestQueries(t *testing.T) {
	t.Parallel()

	if skipETOE || testing.Short() {
		t.Skipf("end to end tests disabled: missing config.json file in etoe directory")
	}

	_, cancel := context.WithCancel(context.Background())
	defer cancel()

	client, err := kusto.New(testConfig.kcsb)
	if err != nil {
		panic(err)
	}

	t.Cleanup(func() {
		t.Log("Closing client")
		require.NoError(t, client.Close())
		t.Log("Closed client")
	})

	pCountStmt := kusto.NewStmt("table(tableName) | count").MustDefinitions(
		kusto.NewDefinitions().Must(
			kusto.ParamTypes{
				"tableName": kusto.ParamType{Type: types.String},
			},
		),
	)

	allDataTypesTable := fmt.Sprintf("goe2e_all_data_types_%d_%d", time.Now().UnixNano(), rand.Int())
	err = createIngestionTable(t, client, allDataTypesTable, true)
	require.NoError(t, err)

	tests := []struct {
		// desc is a description of a test.
		desc string
		// stmt is the Kusot Stmt that will be sent.
		stmt kusto.Stmt
		// setup is a function that will be called before the test runs.
		setup func() error
		// teardown is a functiont that will be called before the test ends.
		teardown func() error
		qcall    queryFunc
		mcall    mgmtFunc
		qjcall   queryJsonFunc
		options  interface{} // either []kusto.QueryOption or []kusto.MgmtOption
		// doer is called from within the function passed to RowIterator.Do(). It allows us to collect the data we receive.
		doer func(row *table.Row, update interface{}) error
		// gotInit creates the variable that will be used by doer's update argument.
		gotInit func() interface{}
		// want is the data we want to receive from the query.
		want interface{}
	}{
		{
			desc: "Query: Retrieve count of the number of rows that match",
			stmt: pCountStmt.MustParameters(
				kusto.NewParameters().Must(kusto.QueryValues{"tableName": allDataTypesTable}),
			),
			qcall: client.Query,
			doer: func(row *table.Row, update interface{}) error {
				rec := CountResult{}
				if err := row.ToStruct(&rec); err != nil {
					return err
				}
				recs := update.(*[]CountResult)
				*recs = append(*recs, rec)
				return nil
			},
			gotInit: func() interface{} {
				v := []CountResult{}
				return &v
			},
			want: &[]CountResult{{Count: 1}},
		},
		{
			desc:  "Mgmt(regression github.com/Azure/azure-kusto-go/issues/11): make sure we can retrieve .show databases, but we do not check the results at this time",
			stmt:  kusto.NewStmt(`.show databases`),
			mcall: client.Mgmt,
			doer: func(row *table.Row, update interface{}) error {
				return nil
			},
			gotInit: func() interface{} {
				return nil
			},
		},
		{
			desc:  "Mgmt(https://github.com/Azure/azure-kusto-go/issues/55): transformations on mgmt queries",
			stmt:  kusto.NewStmt(`.show databases | project A="1" | take 1`),
			mcall: client.Mgmt,
			doer: func(row *table.Row, update interface{}) error {
				rec := MgmtProjectionResult{}
				if err := row.ToStruct(&rec); err != nil {
					return err
				}
				recs := update.(*[]MgmtProjectionResult)
				*recs = append(*recs, rec)
				return nil
			},
			gotInit: func() interface{} {
				v := []MgmtProjectionResult{}
				return &v
			},
			want: &[]MgmtProjectionResult{{A: "1"}},
		},
		{
			desc: "Mgmt(https://github.com/Azure/azure-kusto-go/issues/55): transformations on mgmt queries - multiple tables",
			stmt: kusto.NewStmt(`.show databases | project A="1" | take 1;`, kusto.UnsafeStmt(unsafe.Stmt{Add: true})).UnsafeAdd(allDataTypesTable).Add(
				" | project A=\"2\" | take 1"),
			mcall: client.Mgmt,
			doer: func(row *table.Row, update interface{}) error {
				rec := MgmtProjectionResult{}
				if err := row.ToStruct(&rec); err != nil {
					return err
				}
				recs := update.(*[]MgmtProjectionResult)
				*recs = append(*recs, rec)
				return nil
			},
			gotInit: func() interface{} {
				v := []MgmtProjectionResult{}
				return &v
			},
			want: &[]MgmtProjectionResult{{A: "1"}, {A: "2"}},
		},
		{
			desc:  "Query: Progressive query: make sure we can convert all data types from a row",
			stmt:  pTableStmt.MustParameters(kusto.NewParameters().Must(kusto.QueryValues{"tableName": allDataTypesTable})),
			qcall: client.Query,
			doer: func(row *table.Row, update interface{}) error {
				rec := AllDataType{}
				if err := row.ToStruct(&rec); err != nil {
					return err
				}

				valuesRec := AllDataType{}

				err := row.ExtractValues(&valuesRec.Vnum,
					&valuesRec.Vdec,
					&valuesRec.Vdate,
					&valuesRec.Vspan,
					&valuesRec.Vobj,
					&valuesRec.Vb,
					&valuesRec.Vreal,
					&valuesRec.Vstr,
					&valuesRec.Vlong,
					&valuesRec.Vguid,
				)

				if err != nil {
					return err
				}

				assert.Equal(t, rec, valuesRec)

				recs := update.(*[]AllDataType)
				*recs = append(*recs, rec)
				return nil
			},
			gotInit: func() interface{} {
				ad := []AllDataType{}
				return &ad
			},
			want: &[]AllDataType{getExpectedResult()},
		},
		{
			desc:    "Query: Non-Progressive query: make sure we can convert all data types from a row",
			stmt:    pTableStmt.MustParameters(kusto.NewParameters().Must(kusto.QueryValues{"tableName": allDataTypesTable})),
			qcall:   client.Query,
			options: []kusto.QueryOption{kusto.ResultsProgressiveDisable()},
			doer: func(row *table.Row, update interface{}) error {
				rec := AllDataType{}
				if err := row.ToStruct(&rec); err != nil {
					return err
				}

				valuesRec := AllDataType{}

				err := row.ExtractValues(&valuesRec.Vnum,
					&valuesRec.Vdec,
					&valuesRec.Vdate,
					&valuesRec.Vspan,
					&valuesRec.Vobj,
					&valuesRec.Vb,
					&valuesRec.Vreal,
					&valuesRec.Vstr,
					&valuesRec.Vlong,
					&valuesRec.Vguid,
				)

				if err != nil {
					return err
				}

				assert.Equal(t, rec, valuesRec)

				recs := update.(*[]AllDataType)
				*recs = append(*recs, rec)
				return nil
			},
			gotInit: func() interface{} {
				ad := []AllDataType{}
				return &ad
			},
			want: &[]AllDataType{getExpectedResult()},
		},
		{
			desc: "Query: All parameter types are working",
			stmt: pTableStmt.Add(" | where  vnum == num and vdec == dec and vdate == dt and vspan == span and tostring(vobj) == tostring(obj) and vb == b and vreal" +
				" == rl and vstr == str and vlong == lg and vguid == guid ").
				MustDefinitions(kusto.NewDefinitions().Must(
					kusto.ParamTypes{
						"tableName": kusto.ParamType{Type: types.String},
						"num":       kusto.ParamType{Type: types.Int},
						"dec":       kusto.ParamType{Type: types.Decimal},
						"dt":        kusto.ParamType{Type: types.DateTime},
						"span":      kusto.ParamType{Type: types.Timespan},
						"obj":       kusto.ParamType{Type: types.Dynamic},
						"b":         kusto.ParamType{Type: types.Bool},
						"rl":        kusto.ParamType{Type: types.Real},
						"str":       kusto.ParamType{Type: types.String},
						"lg":        kusto.ParamType{Type: types.Long},
						"guid":      kusto.ParamType{Type: types.GUID},
					})).
				MustParameters(kusto.NewParameters().Must(kusto.
					QueryValues{
					"tableName": allDataTypesTable,
					"num":       int32(1),
					"dec":       "2.00000000000001",
					"dt":        time.Date(2020, 03, 04, 14, 05, 01, 310996500, time.UTC),
					"span":      time.Hour + 23*time.Minute + 45*time.Second + 678900000*time.Nanosecond,
					"obj":       map[string]interface{}{"moshe": "value"},
					"b":         true,
					"rl":        0.01,
					"str":       "asdf",
					"lg":        int64(9223372036854775807),
					"guid":      uuid.MustParse("74be27de-1e4e-49d9-b579-fe0b331d3642"),
				})),
			qcall:   client.Query,
			options: []kusto.QueryOption{kusto.ResultsProgressiveDisable()},
			doer: func(row *table.Row, update interface{}) error {
				rec := AllDataType{}
				if err := row.ToStruct(&rec); err != nil {
					return err
				}

				valuesRec := AllDataType{}

				err := row.ExtractValues(&valuesRec.Vnum,
					&valuesRec.Vdec,
					&valuesRec.Vdate,
					&valuesRec.Vspan,
					&valuesRec.Vobj,
					&valuesRec.Vb,
					&valuesRec.Vreal,
					&valuesRec.Vstr,
					&valuesRec.Vlong,
					&valuesRec.Vguid,
				)

				if err != nil {
					return err
				}

				assert.Equal(t, rec, valuesRec)

				recs := update.(*[]AllDataType)
				*recs = append(*recs, rec)
				return nil
			},
			gotInit: func() interface{} {
				ad := []AllDataType{}
				return &ad
			},
			want: &[]AllDataType{getExpectedResult()},
		},
		{
			desc: "Query: All parameter types are working with defaults",
			stmt: pTableStmt.Add(" | where  vnum == num and vdec == dec and vdate == dt and vspan == span and vb == b and vreal == rl and vstr == str and vlong == lg and vguid == guid ").
				MustDefinitions(kusto.NewDefinitions().Must(
					kusto.ParamTypes{
						"tableName": kusto.ParamType{Type: types.String, Default: allDataTypesTable},
						"num":       kusto.ParamType{Type: types.Int, Default: int32(1)},
						"dec":       kusto.ParamType{Type: types.Decimal, Default: "2.00000000000001"},
						"dt":        kusto.ParamType{Type: types.DateTime, Default: time.Date(2020, 03, 04, 14, 05, 01, 310996500, time.UTC)},
						"span":      kusto.ParamType{Type: types.Timespan, Default: time.Hour + 23*time.Minute + 45*time.Second + 678900000*time.Nanosecond},
						"b":         kusto.ParamType{Type: types.Bool, Default: true},
						"rl":        kusto.ParamType{Type: types.Real, Default: 0.01},
						"str":       kusto.ParamType{Type: types.String, Default: "asdf"},
						"lg":        kusto.ParamType{Type: types.Long, Default: int64(9223372036854775807)},
						"guid":      kusto.ParamType{Type: types.GUID, Default: uuid.MustParse("74be27de-1e4e-49d9-b579-fe0b331d3642")},
					})),
			qcall:   client.Query,
			options: []kusto.QueryOption{kusto.ResultsProgressiveDisable()},
			doer: func(row *table.Row, update interface{}) error {
				rec := AllDataType{}
				if err := row.ToStruct(&rec); err != nil {
					return err
				}

				valuesRec := AllDataType{}

				err := row.ExtractValues(&valuesRec.Vnum,
					&valuesRec.Vdec,
					&valuesRec.Vdate,
					&valuesRec.Vspan,
					&valuesRec.Vobj,
					&valuesRec.Vb,
					&valuesRec.Vreal,
					&valuesRec.Vstr,
					&valuesRec.Vlong,
					&valuesRec.Vguid,
				)

				if err != nil {
					return err
				}

				assert.Equal(t, rec, valuesRec)

				recs := update.(*[]AllDataType)
				*recs = append(*recs, rec)
				return nil
			},
			gotInit: func() interface{} {
				ad := []AllDataType{}
				return &ad
			},
			want: &[]AllDataType{getExpectedResult()},
		},
		{
			desc:    "Query: make sure Dynamic data type variations can be parsed",
			stmt:    kusto.NewStmt(`print PlainValue = dynamic('1'), PlainArray = dynamic('[1,2,3]'), PlainJson= dynamic('{ "a": 1}'), JsonArray= dynamic('[{ "a": 1}, { "a": 2}]')`),
			qcall:   client.Query,
			options: []kusto.QueryOption{kusto.ResultsProgressiveDisable()},
			doer: func(row *table.Row, update interface{}) error {
				rec := DynamicTypeVariations{}
				if err := row.ToStruct(&rec); err != nil {
					return err
				}
				recs := update.(*[]DynamicTypeVariations)

				valuesRec := DynamicTypeVariations{}

				err := row.ExtractValues(&valuesRec.PlainValue,
					&valuesRec.PlainArray,
					&valuesRec.PlainJson,
					&valuesRec.JsonArray,
				)

				if err != nil {
					return err
				}

				assert.Equal(t, rec, valuesRec)

				*recs = append(*recs, rec)
				return nil
			},
			gotInit: func() interface{} {
				ad := []DynamicTypeVariations{}
				return &ad
			},
			want: &[]DynamicTypeVariations{
				{
					PlainValue: value.Dynamic{Value: []byte("1"), Valid: true},
					PlainArray: value.Dynamic{Value: []byte("[1,2,3]"), Valid: true},
					PlainJson:  value.Dynamic{Value: []byte(`{ "a": 1}`), Valid: true},
					JsonArray:  value.Dynamic{Value: []byte(`[{ "a": 1}, { "a": 2}]`), Valid: true},
				},
			},
		},
		{
			desc: "Query: Use many options",
			stmt: pCountStmt.MustParameters(
				kusto.NewParameters().Must(kusto.QueryValues{"tableName": allDataTypesTable}),
			),
			options: []kusto.QueryOption{kusto.QueryNow(time.Now()), kusto.NoRequestTimeout(), kusto.NoTruncation(), kusto.RequestAppName("bd1e472c-a8e4-4c6e-859d-c86d72253197"),
				kusto.RequestDescription("9bff424f-711d-48b8-9a6e-d3a618748334"), kusto.Application("aaa"), kusto.User("bbb"),
				kusto.CustomQueryOption("additional", "additional")},
			qcall: client.Query,
			doer: func(row *table.Row, update interface{}) error {
				rec := CountResult{}
				if err := row.ToStruct(&rec); err != nil {
					return err
				}
				recs := update.(*[]CountResult)
				*recs = append(*recs, rec)
				return nil
			},
			gotInit: func() interface{} {
				v := []CountResult{}
				return &v
			},
			want: &[]CountResult{{Count: 1}},
		},
		{
			desc: "Query: get json",
			stmt: pCountStmt.MustParameters(
				kusto.NewParameters().Must(kusto.QueryValues{"tableName": allDataTypesTable}),
			),
			qjcall: client.QueryToJson,
			want:   "[{\"FrameType\":\"DataSetHeader\",\"IsProgressive\":true,\"Version\":\"v2.0\"},{\"FrameType\":\"DataTable\",\"TableId\":<NUM>,\"TableKind\":\"QueryProperties\",\"TableName\":\"@ExtendedProperties\",\"Columns\":[{\"ColumnName\":\"TableId\",\"ColumnType\":\"int\"},{\"ColumnName\":\"Key\",\"ColumnType\":\"string\"},{\"ColumnName\":\"Value\",\"ColumnType\":\"dynamic\"}],\"Rows\":[[1,\"Visualization\",\"{\\\"Visualization\\\":null,\\\"Title\\\":null,\\\"XColumn\\\":null,\\\"Series\\\":null,\\\"YColumns\\\":null,\\\"AnomalyColumns\\\":null,\\\"XTitle\\\":null,\\\"YTitle\\\":null,\\\"XAxis\\\":null,\\\"YAxis\\\":null,\\\"Legend\\\":null,\\\"YSplit\\\":null,\\\"Accumulate\\\":false,\\\"IsQuerySorted\\\":false,\\\"Kind\\\":null,\\\"Ymin\\\":\\\"NaN\\\",\\\"Ymax\\\":\\\"NaN\\\",\\\"Xmin\\\":null,\\\"Xmax\\\":null}\"]]},{\"FrameType\":\"TableHeader\",\"TableId\":<NUM>,\"TableKind\":\"PrimaryResult\",\"TableName\":\"PrimaryResult\",\"Columns\":[{\"ColumnName\":\"Count\",\"ColumnType\":\"long\"}]},{\"FrameType\":\"TableFragment\",\"TableFragmentType\":\"DataAppend\",\"TableId\":<NUM>,\"Rows\":[[1]]},{\"FrameType\":\"TableProgress\",\"TableId\":<NUM>,\"TableProgress\"<TIME>},{\"FrameType\":\"TableCompletion\",\"TableId\":<NUM>,\"RowCount\":1},{\"FrameType\":\"DataTable\",\"TableId\":<NUM>,\"TableKind\":\"QueryCompletionInformation\",\"TableName\":\"QueryCompletionInformation\",\"Columns\":[{\"ColumnName\":\"Timestamp\",\"ColumnType\":\"datetime\"},{\"ColumnName\":\"ClientRequestId\",\"ColumnType\":\"string\"},{\"ColumnName\":\"ActivityId\",\"ColumnType\":\"guid\"},{\"ColumnName\":\"SubActivityId\",\"ColumnType\":\"guid\"},{\"ColumnName\":\"ParentActivityId\",\"ColumnType\":\"guid\"},{\"ColumnName\":\"Level\",\"ColumnType\":\"int\"},{\"ColumnName\":\"LevelName\",\"ColumnType\":\"string\"},{\"ColumnName\":\"StatusCode\",\"ColumnType\":\"int\"},{\"ColumnName\":\"StatusCodeName\",\"ColumnType\":\"string\"},{\"ColumnName\":\"EventType\",\"ColumnType\":\"int\"},{\"ColumnName\":\"EventTypeName\",\"ColumnType\":\"string\"},{\"ColumnName\":\"Payload\",\"ColumnType\":\"string\"}],\"Rows\":[[\"<TIME>\",\"KGC.execute;<GUID>\",\"<GUID>\",\"<GUID>\",\"<GUID>\",4,\"Info\",0,\"S_OK (0)\",4,\"QueryInfo\",\"{\\\"Count\\\":<NUM>,\\\"Text\\\":\\\"Query completed successfully\\\"}\"],[\"<TIME>\",\"KGC.execute;<GUID>\",\"<GUID>\",\"<GUID>\",\"<GUID>\",4,\"Info\",0,\"S_OK (0)\",5,\"WorkloadGroup\",\"{\\\"Count\\\":<NUM>,\\\"Text\\\":\\\"default\\\"}\"],[\"<TIME>\",\"KGC.execute;<GUID>\",\"<GUID>\",\"<GUID>\",\"<GUID>\",4,\"Info\",0,\"S_OK (0)\",6,\"EffectiveRequestOptions\",\"{\\\"Count\\\":<NUM>,\\\"Text\\\":\\\"{\\\\\\\"DataScope\\\\\\\":\\\\\\\"All\\\\\\\",\\\\\\\"QueryConsistency\\\\\\\":\\\\\\\"strongconsistency\\\\\\\",\\\\\\\"MaxMemoryConsumptionPerIterator\\\\\\\":<NUM>,\\\\\\\"MaxMemoryConsumptionPerQueryPerNode\\\\\\\":<NUM>,\\\\\\\"QueryFanoutNodesPercent\\\\\\\":<NUM>,\\\\\\\"QueryFanoutThreadsPercent\\\\\\\":100}\\\"}\"],[\"<TIME>\",\"KGC.execute;<GUID>\",\"<GUID>\",\"<GUID>\",\"<GUID>\",6,\"Stats\",0,\"S_OK (0)\",0,\"QueryResourceConsumption\",\"{\\\"ExecutionTime\\\"<TIME>,\\\"resource_usage\\\":{\\\"cache\\\":{\\\"memory\\\":{\\\"hits\\\":<NUM>,\\\"misses\\\":<NUM>,\\\"total\\\":0},\\\"disk\\\":{\\\"hits\\\":<NUM>,\\\"misses\\\":<NUM>,\\\"total\\\":0},\\\"shards\\\":{\\\"hot\\\":{\\\"hitbytes\\\":<NUM>,\\\"missbytes\\\":<NUM>,\\\"retrievebytes\\\":0},\\\"cold\\\":{\\\"hitbytes\\\":<NUM>,\\\"missbytes\\\":<NUM>,\\\"retrievebytes\\\":0},\\\"bypassbytes\\\":0}},\\\"cpu\\\":{\\\"user\\\":\\\"00:00:00\\\",\\\"kernel\\\":\\\"00:00:00\\\",\\\"total cpu\\\":\\\"00:00:00\\\"},\\\"memory\\\":{\\\"peak_per_node\\\":524384},\\\"network\\\":{\\\"inter_cluster_total_bytes\\\":<NUM>,\\\"cross_cluster_total_bytes\\\":0}},\\\"input_dataset_statistics\\\":{\\\"extents\\\":{\\\"total\\\":<NUM>,\\\"scanned\\\":<NUM>,\\\"scanned_min_datetime\\\":\\\"<TIME>\\\",\\\"scanned_max_datetime\\\":\\\"<TIME>\\\"},\\\"rows\\\":{\\\"total\\\":<NUM>,\\\"scanned\\\":0},\\\"rowstores\\\":{\\\"scanned_rows\\\":<NUM>,\\\"scanned_values_size\\\":0},\\\"shards\\\":{\\\"queries_generic\\\":<NUM>,\\\"queries_specialized\\\":0}},\\\"dataset_statistics\\\":[{\\\"table_row_count\\\":<NUM>,\\\"table_size\\\":9}],\\\"cross_cluster_resource_usage\\\":{}}\"]]},{\"FrameType\":\"DataSetCompletion\",\"HasErrors\":false,\"Cancelled\":false}]",
		},
	}

	for _, test := range tests {
		test := test // capture
		t.Run(test.desc, func(t *testing.T) {
			t.Parallel()
			if test.setup != nil {
				if err := test.setup(); err != nil {
					panic(err)
				}
			}
			if test.teardown != nil {
				defer func() {
					if err := test.teardown(); err != nil {
						panic(err)
					}
				}()
			}

			var iter *kusto.RowIterator
			var err error
			switch {
			case test.qcall != nil:
				var options []kusto.QueryOption
				if test.options != nil {
					options = test.options.([]kusto.QueryOption)
				}
				iter, err = test.qcall(context.Background(), testConfig.Database, test.stmt, options...)

				require.Nilf(t, err, "TestQueries(%s): had test.qcall error: %s", test.desc, err)

			case test.mcall != nil:
				var options []kusto.MgmtOption
				if test.options != nil {
					options = test.options.([]kusto.MgmtOption)
				}
				iter, err = test.mcall(context.Background(), testConfig.Database, test.stmt, options...)

				require.Nilf(t, err, "TestQueries(%s): had test.mcall error: %s", test.desc, err)

			case test.qjcall != nil:
				var options []kusto.QueryOption
				if test.options != nil {
					options = test.options.([]kusto.QueryOption)
				}
				json, err := test.qjcall(context.Background(), testConfig.Database, test.stmt, options...)
				require.Nilf(t, err, "TestQueries(%s): had test.qjcall error: %s", test.desc, err)

				// replace guids with <GUID>
				guidRegex := regexp.MustCompile(`(\w+-){4}\w+`)
				json = guidRegex.ReplaceAllString(json, "<GUID>")

				timeRegex := regexp.MustCompile(`([0:]+\.(\d)+)|([\d\-]+T[\d\-.:]+Z)`)
				json = timeRegex.ReplaceAllString(json, "<TIME>")

				numRegex := regexp.MustCompile(`":\d+,`)
				json = numRegex.ReplaceAllString(json, `":<NUM>,`)

				require.Equal(t, test.want, json)
				return

			default:
				require.Fail(t, "test setup failure")
			}

			defer iter.Stop()

			var got = test.gotInit()
			err = iter.DoOnRowOrError(func(row *table.Row, e *errors.Error) error {
				return test.doer(row, got)
			})

			require.Nilf(t, err, "TestQueries(%s): had iter.Do() error: %s", test.desc, err)

			require.Equal(t, test.want, got)
		})
	}
}

func TestStatement(t *testing.T) {
	t.Parallel()

	if skipETOE || testing.Short() {
		t.Skipf("end to end tests disabled: missing config.json file in etoe directory")
	}

	_, cancel := context.WithCancel(context.Background())
	defer cancel()

	client, err := kusto.New(testConfig.kcsb)
	if err != nil {
		panic(err)
	}

	t.Cleanup(func() {
		t.Log("Closing client")
		require.NoError(t, client.Close())
		t.Log("Closed client")
	})

	allDataTypesTable := fmt.Sprintf("goe2e_all_data_types_%d_%d", time.Now().UnixNano(), rand.Int())
	require.NoError(t, createIngestionTable(t, client, allDataTypesTable, true))
	dt, err := time.Parse(time.RFC3339Nano, "2020-03-04T14:05:01.3109965Z")
	ts, err := time.ParseDuration("1h23m45.6789s")
	guid, err := uuid.Parse("74be27de-1e4e-49d9-b579-fe0b331d3642")
	tests := []struct {
		// desc is a description of a test.
		desc string
		// stmt is the Kusot Stmt that will be sent.
		stmt kusto.Statement
		// setup is a function that will be called before the test runs.
		setup func() error
		// teardown is a functiont that will be called before the test ends.
		teardown func() error
		qcall    queryFunc
		mcall    mgmtFunc
		qjcall   queryJsonFunc
		options  interface{} // either []kusto.QueryOption or []kusto.MgmtOption
		// doer is called from within the function passed to RowIterator.Do(). It allows us to collect the data we receive.
		doer func(row *table.Row, update interface{}) error
		// gotInit creates the variable that will be used by doer's update argument.
		gotInit func() interface{}
		// want is the data we want to receive from the query.
		want interface{}
		// should the test fail
		failFlag bool
	}{
		{
			desc: "Complex query with Statement Builder",
			stmt: kql.NewBuilder("").
				AddDatabase(testConfig.Database).AddLiteral(".").
				AddTable(allDataTypesTable).AddLiteral(" | where ").
				AddColumn("vnum").AddLiteral(" == ").AddInt(1).AddLiteral(" and ").
				AddColumn("vdec").AddLiteral(" == ").AddDecimal(decimal.RequireFromString("2.00000000000001")).AddLiteral(" and ").
				AddColumn("vdate").AddLiteral(" == ").AddDateTime(dt).AddLiteral(" and ").
				AddColumn("vspan").AddLiteral(" == ").AddTimespan(ts).AddLiteral(" and ").
				AddFunction("tostring").AddLiteral("(").AddColumn("vobj").AddLiteral(")").
				AddLiteral(" == ").AddFunction("tostring").AddLiteral("(").
				AddDynamic(map[string]interface{}{"moshe": "value"}).AddLiteral(")").AddLiteral(" and ").
				AddColumn("vb").AddLiteral(" == ").AddBool(true).AddLiteral(" and ").
				AddColumn("vreal").AddLiteral(" == ").AddReal(0.01).AddLiteral(" and ").
				AddColumn("vstr").AddLiteral(" == ").AddString("asdf").AddLiteral(" and ").
				AddColumn("vlong").AddLiteral(" == ").AddLong(9223372036854775807).AddLiteral(" and ").
				AddColumn("vguid").AddLiteral(" == ").AddGUID(guid),
			options: []kusto.QueryOption{},
			qcall:   client.Query,
			doer: func(row *table.Row, update interface{}) error {
				rec := AllDataType{}
				if err := row.ToStruct(&rec); err != nil {
					return err
				}

				valuesRec := AllDataType{}

				err := row.ExtractValues(&valuesRec.Vnum,
					&valuesRec.Vdec,
					&valuesRec.Vdate,
					&valuesRec.Vspan,
					&valuesRec.Vobj,
					&valuesRec.Vb,
					&valuesRec.Vreal,
					&valuesRec.Vstr,
					&valuesRec.Vlong,
					&valuesRec.Vguid,
				)

				if err != nil {
					return err
				}

				assert.Equal(t, rec, valuesRec)

				recs := update.(*[]AllDataType)
				*recs = append(*recs, rec)
				return nil
			},
			gotInit: func() interface{} {
				ad := []AllDataType{}
				return &ad
			},
			failFlag: false,
			want:     &[]AllDataType{getExpectedResult()},
		},
		{
			desc: "Complex query with Statement Builder and parameters",
			stmt: kql.NewBuilder("table(tableName) | where vnum == num and vdec == dec and vdate == dt and vspan == span and tostring(vobj) == tostring(obj) and vb == b and vreal == rl and vstr == str and vlong == lg and vguid == guid"),
			options: []kusto.QueryOption{kusto.QueryParameters(*kql.NewParameters().
				AddString("tableName", allDataTypesTable).
				AddInt("num", 1).
				AddDecimal("dec", decimal.RequireFromString("2.00000000000001")).
				AddDateTime("dt", dt).
				AddTimespan("span", ts).
				AddDynamic("obj", map[string]interface{}{
					"moshe": "value",
				}).
				AddBool("b", true).
				AddReal("rl", 0.01).
				AddString("str", "asdf").
				AddLong("lg", 9223372036854775807).
				AddGUID("guid", guid))},
			qcall: client.Query,
			doer: func(row *table.Row, update interface{}) error {
				rec := AllDataType{}
				if err := row.ToStruct(&rec); err != nil {
					return err
				}

				valuesRec := AllDataType{}

				err := row.ExtractValues(&valuesRec.Vnum,
					&valuesRec.Vdec,
					&valuesRec.Vdate,
					&valuesRec.Vspan,
					&valuesRec.Vobj,
					&valuesRec.Vb,
					&valuesRec.Vreal,
					&valuesRec.Vstr,
					&valuesRec.Vlong,
					&valuesRec.Vguid,
				)

				if err != nil {
					return err
				}

				assert.Equal(t, rec, valuesRec)

				recs := update.(*[]AllDataType)
				*recs = append(*recs, rec)
				return nil
			},
			gotInit: func() interface{} {
				ad := []AllDataType{}
				return &ad
			},
			failFlag: false,
			want:     &[]AllDataType{getExpectedResult()},
		},
		{
			desc: "Complex query with Statement Builder - Fail due to wrong table name (escaped)",
			stmt: kql.NewBuilder("table(tableName) | where vstr == txt"),
			options: []kusto.QueryOption{kusto.QueryParameters(*kql.NewParameters().
				AddString("tableName", "goe2e_all_data_types\"").
				AddString("txt", "asdf"))},
			qcall: client.Query,
			doer: func(row *table.Row, update interface{}) error {
				rec := AllDataType{}
				if err := row.ToStruct(&rec); err != nil {
					return err
				}

				valuesRec := AllDataType{}

				err := row.ExtractValues(&valuesRec.Vnum,
					&valuesRec.Vdec,
					&valuesRec.Vdate,
					&valuesRec.Vspan,
					&valuesRec.Vobj,
					&valuesRec.Vb,
					&valuesRec.Vreal,
					&valuesRec.Vstr,
					&valuesRec.Vlong,
					&valuesRec.Vguid,
				)

				if err != nil {
					return err
				}

				assert.Equal(t, rec, valuesRec)

				recs := update.(*[]AllDataType)
				*recs = append(*recs, rec)
				return nil
			},
			gotInit: func() interface{} {
				ad := []AllDataType{}
				return &ad
			},
			failFlag: true,
			want:     &[]AllDataType{},
		},
	}

	for _, test := range tests {
		test := test // capture
		t.Run(test.desc, func(t *testing.T) {
			t.Parallel()
			if test.setup != nil {
				if err := test.setup(); err != nil {
					panic(err)
				}
			}
			if test.teardown != nil {
				defer func() {
					if err := test.teardown(); err != nil {
						panic(err)
					}
				}()
			}

			var iter *kusto.RowIterator
			var err error
			switch {
			case test.qcall != nil:
				var options []kusto.QueryOption
				if test.options != nil {
					options = test.options.([]kusto.QueryOption)
				}
				iter, err = test.qcall(context.Background(), testConfig.Database, test.stmt, options...)
				if (!test.failFlag && err != nil) || (test.failFlag && err == nil) {
					require.Nilf(t, err, "TestQueries(%s): had iter.Do() error: %s.", test.desc, err)
				}

			default:
				require.Fail(t, "test setup failure")
			}

			var got = test.gotInit()
			if iter != nil {
				defer iter.Stop()
				err = iter.DoOnRowOrError(func(row *table.Row, e *errors.Error) error {
					return test.doer(row, got)
				})
				require.Nilf(t, err, "TestQueries(%s): had iter.Do() error: %s.", test.desc, err)
			}

			require.Equal(t, test.want, got)
		})
	}
}

func TestFileIngestion(t *testing.T) { //ok
	t.Parallel()

	if skipETOE || testing.Short() {
		t.Skipf("end to end tests disabled: missing config.json file in etoe directory")
	}

	client, err := kusto.New(testConfig.kcsb)
	if err != nil {
		panic(err)
	}

	t.Cleanup(func() {
		t.Log("Closing client")
		require.NoError(t, client.Close())
		t.Log("Closed client")
	})

	queuedTable := "goe2e_queued_file_logs"
	streamingTable := "goe2e_streaming_file_logs"
	managedTable := "goe2e_managed_streaming_file_logs"

	queuedIngestor, err := ingest.New(client, testConfig.Database, queuedTable)
	if err != nil {
		panic(err)
	}
	t.Cleanup(func() {
		t.Log("Closing queuedIngestor")
		require.NoError(t, queuedIngestor.Close())
		t.Log("Closed queuedIngestor")
	})

	streamingIngestor, err := ingest.NewStreaming(client, testConfig.Database, streamingTable)
	if err != nil {
		panic(err)
	}

	t.Cleanup(func() {
		t.Log("Closing streamingIngestor")
		require.NoError(t, streamingIngestor.Close())
		t.Log("Closed streamingIngestor")
	})

	managedIngestor, err := ingest.NewManaged(client, testConfig.Database, managedTable)
	if err != nil {
		panic(err)
	}

	t.Cleanup(func() {
		t.Log("Closing managedIngestor")
		require.NoError(t, managedIngestor.Close())
		t.Log("Closed managedIngestor")
	})

	mockRows := createMockLogRows()

	tests := []struct {
		// desc describes the test.
		desc string
		// the type of queuedIngestor for the test
		ingestor ingest.Ingestor
		// src represents where we are getting our data.
		src string
		// options are options used on ingesting.
		options []ingest.FileOption
		// stmt is used to query for the results.
		stmt kusto.Stmt
		// table is the name of the table to create and use as a parameter.
		table string
		// teardown is a function that will be called before the test ends.
		teardown func() error
		// doer is called from within the function passed to RowIterator.Do(). It allows us to collect the data we receive.
		doer func(row *table.Row, update interface{}) error
		// gotInit creates the variable that will be used by doer's update argument.
		gotInit func() interface{}
		// want is the data we want to receive from the query.
		want interface{}
		// wantErr indicates what type of error we expect. nil if we don't expect
		wantErr error
	}{
		{
			desc:     "Ingest from blob with bad existing mapping",
			ingestor: queuedIngestor,
			src:      "https://adxingestiondemo.blob.core.windows.net/data/demo.json",
			options:  []ingest.FileOption{ingest.IngestionMappingRef("Logs_bad_mapping", ingest.JSON)},
			wantErr: ingest.StatusFromMapForTests(map[string]interface{}{
				"Status":        "Failed",
				"FailureStatus": "Permanent",
				"ErrorCode":     "BadRequest_MappingReferenceWasNotFound",
			}),
		},
		{
			desc:     "Ingest from blob with streaming ingestion should fail",
			ingestor: streamingIngestor,
			src:      "https://adxingestiondemo.blob.core.windows.net/data/demo.json",
			options:  []ingest.FileOption{ingest.IngestionMappingRef("Logs_mapping", ingest.JSON)},
			wantErr:  ingest.FileIsBlobErr,
		},
		{
			desc:     "Ingest from blob with existing mapping",
			ingestor: queuedIngestor,
			src:      "https://adxingestiondemo.blob.core.windows.net/data/demo.json",
			options:  []ingest.FileOption{ingest.IngestionMappingRef("Logs_mapping", ingest.JSON)},
			stmt:     pCountStmt,
			table:    queuedTable,
			doer: func(row *table.Row, update interface{}) error {
				rec := CountResult{}
				if err := row.ToStruct(&rec); err != nil {
					return err
				}
				recs := update.(*[]CountResult)
				*recs = append(*recs, rec)
				return nil
			},
			gotInit: func() interface{} {
				v := []CountResult{}
				return &v
			},
			want: &[]CountResult{{Count: 500}},
		},
		{
			desc:     "Ingest from blob with existing mapping managed",
			ingestor: managedIngestor,
			src:      "https://adxingestiondemo.blob.core.windows.net/data/demo.json",
			options:  []ingest.FileOption{ingest.IngestionMappingRef("Logs_mapping", ingest.JSON)},
			stmt:     pCountStmt,
			table:    managedTable,
			doer: func(row *table.Row, update interface{}) error {
				rec := CountResult{}
				if err := row.ToStruct(&rec); err != nil {
					return err
				}
				recs := update.(*[]CountResult)
				*recs = append(*recs, rec)
				return nil
			},
			gotInit: func() interface{} {
				v := []CountResult{}
				return &v
			},
			want: &[]CountResult{{Count: 500}},
		},
		{
			desc:     "Ingest from blob with inline mapping",
			ingestor: queuedIngestor,
			src:      "https://adxingestiondemo.blob.core.windows.net/data/demo.json",
			options: []ingest.FileOption{
				ingest.IngestionMapping(
					"[{\"column\":\"header_time\",\"datatype\":\"datetime\",\"Properties\":{\"path\":\"$.header.time\"}},{\"column\":\"header_id\",\"datatype\":\"guid\",\"Properties\":{\"path\":\"$.header.id\"}},{\"column\":\"header_api_version\",\"Properties\":{\"path\":\"$.header.api_version\"},\"datatype\":\"string\"},{\"column\":\"payload_data\",\"datatype\":\"string\",\"Properties\":{\"path\":\"$.payload.data\"}},{\"column\":\"payload_user\",\"datatype\":\"string\",\"Properties\":{\"path\":\"$.payload.user\"}}]",
					ingest.JSON,
				),
			},
			table: queuedTable,
			stmt:  pCountStmt,
			doer: func(row *table.Row, update interface{}) error {
				rec := CountResult{}
				if err := row.ToStruct(&rec); err != nil {
					return err
				}
				recs := update.(*[]CountResult)
				*recs = append(*recs, rec)
				return nil
			},
			gotInit: func() interface{} {
				v := []CountResult{}
				return &v
			},
			want: &[]CountResult{{Count: 500}},
		},
		{
			desc:     "Ingestion from local file queued",
			ingestor: queuedIngestor,
			src:      csvFileFromString(t),
			stmt:     pCountStmt,
			table:    queuedTable,
			doer: func(row *table.Row, update interface{}) error {
				rec := CountResult{}
				if err := row.ToStruct(&rec); err != nil {
					return err
				}
				recs := update.(*[]CountResult)
				*recs = append(*recs, rec)
				return nil
			},
			gotInit: func() interface{} {
				v := []CountResult{}
				return &v
			},
			want: &[]CountResult{{Count: 3}},
		},
		{
			desc:     "Ingestion from local file test 2 queued",
			ingestor: queuedIngestor,
			src:      createCsvFileFromData(t, mockRows),
			stmt:     pTableStmt.Add(" | order by header_api_version asc"),
			table:    queuedTable,
			doer: func(row *table.Row, update interface{}) error {
				rec := LogRow{}
				if err := row.ToStruct(&rec); err != nil {
					return err
				}
				recs := update.(*[]LogRow)
				*recs = append(*recs, rec)
				return nil
			},
			gotInit: func() interface{} {
				v := []LogRow{}
				return &v
			},
			want: &mockRows,
		},
		{
			desc:     "Ingest from local with existing mapping streaming",
			ingestor: streamingIngestor,
			src:      "testdata/demo.json",
			options:  []ingest.FileOption{ingest.IngestionMappingRef("Logs_mapping", ingest.JSON)},
			stmt: pCountStmt.MustParameters(
				kusto.NewParameters().Must(
					kusto.QueryValues{"tableName": streamingTable},
				),
			),
			table: streamingTable,
			doer: func(row *table.Row, update interface{}) error {
				rec := CountResult{}
				if err := row.ToStruct(&rec); err != nil {
					return err
				}
				recs := update.(*[]CountResult)
				*recs = append(*recs, rec)
				return nil
			},
			gotInit: func() interface{} {
				v := []CountResult{}
				return &v
			},
			want: &[]CountResult{{Count: 500}},
		},
		{
			desc:     "Ingestion from local file streaming",
			ingestor: streamingIngestor,
			src:      csvFileFromString(t),
			stmt:     pCountStmt,
			table:    streamingTable,
			doer: func(row *table.Row, update interface{}) error {
				rec := CountResult{}
				if err := row.ToStruct(&rec); err != nil {
					return err
				}
				recs := update.(*[]CountResult)
				*recs = append(*recs, rec)
				return nil
			},
			gotInit: func() interface{} {
				v := []CountResult{}
				return &v
			},
			want: &[]CountResult{{Count: 3}},
		},
		{
			desc:     "Ingestion from local file test 2 streaming",
			ingestor: streamingIngestor,
			src:      createCsvFileFromData(t, mockRows),
			stmt:     pTableStmt.Add(" | order by header_api_version asc"),
			table:    streamingTable,
			doer: func(row *table.Row, update interface{}) error {
				rec := LogRow{}
				if err := row.ToStruct(&rec); err != nil {
					return err
				}
				recs := update.(*[]LogRow)
				*recs = append(*recs, rec)
				return nil
			},
			gotInit: func() interface{} {
				v := []LogRow{}
				return &v
			},
			want: &mockRows,
		},
		{
			desc:     "Ingest from local with existing mapping managed streaming",
			ingestor: managedIngestor,
			src:      "testdata/demo.json",
			options:  []ingest.FileOption{ingest.IngestionMappingRef("Logs_mapping", ingest.JSON)},
			stmt:     pCountStmt,
			table:    managedTable,
			doer: func(row *table.Row, update interface{}) error {
				rec := CountResult{}
				if err := row.ToStruct(&rec); err != nil {
					return err
				}
				recs := update.(*[]CountResult)
				*recs = append(*recs, rec)
				return nil
			},
			gotInit: func() interface{} {
				v := []CountResult{}
				return &v
			},
			want: &[]CountResult{{Count: 500}},
		},
		{
			desc:     "Ingest big file managed streaming",
			ingestor: managedIngestor,
			src:      bigCsvFileFromString(t),
			options:  []ingest.FileOption{ingest.DontCompress()},
			stmt:     pCountStmt,
			table:    managedTable,
			doer: func(row *table.Row, update interface{}) error {
				rec := CountResult{}
				if err := row.ToStruct(&rec); err != nil {
					return err
				}
				recs := update.(*[]CountResult)
				*recs = append(*recs, rec)
				return nil
			},
			gotInit: func() interface{} {
				v := []CountResult{}
				return &v
			},
			want: &[]CountResult{{Count: 3}},
		},
	}

	for _, test := range tests {
		test := test // capture
		t.Run(test.desc, func(t *testing.T) {
			t.Parallel()

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			if test.table != "" {
				fTable := fmt.Sprintf("%s_%d_%d", test.table, time.Now().UnixNano(), rand.Int())
				require.NoError(t, createIngestionTable(t, client, fTable, false))
				test.options = append(test.options, ingest.Table(fTable))
				test.stmt = test.stmt.MustParameters(
					kusto.NewParameters().Must(
						kusto.QueryValues{"tableName": fTable},
					))
			}

			if test.teardown != nil {
				defer func() {
					if err := test.teardown(); err != nil {
						panic(err)
					}
				}()
			}

			_, isQueued := test.ingestor.(*ingest.Ingestion)
			_, isManaged := test.ingestor.(*ingest.Managed)
			if isQueued || isManaged {
				test.options = append(test.options, ingest.FlushImmediately(), ingest.ReportResultToTable())
			}

			res, err := test.ingestor.FromFile(ctx, test.src, test.options...)
			if err == nil {
				err = <-res.Wait(ctx)
			}

			if !assertErrorsMatch(t, err, test.wantErr) {
				t.Errorf("TestFileIngestion(%s): ingestor.FromFile(): got err == %v, want err == %v", test.desc, err, test.wantErr)
				return
			}

			if err != nil {
				return
			}

			require.NoError(t, waitForIngest(t, ctx, client, testConfig.Database, test.stmt, test.doer, test.want, test.gotInit))
		})
	}
}

func TestReaderIngestion(t *testing.T) { // ok
	t.Parallel()

	if skipETOE || testing.Short() {
		t.SkipNow()
	}

	queuedTable := "goe2e_queued_reader_logs"
	streamingTable := "goe2e_streaming_reader_logs"
	managedTable := "goe2e_managed_streaming_reader_logs"

	client, err := kusto.New(testConfig.kcsb)
	if err != nil {
		panic(err)
	}

	t.Cleanup(func() {
		t.Log("Closing client")
		require.NoError(t, client.Close())
		t.Log("Closed client")
	})

	queuedIngestor, err := ingest.New(client, testConfig.Database, queuedTable)
	if err != nil {
		panic(err)
	}

	t.Cleanup(func() {
		t.Log("Closing queuedIngestor")
		require.NoError(t, queuedIngestor.Close())
		t.Log("Closed queuedIngestor")
	})

	streamingIngestor, err := ingest.NewStreaming(client, testConfig.Database, streamingTable)
	if err != nil {
		panic(err)
	}

	t.Cleanup(func() {
		t.Log("Closing streamingIngestor")
		require.NoError(t, streamingIngestor.Close())
		t.Log("Closed streamingIngestor")
	})

	managedIngestor, err := ingest.NewManaged(client, testConfig.Database, managedTable)
	if err != nil {
		panic(err)
	}

	t.Cleanup(func() {
		t.Log("Closing managedIngestor")
		require.NoError(t, managedIngestor.Close())
		t.Log("Closed managedIngestor")
	})

	mockRows := createMockLogRows()

	tests := []struct {
		// desc describes the test.
		desc string
		// the type of queuedIngestor for the test
		ingestor ingest.Ingestor
		// src represents where we are getting our data.
		src string
		// options are options used on ingesting.
		options []ingest.FileOption
		// stmt is used to query for the results.
		stmt kusto.Stmt
		// table is the name of the table to create and use as a parameter.
		table string
		// teardown is a function that will be called before the test ends.
		teardown func() error
		// doer is called from within the function passed to RowIterator.Do(). It allows us to collect the data we receive.
		doer func(row *table.Row, update interface{}) error
		// gotInit creates the variable that will be used by doer's update argument.
		gotInit func() interface{}
		// want is the data we want to receive from the query.
		want interface{}
		// wantErr indicates what type of error we expect. nil if we don't expect
		wantErr error
	}{
		{
			desc:     "Ingest from reader with bad existing mapping",
			ingestor: queuedIngestor,
			src:      "testdata/demo.json",
			options:  []ingest.FileOption{ingest.FileFormat(ingest.JSON), ingest.IngestionMappingRef("Logs_bad_mapping", ingest.JSON)},
			wantErr: ingest.StatusFromMapForTests(map[string]interface{}{
				"Status":        "Failed",
				"FailureStatus": "Permanent",
				"ErrorCode":     "BadRequest_MappingReferenceWasNotFound",
			}),
		},
		{
			desc:     "Ingest with existing mapping",
			ingestor: queuedIngestor,
			src:      "testdata/demo.json",
			options: []ingest.FileOption{
				ingest.FileFormat(ingest.JSON),
				ingest.IngestionMappingRef("Logs_mapping", ingest.JSON),
			},
			stmt:  pCountStmt,
			table: queuedTable,
			doer: func(row *table.Row, update interface{}) error {
				rec := CountResult{}
				if err := row.ToStruct(&rec); err != nil {
					return err
				}
				recs := update.(*[]CountResult)
				*recs = append(*recs, rec)
				return nil
			},
			gotInit: func() interface{} {
				v := []CountResult{}
				return &v
			},
			want: &[]CountResult{{Count: 500}},
		},
		{
			desc:     "Ingest with inline mapping",
			ingestor: queuedIngestor,
			src:      "testdata/demo.json",
			options: []ingest.FileOption{
				ingest.FileFormat(ingest.JSON),
				ingest.IngestionMapping(
					"[{\"column\":\"header_time\",\"datatype\":\"datetime\",\"Properties\":{\"path\":\"$.header.time\"}},{\"column\":\"header_id\",\"datatype\":\"guid\",\"Properties\":{\"path\":\"$.header.id\"}},{\"column\":\"header_api_version\",\"Properties\":{\"path\":\"$.header.api_version\"},\"datatype\":\"string\"},{\"column\":\"payload_data\",\"datatype\":\"string\",\"Properties\":{\"path\":\"$.payload.data\"}},{\"column\":\"payload_user\",\"datatype\":\"string\",\"Properties\":{\"path\":\"$.payload.user\"}}]",
					ingest.JSON,
				),
			},
			stmt:  pCountStmt,
			table: queuedTable,
			doer: func(row *table.Row, update interface{}) error {
				rec := CountResult{}
				if err := row.ToStruct(&rec); err != nil {
					return err
				}
				recs := update.(*[]CountResult)
				*recs = append(*recs, rec)
				return nil
			},
			gotInit: func() interface{} {
				v := []CountResult{}
				return &v
			},
			want: &[]CountResult{{Count: 500}},
		},
		{
			desc:     "Ingestion from mock data",
			ingestor: queuedIngestor,
			src:      createCsvFileFromData(t, mockRows),
			options: []ingest.FileOption{
				ingest.FileFormat(ingest.CSV),
			},
			stmt:  pTableStmt.Add(" | order by header_api_version asc"),
			table: queuedTable,
			doer: func(row *table.Row, update interface{}) error {
				rec := LogRow{}
				if err := row.ToStruct(&rec); err != nil {
					return err
				}
				recs := update.(*[]LogRow)
				*recs = append(*recs, rec)
				return nil
			},
			gotInit: func() interface{} {
				v := []LogRow{}
				return &v
			},
			want: &mockRows,
		},
		{
			desc:     "Ingest from local with existing mapping streaming",
			ingestor: streamingIngestor,
			src:      "testdata/demo.json",
			options: []ingest.FileOption{
				ingest.IngestionMappingRef("Logs_mapping", ingest.JSON),
				ingest.FileFormat(ingest.JSON),
			},
			stmt:  pCountStmt,
			table: streamingTable,
			doer: func(row *table.Row, update interface{}) error {
				rec := CountResult{}
				if err := row.ToStruct(&rec); err != nil {
					return err
				}
				recs := update.(*[]CountResult)
				*recs = append(*recs, rec)
				return nil
			},
			gotInit: func() interface{} {
				v := []CountResult{}
				return &v
			},
			want: &[]CountResult{{Count: 500}},
		},
		{
			desc:     "Ingestion from local file streaming",
			ingestor: streamingIngestor,
			src:      csvFileFromString(t),
			options: []ingest.FileOption{
				ingest.FileFormat(ingest.CSV),
			},
			stmt:  pCountStmt,
			table: streamingTable,
			doer: func(row *table.Row, update interface{}) error {
				rec := CountResult{}
				if err := row.ToStruct(&rec); err != nil {
					return err
				}
				recs := update.(*[]CountResult)
				*recs = append(*recs, rec)
				return nil
			},
			gotInit: func() interface{} {
				v := []CountResult{}
				return &v
			},
			want: &[]CountResult{{Count: 3}},
		},
		{
			desc:     "Ingestion from local file test 2 streaming",
			ingestor: streamingIngestor,
			options: []ingest.FileOption{
				ingest.FileFormat(ingest.CSV),
			},
			src:   createCsvFileFromData(t, mockRows),
			stmt:  pTableStmt.Add(" | order by header_api_version asc"),
			table: streamingTable,
			doer: func(row *table.Row, update interface{}) error {
				rec := LogRow{}
				if err := row.ToStruct(&rec); err != nil {
					return err
				}
				recs := update.(*[]LogRow)
				*recs = append(*recs, rec)
				return nil
			},
			gotInit: func() interface{} {
				v := []LogRow{}
				return &v
			},
			want: &mockRows,
		},
		{
			desc:     "Ingest from local with existing mapping managed streaming",
			ingestor: managedIngestor,
			src:      "testdata/demo.json",
			options: []ingest.FileOption{
				ingest.IngestionMappingRef("Logs_mapping", ingest.JSON),
				ingest.FileFormat(ingest.JSON),
			},
			stmt:  pCountStmt,
			table: managedTable,
			doer: func(row *table.Row, update interface{}) error {
				rec := CountResult{}
				if err := row.ToStruct(&rec); err != nil {
					return err
				}
				recs := update.(*[]CountResult)
				*recs = append(*recs, rec)
				return nil
			},
			gotInit: func() interface{} {
				v := []CountResult{}
				return &v
			},
			want: &[]CountResult{{Count: 500}},
		},
	}

	for _, test := range tests {
		test := test // capture
		t.Run(test.desc, func(t *testing.T) {
			t.Parallel()

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			if test.table != "" {
				fTable := fmt.Sprintf("%s_%d_%d", test.table, time.Now().UnixNano(), rand.Int())
				require.NoError(t, createIngestionTable(t, client, fTable, false))
				test.options = append(test.options, ingest.Table(fTable))
				test.stmt = test.stmt.MustParameters(
					kusto.NewParameters().Must(
						kusto.QueryValues{"tableName": fTable},
					))
			}

			if test.teardown != nil {
				defer func() {
					if err := test.teardown(); err != nil {
						panic(err)
					}
				}()
			}

			_, isQueued := test.ingestor.(*ingest.Ingestion)
			_, isManaged := test.ingestor.(*ingest.Managed)
			if isQueued || isManaged {
				test.options = append(test.options, ingest.FlushImmediately(), ingest.ReportResultToTable())
			}

			f, err := os.Open(test.src)
			if err != nil {
				panic(err)
			}

			// We could do this other ways that are simplier for testing, but this mimics what the user will likely do.
			reader, writer := io.Pipe()
			go func() {
				defer func(writer *io.PipeWriter) {
					err := writer.Close()
					if err != nil {
						t.Errorf("Failed to close writer %v", err)
					}
				}(writer)
				_, err := io.Copy(writer, f)
				if err != nil {
					t.Errorf("Failed to copy io: %v", err)
				}
			}()

			res, err := test.ingestor.FromReader(ctx, reader, test.options...)
			if err == nil {
				err = <-res.Wait(ctx)
			}

			if !assertErrorsMatch(t, err, test.wantErr) {
				t.Errorf("TestFileIngestion(%s): ingestor.FromReader(): got err == %v, want err == %v", test.desc, err, test.wantErr)
				return
			}

			if err != nil {
				return
			}

			require.NoError(t, waitForIngest(t, ctx, client, testConfig.Database, test.stmt, test.doer, test.want, test.gotInit))
		})
	}
}

func TestMultipleClusters(t *testing.T) { //ok
	t.Parallel()

	if skipETOE || testing.Short() {
		t.Skipf("end to end tests disabled: missing config.json file in etoe directory")
	}
	if testConfig.SecondaryEndpoint == "" || testConfig.SecondaryDatabase == "" {
		t.Skipf("multiple clusters tests diasbled: needs SecondaryEndpoint and SecondaryDatabase")
	}

	client, err := kusto.New(testConfig.kcsb)
	if err != nil {
		panic(err)
	}

	t.Cleanup(func() {
		t.Log("Closing client")
		require.NoError(t, client.Close())
		t.Log("Closed client")
	})

	skcsb := kusto.NewConnectionStringBuilder(testConfig.SecondaryEndpoint).WithAadAppKey(testConfig.ClientID, testConfig.ClientSecret, testConfig.TenantID)

	secondaryClient, err := kusto.New(skcsb)
	if err != nil {
		panic(err)
	}

	t.Cleanup(func() {
		t.Log("Closing secondaryClient")
		require.NoError(t, secondaryClient.Close())
		t.Log("Closed secondaryClient")
	})

	queuedTable := "goe2e_queued_multiple_logs"
	secondaryQueuedTable := "goe2e_secondary_queued_multiple_logs"
	streamingTable := "goe2e_streaming_multiple_logs"
	secondaryStreamingTable := "goe2e_secondary_streaming_multiple_logs"

	queuedIngestor, err := ingest.New(client, testConfig.Database, queuedTable)
	if err != nil {
		panic(err)
	}
	t.Cleanup(func() {
		t.Log("Closing queuedIngestor")
		require.NoError(t, queuedIngestor.Close())
		t.Log("Closed queuedIngestor")
	})

	streamingIngestor, err := ingest.NewStreaming(client, testConfig.Database, streamingTable)
	if err != nil {
		panic(err)
	}

	t.Cleanup(func() {
		t.Log("Closing streamingIngestor")
		require.NoError(t, streamingIngestor.Close())
		t.Log("Closed streamingIngestor")
	})

	secondaryQueuedIngestor, err := ingest.New(secondaryClient, testConfig.SecondaryDatabase, queuedTable)
	if err != nil {
		panic(err)
	}

	t.Cleanup(func() {
		t.Log("Closing secondaryQueuedIngestor")
		require.NoError(t, secondaryQueuedIngestor.Close())
		t.Log("Closed secondaryQueuedIngestor")
	})

	secondaryStreamingIngestor, err := ingest.NewStreaming(secondaryClient, testConfig.SecondaryDatabase, streamingTable)
	if err != nil {
		panic(err)
	}
	t.Cleanup(func() {
		t.Log("Closing secondaryStreamingIngestor")
		require.NoError(t, secondaryStreamingIngestor.Close())
		t.Log("Closed secondaryStreamingIngestor")
	})

	tests := []struct {
		// desc describes the test.
		desc string
		// table is the name of the table to create and use as a parameter.
		table string
		// secondaryTable is the name of the table to create in the secondary DB and use as a parameter.
		secondaryTable string
		// the type of ingestor for the test
		ingestor ingest.Ingestor
		// the type of ingsetor for the secondary cluster for the test
		secondaryIngestor ingest.Ingestor
		// src represents where we are getting our data.
		src string
		// stmt is used to query for the results.
		stmt kusto.Stmt
		// doer is called from within the function passed to RowIterator.Do(). It allows us to collect the data we receive.
		doer func(row *table.Row, update interface{}) error
		// gotInit creates the variable that will be used by doer's update argument.
		gotInit func() interface{}
		// want is the data we want to receive from the query.
		want interface{}
	}{
		{
			desc:              "Ingestion from multiple clusters with queued ingestion",
			table:             queuedTable,
			secondaryTable:    secondaryQueuedTable,
			ingestor:          queuedIngestor,
			secondaryIngestor: secondaryQueuedIngestor,
			src:               csvFileFromString(t),
			stmt:              pCountStmt,
			doer: func(row *table.Row, update interface{}) error {
				rec := CountResult{}
				if err := row.ToStruct(&rec); err != nil {
					return err
				}
				recs := update.(*[]CountResult)
				*recs = append(*recs, rec)
				return nil
			},
			gotInit: func() interface{} {
				v := []CountResult{}
				return &v
			},
			want: &[]CountResult{{Count: 3}},
		},
		{
			desc:              "Ingestion from local file streaming",
			table:             streamingTable,
			secondaryTable:    secondaryStreamingTable,
			ingestor:          streamingIngestor,
			secondaryIngestor: secondaryStreamingIngestor,
			src:               csvFileFromString(t),
			stmt:              pCountStmt,
			doer: func(row *table.Row, update interface{}) error {
				rec := CountResult{}
				if err := row.ToStruct(&rec); err != nil {
					return err
				}
				recs := update.(*[]CountResult)
				*recs = append(*recs, rec)
				return nil
			},
			gotInit: func() interface{} {
				v := []CountResult{}
				return &v
			},
			want: &[]CountResult{{Count: 3}},
		},
	}

	for _, test := range tests {
		test := test // capture
		t.Run(test.desc, func(t *testing.T) {
			t.Parallel()

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			fTable := fmt.Sprintf("%s_%d_%d", test.table, time.Now().UnixNano(), rand.Int())
			fSecondaryTable := fmt.Sprintf("%s_%d_%d", test.secondaryTable, time.Now().UnixNano(), rand.Int())

			var wg sync.WaitGroup
			var primaryErr error
			var secondaryErr error

			// Run ingestion to primary database in a Goroutine
			wg.Add(1)
			go func() {
				defer wg.Done()

				require.NoError(t, createIngestionTableWithDB(t, client, testConfig.Database, fTable, false))

				test.stmt = test.stmt.MustParameters(
					kusto.NewParameters().Must(
						kusto.QueryValues{"tableName": fTable},
					))

				var options []ingest.FileOption
				if _, ok := test.ingestor.(*ingest.Ingestion); ok {
					options = append(options, ingest.FlushImmediately(), ingest.ReportResultToTable())
				}
				firstOptions := append(options, ingest.Database(testConfig.Database), ingest.Table(fTable))

				res, err := test.ingestor.FromFile(ctx, test.src, firstOptions...)
				if err == nil {
					err = <-res.Wait(ctx)
				}

				primaryErr = err

				if !assertErrorsMatch(t, err, nil) {
					t.Errorf("TestMultipleClusters(%s): ingestor.FromFile(): got err == %v, want err == %v", test.desc, err, nil)
				}

				require.NoError(t, waitForIngest(t, ctx, client, testConfig.Database, test.stmt, test.doer, test.want, test.gotInit))
			}()

			// Run ingestion to secondary database in a Goroutine
			wg.Add(1)
			go func() {
				defer wg.Done()

				require.NoError(t, createIngestionTableWithDB(t, secondaryClient, testConfig.SecondaryDatabase, fSecondaryTable, false))

				secondaryStmt := test.stmt.MustParameters(
					kusto.NewParameters().Must(
						kusto.QueryValues{"tableName": fSecondaryTable},
					))

				var options []ingest.FileOption
				if _, ok := test.secondaryIngestor.(*ingest.Ingestion); ok {
					options = append(options, ingest.FlushImmediately(), ingest.ReportResultToTable())
				}
				secondaryOptions := append(options, ingest.Database(testConfig.SecondaryDatabase), ingest.Table(fSecondaryTable))

				res, err := test.secondaryIngestor.FromFile(ctx, test.src, secondaryOptions...)
				if err == nil {
					err = <-res.Wait(ctx)
				}

				secondaryErr = err

				if !assertErrorsMatch(t, err, nil) {
					t.Errorf("TestMultipleClusters(%s): ingestor.FromFile(): got err == %v, want err == %v", test.desc, err, nil)
				}

				require.NoError(t, waitForIngest(t, ctx, secondaryClient, testConfig.SecondaryDatabase, secondaryStmt, test.doer, test.want, test.gotInit))
			}()

			// Wait for both Goroutines to finish
			wg.Wait()

			// Check if there were any errors during ingestion
			if primaryErr != nil || secondaryErr != nil {
				t.Errorf("TestMultipleClusters(%s): Got errors during ingestion. primaryErr: %v, secondaryErr: %v", test.desc, primaryErr, secondaryErr)
			}
		})
	}
}

func TestStreamingIngestion(t *testing.T) { //OK
	t.Parallel()

	if skipETOE || testing.Short() {
		t.SkipNow()
	}
	client, err := kusto.New(testConfig.kcsb)
	if err != nil {
		panic(err)
	}

	t.Cleanup(func() {
		t.Log("Closing client")
		require.NoError(t, client.Close())
		t.Log("Closed client")
	})

	tableName := fmt.Sprintf("goe2e_streaming_datatypes_%d", time.Now().Unix())
	err = createIngestionTable(t, client, tableName, false)
	if err != nil {
		panic(err)
	}

	tests := []struct {
		// desc describes the test.
		desc string
		// segment represents a data segment in our stream.
		segment []byte
		// mapping is the name of the mapping reference to be used.
		mapping string
		// stmt is used to query for the results.
		stmt kusto.Stmt
		// doer is called from within the function passed to RowIterator.Do(). It allows us to collect the data we receive.
		doer func(row *table.Row, update interface{}) error
		// gotInit creates the variable that will be used by doer's update argument.
		gotInit func() interface{}
		// want is the data we want to receive from the query.
		want interface{}
		// wantErr indicates that we want the ingestion to fail before the query.
		wantErr bool
	}{
		{
			desc:    "Streaming ingestion with bad existing mapping",
			segment: []byte(createStringyLogsData()),
			mapping: "Logs_bad_mapping",
			wantErr: true,
		},
		{
			desc:    "Test successful streaming ingestion",
			segment: []byte(createStringyLogsData()),
			mapping: "Logs_mapping",
			stmt:    pCountStmt,
			doer: func(row *table.Row, update interface{}) error {
				rec := CountResult{}
				if err := row.ToStruct(&rec); err != nil {
					return err
				}
				recs := update.(*[]CountResult)
				*recs = append(*recs, rec)
				return nil
			},
			gotInit: func() interface{} {
				v := []CountResult{}
				return &v
			},
			want: &[]CountResult{{Count: 4}},
		},
	}

	for _, test := range tests {
		test := test // Capture
		t.Run(test.desc, func(t *testing.T) {
			t.Parallel()

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			ingestor, err := ingest.New(client, testConfig.Database, tableName)
			t.Cleanup(func() {
				t.Log("Closing ingestor")
				require.NoError(t, ingestor.Close())
				t.Log("Closed ingestor")
			})

			if err != nil {
				panic(err)
			}

			err = ingestor.Stream( //nolint:staticcheck // It is deprecated, but we want to test it.
				context.Background(),
				test.segment,
				ingest.JSON,
				test.mapping,
			)

			switch {
			case err == nil && test.wantErr:
				t.Errorf("TestStreamingIngestion(%s): ingestor.Stream(): got err == nil, want err != nil", test.desc)
			case err != nil && !test.wantErr:
				t.Errorf("TestStreamingIngestion(%s): ingestor.Stream(): got err == %s, want err == nil", test.desc, err)
			case err != nil:
				return
			}

			stmt := test.stmt.MustParameters(
				kusto.NewParameters().Must(
					kusto.QueryValues{"tableName": tableName},
				),
			)

			if err := waitForIngest(t, ctx, client, testConfig.Database, stmt, test.doer, test.want, test.gotInit); err != nil {
				t.Errorf("TestStreamingIngestion(%s): %s", test.desc, err)
			}
		})
	}
}

func TestError(t *testing.T) {
	t.Parallel()

	client, err := kusto.New(testConfig.kcsb)
	require.NoError(t, err)

	t.Cleanup(func() {
		t.Log("Closing client")
		require.NoError(t, client.Close())
		t.Log("Closed client")
	})

	_, err = client.Query(context.Background(), testConfig.Database, pCountStmt.MustParameters(
		kusto.NewParameters().Must(kusto.QueryValues{"tableName": uuid.New().String()}),
	))

	kustoError, ok := errors.GetKustoError(err)
	require.True(t, ok)
	assert.Equal(t, errors.OpQuery, kustoError.Op)
	assert.Equal(t, errors.KHTTPError, kustoError.Kind)
	assert.True(t, strings.Contains(kustoError.Error(), "Failed to resolve table expression"))
	assert.True(t, isASCII(kustoError.Error()))
}

func assertErrorsMatch(t *testing.T, got, want error) bool {
	if ingest.IsStatusRecord(got) {
		if want == nil || !ingest.IsStatusRecord(want) {
			return false
		}

		codeGot, _ := ingest.GetErrorCode(got)
		codeWant, _ := ingest.GetErrorCode(want)

		statusGot, _ := ingest.GetIngestionStatus(got)
		statusWant, _ := ingest.GetIngestionStatus(want)

		failureStatusGot, _ := ingest.GetIngestionFailureStatus(got)
		failureStatusWant, _ := ingest.GetIngestionFailureStatus(want)

		return assert.Equal(t, codeWant, codeGot) &&
			assert.Equal(t, statusWant, statusGot) &&
			assert.Equal(t, failureStatusWant, failureStatusGot)

	} else if e, ok := got.(*errors.Error); ok {
		if want == nil {
			return false
		}
		if wantE, ok := want.(*errors.Error); ok {
			return assert.Equal(t, wantE.Op, e.Op) && assert.Equal(t, wantE.Kind, e.Kind)
		}
		return false
	}

	return assert.Equal(t, want, got)
}

func TestNoRedirects(t *testing.T) {
	redirectCodes := []int{301, 302, 307, 308}
	for _, code := range redirectCodes {
		code := code
		t.Run(fmt.Sprintf("Fail at cloud %d", code), func(t *testing.T) {
			t.Parallel()
			client, err := kusto.New(kusto.NewConnectionStringBuilder(fmt.Sprintf("https://statusreturner.azurewebsites.net/nocloud/%d", code)).WithDefaultAzureCredential())
			require.NoError(t, err)
			t.Cleanup(func() {
				t.Log("Closing client")
				require.NoError(t, client.Close())
				t.Log("Closed client")
			})

			_, err = client.Query(context.Background(), "db", kql.NewBuilder("table"))
			require.Error(t, err)
			assert.Contains(t, err.Error(), fmt.Sprintf("%d", code))
		})

		t.Run(fmt.Sprintf("Fail at client %d", code), func(t *testing.T) {
			t.Parallel()
			client, err := kusto.New(kusto.NewConnectionStringBuilder(fmt.Sprintf("https://statusreturner.azurewebsites.net/%d", code)).WithDefaultAzureCredential())
			require.NoError(t, err)
			t.Cleanup(func() {
				t.Log("Closing client")
				require.NoError(t, client.Close())
				t.Log("Closed client")
			})

			_, err = client.Query(context.Background(), "db", kql.NewBuilder("table"))
			require.Error(t, err)
			convErr, ok := err.(*errors.HttpError)
			require.True(t, ok)
			assert.Equal(t, code, convErr.StatusCode)
		})
	}
}

func getExpectedResult() AllDataType {
	t, err := time.Parse(time.RFC3339Nano, "2020-03-04T14:05:01.3109965Z")
	if err != nil {
		panic(err)
	}
	d, err := time.ParseDuration("1h23m45.6789s")
	if err != nil {
		panic(err)
	}
	g, err := uuid.Parse("74be27de-1e4e-49d9-b579-fe0b331d3642")
	if err != nil {
		panic(err)
	}

	return AllDataType{
		Vnum: 1,
		Vdec: value.Decimal{
			Value: "2.00000000000001",
			Valid: true,
		},
		Vdate: t,
		Vspan: value.Timespan{Value: d, Valid: true},
		Vobj: map[string]interface{}{
			"moshe": "value",
		},
		Vb:    true,
		Vreal: 0.01,
		Vstr:  "asdf",
		Vlong: 9223372036854775807,
		Vguid: value.GUID{
			Value: g,
			Valid: true,
		},
	}
}

func createIngestionTable(t *testing.T, client *kusto.Client, tableName string, isAllTypes bool) error {
	return createIngestionTableWithDB(t, client, testConfig.Database, tableName, isAllTypes)
}

func createIngestionTableWithDB(t *testing.T, client *kusto.Client, database string, tableName string, isAllTypes bool) error {
	defaultScheme := "(header_time: datetime, header_id: guid, header_api_version: string, payload_data: string, payload_user: string)"
	return createIngestionTableWithDBAndScheme(t, client, database, tableName, isAllTypes, defaultScheme)
}

func createIngestionTableWithDBAndScheme(t *testing.T, client *kusto.Client, database string, tableName string, isAllTypes bool, scheme string) error {
	t.Logf("Creating ingestion table %s", tableName)
	dropUnsafe := kusto.NewStmt(".drop table ", kusto.UnsafeStmt(unsafe.Stmt{Add: true})).UnsafeAdd(tableName).Add(" ifexists")
	var createUnsafe kusto.Stmt
	if isAllTypes {
		createUnsafe = kusto.NewStmt(".set ", kusto.UnsafeStmt(unsafe.Stmt{Add: true})).UnsafeAdd(tableName).Add(" <| datatable(vnum:int, vdec:decimal, vdate:datetime, vspan:timespan, vobj:dynamic, vb:bool, vreal:real, vstr:string, vlong:long, vguid:guid)\n[\n    1, decimal(2.00000000000001), datetime(2020-03-04T14:05:01.3109965Z), time(01:23:45.6789000), dynamic({\n  \"moshe\": \"value\"\n}), true, 0.01, \"asdf\", 9223372036854775807, guid(74be27de-1e4e-49d9-b579-fe0b331d3642), \n]")
	} else {
		createUnsafe = kusto.NewStmt(".create table ", kusto.UnsafeStmt(unsafe.Stmt{Add: true})).UnsafeAdd(tableName).UnsafeAdd(" " + scheme + " ")
	}

	addMappingUnsafe := kusto.NewStmt(".create table ", kusto.UnsafeStmt(unsafe.Stmt{Add: true})).UnsafeAdd(tableName).Add(" ingestion json mapping 'Logs_mapping' '[{\"column\":\"header_time\",\"path\":\"$.header.time\",\"datatype\":\"datetime\"},{\"column\":\"header_id\",\"path\":\"$.header.id\",\"datatype\":\"guid\"},{\"column\":\"header_api_version\",\"path\":\"$.header.api_version\",\"datatype\":\"string\"},{\"column\":\"payload_data\",\"path\":\"$.payload.data\",\"datatype\":\"string\"},{\"column\":\"payload_user\",\"path\":\"$.payload.user\",\"datatype\":\"string\"}]'")

	t.Cleanup(func() {
		t.Logf("Dropping ingestion table %s", tableName)
		_ = executeCommands(client, database, dropUnsafe)
		t.Logf("Dropped ingestion table %s", tableName)
	})

	return executeCommands(client, database, dropUnsafe, createUnsafe, addMappingUnsafe, clearStreamingCacheStatement)
}

func createMockLogRows() []LogRow {
	fakeUid, _ := uuid.Parse("11196991-b193-4610-ae12-bcc03d092927")
	fakeTime, _ := time.Parse(time.RFC3339Nano, "2020-03-10T20:59:30.694177Z")
	return []LogRow{
		// One empty line
		{
			HeaderTime:       value.DateTime{},
			HeaderId:         value.GUID{},
			HeaderApiVersion: value.String{Value: "", Valid: true},
			PayloadData:      value.String{Value: "", Valid: true},
			PayloadUser:      value.String{Value: "", Valid: true},
		},
		// One full line
		{
			HeaderTime:       value.DateTime{Value: fakeTime, Valid: true},
			HeaderId:         value.GUID{Value: fakeUid, Valid: true},
			HeaderApiVersion: value.String{Value: "v0.0.1", Valid: true},
			PayloadData:      value.String{Value: "Hello world!", Valid: true},
			PayloadUser:      value.String{Value: "Daniel Dubovski", Valid: true},
		},
		// Partial Data
		{
			HeaderTime:       value.DateTime{Value: fakeTime, Valid: true},
			HeaderId:         value.GUID{},
			HeaderApiVersion: value.String{Value: "v0.0.2", Valid: true},
			PayloadData:      value.String{Value: "", Valid: true},
			PayloadUser:      value.String{Value: "", Valid: true},
		},
	}
}

func createCsvFileFromData(t *testing.T, data []LogRow) string {
	fname := fmt.Sprintf("data_%d_%d.csv", time.Now().UnixNano(), rand.Int())
	file, err := os.Create(fname)
	if err != nil {
		panic(err)
	}
	defer func(file *os.File) {
		err := file.Close()
		if err != nil {
			panic(err)
		}
	}(file)

	t.Cleanup(func() {
		t.Logf("Removing file %s", fname)
		err := os.Remove(fname)
		if err != nil {
			t.Logf("Failed to remove file %s", fname)
		}
	})

	writer := csv.NewWriter(file)
	defer writer.Flush()

	for _, d := range data {
		err := writer.Write(d.CSVMarshal())
		if err != nil {
			panic(err)
		}
	}

	return fname
}
func fileFromString(t *testing.T, raw string) string {
	fname := fmt.Sprintf("data_%d_%d.csv", time.Now().UnixNano(), rand.Int())
	file, err := os.Create(fname)
	if err != nil {
		panic(err)
	}

	defer func(file *os.File) {
		err := file.Close()
		if err != nil {
			panic(err)
		}
	}(file)

	t.Cleanup(func() {
		t.Logf("Removing file %s", fname)
		err := os.Remove(fname)
		if err != nil {
			t.Logf("Failed to remove file %s", fname)
		}
	})

	writer := io.StringWriter(file)
	if _, err := writer.WriteString(raw); err != nil {
		panic(err)
	}

	return fname
}

func csvFileFromString(t *testing.T) string {
	return fileFromString(t, `,,,,
	2020-03-10T20:59:30.694177Z,11196991-b193-4610-ae12-bcc03d092927,v0.0.1,Hello world!,Daniel Dubovski
	2020-03-10T20:59:30.694177Z,,v0.0.2,,`)
}

func bigCsvFileFromString(t *testing.T) string {
	return fileFromString(t, `,,,,
	2020-03-10T20:59:30.694177Z,11196991-b193-4610-ae12-bcc03d092927,v0.0.1,`+strings.Repeat("Hello world!", 4*1024*1024)+`,Daniel Dubovski
	2020-03-10T20:59:30.694177Z,,v0.0.2,,`)
}

func createStringyLogsData() string {
	return "{\"header\":{\"time\":\"24-Aug-18 09:42:15\", \"id\":\"0944f542-a637-411b-94dd-8874992d6ebc\", \"api_version\":\"v2\"}, \"payload\":{\"data\":\"NEEUGQSPIPKDPQPIVFE\", \"user\":\"owild@fabrikam.com\"}}\n" +
		"{\"header\":{\"time\":\"24-Aug-18 09:42:27\", \"id\":\"09f7c3a2-27e0-4a9b-b00a-3538fb50fb51\", \"api_version\":\"v1\"}, \"payload\":{\"data\":\"MSLAMKKSTOKEWCQKFHISYDRBGGJAMTOGCGSCUPFFYXROFLTGFUZBNSZIAKUFBJGZAECQJNQPBDUBMDWUNCVRUMTJGKBKUADOQRNAIDWRDJZJYYVXNARYNOEOLTJZMGVBZFKVPWLKGENLMJKIOEWUIFACMZOPTXEXOYJTNAHQOGSJATBBJBKHJATUEIIPHWRIZQXOZQUNWGGBMRBTYMFRMWONFPOESRJSPJJKVNCSHXLDURHM\", \"user\":\"owild@fabrikam.com\"}}\n" +
		"{\"header\":{\"time\":\"24-Aug-18 09:42:47\", \"id\":\"e0e4a6dd-8823-412f-ad0c-84b55267518f\", \"api_version\":\"v1\"}, \"payload\":{\"data\":\"QZWCBJJKBPVEWNLDIQXLKNKPLKTNIBXDAOBPNGJMDSQRBGGGFDERQGJDPHRQQWBZSSEIMWQBGLHSWTOEEMHEWGMUEYAFOSVHQQZICYUJNDKEYRGVTNMDOXDMGJDNVKMOPZCGUFBFSXQTVHVNREMBFSTSNMCSVGODRVOZOABNLGKRGJQZOPWQXKJXGJSHDJKMJNCASVYRDZ\", \"user\":\"jane.austin@fabrikam.com\"}}\n" +
		"{\"header\":{\"time\":\"24-Aug-18 09:42:56\", \"id\":\"e52cd01e-6984-4821-a4aa-a97c334517e5\", \"api_version\":\"v2\"}, \"payload\":{\"data\":\"LEWDDGKXFGMRTFITKCWYH\", \"user\":\"owild@fabrikam.com\"}}\n"
}

func executeCommands(client *kusto.Client, database string, commandsToRun ...kusto.Stmt) error {
	for _, cmd := range commandsToRun {
		if _, err := client.Mgmt(context.Background(), database, cmd); err != nil {
			return err
		}
	}

	return nil
}

func waitForIngest(t *testing.T, ctx context.Context, client *kusto.Client, database string, stmt kusto.Stmt, doer func(row *table.Row, update interface{}) error, want interface{}, gotInit func() interface{}) error {

	deadline := time.Now().Add(1 * time.Minute)

	failed := false
	var got interface{}
	var err error
	shouldContinue := true

	for shouldContinue {
		shouldContinue, err = func() (bool, error) {
			if time.Now().After(deadline) {
				return false, nil
			}
			failed = false

			iter, err := client.Query(ctx, database, stmt)
			if err != nil {
				return false, err
			}
			defer iter.Stop()

			got = gotInit()
			err = iter.DoOnRowOrError(func(row *table.Row, e *errors.Error) error {
				if e != nil {
					require.NoError(t, e)
				}
				return doer(row, got)
			})
			if !assert.NoError(t, err) {
				return false, err
			}

			if !assert.ObjectsAreEqualValues(want, got) {
				failed = true
				time.Sleep(100 * time.Millisecond)
				return true, nil
			}

			properties, err := iter.GetExtendedProperties()
			if !assert.NoError(t, err) {
				return false, err
			}

			assert.Equal(t, frames.QueryProperties, properties.TableKind)
			assert.Equal(t, "TableId", properties.Columns[0].Name)
			assert.Equal(t, "Key", properties.Columns[1].Name)
			assert.Equal(t, "Value", properties.Columns[2].Name)

			completion, err := iter.GetQueryCompletionInformation()
			if !assert.NoError(t, err) {
				return false, err
			}

			assert.Equal(t, frames.QueryCompletionInformation, completion.TableKind)
			assert.Equal(t, "Timestamp", completion.Columns[0].Name)
			assert.Equal(t, "ClientRequestId", completion.Columns[1].Name)
			assert.Equal(t, "ActivityId", completion.Columns[2].Name)

			return false, err
		}()
	}
	if failed {
		require.EqualValues(t, want, got)
	}

	return err
}

func isASCII(s string) bool {
	for _, c := range s {
		if c > unicode.MaxASCII {
			return false
		}
	}
	return true
}

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}
