package etoe

import (
	"context"
	"fmt"
	"github.com/Azure/azure-kusto-go/azkustodata"
	"github.com/Azure/azure-kusto-go/azkustodata/errors"
	"github.com/Azure/azure-kusto-go/azkustodata/kql"
	"github.com/Azure/azure-kusto-go/azkustodata/query"
	v1 "github.com/Azure/azure-kusto-go/azkustodata/query/v1"
	v2 "github.com/Azure/azure-kusto-go/azkustodata/query/v2"
	"github.com/Azure/azure-kusto-go/azkustodata/testshared"
	"github.com/Azure/azure-kusto-go/azkustodata/utils"
	"github.com/Azure/azure-kusto-go/azkustodata/value"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azidentity"
	"github.com/google/uuid"
	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"
	"math/rand"
	"net/http"
	"regexp"
	"strings"
	"testing"
	"time"
	"unicode"
)

type queryFunc func(ctx context.Context, db string, query azkustodata.Statement, options ...azkustodata.QueryOption) (v2.FullDataset, error)

type mgmtFunc func(ctx context.Context, db string, query azkustodata.Statement, options ...azkustodata.QueryOption) (v1.Dataset, error)

// TODO: tests for iterative query

type queryJsonFunc func(ctx context.Context, db string, query azkustodata.Statement, options ...azkustodata.QueryOption) (string, error)
type DynamicTypeVariations struct {
	PlainValue value.Dynamic
	PlainArray value.Dynamic
	PlainJson  value.Dynamic
	JsonArray  value.Dynamic
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

func isASCII(s string) bool {
	for _, c := range s {
		if c > unicode.MaxASCII {
			return false
		}
	}
	return true
}

func TestAuth(t *testing.T) {
	t.Parallel()
	transporter := utils.Transporter{ // using custom transporter to make sure it closes
		Http: &http.Client{
			Transport: &http.Transport{
				IdleConnTimeout:   0,
				DisableKeepAlives: true,
			},
		},
	}
	defaultCred, err := azidentity.NewDefaultAzureCredential(&azidentity.DefaultAzureCredentialOptions{
		ClientOptions: azcore.ClientOptions{
			Transport: &transporter,
		},
	})
	require.NoError(t, err)
	credential, err := azidentity.NewChainedTokenCredential([]azcore.TokenCredential{
		defaultCred,
	}, &azidentity.ChainedTokenCredentialOptions{})
	require.NoError(t, err)

	tests := []struct {
		desc string
		kcsb *azkustodata.ConnectionStringBuilder
	}{
		{
			desc: "Default",
			kcsb: azkustodata.NewConnectionStringBuilder(testConfig.Endpoint).WithDefaultAzureCredential(),
		},
		{
			desc: "With TokenCredential",
			kcsb: azkustodata.NewConnectionStringBuilder(testConfig.Endpoint).WithTokenCredential(credential),
		},
	}

	for _, test := range tests {
		test := test
		t.Run(test.desc, func(t *testing.T) {
			t.Parallel()
			client, err := azkustodata.New(test.kcsb)
			require.NoError(t, err)
			defer func(client *azkustodata.Client) {
				err := client.Close()
				if err != nil {
					require.NoError(t, err)
				}
			}(client)

			res, err := client.Query(context.Background(), testConfig.Database, kql.New("print 1"))
			assert.NoError(t, err)
			rows, err := res.Results()[0].GetAllRows()
			assert.NoError(t, err)
			assert.Equal(t, "1\n", rows[0].String())
		})
	}

}

func TestQueries(t *testing.T) {
	t.Parallel()

	if skipETOE || testing.Short() {
		t.Skipf("end to end tests disabled: missing config.json file in etoe directory")
	}

	_, cancel := context.WithCancel(context.Background())
	defer cancel()
	client, err := azkustodata.New(testConfig.kcsb)
	if err != nil {
		panic(err)
	}

	t.Cleanup(func() {
		t.Log("Closing client")
		require.NoError(t, client.Close())
		t.Log("Closed client")
	})

	allDataTypesTable := fmt.Sprintf("goe2e_all_data_types_%d_%d", time.Now().UnixNano(), rand.Int())
	err = testshared.CreateAllDataTypesTable(t, client, allDataTypesTable)
	require.NoError(t, err)

	tests := []struct {
		// desc is a description of a test.
		desc string
		// stmt is the query to run.
		stmt azkustodata.Statement
		// setup is a function that will be called before the test runs.
		setup func() error
		// teardown is a functiont that will be called before the test ends.
		teardown func() error
		qcall    queryFunc
		mcall    mgmtFunc
		qjcall   queryJsonFunc
		options  interface{} // either []azkustodata.QueryOption or []azkustodata.MgmtOption
		// doer is called from within the function passed to RowIterator.Do(). It allows us to collect the data we receive.
		doer func(row query.Row, update interface{}) error
		// gotInit creates the variable that will be used by doer's update argument.
		gotInit func() interface{}
		// want is the data we want to receive from the query.
		want interface{}
	}{
		{
			desc:  "Query: Retrieve count of the number of rows that match",
			stmt:  kql.New("").AddTable(allDataTypesTable).AddLiteral("| count"),
			qcall: client.Query,
			doer: func(row query.Row, update interface{}) error {
				rec := testshared.CountResult{}
				if err := row.ToStruct(&rec); err != nil {
					return err
				}
				recs := update.(*[]testshared.CountResult)
				*recs = append(*recs, rec)
				return nil
			},
			gotInit: func() interface{} {
				v := []testshared.CountResult{}
				return &v
			},
			want: &[]testshared.CountResult{{Count: 1}},
		},
		{
			desc:  "Mgmt(regression github.com/Azure/azure-kusto-go/issues/11): make sure we can retrieve .show databases, but we do not check the results at this time",
			stmt:  kql.New(`.show databases`),
			mcall: client.Mgmt,
			doer: func(row query.Row, update interface{}) error {
				return nil
			},
			gotInit: func() interface{} {
				return nil
			},
		},
		{
			desc:  "Mgmt(https://github.com/Azure/azure-kusto-go/issues/55): transformations on mgmt queries",
			stmt:  kql.New(`.show databases | project A="1" | take 1`),
			mcall: client.Mgmt,
			doer: func(row query.Row, update interface{}) error {
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
			desc:  "Mgmt(https://github.com/Azure/azure-kusto-go/issues/55): transformations on mgmt queries - multiple tables",
			stmt:  kql.New(`.show databases | project A="1" | take 1;`).AddTable(allDataTypesTable).AddLiteral(" | project A=\"2\" | take 1"),
			mcall: client.Mgmt,
			doer: func(row query.Row, update interface{}) error {
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
			desc:    "Query: Progressive query: make sure we can convert all data types from a row",
			stmt:    kql.New("").AddTable(allDataTypesTable),
			qcall:   client.Query,
			options: []azkustodata.QueryOption{azkustodata.ResultsProgressiveEnabled()},
			doer: func(row query.Row, update interface{}) error {
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
			desc:  "Query: Non-Progressive query: make sure we can convert all data types from a row",
			stmt:  kql.New("").AddTable(allDataTypesTable),
			qcall: client.Query,
			doer: func(row query.Row, update interface{}) error {
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
			desc:  "Query: make sure Dynamic data type variations can be parsed",
			stmt:  kql.New(`print PlainValue = dynamic('1'), PlainArray = dynamic('[1,2,3]'), PlainJson= dynamic('{ "a": 1}'), JsonArray= dynamic('[{ "a": 1}, { "a": 2}]')`),
			qcall: client.Query,
			doer: func(row query.Row, update interface{}) error {
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
			stmt: kql.New("").AddTable(allDataTypesTable).AddLiteral("| count"),
			options: []azkustodata.QueryOption{azkustodata.QueryNow(time.Now()), azkustodata.NoRequestTimeout(), azkustodata.NoTruncation(), azkustodata.RequestAppName("bd1e472c-a8e4-4c6e-859d-c86d72253197"),
				azkustodata.RequestDescription("9bff424f-711d-48b8-9a6e-d3a618748334"), azkustodata.Application("aaa"), azkustodata.User("bbb"),
				azkustodata.CustomQueryOption("additional", "additional")},
			qcall: client.Query,
			doer: func(row query.Row, update interface{}) error {
				rec := testshared.CountResult{}
				if err := row.ToStruct(&rec); err != nil {
					return err
				}
				recs := update.(*[]testshared.CountResult)
				*recs = append(*recs, rec)
				return nil
			},
			gotInit: func() interface{} {
				v := []testshared.CountResult{}
				return &v
			},
			want: &[]testshared.CountResult{{Count: 1}},
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

			var dataset query.FullDataset
			var err error
			switch {
			case test.qcall != nil:
				var options []azkustodata.QueryOption
				if test.options != nil {
					options = test.options.([]azkustodata.QueryOption)
				}
				dataset, err = test.qcall(context.Background(), testConfig.Database, test.stmt, options...)

				require.Nilf(t, err, "TestQueries(%s): had test.qcall error: %s", test.desc, err)

			case test.mcall != nil:
				var options []azkustodata.QueryOption
				if test.options != nil {
					options = test.options.([]azkustodata.QueryOption)
				}
				dataset, err = test.mcall(context.Background(), testConfig.Database, test.stmt, options...)

				require.Nilf(t, err, "TestQueries(%s): had test.mcall error: %s", test.desc, err)

			case test.qjcall != nil:
				var options []azkustodata.QueryOption
				if test.options != nil {
					options = test.options.([]azkustodata.QueryOption)
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

			var got = test.gotInit()
			results := dataset.Results()
			assert.Len(t, results, 1)
			rows, err := results[0].GetAllRows()
			require.Nilf(t, err, "TestQueries(%s): had table.GetAllTables() error: %s", test.desc, err)

			assert.Len(t, rows, 1)

			err = test.doer(rows[0], got)

			require.Nilf(t, err, "TestQueries(%s): had dataset.Do() error: %s", test.desc, err)

			require.Equal(t, test.want, got)
		})
	}
}

func TestIterativeQuery(t *testing.T) {
	t.Parallel()

	if skipETOE || testing.Short() {
		t.Skipf("end to end tests disabled: missing config.json file in etoe directory")
	}

	_, cancel := context.WithCancel(context.Background())
	defer cancel()
	client, err := azkustodata.New(testConfig.kcsb)
	if err != nil {
		panic(err)
	}

	t.Cleanup(func() {
		t.Log("Closing client")
		require.NoError(t, client.Close())
		t.Log("Closed client")
	})

	allDataTypesTable := fmt.Sprintf("goe2e_v2_all_data_types_%d_%d", time.Now().UnixNano(), rand.Int())
	err = testshared.CreateAllDataTypesTable(t, client, allDataTypesTable)
	require.NoError(t, err)

	err = testshared.CreateAllDataTypesNullTable(t, client, allDataTypesTable+"_null")
	require.NoError(t, err)

	v2, err := client.IterativeQuery(context.Background(), testConfig.Database, kql.New("").AddTable(allDataTypesTable).AddLiteral(";").AddTable(allDataTypesTable+"_null"))

	require.NoError(t, err)

	res := getExpectedResult()

	for tableResult := range v2.Results() {
		require.NoError(t, tableResult.Err())

		tb := tableResult.Table()
		if tb.Name() == allDataTypesTable {
			rows, errs := tb.GetAllRows()
			require.Nilf(t, errs, "TestIterativeQuery: had table.GetAllTables() error: %s", errs)
			structs, errs := query.ToStructs[AllDataType](rows)
			require.Nil(t, errs)
			require.Equal(t, []AllDataType{res}, structs)
		}
		if tb.Name() == allDataTypesTable+"_null" {
			rows, errs := tb.GetAllRows()
			require.Nilf(t, errs, "TestIterativeQuery: had table.GetAllTables() error: %s", errs)
			structs, errs := query.ToStructs[AllDataType](rows)
			require.Nil(t, errs)
			require.Equal(t, []AllDataType{{}}, structs)
		}
	}
}

func TestStatement(t *testing.T) {
	t.Parallel()

	if skipETOE || testing.Short() {
		t.Skipf("end to end tests disabled: missing config.json file in etoe directory")
	}

	_, cancel := context.WithCancel(context.Background())
	defer cancel()

	client, err := azkustodata.New(testConfig.kcsb)
	if err != nil {
		panic(err)
	}

	t.Cleanup(func() {
		t.Log("Closing client")
		require.NoError(t, client.Close())
		t.Log("Closed client")
	})

	allDataTypesTable := fmt.Sprintf("goe2e_all_data_types_%d_%d", time.Now().UnixNano(), rand.Int())
	require.NoError(t, testshared.CreateAllDataTypesTable(t, client, allDataTypesTable))
	dt, err := time.Parse(time.RFC3339Nano, "2020-03-04T14:05:01.3109965Z")
	require.NoError(t, err)
	ts, err := time.ParseDuration("1h23m45.6789s")
	require.NoError(t, err)
	guid, err := uuid.Parse("74be27de-1e4e-49d9-b579-fe0b331d3642")
	require.NoError(t, err)
	tests := []struct {
		// desc is a description of a test.
		desc string
		// stmt is the Kusot Stmt that will be sent.
		stmt azkustodata.Statement
		// setup is a function that will be called before the test runs.
		setup func() error
		// teardown is a functiont that will be called before the test ends.
		teardown func() error
		qcall    queryFunc
		mcall    mgmtFunc
		qjcall   queryJsonFunc
		options  interface{} // either []azkustodata.QueryOption or []azkustodata.MgmtOption
		// doer is called from within the function passed to RowIterator.Do(). It allows us to collect the data we receive.
		doer func(row query.Row, update interface{}) error
		// gotInit creates the variable that will be used by doer's update argument.
		gotInit func() interface{}
		// want is the data we want to receive from the query.
		want interface{}
		// should the test fail
		failFlag bool
	}{
		{
			desc: "Complex query with Builder Builder",
			stmt: kql.New("").
				AddDatabase(testConfig.Database).AddLiteral(".").
				AddTable(allDataTypesTable).AddLiteral(" | where ").
				AddColumn("vnum").AddLiteral(" == ").AddInt(1).AddLiteral(" and ").
				AddColumn("vdec").AddLiteral(" == ").AddDecimal(decimal.RequireFromString("2.00000000000001")).AddLiteral(" and ").
				AddColumn("vdate").AddLiteral(" == ").AddDateTime(dt).AddLiteral(" and ").
				AddColumn("vspan").AddLiteral(" == ").AddTimespan(ts).AddLiteral(" and ").
				AddFunction("tostring").AddLiteral("(").AddColumn("vobj").AddLiteral(")").
				AddLiteral(" == ").AddFunction("tostring").AddLiteral("(").
				AddDynamic(map[string]interface{}{"moshe": "value"}).AddLiteral(")").AddLiteral(" and ").
				AddFunction("tostring").AddLiteral("(").
				AddColumn("vobj").AddLiteral(")").AddLiteral(" == ").AddFunction("tostring").AddLiteral("(").
				AddSerializedDynamic([]byte("{\"moshe\": \"value\"}")).AddLiteral(")").AddLiteral(" and ").
				AddColumn("vb").AddLiteral(" == ").AddBool(true).AddLiteral(" and ").
				AddColumn("vreal").AddLiteral(" == ").AddReal(0.01).AddLiteral(" and ").
				AddColumn("vstr").AddLiteral(" == ").AddString("asdf").AddLiteral(" and ").
				AddColumn("vlong").AddLiteral(" == ").AddLong(9223372036854775807).AddLiteral(" and ").
				AddColumn("vguid").AddLiteral(" == ").AddGUID(guid),
			options: []azkustodata.QueryOption{},
			qcall:   client.Query,
			doer: func(row query.Row, update interface{}) error {
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
			desc: "Complex query with Builder Builder and parameters",
			stmt: kql.New("").
				AddLiteral("table(tableName)").
				AddLiteral(" | where vnum == num").
				AddLiteral(" and vdec == dec").
				AddLiteral(" and vdate == dt").
				AddLiteral(" and vspan == span").
				AddLiteral(" and tostring(vobj) == tostring(obj)").
				AddLiteral(" and vb == b").
				AddLiteral(" and vreal == rl").
				AddLiteral(" and vstr == str").
				AddLiteral(" and vlong == lg").
				AddLiteral(" and vguid == guid"),
			options: []azkustodata.QueryOption{azkustodata.QueryParameters(kql.NewParameters().
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
			doer: func(row query.Row, update interface{}) error {
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
			desc: "Complex query with Builder Builder - Fail due to wrong table name (escaped)",
			stmt: kql.New("table(tableName) | where vstr == txt"),
			options: []azkustodata.QueryOption{azkustodata.QueryParameters(kql.NewParameters().
				AddString("tableName", "goe2e_all_data_types\"").
				AddString("txt", "asdf"))},
			qcall: client.Query,
			doer: func(row query.Row, update interface{}) error {
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

			var res query.FullDataset
			var err error
			switch {
			case test.qcall != nil:
				var options []azkustodata.QueryOption
				if test.options != nil {
					options = test.options.([]azkustodata.QueryOption)
				}
				res, err = test.qcall(context.Background(), testConfig.Database, test.stmt, options...)
				if (!test.failFlag && err != nil) || (test.failFlag && err == nil) {
					require.Nilf(t, err, "TestQueries(%s): had iter.Do() error: %s.", test.desc, err)
				}

			default:
				require.Fail(t, "test setup failure")
			}

			var got = test.gotInit()
			if res != nil {
				rows, err := res.Results()[0].GetAllRows()
				assert.NoError(t, err)
				assert.Len(t, rows, 1)
				err = test.doer(rows[0], got)

				require.Nilf(t, err, "TestQueries(%s): had iter.Do() error: %s.", test.desc, err)
			}

			require.Equal(t, test.want, got)
		})
	}
}

func TestNoRedirects(t *testing.T) {
	redirectCodes := []int{301, 302, 307, 308}
	for _, code := range redirectCodes {
		code := code
		t.Run(fmt.Sprintf("Fail at cloud %d", code), func(t *testing.T) {
			t.Parallel()
			client, err := azkustodata.New(azkustodata.NewConnectionStringBuilder(fmt.Sprintf("https://statusreturner.azurewebsites.net/nocloud/%d", code)).WithDefaultAzureCredential())
			require.NoError(t, err)
			t.Cleanup(func() {
				t.Log("Closing client")
				require.NoError(t, client.Close())
				t.Log("Closed client")
			})

			_, err = client.Query(context.Background(), "db", kql.New("table"))
			require.Error(t, err)
			assert.Contains(t, err.Error(), fmt.Sprintf("%d", code))
		})

		t.Run(fmt.Sprintf("Fail at client %d", code), func(t *testing.T) {
			t.Parallel()
			client, err := azkustodata.New(azkustodata.NewConnectionStringBuilder(fmt.Sprintf("https://statusreturner.azurewebsites.net/%d", code)).WithDefaultAzureCredential())
			require.NoError(t, err)
			t.Cleanup(func() {
				t.Log("Closing client")
				require.NoError(t, client.Close())
				t.Log("Closed client")
			})

			_, err = client.Query(context.Background(), "db", kql.New("table"))

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
			Value: decimal.NewNullDecimal(decimal.RequireFromString("2.00000000000001")),
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
			Value: uuid.NullUUID{UUID: g, Valid: true},
		},
	}
}

func TestError(t *testing.T) {
	t.Parallel()

	client, err := azkustodata.New(testConfig.kcsb)
	require.NoError(t, err)

	t.Cleanup(func() {
		t.Log("Closing client")
		require.NoError(t, client.Close())
		t.Log("Closed client")
	})

	_, err = client.Query(context.Background(), testConfig.Database, kql.New("table(tableName) | count"),
		azkustodata.QueryParameters(kql.NewParameters().AddString("tableName", uuid.NewString())))

	kustoError, ok := errors.GetKustoError(err)
	require.True(t, ok)
	assert.Equal(t, errors.OpQuery, kustoError.Op)
	assert.Equal(t, errors.KHTTPError, kustoError.Kind)
	assert.True(t, strings.Contains(kustoError.Error(), "Failed to resolve table expression"))
	assert.True(t, isASCII(kustoError.Error()))
}
func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}
