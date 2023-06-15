package etoe

import (
	"context"
	"fmt"
	"github.com/Azure/azure-kusto-go/azkustodata"
	"github.com/Azure/azure-kusto-go/azkustodata/errors"
	"github.com/Azure/azure-kusto-go/azkustodata/kql"
	"github.com/Azure/azure-kusto-go/azkustodata/table"
	"github.com/Azure/azure-kusto-go/azkustodata/testshared"
	"github.com/Azure/azure-kusto-go/azkustodata/types"
	"github.com/Azure/azure-kusto-go/azkustodata/value"
	"github.com/google/uuid"
	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"
	"math/rand"
	"regexp"
	"strings"
	"testing"
	"time"
	"unicode"
)

type queryFunc func(ctx context.Context, db string, query azkustodata.Statement, options ...azkustodata.QueryOption) (*azkustodata.RowIterator, error)

type mgmtFunc func(ctx context.Context, db string, query azkustodata.Statement, options ...azkustodata.MgmtOption) (*azkustodata.RowIterator, error)

type queryJsonFunc func(ctx context.Context, db string, query azkustodata.Statement, options ...azkustodata.QueryOption) (string, error)

var pTableStmtOld = azkustodata.NewStmt("table(tableName)").MustDefinitions(
	azkustodata.NewDefinitions().Must(
		azkustodata.ParamTypes{
			"tableName": azkustodata.ParamType{Type: types.String},
		},
	),
)

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

//func TestAuth(t *testing.T) {
//	t.Parallel()
//	transporter := utils.Transporter{ // using custom transporter to make sure it closes
//		Http: &http.Client{
//			Transport: &http.Transport{
//				IdleConnTimeout:   0,
//				DisableKeepAlives: true,
//			},
//		},
//	}
//	defaultCred, err := azidentity.NewDefaultAzureCredential(&azidentity.DefaultAzureCredentialOptions{
//		ClientOptions: azcore.ClientOptions{
//			Transport: &transporter,
//		},
//	})
//	credential, err := azidentity.NewChainedTokenCredential([]azcore.TokenCredential{
//		defaultCred,
//	}, &azidentity.ChainedTokenCredentialOptions{})
//	require.NoError(t, err)
//
//	tests := []struct {
//		desc string
//		kcsb *azkustodata.ConnectionStringBuilder
//	}{
//		{
//			desc: "Default",
//			kcsb: azkustodata.NewConnectionStringBuilder(testConfig.Endpoint).WithDefaultAzureCredential(),
//		},
//		{
//			desc: "With TokenCredential",
//			kcsb: azkustodata.NewConnectionStringBuilder(testConfig.Endpoint).WithTokenCredential(credential),
//		},
//	}
//
//	for _, test := range tests {
//		test := test
//		t.Run(test.desc, func(t *testing.T) {
//			t.Parallel()
//			client, err := azkustodata.New(test.kcsb)
//			require.NoError(t, err)
//			defer client.Close()
//
//			query, err := client.Query(context.Background(), testConfig.Database, kql.New("print 1"))
//			defer func() {
//				query.Stop()
//				_, _ = query.GetQueryCompletionInformation() // make sure it stops
//			}()
//			require.NoError(t, err)
//
//			row, inlineError, err := query.NextRowOrError()
//			require.NoError(t, err)
//			require.Nil(t, inlineError)
//			assert.Equal(t, "1\n", row.String())
//		})
//	}
//
//}

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
	err = testshared.CreateTestTable(t, client, allDataTypesTable, true)
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
		doer func(row *table.Row, update interface{}) error
		// gotInit creates the variable that will be used by doer's update argument.
		gotInit func() interface{}
		// want is the data we want to receive from the query.
		want interface{}
	}{
		{
			desc:  "Query: Retrieve count of the number of rows that match",
			stmt:  kql.New("").AddTable(allDataTypesTable).AddLiteral("| count"),
			qcall: client.Query,
			doer: func(row *table.Row, update interface{}) error {
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
			doer: func(row *table.Row, update interface{}) error {
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
			desc:  "Mgmt(https://github.com/Azure/azure-kusto-go/issues/55): transformations on mgmt queries - multiple tables",
			stmt:  kql.New(`.show databases | project A="1" | take 1;`).AddTable(allDataTypesTable).AddLiteral(" | project A=\"2\" | take 1"),
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
			stmt:  kql.New("").AddTable(allDataTypesTable),
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
			stmt:    kql.New("").AddTable(allDataTypesTable),
			qcall:   client.Query,
			options: []azkustodata.QueryOption{azkustodata.ResultsProgressiveDisable()},
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
			stmt: pTableStmtOld.Add(" | where  vnum == num and vdec == dec and vdate == dt and vspan == span and tostring(vobj) == tostring(obj) and vb == b and vreal" +
				" == rl and vstr == str and vlong == lg and vguid == guid ").
				MustDefinitions(azkustodata.NewDefinitions().Must(
					azkustodata.ParamTypes{
						"tableName": azkustodata.ParamType{Type: types.String},
						"num":       azkustodata.ParamType{Type: types.Int},
						"dec":       azkustodata.ParamType{Type: types.Decimal},
						"dt":        azkustodata.ParamType{Type: types.DateTime},
						"span":      azkustodata.ParamType{Type: types.Timespan},
						"obj":       azkustodata.ParamType{Type: types.Dynamic},
						"b":         azkustodata.ParamType{Type: types.Bool},
						"rl":        azkustodata.ParamType{Type: types.Real},
						"str":       azkustodata.ParamType{Type: types.String},
						"lg":        azkustodata.ParamType{Type: types.Long},
						"guid":      azkustodata.ParamType{Type: types.GUID},
					})).
				MustParameters(azkustodata.NewParameters().Must(azkustodata.QueryValues{
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
			options: []azkustodata.QueryOption{azkustodata.ResultsProgressiveDisable()},
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
			stmt: pTableStmtOld.Add(" | where  vnum == num and vdec == dec and vdate == dt and vspan == span and vb == b and vreal == rl and vstr == str and vlong == lg and vguid == guid ").
				MustDefinitions(azkustodata.NewDefinitions().Must(
					azkustodata.ParamTypes{
						"tableName": azkustodata.ParamType{Type: types.String, Default: allDataTypesTable},
						"num":       azkustodata.ParamType{Type: types.Int, Default: int32(1)},
						"dec":       azkustodata.ParamType{Type: types.Decimal, Default: "2.00000000000001"},
						"dt":        azkustodata.ParamType{Type: types.DateTime, Default: time.Date(2020, 03, 04, 14, 05, 01, 310996500, time.UTC)},
						"span":      azkustodata.ParamType{Type: types.Timespan, Default: time.Hour + 23*time.Minute + 45*time.Second + 678900000*time.Nanosecond},
						"b":         azkustodata.ParamType{Type: types.Bool, Default: true},
						"rl":        azkustodata.ParamType{Type: types.Real, Default: 0.01},
						"str":       azkustodata.ParamType{Type: types.String, Default: "asdf"},
						"lg":        azkustodata.ParamType{Type: types.Long, Default: int64(9223372036854775807)},
						"guid":      azkustodata.ParamType{Type: types.GUID, Default: uuid.MustParse("74be27de-1e4e-49d9-b579-fe0b331d3642")},
					})),
			qcall:   client.Query,
			options: []azkustodata.QueryOption{azkustodata.ResultsProgressiveDisable()},
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
			stmt:    kql.New(`print PlainValue = dynamic('1'), PlainArray = dynamic('[1,2,3]'), PlainJson= dynamic('{ "a": 1}'), JsonArray= dynamic('[{ "a": 1}, { "a": 2}]')`),
			qcall:   client.Query,
			options: []azkustodata.QueryOption{azkustodata.ResultsProgressiveDisable()},
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
			stmt: kql.New("").AddTable(allDataTypesTable).AddLiteral("| count"),
			options: []azkustodata.QueryOption{azkustodata.QueryNow(time.Now()), azkustodata.NoRequestTimeout(), azkustodata.NoTruncation(), azkustodata.RequestAppName("bd1e472c-a8e4-4c6e-859d-c86d72253197"),
				azkustodata.RequestDescription("9bff424f-711d-48b8-9a6e-d3a618748334"), azkustodata.Application("aaa"), azkustodata.User("bbb"),
				azkustodata.CustomQueryOption("additional", "additional")},
			qcall: client.Query,
			doer: func(row *table.Row, update interface{}) error {
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

			var iter *azkustodata.RowIterator
			var err error
			switch {
			case test.qcall != nil:
				var options []azkustodata.QueryOption
				if test.options != nil {
					options = test.options.([]azkustodata.QueryOption)
				}
				iter, err = test.qcall(context.Background(), testConfig.Database, test.stmt, options...)

				require.Nilf(t, err, "TestQueries(%s): had test.qcall error: %s", test.desc, err)

			case test.mcall != nil:
				var options []azkustodata.MgmtOption
				if test.options != nil {
					options = test.options.([]azkustodata.MgmtOption)
				}
				iter, err = test.mcall(context.Background(), testConfig.Database, test.stmt, options...)

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
	require.NoError(t, testshared.CreateTestTable(t, client, allDataTypesTable, true))
	dt, err := time.Parse(time.RFC3339Nano, "2020-03-04T14:05:01.3109965Z")
	ts, err := time.ParseDuration("1h23m45.6789s")
	guid, err := uuid.Parse("74be27de-1e4e-49d9-b579-fe0b331d3642")
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
		doer func(row *table.Row, update interface{}) error
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
				AddColumn("vb").AddLiteral(" == ").AddBool(true).AddLiteral(" and ").
				AddColumn("vreal").AddLiteral(" == ").AddReal(0.01).AddLiteral(" and ").
				AddColumn("vstr").AddLiteral(" == ").AddString("asdf").AddLiteral(" and ").
				AddColumn("vlong").AddLiteral(" == ").AddLong(9223372036854775807).AddLiteral(" and ").
				AddColumn("vguid").AddLiteral(" == ").AddGUID(guid),
			options: []azkustodata.QueryOption{},
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
			desc: "Complex query with Builder Builder and parameters",
			stmt: kql.New("table(tableName) | where vnum == num and vdec == dec and vdate == dt and vspan == span and tostring(vobj) == tostring(obj) and vb == b and vreal == rl and vstr == str and vlong == lg and vguid == guid"),
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
			desc: "Complex query with Builder Builder - Fail due to wrong table name (escaped)",
			stmt: kql.New("table(tableName) | where vstr == txt"),
			options: []azkustodata.QueryOption{azkustodata.QueryParameters(kql.NewParameters().
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

			var iter *azkustodata.RowIterator
			var err error
			switch {
			case test.qcall != nil:
				var options []azkustodata.QueryOption
				if test.options != nil {
					options = test.options.([]azkustodata.QueryOption)
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

			q, err := client.Query(context.Background(), "db", kql.New("table"))
			if q != nil {
				defer q.Stop()
			}
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

			q, err := client.Query(context.Background(), "db", kql.New("table"))
			if q != nil {
				defer q.Stop()
			}
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

func TestError(t *testing.T) {
	t.Parallel()

	client, err := azkustodata.New(testConfig.kcsb)
	require.NoError(t, err)

	t.Cleanup(func() {
		t.Log("Closing client")
		require.NoError(t, client.Close())
		t.Log("Closed client")
	})

	q, err := client.Query(context.Background(), testConfig.Database, kql.New("table(tableName) | count"),
		azkustodata.QueryParameters(kql.NewParameters().AddString("tableName", uuid.NewString())))

	if q != nil {
		defer q.Stop()
	}

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
