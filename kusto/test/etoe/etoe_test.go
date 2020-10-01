package etoe

import (
	"context"
	"encoding/csv"
	"fmt"
	"io"
	"os"
	"testing"
	"time"

	"github.com/Azure/azure-kusto-go/kusto"
	"github.com/Azure/azure-kusto-go/kusto/data/table"
	"github.com/Azure/azure-kusto-go/kusto/data/types"
	"github.com/Azure/azure-kusto-go/kusto/data/value"
	"github.com/Azure/azure-kusto-go/kusto/ingest"
	"github.com/Azure/azure-kusto-go/kusto/unsafe"

	"github.com/google/uuid"
	"github.com/kylelemons/godebug/pretty"
)

var (
	pCountStmt = kusto.NewStmt("table(tableName) | count").MustDefinitions(
		kusto.NewDefinitions().Must(
			kusto.ParamTypes{
				"tableName": kusto.ParamType{Type: types.String},
			},
		),
	)

	ingestInlineStmt = kusto.NewStmt(".ingest inline into table AllDataTypes <|1,2.00000000000001,'2020-03-04T14:05:01.3109965Z',1:23:45.6789,{\"moshe\": \"value\"}, 1 ,0.01,asdf,9223372036854775807,74be27de-1e4e-49d9-b579-fe0b331d3642")
	createStmt       = kusto.NewStmt(".create table AllDataTypes (vnum: int, vdec: decimal, vdate: datetime, vspan: timespan, vobj: dynamic, vb: bool, vreal: real, vstr: string, vlong: long, vguid: guid)")
	dropStmt         = kusto.NewStmt(".drop table AllDataTypes ifexists")
)

type CountResult struct {
	Count int64
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

type queryFunc func(ctx context.Context, db string, query kusto.Stmt, options ...kusto.QueryOption) (*kusto.RowIterator, error)

type mgmtFunc func(ctx context.Context, db string, query kusto.Stmt, options ...kusto.MgmtOption) (*kusto.RowIterator, error)

func TestQueries(t *testing.T) {
	if skipETOE || testing.Short() {
		t.Skipf("end to end tests disabled: missing config.json file in etoe directory")
	}

	t.Parallel()

	_, cancel := context.WithCancel(context.Background())
	defer cancel()

	client, err := kusto.New(testConfig.Endpoint, testConfig.Authorizer)
	if err != nil {
		panic(err)
	}

	pCountStmt := kusto.NewStmt("table(tableName) | count").MustDefinitions(
		kusto.NewDefinitions().Must(
			kusto.ParamTypes{
				"tableName": kusto.ParamType{Type: types.String},
			},
		),
	)

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
		options  interface{} // either []kusto.QueryOption or []kusto.MgmtOption
		// doer is called from within the function passed to RowIterator.Do(). It allows us to collect the data we receive.
		doer func(row *table.Row, update interface{}) error
		// gotInit creates the variable that will be used by doer's update argument.
		gotInit func() interface{}
		// want is the data we want to receive from the query.
		want interface{}
		// compare allows the test to have a custom compare operation. If nil, the data from doer's update argument is
		// compared against want using pretty.Compare().
		compare func(got, want interface{}) error
	}{
		{
			desc: "Query: Retrieve count of the number of rows that match",
			stmt: pCountStmt.MustParameters(
				kusto.NewParameters().Must(kusto.QueryValues{"tableName": "AllDataTypes"}),
			),
			setup: func() error {
				return createAllDataTypesTable(client)
			},
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
			want: []CountResult{{Count: 1}},
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
			desc:  "Query: Progressive query: make sure we can convert all data types from a row",
			stmt:  kusto.NewStmt("AllDataTypes"),
			qcall: client.Query,
			doer: func(row *table.Row, update interface{}) error {
				rec := AllDataType{}
				if err := row.ToStruct(&rec); err != nil {
					return err
				}
				recs := update.(*[]AllDataType)
				*recs = append(*recs, rec)
				return nil
			},
			gotInit: func() interface{} {
				ad := []AllDataType{}
				return &ad
			},
			want: []AllDataType{getExpectedResult()},
		},
		{
			desc:    "Query: Non-Progressive query: make sure we can convert all data types from a row",
			stmt:    kusto.NewStmt("AllDataTypes"),
			qcall:   client.Query,
			options: []kusto.QueryOption{kusto.ResultsProgressiveDisable()},
			doer: func(row *table.Row, update interface{}) error {
				rec := AllDataType{}
				if err := row.ToStruct(&rec); err != nil {
					return err
				}
				recs := update.(*[]AllDataType)
				*recs = append(*recs, rec)
				return nil
			},
			gotInit: func() interface{} {
				ad := []AllDataType{}
				return &ad
			},
			want: []AllDataType{getExpectedResult()},
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
				*recs = append(*recs, rec)
				return nil
			},
			gotInit: func() interface{} {
				ad := []DynamicTypeVariations{}
				return &ad
			},
			want: []DynamicTypeVariations{
				{
					PlainValue: value.Dynamic{Value: []byte("1"), Valid: true},
					PlainArray: value.Dynamic{Value: []byte("[1,2,3]"), Valid: true},
					PlainJson:  value.Dynamic{Value: []byte(`{ "a": 1}`), Valid: true},
					JsonArray:  value.Dynamic{Value: []byte(`[{ "a": 1}, { "a": 2}]`), Valid: true},
				},
			},
		},
	}

	for _, test := range tests {
		func() {
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

				if err != nil {
					t.Errorf("TestQueries(%s): had test.qcall error: %s", test.desc, err)
					return
				}

			case test.mcall != nil:
				var options []kusto.MgmtOption
				if test.options != nil {
					options = test.options.([]kusto.MgmtOption)
				}
				iter, err = test.mcall(context.Background(), testConfig.Database, test.stmt, options...)

				if err != nil {
					t.Errorf("TestQueries(%s): had test.mcall error: %s", test.desc, err)
					return
				}
			default:
				panic("test setup failure")
			}

			defer iter.Stop()

			var got = test.gotInit()
			err = iter.Do(func(row *table.Row) error {
				return test.doer(row, got)
			})

			if err != nil {
				t.Errorf("TestQueries(%s): had iter.Do() error: %s", test.desc, err)
				return
			}

			if test.compare != nil {
				if err := test.compare(got, test.want); err != nil {
					t.Errorf("TestQueries(%s): %s", test.desc, err)
				}
				return
			}
			if diff := pretty.Compare(test.want, got); diff != "" {
				t.Errorf("TestQueries(%s): -want/+got:\n%s", test.desc, diff)
			}
		}()
	}
}

func TestFileIngestion(t *testing.T) {
	t.Parallel()

	if skipETOE || testing.Short() {
		t.Skipf("end to end tests disabled: missing config.json file in etoe directory")
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	client, err := kusto.New(testConfig.Endpoint, testConfig.Authorizer)
	if err != nil {
		panic(err)
	}

	ingestor, err := ingest.New(client, testConfig.Database, "Logs")
	if err != nil {
		panic(err)
	}

	mockRows := createMockLogRows()

	tests := []struct {
		// desc describes the test.
		desc string
		// src represents where we are getting our data.
		src string
		// options are options used on ingesting.
		options []ingest.FileOption
		// stmt is used to query for the results.
		stmt kusto.Stmt
		// setup is a function that will be called before the test runs.
		setup func() error
		// teardown is a function that will be called before the test ends.
		teardown func() error
		// doer is called from within the function passed to RowIterator.Do(). It allows us to collect the data we receive.
		doer func(row *table.Row, update interface{}) error
		// gotInit creates the variable that will be used by doer's update argument.
		gotInit func() interface{}
		// want is the data we want to receive from the query.
		want interface{}
		// wantErr indicates that we want the ingestion to fail before the query.
		wantErr bool
		// compare allows the test to have a custom compare operation. If nil, the data from doer's update argument is
		// compared against want using pretty.Compare().
		compare func(got, want interface{}) error
	}{
		{
			desc:    "Ingest from blob with bad existing mapping",
			src:     "https://adxingestiondemo.blob.core.windows.net/data/demo.json",
			options: []ingest.FileOption{ingest.IngestionMappingRef("Logs_bad_mapping", ingest.JSON)},
			wantErr: true,
		},
		{
			desc:    "Ingest from blob with existing mapping",
			src:     "https://adxingestiondemo.blob.core.windows.net/data/demo.json",
			options: []ingest.FileOption{ingest.IngestionMappingRef("Logs_mapping", ingest.JSON)},
			stmt: pCountStmt.MustParameters(
				kusto.NewParameters().Must(
					kusto.QueryValues{"tableName": "Logs"},
				),
			),
			setup: func() error { return createIngestionTable(client) },
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
			want: []CountResult{{Count: 500}},
		},
		{
			desc: "Ingest from blob with inline mapping",
			src:  "https://adxingestiondemo.blob.core.windows.net/data/demo.json",
			options: []ingest.FileOption{
				ingest.IngestionMapping(
					"[{\"column\":\"header_time\",\"datatype\":\"datetime\",\"Properties\":{\"path\":\"$.header.time\"}},{\"column\":\"header_id\",\"datatype\":\"guid\",\"Properties\":{\"path\":\"$.header.id\"}},{\"column\":\"header_api_version\",\"Properties\":{\"path\":\"$.header.api_version\"},\"datatype\":\"string\"},{\"column\":\"payload_data\",\"datatype\":\"string\",\"Properties\":{\"path\":\"$.payload.data\"}},{\"column\":\"payload_user\",\"datatype\":\"string\",\"Properties\":{\"path\":\"$.payload.user\"}}]",
					ingest.JSON,
				),
			},
			stmt: pCountStmt.MustParameters(
				kusto.NewParameters().Must(
					kusto.QueryValues{"tableName": "Logs"},
				),
			),
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
			want: []CountResult{{Count: 1000}}, // The count is the last ingestion + this one (500).
		},
		{
			desc: "Ingestion from local file",
			src:  csvFileFromString(),
			stmt: pCountStmt.MustParameters(
				kusto.NewParameters().Must(
					kusto.QueryValues{"tableName": "Logs"},
				),
			),
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
			want: []CountResult{{Count: 1003}}, // The count is the sum of all previous ingestions + 3.
		},
		{
			desc:  "Ingestion from local file test 2",
			src:   createCsvFileFromData(mockRows),
			stmt:  kusto.NewStmt("Logs | order by header_api_version asc"),
			setup: func() error { return createIngestionTable(client) },
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
			want: mockRows,
		},
	}

	for _, test := range tests {
		func() {
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

			test.options = append(test.options, ingest.FlushImmediately(), ingest.ReportResultToTable())

			res, err := ingestor.FromFile(ctx, test.src, test.options...)
			if err == nil {
				err = <-res.Wait(ctx)
			}

			switch {
			case err == nil && test.wantErr:
				t.Errorf("TestFileIngestion(%s): ingestor.FromFile(): got err == nil, want err != nil", test.desc)
				return
			case err != nil && !test.wantErr:
				t.Errorf("TestFileIngestion(%s): ingestor.FromFile(): got err == %s, want err == nil", test.desc, err)
				return
			case err != nil:
				return
			}

			if err := waitForIngest(ctx, client, test.stmt, test.compare, test.doer, test.want, test.gotInit); err != nil {
				t.Errorf("TestFileIngestion(%s): %s", test.desc, err)
			}
		}()
	}
}

func TestReaderIngestion(t *testing.T) {
	if skipETOE || testing.Short() {
		t.SkipNow()
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	client, err := kusto.New(testConfig.Endpoint, testConfig.Authorizer)
	if err != nil {
		panic(err)
	}

	ingestor, err := ingest.New(client, testConfig.Database, "Logs")
	if err != nil {
		panic(err)
	}

	mockRows := createMockLogRows()

	tests := []struct {
		// desc describes the test.
		desc string
		// src represents where we are getting our data.
		src string
		// options are options used on ingesting.
		options []ingest.FileOption
		// stmt is used to query for the results.
		stmt kusto.Stmt
		// setup is a function that will be called before the test runs.
		setup func() error
		// teardown is a function that will be called before the test ends.
		teardown func() error
		// doer is called from within the function passed to RowIterator.Do(). It allows us to collect the data we receive.
		doer func(row *table.Row, update interface{}) error
		// gotInit creates the variable that will be used by doer's update argument.
		gotInit func() interface{}
		// want is the data we want to receive from the query.
		want interface{}
		// wantErr indicates that we want the ingestion to fail before the query.
		wantErr bool
		// compare allows the test to have a custom compare operation. If nil, the data from doer's update argument is
		// compared against want using pretty.Compare().
		compare func(got, want interface{}) error
	}{
		{
			desc:    "Ingest from blob with bad existing mapping",
			src:     "testdata/demo.json",
			options: []ingest.FileOption{ingest.IngestionMappingRef("Logs_bad_mapping", ingest.JSON)},
			wantErr: true,
		},
		{
			desc: "Ingest with existing mapping",
			src:  "testdata/demo.json",
			options: []ingest.FileOption{
				ingest.FileFormat(ingest.JSON),
				ingest.IngestionMappingRef("Logs_mapping", ingest.JSON),
			},
			stmt: pCountStmt.MustParameters(
				kusto.NewParameters().Must(
					kusto.QueryValues{"tableName": "Logs"},
				),
			),
			setup: func() error { return createIngestionTable(client) },
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
			want: []CountResult{{Count: 500}},
		},
		{
			desc: "Ingest with inline mapping",
			src:  "testdata/demo.json",
			options: []ingest.FileOption{
				ingest.FileFormat(ingest.JSON),
				ingest.IngestionMapping(
					"[{\"column\":\"header_time\",\"datatype\":\"datetime\",\"Properties\":{\"path\":\"$.header.time\"}},{\"column\":\"header_id\",\"datatype\":\"guid\",\"Properties\":{\"path\":\"$.header.id\"}},{\"column\":\"header_api_version\",\"Properties\":{\"path\":\"$.header.api_version\"},\"datatype\":\"string\"},{\"column\":\"payload_data\",\"datatype\":\"string\",\"Properties\":{\"path\":\"$.payload.data\"}},{\"column\":\"payload_user\",\"datatype\":\"string\",\"Properties\":{\"path\":\"$.payload.user\"}}]",
					ingest.JSON,
				),
			},
			stmt: pCountStmt.MustParameters(
				kusto.NewParameters().Must(
					kusto.QueryValues{"tableName": "Logs"},
				),
			),
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
			want: []CountResult{{Count: 1000}}, // The count is the last ingestion + this one (500).
		},
		{
			desc: "Ingestion from mock data",
			src:  createCsvFileFromData(mockRows),
			options: []ingest.FileOption{
				ingest.FileFormat(ingest.CSV),
			},
			stmt:  kusto.NewStmt("Logs | order by header_api_version asc"),
			setup: func() error { return createIngestionTable(client) },
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
			want: mockRows,
		},
	}

	for _, test := range tests {
		func() {
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

			test.options = append(test.options, ingest.FlushImmediately())

			f, err := os.Open(test.src)
			if err != nil {
				panic(err)
			}

			// We could do this other ways that are simplier for testing, but this mimics what the user will likely do.
			reader, writer := io.Pipe()
			go func() {
				defer writer.Close()
				io.Copy(writer, f)
			}()

			res, err := ingestor.FromReader(ctx, reader, test.options...)
			if err == nil {
				err = <-res.Wait(ctx)
			}

			switch {
			case err == nil && test.wantErr:
				t.Errorf("TestReaderIngestion(%s): ingestor.FromFile(): got err == nil, want err != nil", test.desc)
				return
			case err != nil && !test.wantErr:
				t.Errorf("TestReaderIngestion(%s): ingestor.FromFile(): got err == %s, want err == nil", test.desc, err)
				return
			case err != nil:
				return
			}

			if err := waitForIngest(ctx, client, test.stmt, test.compare, test.doer, test.want, test.gotInit); err != nil {
				t.Errorf("TestReaderIngestion(%s): %s", test.desc, err)
			}
		}()
	}
}

func TestStreamingIngestion(t *testing.T) {
	t.Parallel()

	if skipETOE || testing.Short() {
		t.SkipNow()
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	client, err := kusto.New(testConfig.Endpoint, testConfig.Authorizer)
	if err != nil {
		panic(err)
	}

	setupFunc := func(tableName string) error {
		if err := createAllDataTypesUnsafeTable(client, tableName); err != nil {
			return err
		}

		// This is needed because of a bug in the backend that sometimes causes the tables not to drop and get stuck.
		_, err := client.Mgmt(
			context.Background(),
			testConfig.Database,
			kusto.NewStmt(".clear database cache streamingingestion schema"),
		)
		return err
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
		// compare allows the test to have a custom compare operation. If nil, the data from doer's update argument is
		// compared against want using pretty.Compare().
		compare func(got, want interface{}) error
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
			want: []CountResult{{Count: 4}},
		},
	}

	for _, test := range tests {
		tableName := fmt.Sprintf("Logs_%d", time.Now().Unix())
		if err := setupFunc(tableName); err != nil {
			panic(err)
		}
		defer func() {
			client.Mgmt(
				context.Background(),
				testConfig.Database,
				kusto.NewStmt(".drop table ", kusto.UnsafeStmt(unsafe.Stmt{Add: true})).UnsafeAdd(tableName).Add(" ifexists"),
			)
		}()

		ingestor, err := ingest.New(client, testConfig.Database, tableName)
		if err != nil {
			panic(err)
		}

		err = ingestor.Stream(
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
			continue
		}

		stmt := test.stmt.MustParameters(
			kusto.NewParameters().Must(
				kusto.QueryValues{"tableName": tableName},
			),
		)

		if err := waitForIngest(ctx, client, stmt, test.compare, test.doer, test.want, test.gotInit); err != nil {
			t.Errorf("TestStreamingIngestion(%s): %s", test.desc, err)
		}
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

func createAllDataTypesUnsafeTable(client *kusto.Client, tableName string) error {
	dropUnsafe := kusto.NewStmt(".drop table ", kusto.UnsafeStmt(unsafe.Stmt{Add: true})).UnsafeAdd(tableName).Add(" ifexists")
	createUnsafe := kusto.NewStmt(".create table ", kusto.UnsafeStmt(unsafe.Stmt{Add: true})).UnsafeAdd(tableName).Add(" (header_time: datetime, header_id: guid, header_api_version: string, payload_data: string, payload_user: string) ")
	addMappingUnsafe := kusto.NewStmt(".create table ", kusto.UnsafeStmt(unsafe.Stmt{Add: true})).UnsafeAdd(tableName).Add(" ingestion json mapping 'Logs_mapping' '[{\"column\":\"header_time\",\"path\":\"$.header.time\",\"datatype\":\"datetime\"},{\"column\":\"header_id\",\"path\":\"$.header.id\",\"datatype\":\"guid\"},{\"column\":\"header_api_version\",\"path\":\"$.header.api_version\",\"datatype\":\"string\"},{\"column\":\"payload_data\",\"path\":\"$.payload.data\",\"datatype\":\"string\"},{\"column\":\"payload_user\",\"path\":\"$.payload.user\",\"datatype\":\"string\"}]'")

	return executeCommands(client, dropUnsafe, createUnsafe, addMappingUnsafe)
}

func createAllDataTypesTable(client *kusto.Client) error {
	return executeCommands(client, dropStmt, createStmt, ingestInlineStmt)
}

func createMockLogRows() []LogRow {
	fake_uid, _ := uuid.Parse("11196991-b193-4610-ae12-bcc03d092927")
	fake_time, _ := time.Parse(time.RFC3339Nano, "2020-03-10T20:59:30.694177Z")
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
			HeaderTime:       value.DateTime{fake_time, true},
			HeaderId:         value.GUID{fake_uid, true},
			HeaderApiVersion: value.String{"v0.0.1", true},
			PayloadData:      value.String{"Hello world!", true},
			PayloadUser:      value.String{"Daniel Dubovski", true},
		},
		// Partial Data
		{
			HeaderTime:       value.DateTime{fake_time, true},
			HeaderId:         value.GUID{},
			HeaderApiVersion: value.String{"v0.0.2", true},
			PayloadData:      value.String{Value: "", Valid: true},
			PayloadUser:      value.String{Value: "", Valid: true},
		},
	}
}

func createCsvFileFromData(data []LogRow) string {
	fname := "data2.csv"
	file, err := os.Create(fname)
	if err != nil {
		panic(err)
	}
	defer file.Close()

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

func csvFileFromString() string {
	const raw = `,,,,
	2020-03-10T20:59:30.694177Z,11196991-b193-4610-ae12-bcc03d092927,v0.0.1,Hello world!,Daniel Dubovski
	2020-03-10T20:59:30.694177Z,,v0.0.2,,`

	fname := "data2.csv"
	file, err := os.Create(fname)
	if err != nil {
		panic(err)
	}

	defer file.Close()

	writer := io.StringWriter(file)
	if _, err := writer.WriteString(raw); err != nil {
		panic(err)
	}

	return fname
}

func createIngestionTable(client *kusto.Client) error {
	var dropStmt = kusto.NewStmt(".drop table Logs ifexists")
	var createStmt = kusto.NewStmt(".create table Logs (header_time: datetime, header_id: guid, header_api_version: string, payload_data: string, payload_user: string) ")
	var addMappingStmt = kusto.NewStmt(".create table Logs ingestion json mapping 'Logs_mapping' '[{\"column\":\"header_time\",\"path\":\"$.header.time\",\"datatype\":\"datetime\"},{\"column\":\"header_id\",\"path\":\"$.header.id\",\"datatype\":\"guid\"},{\"column\":\"header_api_version\",\"path\":\"$.header.api_version\",\"datatype\":\"string\"},{\"column\":\"payload_data\",\"path\":\"$.payload.data\",\"datatype\":\"string\"},{\"column\":\"payload_user\",\"path\":\"$.payload.user\",\"datatype\":\"string\"}]'")

	commandsToRun := []kusto.Stmt{dropStmt, createStmt, addMappingStmt}

	for _, cmd := range commandsToRun {
		if _, err := client.Mgmt(context.Background(), testConfig.Database, cmd); err != nil {
			return err
		}
	}

	return nil
}

func createStringyLogsData() string {
	return "{\"header\":{\"time\":\"24-Aug-18 09:42:15\", \"id\":\"0944f542-a637-411b-94dd-8874992d6ebc\", \"api_version\":\"v2\"}, \"payload\":{\"data\":\"NEEUGQSPIPKDPQPIVFE\", \"user\":\"owild@fabrikam.com\"}}\n" +
		"{\"header\":{\"time\":\"24-Aug-18 09:42:27\", \"id\":\"09f7c3a2-27e0-4a9b-b00a-3538fb50fb51\", \"api_version\":\"v1\"}, \"payload\":{\"data\":\"MSLAMKKSTOKEWCQKFHISYDRBGGJAMTOGCGSCUPFFYXROFLTGFUZBNSZIAKUFBJGZAECQJNQPBDUBMDWUNCVRUMTJGKBKUADOQRNAIDWRDJZJYYVXNARYNOEOLTJZMGVBZFKVPWLKGENLMJKIOEWUIFACMZOPTXEXOYJTNAHQOGSJATBBJBKHJATUEIIPHWRIZQXOZQUNWGGBMRBTYMFRMWONFPOESRJSPJJKVNCSHXLDURHM\", \"user\":\"owild@fabrikam.com\"}}\n" +
		"{\"header\":{\"time\":\"24-Aug-18 09:42:47\", \"id\":\"e0e4a6dd-8823-412f-ad0c-84b55267518f\", \"api_version\":\"v1\"}, \"payload\":{\"data\":\"QZWCBJJKBPVEWNLDIQXLKNKPLKTNIBXDAOBPNGJMDSQRBGGGFDERQGJDPHRQQWBZSSEIMWQBGLHSWTOEEMHEWGMUEYAFOSVHQQZICYUJNDKEYRGVTNMDOXDMGJDNVKMOPZCGUFBFSXQTVHVNREMBFSTSNMCSVGODRVOZOABNLGKRGJQZOPWQXKJXGJSHDJKMJNCASVYRDZ\", \"user\":\"jane.austin@fabrikam.com\"}}\n" +
		"{\"header\":{\"time\":\"24-Aug-18 09:42:56\", \"id\":\"e52cd01e-6984-4821-a4aa-a97c334517e5\", \"api_version\":\"v2\"}, \"payload\":{\"data\":\"LEWDDGKXFGMRTFITKCWYH\", \"user\":\"owild@fabrikam.com\"}}\n"
}

func executeCommands(client *kusto.Client, commandsToRun ...kusto.Stmt) error {
	for _, cmd := range commandsToRun {
		if _, err := client.Mgmt(context.Background(), testConfig.Database, cmd); err != nil {
			return err
		}
	}

	return nil
}

func waitForIngest(ctx context.Context, client *kusto.Client, stmt kusto.Stmt, compare func(got, want interface{}) error,
	doer func(row *table.Row, update interface{}) error, want interface{}, gotInit func() interface{}) error {

	deadline := time.Now().Add(1 * time.Minute)

	var loopErr error
	for {
		if time.Now().After(deadline) {
			break
		}
		time.Sleep(5 * time.Second)
		loopErr = nil

		iter, err := client.Query(ctx, testConfig.Database, stmt)
		if err != nil {
			return err
		}
		defer iter.Stop()

		var got = gotInit()
		err = iter.Do(func(row *table.Row) error {
			return doer(row, got)
		})
		if err != nil {
			return fmt.Errorf("had iter.Do() error: %s", err)
		}

		if compare != nil {
			if err := compare(got, want); err != nil {
				loopErr = err
				continue
			}
			break
		}
		if diff := pretty.Compare(want, got); diff != "" {
			loopErr = fmt.Errorf("-want/+got:\n%s", diff)
			continue
		}
		break
	}
	return loopErr
}
