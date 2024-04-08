package etoe

import (
	"context"
	"encoding/csv"
	"fmt"
	"github.com/Azure/azure-kusto-go/azkustodata"
	"github.com/Azure/azure-kusto-go/azkustodata/errors"
	"github.com/Azure/azure-kusto-go/azkustodata/kql"
	"github.com/Azure/azure-kusto-go/azkustodata/query"
	"github.com/Azure/azure-kusto-go/azkustodata/testshared"
	"github.com/Azure/azure-kusto-go/azkustodata/value"
	"github.com/Azure/azure-kusto-go/azkustoingest"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"
	"io"
	"math/rand"
	"os"
	"strings"
	"sync"
	"testing"
	"time"
)

var (
	countStatement = kql.New("table(tableName) | count")
)

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

func TestFileIngestion(t *testing.T) { //ok
	t.Parallel()

	if skipETOE || testing.Short() {
		t.Skipf("end to end tests disabled: missing config.json file in etoe directory")
	}

	client, err := azkustodata.New(testConfig.kcsb)
	require.NoError(t, err)

	t.Cleanup(func() {
		t.Log("Closing client")
		require.NoError(t, client.Close())
		t.Log("Closed client")
	})

	queuedTable := "goe2e_queued_file_logs"
	streamingTable := "goe2e_streaming_file_logs"
	managedTable := "goe2e_managed_streaming_file_logs"

	queuedIngestor, err := azkustoingest.New(testConfig.kcsb, azkustoingest.WithDefaultDatabase(testConfig.Database), azkustoingest.WithDefaultTable(queuedTable))
	require.NoError(t, err)
	t.Cleanup(func() {
		t.Log("Closing queuedIngestor")
		require.NoError(t, queuedIngestor.Close())
		t.Log("Closed queuedIngestor")
	})

	streamingIngestor, err := azkustoingest.NewStreaming(testConfig.kcsb, azkustoingest.WithDefaultDatabase(testConfig.Database), azkustoingest.WithDefaultTable(streamingTable))
	require.NoError(t, err)

	t.Cleanup(func() {
		t.Log("Closing streamingIngestor")
		require.NoError(t, streamingIngestor.Close())
		t.Log("Closed streamingIngestor")
	})

	managedIngestor, err := azkustoingest.NewManaged(testConfig.kcsb, azkustoingest.WithDefaultDatabase(testConfig.Database), azkustoingest.WithDefaultTable(managedTable))
	require.NoError(t, err)

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
		ingestor azkustoingest.Ingestor
		// src represents where we are getting our data.
		src string
		// options are options used on ingesting.
		options []azkustoingest.FileOption
		// stmt is used to query for the results.
		stmt azkustodata.Statement
		// table is the name of the table to create and use as a parameter.
		table string
		// teardown is a function that will be called before the test ends.
		teardown func() error
		// doer is called from within the function passed to RowIterator.Do(). It allows us to collect the data we receive.
		doer func(row query.Row, update interface{}) error
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
			options:  []azkustoingest.FileOption{azkustoingest.IngestionMappingRef("Logs_bad_mapping", azkustoingest.JSON)},
			wantErr: azkustoingest.StatusFromMapForTests(map[string]interface{}{
				"Status":        "Failed",
				"FailureStatus": "Permanent",
				"ErrorCode":     "BadRequest_MappingReferenceWasNotFound",
			}),
		},
		{
			desc:     "Streaming ingest from blob",
			ingestor: streamingIngestor,
			src:      "https://adxingestiondemo.blob.core.windows.net/data/demo.json",
			options:  []azkustoingest.FileOption{azkustoingest.IngestionMappingRef("Logs_mapping", azkustoingest.JSON)},
			stmt:     countStatement,
			table:    streamingTable,
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
				var v []testshared.CountResult
				return &v
			},
			want: &[]testshared.CountResult{{Count: 500}},
		},
		{
			desc:     "Managed streaming ingest from blob",
			ingestor: managedIngestor,
			src:      "https://adxingestiondemo.blob.core.windows.net/data/demo.json",
			options:  []azkustoingest.FileOption{azkustoingest.IngestionMappingRef("Logs_mapping", azkustoingest.JSON)},
			stmt:     countStatement,
			table:    managedTable,
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
				var v []testshared.CountResult
				return &v
			},
			want: &[]testshared.CountResult{{Count: 500}},
		},
		{
			desc:     "Ingest from blob with existing mapping",
			ingestor: queuedIngestor,
			src:      "https://adxingestiondemo.blob.core.windows.net/data/demo.json",
			options:  []azkustoingest.FileOption{azkustoingest.IngestionMappingRef("Logs_mapping", azkustoingest.JSON)},
			stmt:     countStatement,
			table:    queuedTable,
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
				var v []testshared.CountResult
				return &v
			},
			want: &[]testshared.CountResult{{Count: 500}},
		},
		{
			desc:     "Ingest from csv with ignore first record",
			ingestor: queuedIngestor,
			src:      csvFileFromString(t),
			options:  []azkustoingest.FileOption{azkustoingest.IgnoreFirstRecord()},
			stmt:     countStatement,
			table:    queuedTable,
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
				var v []testshared.CountResult
				return &v
			},
			want: &[]testshared.CountResult{{Count: 2}},
		},
		{
			desc:     "Ingest from blob with existing mapping managed",
			ingestor: managedIngestor,
			src:      "https://adxingestiondemo.blob.core.windows.net/data/demo.json",
			options:  []azkustoingest.FileOption{azkustoingest.IngestionMappingRef("Logs_mapping", azkustoingest.JSON)},
			stmt:     countStatement,
			table:    managedTable,
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
				var v []testshared.CountResult
				return &v
			},
			want: &[]testshared.CountResult{{Count: 500}},
		},
		{
			desc:     "Ingest from blob with inline mapping",
			ingestor: queuedIngestor,
			src:      "https://adxingestiondemo.blob.core.windows.net/data/demo.json",
			options: []azkustoingest.FileOption{
				azkustoingest.IngestionMapping(
					"[{\"column\":\"header_time\",\"datatype\":\"datetime\",\"Properties\":{\"path\":\"$.header.time\"}},{\"column\":\"header_id\",\"datatype\":\"guid\",\"Properties\":{\"path\":\"$.header.id\"}},{\"column\":\"header_api_version\",\"Properties\":{\"path\":\"$.header.api_version\"},\"datatype\":\"string\"},{\"column\":\"payload_data\",\"datatype\":\"string\",\"Properties\":{\"path\":\"$.payload.data\"}},{\"column\":\"payload_user\",\"datatype\":\"string\",\"Properties\":{\"path\":\"$.payload.user\"}}]",
					azkustoingest.JSON,
				),
			},
			stmt:  countStatement,
			table: queuedTable,
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
				var v []testshared.CountResult
				return &v
			},
			want: &[]testshared.CountResult{{Count: 500}},
		},
		{
			desc:     "Ingestion from local file queued",
			ingestor: queuedIngestor,
			src:      csvFileFromString(t),
			stmt:     countStatement,
			table:    queuedTable,
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
				var v []testshared.CountResult
				return &v
			},
			want: &[]testshared.CountResult{{Count: 3}},
		},
		{
			desc:     "Ingestion from local file test 2 queued",
			ingestor: queuedIngestor,
			src:      createCsvFileFromData(t, mockRows),
			stmt:     kql.New("table(tableName) | order by header_api_version asc"),
			table:    queuedTable,
			doer: func(row query.Row, update interface{}) error {
				rec := LogRow{}
				if err := row.ToStruct(&rec); err != nil {
					return err
				}
				recs := update.(*[]LogRow)
				*recs = append(*recs, rec)
				return nil
			},
			gotInit: func() interface{} {
				var v []LogRow
				return &v
			},
			want: &mockRows,
		},
		{
			desc:     "Ingest from local with existing mapping streaming",
			ingestor: streamingIngestor,
			src:      "testdata/demo.json",
			options:  []azkustoingest.FileOption{azkustoingest.IngestionMappingRef("Logs_mapping", azkustoingest.JSON)},
			stmt:     countStatement,
			table:    streamingTable,
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
				var v []testshared.CountResult
				return &v
			},
			want: &[]testshared.CountResult{{Count: 500}},
		},
		{
			desc:     "Ingestion from local file streaming",
			ingestor: streamingIngestor,
			src:      csvFileFromString(t),
			stmt:     countStatement,
			table:    streamingTable,
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
				var v []testshared.CountResult
				return &v
			},
			want: &[]testshared.CountResult{{Count: 3}},
		},
		{
			desc:     "Ingestion from local file test 2 streaming",
			ingestor: streamingIngestor,
			src:      createCsvFileFromData(t, mockRows),
			stmt:     kql.New("table(tableName)  | order by header_api_version asc"),
			table:    streamingTable,
			doer: func(row query.Row, update interface{}) error {
				rec := LogRow{}
				if err := row.ToStruct(&rec); err != nil {
					return err
				}
				recs := update.(*[]LogRow)
				*recs = append(*recs, rec)
				return nil
			},
			gotInit: func() interface{} {
				var v []LogRow
				return &v
			},
			want: &mockRows,
		},
		{
			desc:     "Ingest from local with existing mapping managed streaming",
			ingestor: managedIngestor,
			src:      "testdata/demo.json",
			options:  []azkustoingest.FileOption{azkustoingest.IngestionMappingRef("Logs_mapping", azkustoingest.JSON)},
			stmt:     countStatement,
			table:    managedTable,
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
				var v []testshared.CountResult
				return &v
			},
			want: &[]testshared.CountResult{{Count: 500}},
		},
		{
			desc:     "Ingest big file managed streaming",
			ingestor: managedIngestor,
			src:      bigCsvFileFromString(t),
			options:  []azkustoingest.FileOption{azkustoingest.DontCompress()},
			stmt:     countStatement,
			table:    managedTable,
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
				var v []testshared.CountResult
				return &v
			},
			want: &[]testshared.CountResult{{Count: 3}},
		},
	}

	for _, test := range tests {
		test := test // capture
		t.Run(test.desc, func(t *testing.T) {
			t.Parallel()

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			fTable := ""
			if test.table != "" {
				fTable = fmt.Sprintf("%s_%d_%d", test.table, time.Now().UnixNano(), rand.Int())
				require.NoError(t, testshared.CreateTestTable(t, client, fTable))
				test.options = append(test.options, azkustoingest.Table(fTable))
			}

			if test.teardown != nil {
				defer func() {
					if err := test.teardown(); err != nil {
						panic(err)
					}
				}()
			}

			_, isQueued := test.ingestor.(*azkustoingest.Ingestion)
			_, isManaged := test.ingestor.(*azkustoingest.Managed)
			if isQueued || isManaged {
				test.options = append(test.options, azkustoingest.FlushImmediately(), azkustoingest.ReportResultToTable())
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

			require.NoError(t, waitForIngest(t, ctx, client, testConfig.Database, fTable, test.stmt, test.doer, test.want, test.gotInit))
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

	client, err := azkustodata.New(testConfig.kcsb)
	if err != nil {
		panic(err)
	}

	t.Cleanup(func() {
		t.Log("Closing client")
		require.NoError(t, client.Close())
		t.Log("Closed client")
	})

	queuedIngestor, err := azkustoingest.New(testConfig.kcsb, azkustoingest.WithDefaultDatabase(testConfig.Database), azkustoingest.WithDefaultTable(queuedTable))
	if err != nil {
		panic(err)
	}

	t.Cleanup(func() {
		t.Log("Closing queuedIngestor")
		require.NoError(t, queuedIngestor.Close())
		t.Log("Closed queuedIngestor")
	})

	streamingIngestor, err := azkustoingest.NewStreaming(testConfig.kcsb, azkustoingest.WithDefaultDatabase(testConfig.Database), azkustoingest.WithDefaultTable(streamingTable))
	if err != nil {
		panic(err)
	}

	t.Cleanup(func() {
		t.Log("Closing streamingIngestor")
		require.NoError(t, streamingIngestor.Close())
		t.Log("Closed streamingIngestor")
	})

	managedIngestor, err := azkustoingest.NewManaged(testConfig.kcsb, azkustoingest.WithDefaultDatabase(testConfig.Database), azkustoingest.WithDefaultTable(managedTable))
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
		ingestor azkustoingest.Ingestor
		// src represents where we are getting our data.
		src string
		// options are options used on ingesting.
		options []azkustoingest.FileOption
		// stmt is used to query for the results.
		stmt azkustodata.Statement
		// table is the name of the table to create and use as a parameter.
		table string
		// teardown is a function that will be called before the test ends.
		teardown func() error
		// doer is called from within the function passed to RowIterator.Do(). It allows us to collect the data we receive.
		doer func(row query.Row, update interface{}) error
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
			options:  []azkustoingest.FileOption{azkustoingest.FileFormat(azkustoingest.JSON), azkustoingest.IngestionMappingRef("Logs_bad_mapping", azkustoingest.JSON)},
			wantErr: azkustoingest.StatusFromMapForTests(map[string]interface{}{
				"Status":        "Failed",
				"FailureStatus": "Permanent",
				"ErrorCode":     "BadRequest_MappingReferenceWasNotFound",
			}),
		},
		{
			desc:     "Ingest with existing mapping",
			ingestor: queuedIngestor,
			src:      "testdata/demo.json",
			options: []azkustoingest.FileOption{
				azkustoingest.FileFormat(azkustoingest.JSON),
				azkustoingest.IngestionMappingRef("Logs_mapping", azkustoingest.JSON),
			},
			stmt:  countStatement,
			table: queuedTable,
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
				var v []testshared.CountResult
				return &v
			},
			want: &[]testshared.CountResult{{Count: 500}},
		},
		{
			desc:     "Ingest with inline mapping",
			ingestor: queuedIngestor,
			src:      "testdata/demo.json",
			options: []azkustoingest.FileOption{
				azkustoingest.FileFormat(azkustoingest.JSON),
				azkustoingest.IngestionMapping(
					"[{\"column\":\"header_time\",\"datatype\":\"datetime\",\"Properties\":{\"path\":\"$.header.time\"}},{\"column\":\"header_id\",\"datatype\":\"guid\",\"Properties\":{\"path\":\"$.header.id\"}},{\"column\":\"header_api_version\",\"Properties\":{\"path\":\"$.header.api_version\"},\"datatype\":\"string\"},{\"column\":\"payload_data\",\"datatype\":\"string\",\"Properties\":{\"path\":\"$.payload.data\"}},{\"column\":\"payload_user\",\"datatype\":\"string\",\"Properties\":{\"path\":\"$.payload.user\"}}]",
					azkustoingest.JSON,
				),
			},
			stmt:  countStatement,
			table: queuedTable,
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
				var v []testshared.CountResult
				return &v
			},
			want: &[]testshared.CountResult{{Count: 500}},
		},
		{
			desc:     "Ingestion from mock data",
			ingestor: queuedIngestor,
			src:      createCsvFileFromData(t, mockRows),
			options: []azkustoingest.FileOption{
				azkustoingest.FileFormat(azkustoingest.CSV),
			},
			stmt:  kql.New("table(tableName) | order by header_api_version asc"),
			table: queuedTable,
			doer: func(row query.Row, update interface{}) error {
				rec := LogRow{}
				if err := row.ToStruct(&rec); err != nil {
					return err
				}
				recs := update.(*[]LogRow)
				*recs = append(*recs, rec)
				return nil
			},
			gotInit: func() interface{} {
				var v []LogRow
				return &v
			},
			want: &mockRows,
		},
		{
			desc:     "Ingest from local with existing mapping streaming",
			ingestor: streamingIngestor,
			src:      "testdata/demo.json",
			options: []azkustoingest.FileOption{
				azkustoingest.IngestionMappingRef("Logs_mapping", azkustoingest.JSON),
				azkustoingest.FileFormat(azkustoingest.JSON),
			},
			stmt:  countStatement,
			table: streamingTable,
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
				var v []testshared.CountResult
				return &v
			},
			want: &[]testshared.CountResult{{Count: 500}},
		},
		{
			desc:     "Ingestion from local file streaming",
			ingestor: streamingIngestor,
			src:      csvFileFromString(t),
			options: []azkustoingest.FileOption{
				azkustoingest.FileFormat(azkustoingest.CSV),
			},
			stmt:  countStatement,
			table: streamingTable,
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
				var v []testshared.CountResult
				return &v
			},
			want: &[]testshared.CountResult{{Count: 3}},
		},
		{
			desc:     "Ingestion from local file test 2 streaming",
			ingestor: streamingIngestor,
			options: []azkustoingest.FileOption{
				azkustoingest.FileFormat(azkustoingest.CSV),
			},
			src:   createCsvFileFromData(t, mockRows),
			stmt:  kql.New("table(tableName) | order by header_api_version asc"),
			table: streamingTable,
			doer: func(row query.Row, update interface{}) error {
				rec := LogRow{}
				if err := row.ToStruct(&rec); err != nil {
					return err
				}
				recs := update.(*[]LogRow)
				*recs = append(*recs, rec)
				return nil
			},
			gotInit: func() interface{} {
				var v []LogRow
				return &v
			},
			want: &mockRows,
		},
		{
			desc:     "Ingest from local with existing mapping managed streaming",
			ingestor: managedIngestor,
			src:      "testdata/demo.json",
			options: []azkustoingest.FileOption{
				azkustoingest.IngestionMappingRef("Logs_mapping", azkustoingest.JSON),
				azkustoingest.FileFormat(azkustoingest.JSON),
			},
			stmt:  countStatement,
			table: managedTable,
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
				var v []testshared.CountResult
				return &v
			},
			want: &[]testshared.CountResult{{Count: 500}},
		},
	}

	for _, test := range tests {
		test := test // capture
		t.Run(test.desc, func(t *testing.T) {
			t.Parallel()

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			var fTable string
			if test.table != "" {
				fTable = fmt.Sprintf("%s_%d_%d", test.table, time.Now().UnixNano(), rand.Int())
				require.NoError(t, testshared.CreateTestTable(t, client, fTable))
				test.options = append(test.options, azkustoingest.Table(fTable))
			}

			if test.teardown != nil {
				defer func() {
					if err := test.teardown(); err != nil {
						panic(err)
					}
				}()
			}

			_, isQueued := test.ingestor.(*azkustoingest.Ingestion)
			_, isManaged := test.ingestor.(*azkustoingest.Managed)
			if isQueued || isManaged {
				test.options = append(test.options, azkustoingest.FlushImmediately(), azkustoingest.ReportResultToTable())
			}

			f, err := os.Open(test.src)
			if err != nil {
				panic(err)
			}
			defer f.Close()

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

			require.NoError(t, waitForIngest(t, ctx, client, testConfig.Database, fTable, test.stmt, test.doer, test.want, test.gotInit))
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

	client, err := azkustodata.New(testConfig.kcsb)
	if err != nil {
		panic(err)
	}

	t.Cleanup(func() {
		t.Log("Closing client")
		require.NoError(t, client.Close())
		t.Log("Closed client")
	})
	var skcsb *azkustodata.ConnectionStringBuilder

	if testConfig.ClientID == "" || testConfig.ClientSecret == "" || testConfig.TenantID == "" {
		skcsb = azkustodata.NewConnectionStringBuilder(testConfig.SecondaryEndpoint).WithAzCli()
	} else {
		skcsb = azkustodata.NewConnectionStringBuilder(testConfig.SecondaryEndpoint).WithAadAppKey(testConfig.ClientID, testConfig.ClientSecret, testConfig.TenantID)
	}

	secondaryClient, err := azkustodata.New(skcsb)
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

	queuedIngestor, err := azkustoingest.New(testConfig.kcsb, azkustoingest.WithDefaultDatabase(testConfig.Database), azkustoingest.WithDefaultTable(queuedTable))
	if err != nil {
		panic(err)
	}
	t.Cleanup(func() {
		t.Log("Closing queuedIngestor")
		require.NoError(t, queuedIngestor.Close())
		t.Log("Closed queuedIngestor")
	})

	streamingIngestor, err := azkustoingest.NewStreaming(testConfig.kcsb, azkustoingest.WithDefaultDatabase(testConfig.Database), azkustoingest.WithDefaultTable(streamingTable))
	if err != nil {
		panic(err)
	}

	t.Cleanup(func() {
		t.Log("Closing streamingIngestor")
		require.NoError(t, streamingIngestor.Close())
		t.Log("Closed streamingIngestor")
	})

	secondaryQueuedIngestor, err := azkustoingest.New(skcsb, azkustoingest.WithDefaultDatabase(testConfig.SecondaryDatabase), azkustoingest.WithDefaultTable(queuedTable))
	if err != nil {
		panic(err)
	}

	t.Cleanup(func() {
		t.Log("Closing secondaryQueuedIngestor")
		require.NoError(t, secondaryQueuedIngestor.Close())
		t.Log("Closed secondaryQueuedIngestor")
	})

	secondaryStreamingIngestor, err := azkustoingest.NewStreaming(skcsb, azkustoingest.WithDefaultDatabase(testConfig.SecondaryDatabase), azkustoingest.WithDefaultTable(streamingTable))
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
		ingestor azkustoingest.Ingestor
		// the type of ingsetor for the secondary cluster for the test
		secondaryIngestor azkustoingest.Ingestor
		// src represents where we are getting our data.
		src string
		// stmt is used to query for the results.
		stmt azkustodata.Statement
		// doer is called from within the function passed to RowIterator.Do(). It allows us to collect the data we receive.
		doer func(row query.Row, update interface{}) error
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
			stmt:              countStatement,
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
				var v []testshared.CountResult
				return &v
			},
			want: &[]testshared.CountResult{{Count: 3}},
		},
		{
			desc:              "Ingestion from local file streaming",
			table:             streamingTable,
			secondaryTable:    secondaryStreamingTable,
			ingestor:          streamingIngestor,
			secondaryIngestor: secondaryStreamingIngestor,
			src:               csvFileFromString(t),
			stmt:              countStatement,
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
				var v []testshared.CountResult
				return &v
			},
			want: &[]testshared.CountResult{{Count: 3}},
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

				require.NoError(t, testshared.CreateDefaultTestTableWithDB(t, client, testConfig.Database, fTable))

				var options []azkustoingest.FileOption
				if _, ok := test.ingestor.(*azkustoingest.Ingestion); ok {
					options = append(options, azkustoingest.FlushImmediately(), azkustoingest.ReportResultToTable())
				}
				firstOptions := append(options, azkustoingest.Database(testConfig.Database), azkustoingest.Table(fTable))

				res, err := test.ingestor.FromFile(ctx, test.src, firstOptions...)
				if err == nil {
					err = <-res.Wait(ctx)
				}

				primaryErr = err

				if !assertErrorsMatch(t, err, nil) {
					t.Errorf("TestMultipleClusters(%s): ingestor.FromFile(): got err == %v, want err == %v", test.desc, err, nil)
				}

				require.NoError(t, waitForIngest(t, ctx, client, testConfig.Database, fTable, test.stmt, test.doer, test.want, test.gotInit))
			}()

			// Run ingestion to secondary database in a Goroutine
			wg.Add(1)
			go func() {
				defer wg.Done()

				require.NoError(t, testshared.CreateDefaultTestTableWithDB(t, secondaryClient, testConfig.SecondaryDatabase, fSecondaryTable))

				var options []azkustoingest.FileOption
				if _, ok := test.secondaryIngestor.(*azkustoingest.Ingestion); ok {
					options = append(options, azkustoingest.FlushImmediately(), azkustoingest.ReportResultToTable())
				}
				secondaryOptions := append(options, azkustoingest.Database(testConfig.SecondaryDatabase), azkustoingest.Table(fSecondaryTable))

				res, err := test.secondaryIngestor.FromFile(ctx, test.src, secondaryOptions...)
				if err == nil {
					err = <-res.Wait(ctx)
				}

				secondaryErr = err

				if !assertErrorsMatch(t, err, nil) {
					t.Errorf("TestMultipleClusters(%s): ingestor.FromFile(): got err == %v, want err == %v", test.desc, err, nil)
				}

				require.NoError(t, waitForIngest(t, ctx, secondaryClient, testConfig.SecondaryDatabase, fSecondaryTable, test.stmt, test.doer, test.want, test.gotInit))
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

func assertErrorsMatch(t *testing.T, got, want error) bool {
	if azkustoingest.IsStatusRecord(got) {
		if want == nil || !azkustoingest.IsStatusRecord(want) {
			return false
		}

		codeGot, _ := azkustoingest.GetErrorCode(got)
		codeWant, _ := azkustoingest.GetErrorCode(want)

		statusGot, _ := azkustoingest.GetIngestionStatus(got)
		statusWant, _ := azkustoingest.GetIngestionStatus(want)

		failureStatusGot, _ := azkustoingest.GetIngestionFailureStatus(got)
		failureStatusWant, _ := azkustoingest.GetIngestionFailureStatus(want)

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

func createMockLogRows() []LogRow {
	fakeUid, _ := uuid.Parse("11196991-b193-4610-ae12-bcc03d092927")
	fakeTime, _ := time.Parse(time.RFC3339Nano, "2020-03-10T20:59:30.694177Z")
	return []LogRow{
		// One empty line
		{
			HeaderTime:       *value.NewNullDateTime(),
			HeaderId:         *value.NewNullGUID(),
			HeaderApiVersion: *value.NewString(""),
			PayloadData:      *value.NewString(""),
			PayloadUser:      *value.NewString(""),
		},
		// One full line
		{
			HeaderTime:       *value.NewDateTime(fakeTime),
			HeaderId:         *value.NewGUID(fakeUid),
			HeaderApiVersion: *value.NewString("v0.0.1"),
			PayloadData:      *value.NewString("Hello world!"),
			PayloadUser:      *value.NewString("Daniel Dubovski"),
		},
		// Partial Data
		{
			HeaderTime:       *value.NewDateTime(fakeTime),
			HeaderId:         *value.NewNullGUID(),
			HeaderApiVersion: *value.NewString("v0.0.2"),
			PayloadData:      *value.NewString(""),
			PayloadUser:      *value.NewString(""),
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

func waitForIngest(t *testing.T, ctx context.Context, client *azkustodata.Client, database string, tableName string, stmt azkustodata.Statement, doer func(row query.Row, update interface{}) error, want interface{}, gotInit func() interface{}) error {

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

			var dataset query.Dataset
			var err error

			if tableName != "" {
				params := azkustodata.QueryParameters(kql.NewParameters().AddString("tableName", tableName))
				dataset, err = client.Query(ctx, database, stmt, params)
			} else {
				dataset, err = client.Query(ctx, database, stmt)
			}
			if err != nil {
				return false, err
			}

			got = gotInit()
			rows := dataset.Tables()[0].Rows()

			for _, row := range rows {
				if err := doer(row, got); err != nil {
					return false, err
				}
			}

			if !assert.NoError(t, err) {
				return false, err
			}

			if !assert.ObjectsAreEqualValues(want, got) {
				failed = true
				time.Sleep(100 * time.Millisecond)
				return true, nil
			}

			return false, err
		}()
	}
	if failed {
		require.EqualValues(t, want, got)
	}

	return err
}

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}
