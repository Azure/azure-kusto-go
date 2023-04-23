package etoe

import (
	"context"
	"fmt"
	"github.com/Azure/azure-kusto-go/azkustodata"
	"github.com/Azure/azure-kusto-go/azkustodata/kql"
	"io"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/Azure/azure-kusto-go/azkustoingest"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	scheme  string = "(rownumber:int, rowguid:string, xdouble:real, xfloat:real, xbool:bool, xint16:int, xint32:int, xint64:long, xuint8:long, xuint16:long, xuint32:long, xuint64:long, xdate:datetime, xsmalltext:string, xtext:string, xnumberAsText:string, xtime:timespan, xtextWithNulls:string, xdynamicWithNulls:dynamic)"
	csvFile string = "testdata/dataset.csv"
)

func TestIngestionStatus(t *testing.T) {
	if skipETOE {
		fmt.Println("end to end tests disabled: missing config.json file in etoe directory and test environment not set")
		return
	}

	tableName := fmt.Sprintf("goe2e_status_reporting_test_%d", time.Now().Unix())

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Minute)
	defer cancel()

	client, err := azkustodata.New(testConfig.kcsb)
	require.NoError(t, err)

	ingestor, err := azkustoingest.New(client, testConfig.Database, tableName)
	require.NoError(t, err)

	t.Cleanup(func() {
		t.Log("Closing client")
		require.NoError(t, client.Close())
		t.Log("Closed client")
		t.Log("Closing ingestor")
		require.NoError(t, ingestor.Close())
		t.Log("Closed ingestor")
	})

	err = createIngestionTableWithDBAndScheme(t, client, testConfig.Database, tableName, false, scheme)
	require.NoError(t, err)

	// Change the ingestion batching time
	batchingStmt := kql.New(".alter table ").AddTable(tableName).AddLiteral(
		" policy ingestionbatching @'{ \"MaximumBatchingTimeSpan\": \"00:00:05\", \"MaximumNumberOfItems\": 500, \"MaximumRawDataSizeMB\": 1024 }' ")
	_, err = client.Mgmt(ctx, testConfig.Database, batchingStmt)
	require.NoError(t, err, "failed to reduce the default batching time")

	// Refresh policy cache on DM
	batchingStmt2 := kql.New(".refresh database '").AddKeyword(testConfig.Database).AddLiteral(
		"' table '").AddTable(tableName).AddLiteral("' cache ingestionbatchingpolicy")
	_, err = client.Mgmt(ctx, "NetDefaultDB", batchingStmt2, azkustodata.IngestionEndpoint())

	require.NoError(t, err, "failed to refresh policy cache on DM")

	t.Run("FromFileWithStatusReportingQueued", func(t *testing.T) {
		t.Parallel()
		ctx, cancel := context.WithTimeout(context.Background(), 1*time.Minute)
		defer cancel()

		count := 5
		var ch [5]chan error
		var errors [5]error

		for i := 0; i < count; i++ {
			res, err := ingestor.FromFile(ctx, csvFile, azkustoingest.ReportResultToTable())
			require.NoError(t, err)

			ch[i] = res.Wait(ctx)
		}

		for i := 0; i < count; i++ {
			errors[i] = <-ch[i]
		}

		for i, err := range errors {
			assert.NoError(t, err, "Exepcted status Succeeded however result on channel %d is:\n%s", i+1, err)
		}
	})

	t.Run("FromReaderWithStatusReportingQueued", func(t *testing.T) {
		t.Parallel()
		ctx, cancel := context.WithTimeout(context.Background(), 1*time.Minute)
		defer cancel()

		count := 5
		var ch [5]chan error
		var errors [5]error

		for i := 0; i < count; i++ {
			f, err := os.Open(csvFile)
			require.NoError(t, err)

			reader, writer := io.Pipe()
			go func() {
				defer func(writer *io.PipeWriter) {
					err := writer.Close()
					require.NoError(t, err)
				}(writer)
				_, err := io.Copy(writer, f)
				require.NoError(t, err)
			}()

			res, err := ingestor.FromReader(ctx, reader, azkustoingest.ReportResultToTable(), azkustoingest.FileFormat(azkustoingest.CSV))
			require.NoError(t, err)
			ch[i] = res.Wait(ctx)
		}

		for i := 0; i < count; i++ {
			errors[i] = <-ch[i]
		}

		for i, err := range errors {
			assert.NoError(t, err, "Exepcted status Succeeded however result on channel %d is:\n%s", i+1, err)
		}
	})

	t.Run("FromFileWithoutStatusReporting", func(t *testing.T) {
		t.Parallel()
		ctx, cancel := context.WithTimeout(context.Background(), 1*time.Minute)
		defer cancel()

		res, err := ingestor.FromFile(ctx, csvFile)
		require.NoError(t, err)

		err = <-res.Wait(ctx)
		assert.NoError(t, err, "Exepcted status Queued however result is:\n%s", err)
	})

	t.Run("FromReaderWithoutStatusReporting", func(t *testing.T) {
		t.Parallel()
		ctx, cancel := context.WithTimeout(context.Background(), 1*time.Minute)
		defer cancel()

		f, err := os.Open(csvFile)
		require.NoError(t, err)

		reader, writer := io.Pipe()
		go func() {
			defer func(writer *io.PipeWriter) {
				err := writer.Close()
				require.NoError(t, err)
			}(writer)
			_, err := io.Copy(writer, f)
			require.NoError(t, err)
		}()

		res, err := ingestor.FromReader(ctx, reader, azkustoingest.FileFormat(azkustoingest.CSV))
		require.NoError(t, err)

		err = <-res.Wait(ctx)
		assert.NoError(t, err, "Exepcted status Queued however result is:\n%s", err)
	})

	t.Run("FromFileWithClientFailure", func(t *testing.T) {
		t.Parallel()
		ctx, cancel := context.WithTimeout(context.Background(), 1*time.Minute)
		defer cancel()

		// Once without status reporting
		_, err = ingestor.FromFile(ctx, "thisfiledoesnotexist.csv")
		assert.Error(t, err, "Test without status reporting:\nExepcted status ClientError however result is:\n%s", err)

		// Once with table status reporting
		_, err = ingestor.FromFile(ctx, "thisfiledoesnotexist.csv", azkustoingest.ReportResultToTable())
		assert.Error(t, err, "Test with status reporting:\nExepcted status ClientError however result is:\n%s", err)

	})

	t.Run("FromReaderWithClientFailure", func(t *testing.T) {
		t.Parallel()
		ctx, cancel := context.WithTimeout(context.Background(), 1*time.Minute)
		defer cancel()

		// Create a faulty json
		reader, writer := io.Pipe()
		go func() {
			defer func(writer *io.PipeWriter) {
				err := writer.Close()
				require.NoError(t, err)
			}(writer)
			_, err := io.Copy(writer, strings.NewReader("{"))
			require.NoError(t, err)
		}()

		// Once without status reporting
		_, err := ingestor.FromReader(ctx, reader)
		assert.NoError(t, err)

		// Once with table status reporting
		res, err := ingestor.FromReader(ctx, reader, azkustoingest.ReportResultToTable())
		assert.NoError(t, err)
		err = <-res.Wait(ctx)
		assert.Error(t, err)
		status, err := azkustoingest.GetIngestionStatus(err)
		assert.NoError(t, err)
		assert.Equal(t, azkustoingest.Failed, status)
	})

	t.Run("FromFileWithStatusReporting", func(t *testing.T) {
		t.Parallel()
		ctx, cancel := context.WithTimeout(context.Background(), 1*time.Minute)
		defer cancel()

		client, err := azkustodata.New(testConfig.kcsb)
		require.NoError(t, err)

		ingestor, err := azkustoingest.New(client, testConfig.Database, tableName)
		require.NoError(t, err)

		t.Cleanup(func() {
			t.Log("Closing client")
			require.NoError(t, client.Close())
			t.Log("Closed client")
			t.Log("Closing ingestor")
			require.NoError(t, ingestor.Close())
			t.Log("Closed ingestor")
		})

		res, err := ingestor.FromFile(ctx, csvFile, azkustoingest.ReportResultToTable(), azkustoingest.FlushImmediately())
		require.NoError(t, err)

		err = <-res.Wait(ctx)
		assert.NoError(t, err, "Exepcted status Succeeded however result is:\n%s", err)
	})

	t.Run("FromReaderWithStatusReporting", func(t *testing.T) {
		t.Parallel()
		ctx, cancel := context.WithTimeout(context.Background(), 1*time.Minute)
		defer cancel()

		f, err := os.Open(csvFile)
		defer f.Close()
		require.NoError(t, err)

		reader, writer := io.Pipe()
		go func() {
			defer func(writer *io.PipeWriter) {
				err := writer.Close()
				require.NoError(t, err)
			}(writer)
			_, err := io.Copy(writer, f)
			require.NoError(t, err)
		}()

		res, err := ingestor.FromReader(ctx, reader, azkustoingest.ReportResultToTable(), azkustoingest.FlushImmediately(), azkustoingest.FileFormat(azkustoingest.CSV))
		require.NoError(t, err)

		err = <-res.Wait(ctx)
		assert.NoError(t, err, "Exepcted status Succeeded however result is:\n%s", err)
	})

}
