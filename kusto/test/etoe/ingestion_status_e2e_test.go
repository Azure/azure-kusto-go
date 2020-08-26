package etoe

import (
	"context"
	"fmt"
	"io"
	"os"
	"testing"
	"time"

	"github.com/Azure/azure-kusto-go/kusto"
	"github.com/Azure/azure-kusto-go/kusto/ingest"
	"github.com/Azure/azure-kusto-go/kusto/unsafe"
)

const (
	tableName string = "GolangStatusReportingTest"
	scheme    string = " (rownumber:int, rowguid:string, xdouble:real, xfloat:real, xbool:bool, xint16:int, xint32:int, xint64:long, xuint8:long, xuint16:long, xuint32:long, xuint64:long, xdate:datetime, xsmalltext:string, xtext:string, xnumberAsText:string, xtime:timespan, xtextWithNulls:string, xdynamicWithNulls:dynamic)"
	csvFile   string = "testdata/dataset.csv"
	verbose   bool   = false
)

var (
	initDone   bool = false
	initFailed bool = true
	testConf   Config
)

func initOnce() error {
	if !initDone {
		initDone = true

		testConf, err := NewConfig()
		if err != nil {
			return fmt.Errorf("end to end tests disabled: missing config.json file in etoe directory or test environment not set - %s", err)
		}

		ctx, cancel := context.WithCancel(context.Background())
		ctx, cancel = context.WithTimeout(ctx, 1*time.Minute)
		defer cancel()

		client, err := kusto.New(testConfig.Endpoint, testConfig.Authorizer)
		if err != nil {
			return err
		}

		// Drop the old table if exists
		dropStmt := kusto.NewStmt(".drop table ", kusto.UnsafeStmt(unsafe.Stmt{Add: true})).UnsafeAdd(tableName).Add(" ifexists")
		_, err = client.Mgmt(ctx, testConf.Database, dropStmt)
		if err != nil {
			return fmt.Errorf("failed to drop the old table:\n" + err.Error())
		}

		// Create a database
		createStmt := kusto.NewStmt(".create table ", kusto.UnsafeStmt(unsafe.Stmt{Add: true})).UnsafeAdd(tableName).UnsafeAdd(scheme)
		_, err = client.Mgmt(ctx, testConf.Database, createStmt)
		if err != nil {
			return fmt.Errorf("failed to create a table:\n" + err.Error())
		}

		// Change the ingetion batching time
		batchingStmt := kusto.NewStmt(".alter table ", kusto.UnsafeStmt(unsafe.Stmt{Add: true})).UnsafeAdd(tableName).Add(" policy ingestionbatching @'{ \"MaximumBatchingTimeSpan\": \"00:00:25\", \"MaximumNumberOfItems\": 500, \"MaximumRawDataSizeMB\": 1024 }' ")
		_, err = client.Mgmt(ctx, testConf.Database, batchingStmt)
		if err != nil {
			return fmt.Errorf("failed to reduce the default batching time\n" + err.Error())
		}

		initFailed = false
	}

	if initFailed {
		return fmt.Errorf("Init once failed in the past")
	}

	return nil
}

func TestIgestionFromFileWithStatusReportingQueued(t *testing.T) {
	err := initOnce()
	if err != nil {
		t.Skipf("Skipping tests: %s", err)
		return
	}

	ctx, cancel := context.WithCancel(context.Background())
	ctx, cancel = context.WithTimeout(ctx, 3*time.Minute)
	defer cancel()

	client, err := kusto.New(testConfig.Endpoint, testConfig.Authorizer)
	if err != nil {
		panic(err)
	}

	ingestor, err := ingest.New(client, testConfig.Database, tableName)
	if err != nil {
		panic(err)
	}

	chan1 := ingestor.FromFile(ctx, csvFile, ingest.ReportResultToTable()).Wait(ctx)
	chan2 := ingestor.FromFile(ctx, csvFile, ingest.ReportResultToTable()).Wait(ctx)
	chan3 := ingestor.FromFile(ctx, csvFile, ingest.ReportResultToTable()).Wait(ctx)

	var results [3]ingest.StatusRecord
	results[0] = <-chan1
	results[1] = <-chan2
	results[2] = <-chan3

	for i, res := range results {
		if res.Status != ingest.Succeeded {
			t.Errorf("Exepcted status Succeeded however result on channel %d is:\n%s", i+1, res.String())
		} else if verbose {
			println(res.String())
			println()
		}
	}
}

func TestIgestionFromFileWithoutStatusReporting(t *testing.T) {
	err := initOnce()
	if err != nil {
		t.Skipf("Skipping tests: %s", err)
		return
	}

	ctx, cancel := context.WithCancel(context.Background())
	ctx, cancel = context.WithTimeout(ctx, 1*time.Minute)
	defer cancel()

	client, err := kusto.New(testConfig.Endpoint, testConfig.Authorizer)
	if err != nil {
		panic(err)
	}

	ingestor, err := ingest.New(client, testConfig.Database, tableName)
	if err != nil {
		panic(err)
	}

	res := <-ingestor.FromFile(ctx, csvFile).Wait(ctx)
	if res.Status != ingest.Queued {
		t.Errorf("Exepcted status Queued however result is:\n%s", res.String())
	} else if verbose {
		println(res.String())
		println()
	}
}

func TestIgestionFromReaderWithoutStatusReporting(t *testing.T) {
	err := initOnce()
	if err != nil {
		t.Skipf("Skipping tests: %s", err)
		return
	}

	ctx, cancel := context.WithCancel(context.Background())
	ctx, cancel = context.WithTimeout(ctx, 1*time.Minute)
	defer cancel()

	client, err := kusto.New(testConfig.Endpoint, testConfig.Authorizer)
	if err != nil {
		panic(err)
	}

	ingestor, err := ingest.New(client, testConfig.Database, tableName)
	if err != nil {
		panic(err)
	}

	f, err := os.Open(csvFile)
	if err != nil {
		panic(err)
	}

	reader, writer := io.Pipe()
	go func() {
		defer writer.Close()
		io.Copy(writer, f)
	}()

	res := <-ingestor.FromReader(ctx, reader, ingest.FileFormat(ingest.CSV)).Wait(ctx)
	if res.Status != ingest.Queued {
		t.Errorf("Exepcted status Queued however result is:\n%s", res.String())
	} else if verbose {
		println(res.String())
		println()
	}
}

func TestIgestionFromFileWithClientFailure(t *testing.T) {
	err := initOnce()
	if err != nil {
		t.Skipf("Skipping tests: %s", err)
		return
	}

	ctx, cancel := context.WithCancel(context.Background())
	ctx, cancel = context.WithTimeout(ctx, 1*time.Minute)
	defer cancel()

	client, err := kusto.New(testConfig.Endpoint, testConfig.Authorizer)
	if err != nil {
		panic(err)
	}

	ingestor, err := ingest.New(client, testConfig.Database, tableName)
	if err != nil {
		panic(err)
	}

	// Once without status reporting
	res := <-ingestor.FromFile(ctx, "thisfiledoesnotexist.csv").Wait(ctx)
	if res.Status != ingest.ClientError {
		t.Errorf("Test without status reporting:\nExepcted status ClientError however result is:\n%s", res.String())
	} else if verbose {
		println("Test without status reporting:")
		println(res.String())
		println()
	}

	// Once with table status reporting
	res = <-ingestor.FromFile(ctx, "thisfiledoesnotexist.csv", ingest.ReportResultToTable()).Wait(ctx)
	if res.Status != ingest.ClientError {
		t.Errorf("Test with status reporting:\nExepcted status ClientError however result is:\n%s", res.String())
	} else if verbose {
		println("Test with status reporting:")
		println(res.String())
		println()
	}
}

func TestIgestionFromReaderWithClientFailure(t *testing.T) {
	err := initOnce()
	if err != nil {
		t.Skipf("Skipping tests: %s", err)
		return
	}

	ctx, cancel := context.WithCancel(context.Background())
	ctx, cancel = context.WithTimeout(ctx, 1*time.Minute)
	defer cancel()

	client, err := kusto.New(testConfig.Endpoint, testConfig.Authorizer)
	if err != nil {
		panic(err)
	}

	ingestor, err := ingest.New(client, testConfig.Database, tableName)
	if err != nil {
		panic(err)
	}

	// Create an empty reader
	reader, writer := io.Pipe()
	writer.Close()

	// Once without status reporting
	res := <-ingestor.FromReader(ctx, reader).Wait(ctx)
	if res.Status != ingest.ClientError {
		t.Errorf("Test without status reporting:\nExepcted status ClientError however result is:\n%s", res.String())
	} else if verbose {
		println("Test without status reporting:")
		println(res.String())
		println()
	}

	// Once with table status reporting
	res = <-ingestor.FromReader(ctx, reader, ingest.ReportResultToTable()).Wait(ctx)
	if res.Status != ingest.ClientError {
		t.Errorf("Test with status reporting:\nExepcted status ClientError however result is:\n%s", res.String())
	} else if verbose {
		println("Test with status reporting:")
		println(res.String())
		println()
	}
}

func TestIgestionFromFileWithStatusReporting(t *testing.T) {
	err := initOnce()
	if err != nil {
		t.Skipf("Skipping tests: %s", err)
		return
	}

	ctx, cancel := context.WithCancel(context.Background())
	ctx, cancel = context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	client, err := kusto.New(testConfig.Endpoint, testConfig.Authorizer)
	if err != nil {
		panic(err)
	}

	ingestor, err := ingest.New(client, testConfig.Database, tableName)
	if err != nil {
		panic(err)
	}

	res := <-ingestor.FromFile(ctx, csvFile, ingest.ReportResultToTable(), ingest.FlushImmediately()).Wait(ctx)
	if res.Status != ingest.Succeeded {
		t.Errorf("Exepcted status Succeeded however result is:\n%s", res.String())
	} else if verbose {
		println(res.String())
		println()
	}
}

func TestIgestionFromReaderWithStatusReporting(t *testing.T) {
	err := initOnce()
	if err != nil {
		t.Skipf("Skipping tests: %s", err)
		return
	}

	ctx, cancel := context.WithCancel(context.Background())
	ctx, cancel = context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	client, err := kusto.New(testConfig.Endpoint, testConfig.Authorizer)
	if err != nil {
		panic(err)
	}

	ingestor, err := ingest.New(client, testConfig.Database, tableName)
	if err != nil {
		panic(err)
	}

	f, err := os.Open(csvFile)
	if err != nil {
		panic(err)
	}

	reader, writer := io.Pipe()
	go func() {
		defer writer.Close()
		io.Copy(writer, f)
	}()

	res := <-ingestor.FromReader(ctx, reader, ingest.ReportResultToTable(), ingest.FlushImmediately(), ingest.FileFormat(ingest.CSV)).Wait(ctx)
	if res.Status != ingest.Succeeded {
		t.Errorf("Exepcted status Succeeded however result is:\n%s", res.String())
	} else if verbose {
		println(res.String())
		println()
	}
}
