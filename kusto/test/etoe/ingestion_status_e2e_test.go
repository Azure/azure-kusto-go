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

func init() {
	if skipETOE {
		fmt.Println("end to end tests disabled: missing config.json file in etoe directory and test environment not set")
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Minute)
	defer cancel()

	client, err := kusto.New(testConfig.Endpoint, testConfig.Authorizer)
	if err != nil {
		panic(err)
	}

	// Drop the old table if exists
	dropStmt := kusto.NewStmt(".drop table ", kusto.UnsafeStmt(unsafe.Stmt{Add: true})).UnsafeAdd(tableName).Add(" ifexists")
	_, err = client.Mgmt(ctx, testConfig.Database, dropStmt)
	if err != nil {
		panic("failed to drop the old table:\n" + err.Error())
	}

	// Create a database
	createStmt := kusto.NewStmt(".create table ", kusto.UnsafeStmt(unsafe.Stmt{Add: true})).UnsafeAdd(tableName).UnsafeAdd(scheme)
	_, err = client.Mgmt(ctx, testConfig.Database, createStmt)
	if err != nil {
		panic("failed to create a table:\n" + err.Error())
	}

	// Change the ingestion batching time
	batchingStmt := kusto.NewStmt(".alter table ", kusto.UnsafeStmt(unsafe.Stmt{Add: true})).UnsafeAdd(tableName).Add(" policy ingestionbatching @'{ \"MaximumBatchingTimeSpan\": \"00:00:25\", \"MaximumNumberOfItems\": 500, \"MaximumRawDataSizeMB\": 1024 }' ")
	_, err = client.Mgmt(ctx, testConfig.Database, batchingStmt)
	if err != nil {
		panic("failed to reduce the default batching time\n" + err.Error())
	}
}

func TestIgestionFromFileWithStatusReportingQueued(t *testing.T) {
	if skipETOE {
		t.Skipf("end to end tests disabled: missing config.json file in etoe directory")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	client, err := kusto.New(testConfig.Endpoint, testConfig.Authorizer)
	if err != nil {
		panic(err)
	}

	ingestor, err := ingest.New(client, testConfig.Database, tableName)
	if err != nil {
		panic(err)
	}

	count := 5
	var ch [5]chan error
	var errors [5]error

	for i := 0; i < count; i++ {
		res, err := ingestor.FromFile(ctx, csvFile, ingest.ReportResultToTable())
		if err != nil {
			panic(err)
		}

		ch[i] = res.Wait(ctx)
	}

	for i := 0; i < count; i++ {
		errors[i] = <-ch[i]
	}

	for i, err := range errors {
		if err != nil {
			t.Errorf("Exepcted status Succeeded however result on channel %d is:\n%s", i+1, err)
		}
	}
}

func TestIgestionFromReaderWithStatusReportingQueued(t *testing.T) {
	if skipETOE {
		t.Skipf("end to end tests disabled: missing config.json file in etoe directory")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	client, err := kusto.New(testConfig.Endpoint, testConfig.Authorizer)
	if err != nil {
		panic(err)
	}

	ingestor, err := ingest.New(client, testConfig.Database, tableName)
	if err != nil {
		panic(err)
	}

	count := 5
	var ch [5]chan error
	var errors [5]error

	for i := 0; i < count; i++ {
		f, err := os.Open(csvFile)
		if err != nil {
			panic(err)
		}

		reader, writer := io.Pipe()
		go func() {
			defer writer.Close()
			io.Copy(writer, f)
		}()

		res, err := ingestor.FromReader(ctx, reader, ingest.ReportResultToTable(), ingest.FileFormat(ingest.CSV))
		ch[i] = res.Wait(ctx)
	}

	for i := 0; i < count; i++ {
		errors[i] = <-ch[i]
	}

	for i, err := range errors {
		if err != nil {
			t.Errorf("Exepcted status Succeeded however result on channel %d is:\n%s", i+1, err)
		}
	}
}

func TestIgestionFromFileWithoutStatusReporting(t *testing.T) {
	if skipETOE {
		t.Skipf("end to end tests disabled: missing config.json file in etoe directory")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Minute)
	defer cancel()

	client, err := kusto.New(testConfig.Endpoint, testConfig.Authorizer)
	if err != nil {
		panic(err)
	}

	ingestor, err := ingest.New(client, testConfig.Database, tableName)
	if err != nil {
		panic(err)
	}

	res, err := ingestor.FromFile(ctx, csvFile)
	if err != nil {
		panic(err)
	}

	err = <-res.Wait(ctx)
	if err != nil {
		t.Errorf("Exepcted status Queued however result is:\n%s", err)
	}
}

func TestIgestionFromReaderWithoutStatusReporting(t *testing.T) {
	if skipETOE {
		t.Skipf("end to end tests disabled: missing config.json file in etoe directory")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Minute)
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

	res, err := ingestor.FromReader(ctx, reader, ingest.FileFormat(ingest.CSV))
	if err != nil {
		panic(err)
	}

	err = <-res.Wait(ctx)
	if err != nil {
		t.Errorf("Exepcted status Queued however result is:\n%s", err)
	}
}

func TestIgestionFromFileWithClientFailure(t *testing.T) {
	if skipETOE {
		t.Skipf("end to end tests disabled: missing config.json file in etoe directory")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Minute)
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
	_, err = ingestor.FromFile(ctx, "thisfiledoesnotexist.csv")
	if err == nil {
		t.Errorf("Test without status reporting:\nExepcted status ClientError however result is:\n%s", err)
	}

	// Once with table status reporting
	_, err = ingestor.FromFile(ctx, "thisfiledoesnotexist.csv", ingest.ReportResultToTable())
	if err == nil {
		t.Errorf("Test with status reporting:\nExepcted status ClientError however result is:\n%s", err)
	}
}

func TestIgestionFromReaderWithClientFailure(t *testing.T) {
	if skipETOE {
		t.Skipf("end to end tests disabled: missing config.json file in etoe directory")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Minute)
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
	_, err = ingestor.FromReader(ctx, reader)
	if err == nil {
		t.Errorf("Test without status reporting:\nExepcted ingestion to fail however result is:\n%s", err)
	}

	// Once with table status reporting
	_, err = ingestor.FromReader(ctx, reader, ingest.ReportResultToTable())
	if err == nil {
		t.Errorf("Test with status reporting:\nExepcted ingestion to fail however result is:\n%s", err)
	}
}

func TestIgestionFromFileWithStatusReporting(t *testing.T) {
	if skipETOE {
		t.Skipf("end to end tests disabled: missing config.json file in etoe directory")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	client, err := kusto.New(testConfig.Endpoint, testConfig.Authorizer)
	if err != nil {
		panic(err)
	}

	ingestor, err := ingest.New(client, testConfig.Database, tableName)
	if err != nil {
		panic(err)
	}

	res, err := ingestor.FromFile(ctx, csvFile, ingest.ReportResultToTable(), ingest.FlushImmediately())
	if err != nil {
		panic(err)
	}

	err = <-res.Wait(ctx)
	if err != nil {
		t.Errorf("Exepcted status Succeeded however result is:\n%s", err)
	}
}

func TestIgestionFromReaderWithStatusReporting(t *testing.T) {
	if skipETOE {
		t.Skipf("end to end tests disabled: missing config.json file in etoe directory")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Minute)
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

	res, err := ingestor.FromReader(ctx, reader, ingest.ReportResultToTable(), ingest.FlushImmediately(), ingest.FileFormat(ingest.CSV))
	if err != nil {
		panic(err)
	}

	err = <-res.Wait(ctx)
	if err != nil {
		t.Errorf("Exepcted status Succeeded however result is:\n%s", err)
	}
}
