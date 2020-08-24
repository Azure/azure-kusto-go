package etoe

import (
	"context"
	"fmt"
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
	verbose   bool   = true
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

		initFailed = false
	}

	if initFailed {
		return fmt.Errorf("Init once failed in the past")
	} else {
		return nil
	}
}

func TestIgestionWithoutStatusReporting(t *testing.T) {
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

func TestIgestionWithWithClientFailure(t *testing.T) {
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

	res := <-ingestor.FromFile(ctx, "thisfiledoesnotexist.csv").Wait(ctx)
	if res.Status != ingest.ClientError {
		t.Errorf("Test without status reporting:\nExepcted status ClientError however result is:\n%s", res.String())
	} else if verbose {
		println("Test without status reporting:")
		println(res.String())
		println()
	}

	res = <-ingestor.FromFile(ctx, "thisfiledoesnotexist.csv", ingest.ReportResultToTable()).Wait(ctx)
	if res.Status != ingest.ClientError {
		t.Errorf("Test with status reporting:\nExepcted status ClientError however result is:\n%s", res.String())
	} else if verbose {
		println("Test with status reporting:")
		println(res.String())
		println()
	}
}

func TestIgestionWithStatusReporting(t *testing.T) {
	err := initOnce()
	if err != nil {
		t.Skipf("Skipping tests: %s", err)
		return
	}

	ctx, cancel := context.WithCancel(context.Background())
	ctx, cancel = context.WithTimeout(ctx, 5*time.Minute)
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
