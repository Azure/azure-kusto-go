package main

import (
	// Common
	"context"
	"fmt"
	"io/ioutil"
	"os"
	"strings"
	"time"

	// for PPROF
	"log"
	"net/http"
	_ "net/http/pprof"

	//Kusto
	"github.com/Azure/azure-kusto-go/kusto"
	"github.com/Azure/azure-kusto-go/kusto/ingest"
	"github.com/Azure/azure-kusto-go/kusto/unsafe"
	"github.com/Azure/go-autorest/autorest/azure/auth"
)

//var databaseName = "benchmark_db"
//var targetCluster = "https://yogiladadx.dev.kusto.windows.net"

var databaseName = "e2e"
var targetCluster = "https://sdkse2etest.eastus.kusto.windows.net"
var tableName = "benchmark_table"
var databaseExists = true
var artifactFolderPath = "artifacts"
var csvFilepath = strings.Join([]string{artifactFolderPath, "dataset.csv"}, "/")
var jsonFilepath = strings.Join([]string{artifactFolderPath, "dataset.json"}, "/")
var jsonMappingFilePath = strings.Join([]string{artifactFolderPath, "json_mapping.json"}, "/")
var jsonMappingName = "benchmark_json_mapping"

func main() {
	pprofEndpoint := "localhost:6060"

	fmt.Println("Starting memory benchmark")
	fmt.Println("Please make sure az-cli is installed and 'az login' was called")
	fmt.Printf("PPROF Endpoint is %s\n", pprofEndpoint)

	go func() {
		log.Println(http.ListenAndServe(pprofEndpoint, nil))
	}()

	client, err := createKustoClient()
	if err != nil {
		fmt.Println("Exit")
		return
	}

	err = setupBenchmarkDatabase(client)
	if err != nil {
		fmt.Println("Exit")
		return
	}

	benchmarkIngestFromFile(client, 10)

	fmt.Println("Press enter to exit")
	fmt.Scanln()
}

func createKustoClient() (*kusto.Client, error) {
	// This requires az-cli installed and az-cli login to succeed
	authorizer, err := auth.NewAuthorizerFromCLIWithResource(targetCluster)
	if err != nil {
		fmt.Println("failed to acquire auth token from az-cli")
		return nil, err
	}

	authorization := kusto.Authorization{Authorizer: authorizer}

	client, err := kusto.New(targetCluster, authorization)
	if err != nil {
		fmt.Printf("failed to create a kusto client - %s\n", err.Error())
		return nil, err
	}

	return client, nil
}

func setupBenchmarkDatabase(client *kusto.Client) error {
	tableScheme := "(rownumber:int, rowguid:string, xdouble:real, xfloat:real, xbool:bool, xint16:int, xint32:int, xint64:long, xuint8:long, xuint16:long, xuint32:long, xuint64:long, xdate:datetime, xsmalltext:string, xtext:string, xnumberAsText:string, xtime:timespan, xtextWithNulls:string, xdynamicWithNulls:dynamic)"
	jsonMapping, err := ioutil.ReadFile(jsonMappingFilePath)
	if err != nil {
		fmt.Printf("Failed reading the json mapping file from '%s' because %s", jsonMappingFilePath, err.Error())
		return err
	}

	createDbUnsafe := kusto.NewStmt(".create database ", kusto.UnsafeStmt(unsafe.Stmt{Add: true})).UnsafeAdd(databaseName).Add(" volatile ifnotexists")
	createTableUnsafe := kusto.NewStmt(".create table ", kusto.UnsafeStmt(unsafe.Stmt{Add: true})).UnsafeAdd(tableName).UnsafeAdd(tableScheme)
	createJSONMapping := kusto.NewStmt(".create-or-alter table ", kusto.UnsafeStmt(unsafe.Stmt{Add: true})).UnsafeAdd(tableName).Add(" ingestion json mapping '").UnsafeAdd(jsonMappingName).Add("' '").UnsafeAdd(string(jsonMapping)).Add("'")

	if !databaseExists {
		if _, err := client.Mgmt(context.Background(), "", createDbUnsafe); err != nil {
			fmt.Printf("Failed to create a databas named '%s' - %s\n", databaseName, err.Error())
			return err
		}
	}

	if _, err := client.Mgmt(context.Background(), databaseName, createTableUnsafe); err != nil {
		fmt.Printf("Failed to create a table named '%s' - %s\n", tableName, err.Error())
		return err
	}

	if _, err := client.Mgmt(context.Background(), databaseName, createJSONMapping); err != nil {
		fmt.Printf("Failed to create Json mapping for table '%s' - %s\n", tableName, err.Error())
		return err
	}

	return nil
}

func benchmarkIngestFromFile(client *kusto.Client, times int) {
	// Setup a maximum time for completion to be 10 minutes.
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()

	ingestor, err := ingest.New(client, databaseName, tableName)
	if err != nil {
		fmt.Println("failed to create an ingestion client")
		return
	}

	// Run the test
	for i := 1; i <= times; i++ {
		//err := ingestor.FromFile(ctx, filepathToIngest, ingest.FlushImmediately())

		reader, err := os.Open(jsonFilepath)
		if err != nil {
			fmt.Printf("failed to open thefile '%s' due to error - %s\n", jsonFilepath, err.Error())
			return
		}

		//err = ingestor.FromReader(ctx, reader, ingest.FlushImmediately(), ingest.FileFormat(ingest.CSV))
		err = ingestor.FromReader(ctx, reader, ingest.IngestionMappingRef(jsonMappingName, ingest.JSON), ingest.FileFormat(ingest.JSON), ingest.FlushImmediately())
		if err != nil {
			fmt.Printf("failed to upload '%s' due to error - %s\n", jsonFilepath, err.Error())
		} else {
			fmt.Printf("%d of %d\n", i, times)
		}
	}
}
