package main

import (
	"context"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	_ "net/http/pprof"

	"github.com/Azure/azure-kusto-go/kusto"
	"github.com/Azure/azure-kusto-go/kusto/ingest"
	"github.com/Azure/azure-kusto-go/kusto/unsafe"
	"github.com/Azure/go-autorest/autorest/azure/auth"
)

var (
	targetCluster       = "https://sdkse2etest.eastus.kusto.windows.net"
	databaseName        = "e2e"
	tableName           = "benchmark_table"
	databaseExists      = true
	artifactFolderPath  = "artifacts"
	csvFilepath         = strings.Join([]string{artifactFolderPath, "dataset.csv"}, "/")
	jsonFilepath        = strings.Join([]string{artifactFolderPath, "dataset.json"}, "/")
	jsonMappingFilePath = strings.Join([]string{artifactFolderPath, "json_mapping.json"}, "/")
	jsonMappingName     = "benchmark_json_mapping"
	testType            = 1
	repetitions         = 10
)

func main() {
	if !validateArgs() {
		return
	}

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

	benchmarkIngestFromFile(client)

	fmt.Println("Press enter to exit")
	fmt.Scanln()
}

func validateArgs() bool {
	usage := `
	The Benchmark tool allows developers to manually test changes to the code for memory usage and performane.
	It runs repetitive calls to SDK APIs and opens a pprof server in port 6060 for memory analysis.
	You may modify the tool to your own needs while developping.
	If you feel the modification is worth publishing to others, please submit a PR with the suggested change

	Note 1: 
		The tool currently requires permissions to ingest, query and create tables on the taregt cluster database.

	Note 2: 
		Authorization relys on Az-Cli integration. To use it:
		1. Install az-cli on your machine
		2. Run 'az login' and  login with your credentials
		3. Run the benchmark tool

	Test Numbers:
		1. Ingest a CSV File
		2. Ingest a Json file from Reader
		3. Ingest a CSV File from Reader
	
	Usage:
		ingest_benchmark [-h|/?|--help] 
			print usage 

		ingest_benchmark <cluster_uri> <databasename> [testNumber [repetitions]]
			runs against a user provided server 

		ingest_benchmark default [testNumber [repetitions]]
			runs against a kusto internal server (permissions required)
	`
	argCount := len(os.Args)
	if argCount == 1 || argCount > 5 {
		fmt.Print(usage)
		return false
	}

	var err error
	switch os.Args[1] {
	case "-h", "/?", "--help":
		fmt.Print(usage)
		return false

	case "default":
		if argCount >= 3 {
			if testType, err = strconv.Atoi(os.Args[2]); err != nil {
				fmt.Print(usage)
				return false
			}
		}

		if argCount == 4 {
			if repetitions, err = strconv.Atoi(os.Args[3]); err != nil {
				fmt.Print(usage)
				return false
			}
		}

		return true

	default:
		if argCount < 3 {
			fmt.Print(usage)
			return false
		}

		targetCluster = os.Args[1]
		databaseName = os.Args[2]

		if argCount >= 4 {
			if testType, err = strconv.Atoi(os.Args[3]); err != nil {
				fmt.Print(usage)
				return false
			}
		}

		if argCount == 5 {
			if repetitions, err = strconv.Atoi(os.Args[4]); err != nil {
				fmt.Print(usage)
				return false
			}
		}

		return true
	}
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

func benchmarkIngestFromFile(client *kusto.Client) {
	// Setup a maximum time for completion to be 10 minutes.
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()

	ingestor, err := ingest.New(client, databaseName, tableName)
	if err != nil {
		fmt.Println("failed to create an ingestion client")
		return
	}

	// Run the test
	for i := 1; i <= repetitions; i++ {
		switch testType {

		case 1:
			// CSV Ingest from file
			err := ingestor.FromFile(ctx, csvFilepath, ingest.FlushImmediately())
			if err != nil {
				fmt.Printf("failed to upload '%s' due to error - %s\n", csvFilepath, err.Error())
			} else {
				fmt.Printf("%d of %d\n", i, repetitions)
			}

		case 2:
			// JSON ingest from reader
			reader, err := os.Open(jsonFilepath)
			if err != nil {
				fmt.Printf("failed to open thefile '%s' due to error - %s\n", jsonFilepath, err.Error())
				return
			}

			err = ingestor.FromReader(ctx, reader, ingest.IngestionMappingRef(jsonMappingName, ingest.JSON), ingest.FileFormat(ingest.JSON), ingest.FlushImmediately())
			if err != nil {
				fmt.Printf("failed to upload '%s' due to error - %s\n", jsonFilepath, err.Error())
			} else {
				fmt.Printf("%d of %d\n", i, repetitions)
			}

		case 3:
			// CSV ingest from reader
			reader, err := os.Open(csvFilepath)
			if err != nil {
				fmt.Printf("failed to open thefile '%s' due to error - %s\n", csvFilepath, err.Error())
				return
			}

			err = ingestor.FromReader(ctx, reader, ingest.FlushImmediately(), ingest.FileFormat(ingest.CSV))
			if err != nil {
				fmt.Printf("failed to upload '%s' due to error - %s\n", csvFilepath, err.Error())
			} else {
				fmt.Printf("%d of %d\n", i, repetitions)
			}

		default:
			fmt.Printf("Undefined Test Number %d\n", i)
			return

		}
	}
}
