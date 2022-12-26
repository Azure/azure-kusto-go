package main

import (
	"encoding/json"
	"fmt"
	"github.com/Azure/azure-kusto-go/kusto"
	"github.com/Azure/azure-kusto-go/kusto/data/types"
	"github.com/Azure/azure-kusto-go/kusto/quickstart/utils"
	"github.com/Azure/azure-kusto-go/kusto/quickstart/utils/authentication"
	"github.com/Azure/azure-kusto-go/kusto/quickstart/utils/queries"
	"github.com/Azure/azure-kusto-go/kusto/unsafe"
	"io/ioutil"
	"os"
)

type SourceType string

const (
	localFileSource SourceType = "localFileSource"
	blobSource      SourceType = "blobSource"
	noSource        SourceType = "nosource"
)

type ConfigJson struct {
	UseExistingTable     bool                                     `json:"useExistingTable"`
	DatabaseName         string                                   `json:"databaseName"`
	TableName            string                                   `json:"tableName"`
	TableSchema          string                                   `json:"tableSchema"`
	KustoUri             string                                   `json:"kustoUri"`
	IngestUri            string                                   `json:"ingestUri"`
	DataToIngest         []ConfigData                             `json:"data"`
	AlterTable           bool                                     `json:"alterTable"`
	QueryData            bool                                     `json:"queryData"`
	IngestData           bool                                     `json:"ingestData"`
	AuthenticationMode   authentication.AuthenticationModeOptions `json:"authenticationMode"`
	WaitForUser          bool                                     `json:"waitForUser"`
	IgnoreFirstRecord    bool                                     `json:"ignoreFirstRecord"`
	WaitForIngestSeconds int                                      `json:"waitForIngestSeconds"`
	BatchingPolicy       string                                   `json:"batchingPolicy"`
}

type ConfigData struct {
	SourceType    SourceType `json:"sourceType"`
	DataSourceUri string     `json:"dataSourceUri"`
	//DataFormat         ingest.DataFormat `json:"format"`
	UseExistingMapping bool   `json:"useExistingMapping"`
	MappingName        string `json:"mappingName"`
	MappingValue       string `json:"mappingValue"`
}

/**
* Loads JSON configuration file, and sets the metadata in place
*
* @return ConfigJson object, allowing access to the metadata fields
 */
func loadConfigs(configFileName string) ConfigJson {
	jsonFile, err := os.Open(configFileName)

	if err != nil {
		panic(fmt.Sprintf("Couldn't read config file from file '%s'\n", err))
	}

	defer func(jsonFile *os.File) {
		cErr := jsonFile.Close()
		if cErr != nil {
			panic("Error closing the file\n")
		}
	}(jsonFile)

	byteValue, _ := ioutil.ReadAll(jsonFile)
	var config ConfigJson
	uErr := json.Unmarshal(byteValue, &config)
	if uErr != nil {
		panic(fmt.Sprintf("Failed to parse configuration JSON: '%s'\n", uErr))
	}

	return config
}

/*
* First phase, pre ingestion - will reach the provided DB with several control commands and a query based on the configuration File.
*
* @param config      ConfigJson object containing the SampleApp configuration
* @param kustoClient Client to run commands
 */
func preIngestionQuerying(config ConfigJson, kustoClient *kusto.Client) {
	if config.UseExistingTable {
		if config.AlterTable {
			// Tip: Usually table was originally created with a schema appropriate for the data being ingested, so this wouldn't be needed.
			// Learn More: For more information about altering table schemas,
			// see: https://docs.microsoft.com/azure/data-explorer/kusto/management/alter-table-command
			waitForUserToProceed(fmt.Sprintf("Alter-merge existing table '%s.%s' to align with the provided schema", config.DatabaseName, config.TableName))
			alterMergeExistingTableToProvidedSchema(kustoClient, config.DatabaseName, config.TableName, config.TableSchema)
		}
		if config.QueryData {
			waitForUserToProceed(fmt.Sprintf("Get existing row count in '%s.%s'", config.DatabaseName, config.TableName))
			queryExistingNumberOfRows(kustoClient, config.DatabaseName, config.TableName)
		}
	} else {
		// Tip: This is generally a one-time configuration
		// Learn More: For more information about creating tables, see: https://docs.microsoft.com/azure/data-explorer/one-click-table
		waitForUserToProceed(fmt.Sprintf("Create table '%s.%s'", config.DatabaseName, config.TableName))
		createNewTable(kustoClient, config.DatabaseName, config.TableName, config.TableSchema)
	}
	// Learn More: Kusto batches data for ingestion efficiency. The default batching policy ingests data when one of the following conditions are met:
	// 1) More than 1,000 files were queued for ingestion for the same table by the same user
	// 2) More than 1GB of data was queued for ingestion for the same table by the same user
	// 3) More than 5 minutes have passed since the first File was queued for ingestion for the same table by the same user
	// For more information about customizing the ingestion batching policy, see:
	// https://docs.microsoft.com/azure/data-explorer/kusto/management/batchingpolicy
	// TODO: Change if needed. Disabled to prevent an existing batching policy from being unintentionally changed
	if false && config.BatchingPolicy != "" {
		waitForUserToProceed(fmt.Sprintf("Alter the batching policy for table '%s.%s'", config.DatabaseName, config.TableName))
		alterBatchingPolicy(kustoClient, config.DatabaseName, config.TableName, config.BatchingPolicy)
	}
}

/**
 * Alter-merges the given existing table to provided schema
 *
 * @param kustoClient  Client to run commands
 * @param databaseName DB name
 * @param tableName    Table name
 * @param tableSchema  Table Schema
 */
func alterMergeExistingTableToProvidedSchema(kustoClient *kusto.Client, databaseName string, tableName string, tableSchema string) {
	command := kusto.NewStmt(".alter-merge table ", kusto.UnsafeStmt(unsafe.Stmt{Add: true, SuppressWarning: true})).UnsafeAdd(tableName).Add(" ").UnsafeAdd(tableSchema)
	queries.ExecuteCommand(kustoClient, databaseName, command)
}

/**
 * Queries the data on the existing number of rows
 *
 * @param kustoClient  Client to run commands
 * @param databaseName DB name
 * @param tableName    Table name
 */
func queryExistingNumberOfRows(kustoClient *kusto.Client, databaseName string, tableName string) {
	rootStmt := kusto.NewStmt("table(_table_name) | count").MustDefinitions(
		kusto.NewDefinitions().Must(
			kusto.ParamTypes{
				"_table_name": kusto.ParamType{Type: types.String},
			},
		),
	)

	command, err := rootStmt.WithParameters(kusto.NewParameters().Must(kusto.QueryValues{"_table_name": tableName}))
	if err != nil {
		fmt.Println("Failed to build query: " + err.Error())
		return
	}
	queries.ExecuteCommand(kustoClient, databaseName, command)

}

/**
 * Creates a new table
 *
 * @param kustoClient  Client to run commands
 * @param databaseName DB name
 * @param tableName    Table name
 * @param tableSchema  Table Schema
 */
func createNewTable(kustoClient *kusto.Client, databaseName string, tableName string, tableSchema string) {
	command := kusto.NewStmt(".create table ", kusto.UnsafeStmt(unsafe.Stmt{Add: true, SuppressWarning: true})).UnsafeAdd(tableName).Add(" ").UnsafeAdd(tableSchema)
	queries.ExecuteCommand(kustoClient, databaseName, command)
}

/**
 * Alters the batching policy based on BatchingPolicy in configuration
 *
 * @param kustoClient    Client to run commands
 * @param databaseName   DB name
 * @param tableName      Table name
 * @param batchingPolicy Ingestion batching policy
 */
func alterBatchingPolicy(kustoClient *kusto.Client, databaseName string, tableName string, batchingPolicy string) {
	/*
	 * Tip 1: Though most users should be fine with the defaults, to speed up ingestion, such as during development and in this sample app, we opt to modify
	 * the default ingestion policy to ingest data after at most 10 seconds. Tip 2: This is generally a one-time configuration. Tip 3: You can also skip the
	 * batching for some files using the Flush-Immediately property, though this option should be used with care as it is inefficient.
	 */
	command := kusto.NewStmt(".alter table ", kusto.UnsafeStmt(unsafe.Stmt{Add: true, SuppressWarning: true})).UnsafeAdd(tableName).Add(" policy ingestionbatching @'").UnsafeAdd(batchingPolicy).Add("'")
	queries.ExecuteCommand(kustoClient, databaseName, command)
	// If it failed to alter the ingestion policy - it could be the result of insufficient permissions. The sample will still run,
	// though ingestion will be delayed for up to 5 minutes.
}

/**
 * Handles UX on prompts and flow of program
 *
 * @param promptMsg Prompt to display to user
 */
func waitForUserToProceed(promptMsg string) {
	fmt.Println()
	fmt.Printf("\nStep %d: %s", step, promptMsg)
	step++
	if waitForUser {
		fmt.Println("\nPress ENTER to proceed with this operation...")
		var c string
		_, _ = fmt.Scanln(&c)
	}

}

var step = 1
var waitForUser bool

func main() {
	fmt.Println("Kusto sample app is starting...")
	const configFileName = "kusto/quickstart/kusto_sample_config.json"
	var config = loadConfigs(configFileName)
	waitForUser = config.WaitForUser
	if config.AuthenticationMode == authentication.UserPrompt {
		waitForUserToProceed("You will be prompted *twice* for credentials during this script. Please return to the console after authenticating.")
	}

	//azAuthorizer, err := auth.NewAuthorizerFromCLIWithResource(config.KustoUri) // Temp az cli connection
	//kustoClient, err := kusto.New(config.KustoUri, kusto.Authorization{Authorizer: azAuthorizer})
	var clientKcs = authentication.GenerateConnectionString(config.KustoUri, config.AuthenticationMode)
	kustoClient, err := kusto.New(clientKcs)
	if err != nil {
		utils.ErrorHandler("Couldn't create Kusto client. Please validate your URIs in the configuration file.", err)
	}
	defer func(client *kusto.Client) {
		err := client.Close()
		if err != nil {
			utils.ErrorHandler("Couldn't close client.", err)
		}
	}(kustoClient)

	preIngestionQuerying(config, kustoClient)

	//ingestClient, err := ingest.New(kustoClient, config.DatabaseName, config.TableName)
	//if err != nil {
	//	utils.ErrorHandler("Couldn't create Ingest client. Please validate your URIs in the configuration file.", err)
	//}
	//// Be sure to close the ingestor when you're done. (Error handling omitted for brevity.)
	//defer func(ingestClient *ingest.Ingestion) {
	//	err := ingestClient.Close()
	//	if err != nil {
	//		utils.ErrorHandler("Couldn't close client.", err)
	//	}
	//}(ingestClient)

	fmt.Println("\nKusto sample app done")
}
