package main

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/Azure/azure-kusto-go/azkustodata"
	"github.com/Azure/azure-kusto-go/azkustodata/errors"
	"github.com/Azure/azure-kusto-go/azkustodata/kql"
	"github.com/Azure/azure-kusto-go/azkustoingest"
	"github.com/Azure/azure-kusto-go/quickstart/utils"
	"github.com/Azure/azure-kusto-go/quickstart/utils/authentication"
	"github.com/Azure/azure-kusto-go/quickstart/utils/ingestion"
	"github.com/Azure/azure-kusto-go/quickstart/utils/queries"
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
	SourceType         SourceType  `json:"sourceType"`
	DataSourceUri      string      `json:"dataSourceUri"`
	DataFormat         interface{} `json:"format"`
	UseExistingMapping bool        `json:"useExistingMapping"`
	MappingName        string      `json:"mappingName"`
	MappingValue       string      `json:"mappingValue"`
}

// loadConfigs Loads JSON configuration file, and sets the metadata in place
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
	for i, dataSource := range config.DataToIngest {
		config.DataToIngest[i].DataFormat = azkustoingest.InferFormatFromFileName(dataSource.DataSourceUri)
	}
	return config
}

// preIngestionQuerying -First phase, pre ingestion - will reach the provided DB with several control commands and a query based on the configuration File.
func preIngestionQuerying(config ConfigJson, kustoClient *azkustodata.Client) {
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

// alterMergeExistingTableToProvidedSchema Alter-merges the given existing table to provided schema
func alterMergeExistingTableToProvidedSchema(kustoClient *azkustodata.Client, databaseName string, tableName string, tableSchema string) {
	// Note - we are using AddUnsafe here to maintain the structure of tableSchema without escaping. Use with caution!
	command := kql.New(".alter-merge table ").AddTable(tableName).AddLiteral(" ").AddUnsafe(tableSchema)
	queries.ExecuteCommand(kustoClient, databaseName, command)
}

// queryExistingNumberOfRows Queries the data on the existing number of rows
func queryExistingNumberOfRows(kustoClient *azkustodata.Client, databaseName string, tableName string) {
	command := kql.New("table(_table_name) | count")
	params := kql.NewParameters().AddString("_table_name", tableName)

	queries.ExecuteCommand(kustoClient, databaseName, command, azkustodata.QueryParameters(params))
}

// queryFirstTwoRows Queries the first two rows of the table
func queryFirstTwoRows(kustoClient *azkustodata.Client, databaseName string, tableName string) {
	command := kql.New("table(_table_name) | take 2")
	params := kql.NewParameters().AddString("_table_name", tableName)

	queries.ExecuteCommand(kustoClient, databaseName, command, azkustodata.QueryParameters(params))
}

// createNewTable Creates a new table
func createNewTable(kustoClient *azkustodata.Client, databaseName string, tableName string, tableSchema string) {
	command := kql.New(".create table ").AddTable(tableName).AddLiteral(" ").AddUnsafe(tableSchema)
	queries.ExecuteCommand(kustoClient, databaseName, command)
}

// alterBatchingPolicy Alters the batching policy based on BatchingPolicy in configuration
func alterBatchingPolicy(kustoClient *azkustodata.Client, databaseName string, tableName string, batchingPolicy string) {
	/*
	 * Tip 1: Though most users should be fine with the defaults, to speed up ingestion, such as during development and in this sample app, we opt to modify
	 * the default ingestion policy to ingest data after at most 10 seconds. Tip 2: This is generally a one-time configuration. Tip 3: You can also skip the
	 * batching for some files using the Flush-Immediately property, though this option should be used with care as it is inefficient.
	 */
	command := kql.New(".alter table ").AddTable(tableName).AddLiteral(" policy ingestionbatching @'").AddUnsafe(batchingPolicy).AddLiteral("'")
	queries.ExecuteCommand(kustoClient, databaseName, command)
	// If it failed to alter the ingestion policy - it could be the result of insufficient permissions. The sample will still run,
	// though ingestion will be delayed for up to 5 minutes.
}

// ingestionPhase - Second phase - The ingestion process
func ingestionPhase(config ConfigJson, ingestClient *azkustoingest.Ingestion) {
	for _, dataSource := range config.DataToIngest {
		// Learn More: For more information about ingesting data to Kusto in Java, see:
		// https://docs.microsoft.com/azure/data-explorer/java-ingest-data
		ingestData(dataSource, dataSource.DataFormat, ingestClient, config.DatabaseName, config.TableName, dataSource.MappingName)
	}
	/*
	 * Note: We poll here the ingestion's target table because monitoring successful ingestions is expensive and not recommended. Instead, the recommended
	 * ingestion monitoring approach is to monitor failures. Learn more:
	 * https://docs.microsoft.com/azure/data-explorer/kusto/api/netfx/kusto-ingest-client-status#tracking-ingestion-status-kustoqueuedingestclient and
	 * https://docs.microsoft.com/azure/data-explorer/using-diagnostic-logs
	 */
	ingestion.WaitForIngestionToComplete(config.WaitForIngestSeconds)
}

// ingestData Ingest data from given source
func ingestData(dataSource ConfigData, dataFormat interface{}, ingestClient *azkustoingest.Ingestion, databaseName string, tableName string, mappingName string) {
	sourceType := dataSource.SourceType
	waitForUserToProceed(fmt.Sprintf("Ingest '%s' from '%s'", dataSource.DataSourceUri, sourceType))
	// Tip: When ingesting json files, if each line represents a single-line json, use MULTIJSON format even if the file only contains one line.
	// If the json contains whitespace formatting, use SINGLEJSON. In this case, only one data row json object is allowed per file.
	//if dataFormat == azkustoingest.JSON {
	//	dataFormat = azkustoingest.MultiJSON
	//}
	ctx := context.Background()
	options := []azkustoingest.FileOption{azkustoingest.IngestionMapping(mappingName, dataFormat.(azkustoingest.DataFormat))}
	filePath := fmt.Sprintf("./%s", dataSource.DataSourceUri)

	// Note: No need to add "nosource" option as in that case the "ingestData" flag will be set to false, and it will be impossible to reach this code
	// segment.
	switch sourceType {
	case localFileSource:
		ingestion.IngestSource(ingestClient, filePath, ctx, options, databaseName, tableName, string(localFileSource))
		break
	case blobSource:
		ingestion.IngestSource(ingestClient, filePath, ctx, options, databaseName, tableName, string(blobSource))
	default:
		err := errors.ES(errors.OpUnknown, errors.KOther, "Unknown source")
		utils.ErrorHandler(fmt.Sprintf("Unknown source '%s' for file '%s'", sourceType, dataSource.DataSourceUri), err)
	}
}

// postIngestionQuerying Third and final phase - simple queries to validate the hopefully successful run of the script
func postIngestionQuerying(kustoClient *azkustodata.Client, databaseName string, tableName string, ingestDataFlag bool) {
	optionalPostIngestionPrompt := ""
	if ingestDataFlag {
		optionalPostIngestionPrompt = "post-ingestion "
	}

	waitForUserToProceed(fmt.Sprintf("Get %srow count for '%s.%s':", optionalPostIngestionPrompt, databaseName, tableName))
	queryExistingNumberOfRows(kustoClient, databaseName, tableName)

	waitForUserToProceed(fmt.Sprintf("Get sample (2 records) of %sdata:", optionalPostIngestionPrompt))
	queryFirstTwoRows(kustoClient, databaseName, tableName)
}

// waitForUserToProceed Handles UX on prompts and flow of program
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
	const configFileName = "./kusto_sample_config.json"
	var config = loadConfigs(configFileName)
	waitForUser = config.WaitForUser
	if config.AuthenticationMode == authentication.UserPrompt {
		waitForUserToProceed("You will be prompted *twice* for credentials during this script. Please return to the console after authenticating.")
	}

	var kustoKcs = authentication.GenerateConnectionString(config.KustoUri, config.AuthenticationMode)
	kustoClient, err := azkustodata.New(kustoKcs)
	if err != nil {
		utils.ErrorHandler("Couldn't create Kusto client. Please validate your URIs in the configuration file.", err)
	}
	defer func(client *azkustodata.Client) {
		err := client.Close()
		if err != nil {
			utils.ErrorHandler("Couldn't close client.", err)
		}
	}(kustoClient)

	preIngestionQuerying(config, kustoClient)

	ingestClient, err := azkustoingest.New(kustoKcs, azkustoingest.WithDefaultDatabase(config.DatabaseName), azkustoingest.WithDefaultTable(config.TableName))
	if err != nil {
		utils.ErrorHandler("Couldn't create Ingestion client. Please validate your URIs in the configuration file.", err)
	}
	defer func(client *azkustoingest.Ingestion) {
		err := client.Close()
		if err != nil {
			utils.ErrorHandler("Couldn't close client.", err)
		}
	}(ingestClient)

	if config.IngestData {
		ingestionPhase(config, ingestClient)
	}
	if config.QueryData {
		postIngestionQuerying(kustoClient, config.DatabaseName, config.TableName, config.IngestData)
	}

	fmt.Println("\nKusto sample app done")
}
