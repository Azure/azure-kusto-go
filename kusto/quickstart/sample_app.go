package main

import (
	"encoding/json"
	"fmt"
	"github.com/Azure/azure-kusto-go/kusto"
	"github.com/Azure/azure-kusto-go/kusto/quickstart/utils"
	"github.com/Azure/go-autorest/autorest/azure/auth"
	"io/ioutil"
	"os"
)

type SourceType string

const (
	localFileSource SourceType = "localFileSource"
	blobSource      SourceType = "blobSource"
	noSource        SourceType = "nosource"
)

type AuthenticationModeOptions string

const (
	UserPrompt      AuthenticationModeOptions = "UserPrompt"
	ManagedIdentity AuthenticationModeOptions = "ManagedIdentity"
	AppKey          AuthenticationModeOptions = "AppKey"
	AppCertificate  AuthenticationModeOptions = "AppCertificate"
)

type ConfigJson struct {
	UseExistingTable     bool                      `json:"useExistingTable"`
	DatabaseName         string                    `json:"databaseName"`
	TableName            string                    `json:"tableName"`
	TableSchema          string                    `json:"tableSchema"`
	KustoUri             string                    `json:"kustoUri"`
	IngestUri            string                    `json:"ingestUri"`
	DataToIngest         []ConfigData              `json:"data"`
	AlterTable           bool                      `json:"alterTable"`
	QueryData            bool                      `json:"queryData"`
	IngestData           bool                      `json:"ingestData"`
	AuthenticationMode   AuthenticationModeOptions `json:"authenticationMode"`
	WaitForUser          bool                      `json:"waitForUser"`
	IgnoreFirstRecord    bool                      `json:"ignoreFirstRecord"`
	WaitForIngestSeconds int                       `json:"waitForIngestSeconds"`
	BatchingPolicy       string                    `json:"batchingPolicy"`
}

type ConfigData struct {
	SourceType    SourceType `json:"sourceType"`
	DataSourceUri string     `json:"dataSourceUri"`
	//DataFormat         ingest.DataFormat `json:"format"`
	UseExistingMapping bool   `json:"useExistingMapping"`
	MappingName        string `json:"mappingName"`
	MappingValue       string `json:"mappingValue"`
}

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

func waitForUserToProceed(promptMsg string, step int, waitForUser bool) {
	fmt.Println()
	fmt.Printf("\nStep %d: %s", step, promptMsg)
	step++
	if waitForUser {
		fmt.Println("Press ENTER to proceed with this operation...")
		var c string
		_, _ = fmt.Scanln(&c)
	}

}

func main() {
	fmt.Println("Kusto sample app is starting...")
	var step = 1
	const configFileName = "kusto/quickstart/kusto_sample_config.json"
	var config = loadConfigs(configFileName)

	if config.AuthenticationMode == UserPrompt {
		waitForUserToProceed("You will be prompted *twice* for credentials during this script. Please return to the console after authenticating.", step, config.WaitForUser)
	}

	azAuthorizer, err := auth.NewAuthorizerFromCLI()

	if err != nil {
		fmt.Println("Failed to acquire auth token from az-cli" + err.Error())
		return
	} // TODO: Temp az cli connection

	kustoClient, err := kusto.New(config.KustoUri, kusto.Authorization{Authorizer: azAuthorizer})
	if err != nil {
		utils.ErrorHandler("Couldn't create Kusto client. Please validate your URIs in the configuration file.", err)
	}
	defer func(client *kusto.Client) {
		err := client.Close()
		if err != nil {
			utils.ErrorHandler("Couldn't close client.", err)
		}
	}(kustoClient)

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
