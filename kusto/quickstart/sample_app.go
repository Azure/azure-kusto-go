package main

import (
	"encoding/json"
	"fmt"
	"github.com/Azure/azure-kusto-go/kusto/ingest"
	"io/ioutil"
	"os"
)

type SourceType string

const (
	localFileSource SourceType = "localFileSource"
	blobSource      SourceType = "blobSource"
)

type ConfigJson struct {
	UseExistingTable bool         `json:"useExistingTable"`
	DatabaseName     string       `json:"databaseName"`
	TableName        string       `json:"tableName"`
	TableSchema      string       `json:"tableSchema"`
	KustoUri         string       `json:"kustoUri"`
	IngestUri        string       `json:"ingestUri"`
	DataToIngest     []ConfigData `json:"data"`
	AlterTable       bool         `json:"alterTable"`
	QueryData        bool         `json:"queryData"`
	IngestData       bool         `json:"ingestData"`
	//AuthenticationMode AuthenticationModeOptions  `json:"authenticationMode"`
	WaitForUser          bool   `json:"waitForUser"`
	IgnoreFirstRecord    bool   `json:"ignoreFirstRecord"`
	WaitForIngestSeconds int    `json:"waitForIngestSeconds"`
	BatchingPolicy       string `json:"batchingPolicy"`
}

type ConfigData struct {
	SourceType         SourceType        `json:"sourceType"`
	DataSourceUri      string            `json:"dataSourceUri"`
	DataFormat         ingest.DataFormat `json:"format"`
	UseExistingMapping bool              `json:"useExistingMapping"`
	MappingName        string            `json:"mappingName"`
	MappingValue       string            `json:"mappingValue"`
}

func load_configs(configFileName string) ConfigJson {
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

func main() {
	fmt.Println("Kusto sample app is starting...")
	const configFileName = "kusto/quickstart/kusto_sample_config.json"
	var config = load_configs(configFileName)
	fmt.Print(config)
	fmt.Println("\nKusto sample app done")
}
