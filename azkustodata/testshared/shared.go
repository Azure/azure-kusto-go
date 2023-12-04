package testshared

import (
	"context"
	"github.com/Azure/azure-kusto-go/azkustodata"
	"github.com/Azure/azure-kusto-go/azkustodata/kql"
	"testing"
)

type CountResult struct {
	Count int64
}

// This is needed because streaming ingestion metadata is cached in the engine and needs to refresh
var clearStreamingCacheStatement = kql.New(".clear database cache streamingingestion schema")

func executeCommands(client *azkustodata.Client, database string, commandsToRun ...azkustodata.Statement) error {
	for _, cmd := range commandsToRun {
		if _, err := client.Mgmt(context.Background(), database, cmd); err != nil {
			return err
		}
	}

	return nil
}

var database = ""

func SetDefaultDatabase(db string) {
	database = db
}

const defaultSchema = "(header_time: datetime, header_id: guid, header_api_version: string, payload_data: string, payload_user: string)"
const allDataTypes = "(vnum:int, vdec:decimal, vdate:datetime, vspan:timespan, vobj:dynamic, vb:bool, vreal:real, vstr:string, vlong:long, vguid:guid)"

const allDataTypesIngest = `1,"2.00000000000001","2020-03-04T14:05:01.3109965Z","01:23:45.6789000","{""moshe"":""value""}",true,0.01,"asdf",9223372036854775807,"74be27de-1e4e-49d9-b579-fe0b331d3642"`

const allDataTypesNull = "(vnum:int, vdec:decimal, vdate:datetime, vspan:timespan, vobj:dynamic, vb:bool, vreal:real, vstr:string, vlong:long, vguid:guid)"

const allDataTypesNullIngest = `null,null,null,null,null,null,null,"",null,null`

const logsMapping = " ingestion json mapping 'Logs_mapping' '[{\"column\":\"header_time\",\"path\":\"$.header.time\",\"datatype\":\"datetime\"},{\"column\":\"header_id\",\"path\":\"$.header.id\",\"datatype\":\"guid\"},{\"column\":\"header_api_version\",\"path\":\"$.header.api_version\",\"datatype\":\"string\"},{\"column\":\"payload_data\",\"path\":\"$.payload.data\",\"datatype\":\"string\"},{\"column\":\"payload_user\",\"path\":\"$.payload.user\",\"datatype\":\"string\"}]'"

func CreateTestTable(t *testing.T, client *azkustodata.Client, tableName string) error {
	return CreateTestTableWithDB(t, client, database, tableName, defaultSchema, "", logsMapping)
}

func CreateAllDataTypesTable(t *testing.T, client *azkustodata.Client, tableName string) error {
	return CreateTestTableWithDB(t, client, database, tableName, allDataTypes, allDataTypesIngest, "")
}

func CreateAllDataTypesNullTable(t *testing.T, client *azkustodata.Client, tableName string) error {
	return CreateTestTableWithDB(t, client, database, tableName, allDataTypesNull, allDataTypesNullIngest, "")
}

func CreateDefaultTestTableWithDB(t *testing.T, client *azkustodata.Client, database string, tableName string) error {
	return CreateTestTableWithDB(t, client, database, tableName, defaultSchema, "", logsMapping)
}

func CreateTestTableWithDB(t *testing.T, client *azkustodata.Client, database string, tableName string, schema string, ingestion string, mapping string) error {
	return CreateTestTableWithDBAndScheme(t, client, database, tableName, schema, ingestion, mapping)
}

func CreateTestTableWithDBAndScheme(t *testing.T, client *azkustodata.Client, database string, tableName string, schema string, ingestion string, mapping string) error {
	t.Logf("Creating ingestion table %s", tableName)
	dropUnsafe := kql.New(".drop table ").AddTable(tableName).AddLiteral(" ifexists")
	var createUnsafe = kql.New(".create table ").AddTable(tableName).AddUnsafe(schema)

	var commands = make([]azkustodata.Statement, 0)
	commands = append(commands, dropUnsafe)
	commands = append(commands, createUnsafe)

	if ingestion != "" {
		commands = append(commands, kql.New(".ingest inline into table ").AddTable(tableName).AddLiteral(" <| \n").AddUnsafe(ingestion).AddLiteral("\n"))
	}

	if mapping != "" {
		commands = append(commands, kql.New(".create table ").AddTable(tableName).AddUnsafe(mapping))
	}

	commands = append(commands, clearStreamingCacheStatement)

	t.Cleanup(func() {
		t.Logf("Dropping ingestion table %s", tableName)
		_ = executeCommands(client, database, dropUnsafe)
		t.Logf("Dropped ingestion table %s", tableName)
	})

	return executeCommands(client, database, commands...)
}
