// Package queries - in charge of querying the data - either with management queries, or data queries
package queries

import (
	"context"
	"fmt"
	"github.com/Azure/azure-kusto-go/azkustodata"
	"github.com/Azure/azure-kusto-go/azkustodata/query"
	"strings"
)

// ExecuteCommand Executes a Command using a premade client
func ExecuteCommand(kustoClient *azkustodata.Client, databaseName string, command azkustodata.Statement, options ...azkustodata.QueryOption) {
	ctx := context.Background()
	var result query.FullDataset
	var err error
	if strings.HasPrefix(command.String(), ".") {
		result, err = kustoClient.Mgmt(ctx, databaseName, command)
	} else {
		result, err = kustoClient.Query(ctx, databaseName, command, options...)
	}

	if err != nil {
		panic(fmt.Sprintf("Command execution failed: '%s'\n", err.Error()))
	}

	for _, table := range result.Tables() {
		if !table.IsPrimaryResult() {
			continue
		}

		for _, row := range table.Rows() {
			fmt.Println(row)
		}
	}

	if err != nil {
		panic(fmt.Sprintf("Failed printing the results: '%s'\n", err.Error()))
	}
}
