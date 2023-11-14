// Package queries - in charge of querying the data - either with management queries, or data queries
package queries

import (
	"context"
	"fmt"
	"github.com/Azure/azure-kusto-go/kusto"
	kustoErrors "github.com/Azure/azure-kusto-go/kusto/data/errors"
	"github.com/Azure/azure-kusto-go/kusto/data/table"
	"strings"
)

// ExecuteCommand Executes a Command using a premade client
func ExecuteCommand(kustoClient *kusto.Client, databaseName string, command kusto.Statement, options ...kusto.QueryOption) {
	ctx := context.Background()
	var iter *kusto.RowIterator
	var err error
	if strings.HasPrefix(command.String(), ".") {
		iter, err = kustoClient.Mgmt(ctx, databaseName, command)
	} else {
		iter, err = kustoClient.Query(ctx, databaseName, command, options...)
	}

	if err != nil {
		panic(fmt.Sprintf("Command execution failed: '%s'\n", err.Error()))
	}
	defer iter.Stop()

	// .DoOnRowOrError() will call the function for every row in the table and throw an error if occurred.
	err = iter.DoOnRowOrError(
		func(row *table.Row, err *kustoErrors.Error) error {
			if err != nil {
				panic(fmt.Sprintf("Failed handling row: '%s'\n", err.Error()))
			}
			if row.Replace {
				fmt.Println("---") // Replace flag indicates that the query result should be cleared and replaced with this row
			}
			fmt.Println(row) // As a convenience, printing a *table.Row will output csv
			return nil
		},
	)
	if err != nil {
		panic(fmt.Sprintf("Failed printing the results: '%s'\n", err.Error()))
	}
}
