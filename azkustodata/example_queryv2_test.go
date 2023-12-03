package azkustodata_test

import (
	"context"
	"github.com/Azure/azure-kusto-go/azkustodata"
	"github.com/Azure/azure-kusto-go/azkustodata/kql"
	"github.com/Azure/azure-kusto-go/azkustodata/query"
	"github.com/Azure/azure-kusto-go/azkustodata/value"
)

func Example() {
	// Create a client using the default Azure credential
	kcsb := azkustodata.NewConnectionStringBuilder("https://help.kusto.windows.net/").WithDefaultAzureCredential()
	client, err := azkustodata.New(kcsb)

	if err != nil {
		panic(err)
	}

	defer client.Close()

	ctx := context.Background()

	// Simple query - single table

	dataset, err := client.QueryV2(ctx, "Samples", kql.New("PopulationData"))

	if err != nil {
		panic(err)
	}

	for tableResult := range dataset.Tables() {
		// Make sure to always check for errors
		if tableResult.Err != nil {
			panic(tableResult.Err)
		}

		// You can access table metadata, such as the table name
		table := tableResult.Table

		println(table.Name())
		println(table.Id())

		// Columns are available as well
		for _, column := range table.Columns() {
			println(column.Name)
		}
		// or by name
		stateCol := table.ColumnByName("State")
		println(stateCol.Name)

		if !table.IsPrimaryResult() {
			// Non-primary tables can be safely ignored
		}

		workWithRow := func(row query.Row) {
			// Each row has an index and a pointer to the table it belongs to
			println(row.Index)
			println(row.Table().Name())

			// There are a few ways to access the values of a row:
			val := row.Value(0)
			println(val)
			println(row.Values()[0])
			println(row.ValueByColumn(stateCol))

			// Working with values:
			// Get the type of the value
			println(val.GetType()) // prints "string"

			// Get the value as a string
			// Note that values are pointers - since they can be null
			if s, ok := val.GetValue().(*string); ok {
				if s != nil {
					println(*s)
				}
			}

			// Or cast directly to the kusto type
			if s, ok := val.GetValue().(value.String); ok {
				if s.Valid {
					println(s.Value)
				}
			}

			// Or convert the row to a struct

			type PopulationData struct {
				State string
				Pop   int `kusto:"Population"` // use the kusto tag to map to a different column name
			}

			var pd PopulationData
			err := row.ToStruct(&pd)
			if err != nil {
				panic(err)
			}
			println(pd.State)
			println(pd.Pop)
		}

		if tb, ok := table.(query.StreamingTable); ok {
			// WARNING: streaming tables must be consumed, or the dataset will be blocked

			// There are a few ways to consume a streaming table:
			// Note: Only one of these methods should be used per table
			// 1. SkipToEnd() - skips all rows and closes the table
			tb.SkipToEnd()

			// 2. Consume() - reads all rows and closes the table
			rows, errors := tb.Consume()
			for _, row := range rows {
				workWithRow(row)
			}
			for _, err := range errors {
				println(err.Error())
			}

			// 3. Rows() - reads rows as they are received
			for rowResult := range tb.Rows() {
				if rowResult.Err != nil {
					println(rowResult.Err.Error())
				} else {
					println(rowResult.Row.Index)
				}
			}
		}

		// tables that aren't streaming are fully loaded into memory
		if tb, ok := table.(query.FullTable); ok {
			tb.Consume() // Can be called multiple times

			// rows are available immediately
			for _, row := range tb.Rows() {
				workWithRow(row)
			}
		}

	}
}
