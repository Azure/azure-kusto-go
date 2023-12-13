package azkustodata_test

import (
	"context"
	"github.com/Azure/azure-kusto-go/azkustodata"
	"github.com/Azure/azure-kusto-go/azkustodata/kql"
	"github.com/Azure/azure-kusto-go/azkustodata/value"
)

func ExampleMgmt() {
	// Create a client using the default Azure credential
	kcsb := azkustodata.NewConnectionStringBuilder("https://help.kusto.windows.net/").WithDefaultAzureCredential()
	client, err := azkustodata.New(kcsb)

	if err != nil {
		panic(err)
	}

	defer func(client *azkustodata.Client) {
		err := client.Close()
		if err != nil {
			panic(err)
		}
	}(client)

	ctx := context.Background()

	// Simple query - single table

	dataset, err := client.MgmtNew(ctx, "Samples", kql.New(".show tables"))

	if err != nil {
		panic(err)
	}

	tb := dataset.PrimaryResults()
	// Or iterate over all primary results

	println(tb.Name())
	println(tb.Id())

	// Columns are available as well
	for _, column := range tb.Columns() {
		println(column.Name())
	}
	// or by name
	stateCol := tb.ColumnByName("State")
	println(stateCol.Name())

	// Use GetAllTables() to get all rows as a slice
	rows, errs := tb.GetAllRows()

	// Make sure to always check for errors
	if errs != nil {
		panic(errs)
	}

	// You can randomly access rows
	row := rows[0]
	println(row.Ordinal())

	// Or iterate over all rows
	for _, row := range rows {
		// Each row has an index and a pointer to the table it belongs to
		println(row.Ordinal())
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

		type ShowTables struct {
			TableName string
			Database  string `kusto:"DatabaseName"` // You can use tags to map to different column names
			Folder    string
			DocString string
		}

		var tableData ShowTables
		err := row.ToStruct(&tableData)
		if err != nil {
			panic(err)
		}
		println(tableData.TableName)
	}

	// Get metadata about the
	println(dataset.Info())
	println(dataset.Status())
}
