package azkustodata_test

import (
	"context"
	"github.com/Azure/azure-kusto-go/azkustodata"
	"github.com/Azure/azure-kusto-go/azkustodata/kql"
	"github.com/Azure/azure-kusto-go/azkustodata/query"
	"github.com/Azure/azure-kusto-go/azkustodata/value"
)

func ExampleQuery() {
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

	dataset, err := client.Query(ctx, "Samples", kql.New("PopulationData"))

	if err != nil {
		// It's possible to get an errors and still get a table - partial results
		println(err.Error())
	}

	if dataset == nil {
		panic("dataset is nil")
	}

	// access tables
	tb1 := dataset.Results()[0]
	println(tb1.Name())

	for _, table := range dataset.Results() {
		println(table.Name())
		println(table.Id())

		// Columns are available as well
		for _, column := range table.Columns() {
			println(column.Name)
		}
		// or by name
		stateCol := table.ColumnByName("State")
		println(stateCol.Name)

		// Use GetAllRows() to get all rows as a slice
		rows, errs := table.GetAllRows()
		if errs != nil {
			panic(errs)
		}

		// Working with rows
		for _, row := range rows {
			// Each row has an index and a pointer to the table it belongs to
			println(row.Ordinal())
			println(row.Table().Name())

			// For convenience, you can get the value from the row in the correct type
			s, err := row.StringByOrdinal(0)
			if err != nil {
				panic(err)
			}
			println(s)
			i, err := row.IntByName("Population")
			if err != nil {
				panic(err)
			}
			println(i) // int is *int32 - since it can be nil

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
			err = row.ToStruct(&pd)
			if err != nil {
				panic(err)
			}
			println(pd.State)
			println(pd.Pop)
		}

	}

	// Alternatively, get the primary results as a slice of rows, if there is only one table
	rows, errs := dataset.PrimaryResults()
	println(len(rows), errs)

	// Or convert the dataset to a slice of structs (or a specific table if there is more than one)
	strts, errs := query.ToStructs[PopulationData](dataset) // or dataset.Results()[i]
	println(len(strts), errs)

	// Get metadata about the query (if it was consumed - otherwise it will be nil)
	println(dataset.Header())
	println(dataset.QueryProperties())
	println(dataset.QueryCompletionInformation())
	println(dataset.Completion())
}
