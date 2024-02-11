package azkustodata_test

import (
	"context"
	"github.com/Azure/azure-kusto-go/azkustodata"
	"github.com/Azure/azure-kusto-go/azkustodata/kql"
	"github.com/Azure/azure-kusto-go/azkustodata/query"
	v2 "github.com/Azure/azure-kusto-go/azkustodata/query/v2"
	"github.com/Azure/azure-kusto-go/azkustodata/value"
)

type PopulationData struct {
	State string
	Pop   int `kusto:"Population"` // use the kusto tag to map to a different column name
}

func ExampleIterativeQuery() {
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

	dataset, err := client.IterativeQuery(ctx, "Samples", kql.New("PopulationData"))

	if err != nil {
		panic(err)
	}

	// In most cases, you will only have a single primary result table, and you don't care about the other metadata tables,
	// so you can just read the first table from the dataset

	tableResult := <-dataset.Tables()
	if tableResult.Err() != nil {
		panic(tableResult.Err())
	}
	table := tableResult.Table()
	println(table.Name())

	// Otherwise, you can iterate through the `Tables()` channel to get them
	// It is only possible to iterate through the results once - since they are streamed

	for tableResult := range dataset.Tables() {
		// Make sure to always check for errors
		if tableResult.Err() != nil {
			// It's possible to get an errors and still get a table - partial results
			println(tableResult.Err())
		}

		// You can access table metadata, such as the table name
		table := tableResult.Table()

		// You can check if the table is a primary result table
		// Primary results will always be the first tables in the dataset
		// otherwise, you can use helper methods to get the secondary tables
		if !table.IsPrimaryResult() {
			queryProps, err := v2.AsQueryProperties(table)
			if err != nil {
				panic(err)
			}
			println(queryProps[0].Value)

			// Or you can simply use any of the normal table methods
			println(table.Name())

			continue
		}

		println(table.Name())
		println(table.Id())

		// Columns are available as well
		for _, column := range table.Columns() {
			println(column.Name)
		}
		// or by name
		stateCol := table.ColumnByName("State")
		println(stateCol.Name)

		// WARNING: streaming tables must be consumed, or the dataset will be blocked

		// There are a few ways to consume a streaming table:
		// Note: Only one of these methods should be used per table
		// 1. Rows() - reads rows as they are received
		for rowResult := range table.Rows() {
			if rowResult.Err() != nil {
				println(rowResult.Err().Error())
			} else {
				println(rowResult.Row().Index())
			}
		}

		// 2. SkipToEnd() - skips all rows and closes the table
		table.SkipToEnd()

		// 3. ToFullTable() - reads all rows into memory and returns a full table
		fullTable, err := table.ToFullTable()
		rows := fullTable.Rows()
		for _, row := range rows {
			println(row.Index())
		}
		if err != nil {
			println(err.Error())
		}

		// Working with rows
		for _, row := range rows {
			// Each row has an index and a pointer to the table it belongs to
			println(row.Index())
			println(row.Table().Name())

			// For convenience, you can get the value from the row in the correct type
			s, err := row.StringByIndex(0)
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
			val, err := row.Value(0)
			if err != nil {
				panic(err)
			}

			println(val)
			println(row.Values()[0])
			println(row.ValueByColumn(stateCol))

			// Get the type of the value
			println(val.GetType()) // prints "string"

			// Get the value as a string
			// Note that values are pointers - since they can be null
			if s, ok := val.GetValue().(*int); ok {
				if s != nil {
					println(*s)
				}
			}

			// Or cast directly to the kusto type
			if s, ok := val.(*value.Int); ok {
				i := s.Ptr()
				if i != nil {
					println(*i)
				}
			}

			// Or convert the row to a struct

			var pd PopulationData
			err = row.ToStruct(&pd)
			if err != nil {
				panic(err)
			}
			println(pd.State)
			println(pd.Pop)

		}

		// Or you can easily get the results as a slice of structs Iteratively
		for res := range query.ToStructsIterative[PopulationData](table) {
			if res.Err != nil {
				println(res.Err.Error())
			} else {
				println(res.Out.State)
			}
		}

		// Or all at once
		strts, errs := query.ToStructs[PopulationData](table)
		if errs != nil {
			panic(errs)
		}
		println(strts)

	}

	// Alternatively, you can consume the stream to get a full dataset
	// Only if you haven't consumed them in any other way
	ds, err := dataset.ToFullDataset()
	if err != nil {
		panic(err)
	}

	// Now you can access tables and row with random access
	rows := ds.Tables()[1].Rows()
	println(rows)
}
