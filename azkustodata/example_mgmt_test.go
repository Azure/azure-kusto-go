package azkustodata_test

import (
	"context"
	"github.com/Azure/azure-kusto-go/azkustodata"
	"github.com/Azure/azure-kusto-go/azkustodata/kql"
	"github.com/Azure/azure-kusto-go/azkustodata/query"
	"github.com/Azure/azure-kusto-go/azkustodata/value"
)

func Example_mgmt() {
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

	dataset, err := client.Mgmt(ctx, "Samples", kql.New(".show tables"))

	if err != nil {
		panic(err)
	}

	// In most cases, you will only have a single primary result table, and you don't care about the other metadata tables,
	// so you can just read the first table from the dataset

	table := dataset.Tables()[0]
	println(table.Name())

	//Other times, you might want to go over all of the primary results tables:

	for _, tb := range dataset.Tables() {
		println(tb.Name())
		println(tb.Id())

		// Columns are available as well
		for _, column := range tb.Columns() {
			println(column.Name())
		}
		// or by name
		stateCol := tb.ColumnByName("State")
		println(stateCol.Name())

		// Use Rows() to get all rows as a slice
		rows := tb.Rows()

		// You can randomly access rows
		row := rows[0]
		println(row.Index())

		// Or iterate over all rows
		for _, row := range rows {
			// Each row has an index and columns
			println(row.Index())
			println(row.Columns())

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

			// Working with values:
			// Get the type of the value
			println(val.GetType()) // prints "string"

			// Get the value as an int
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

			type ShowTables struct {
				TableName string
				Database  string `kusto:"DatabaseName"` // You can use tags to map to different column names
				Folder    string
				DocString string
			}

			var tableData ShowTables
			err = row.ToStruct(&tableData)
			if err != nil {
				panic(err)
			}
			println(tableData.TableName)
		}
	}
	// Or convert the dataset to a slice of structs (or a specific table if there is more than one)
	strts, errs := query.ToStructs[PopulationData](dataset) // or dataset.Tables()[i]
	println(len(strts), errs)

	// Unlike queries, metadata is not included as normal tables, but instead as additional methods.
	// This is due the lack of results streaming.
	println(dataset.Info())
	println(dataset.Status())
}
