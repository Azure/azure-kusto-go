# Migration Guide for Azure Data Explorer (Kusto) Go SDK

Welcome to the migration guide for the Azure Data Explorer (Kusto) Go SDK. This guide is designed to assist you in migrating your application from using the beta version of the SDK (`github.com/Azure/azure-kusto-go`) to the new, stable release, version `1.0.0`.

The release of version `1.0.0` introduces breaking changes, which include changes in dependencies, package arrangement, querying methods, and management commands. Following these steps carefully will ensure a smooth transition to the new version.

## 1. Changes in Dependencies and go.mod File

To migrate from the old SDK to the new one, you'll need to update your `go.mod` file's dependencies.

**Old SDK:**
```bash
go get github.com/Azure/azure-kusto-go
```

**New SDK:**
- For data operations (`github.com/Azure/azure-kusto-go/azkustodata`):
```bash
go get github.com/Azure/azure-kusto-go/azkustodata
```

- For ingestion operations (`github.com/Azure/azure-kusto-go/azkustoingest`):
```bash
go get github.com/Azure/azure-kusto-go/azkustoingest
```

Manually update your go.mod file by replacing `github.com/Azure/azure-kusto-go` with the specific package(s) you need.

## 2. Changes in Package Arrangement

The SDK is now split into two separate packages:
- **azkustodata:** For querying and managing Azure Data Explorer clusters.
- **azkustoingest:** For ingesting data into Azure Data Explorer clusters.

### Importing the New Packages

Depending on your requirements, import one or both of these packages into your Go files.

**For Data Operations:**
```go
import (
    "github.com/Azure/azure-kusto-go/azkustodata"
)
```

**For Ingestion Operations:**
```go
import (
    "github.com/Azure/azure-kusto-go/azkustoingest"
)
```

### Using the New Packages

Update your code to use the new packages.

For exmaple:

Old SDK:
```go
kustoConnectionStringBuilder := kusto.NewConnectionStringBuilder(endpoint)
```

New SDK (For Data Operations):
```go
kustoConnectionStringBuilder := azkustodata.NewConnectionStringBuilder(endpoint)
```

Same for ingestion operations:

Old SDK Ingestion Client Creation(Queued Example):
```go
in, err := ingest.New(kustoClient, "database", "table")
```

New SDK Ingestion Client Creation (Queued Example):
```go
in, err := azkustoingest.New(kustoConnectionString)
```

## 3. Building Queries

The new SDK introduces a new way to build queries.:
The old SDK used a `kusto.NewStmt` method to build queries:
```go
    query := kusto.NewStmt("systemNodes | project CollectionTime, NodeId")
```

For the new SDK, use the `azkustodata/kql` package to build queries, which has a type-safe query builder:
```go
    dt, _ := time.Parse(time.RFC3339Nano, "2020-03-04T14:05:01.3109965Z")
    tableName := "system nodes"
    value := 1
    
    query := kql.New("")
        .AddTable(tableName)
        .AddLiteral(" | where CollectionTime == ").AddDateTime(dt)
        .AddLiteral(" and ")
        .AddLiteral("NodeId == ").AddInt(value) // outputs ['system nodes'] | where CollectionTime == datetime(2020-03-04T14:05:01.3109965Z) and NodeId == int(1)
```

## 4. Querying Data

The new SDK introduces a new way to query data. 

The old SDK used the `Query` method to query data:
```go
import github.com/Azure/azure-kusto-go/kusto/data/table

// Query our database table "systemNodes" for the CollectionTimes and the NodeIds.
iter, err := client.Query(ctx, "database", query)
if err != nil {
	panic("add error handling")
}
defer iter.Stop()

// .Do() will call the function for every row in the table.
err = iter.DoOnRowOrError(
    func(row *table.Row, e *kustoErrors.Error) error {
        if e != nil {
            return e
        }
        if row.Replace {
            fmt.Println("---") // Replace flag indicates that the query result should be cleared and replaced with this row
        }
        fmt.Println(row) // As a convenience, printing a *table.Row will output csv
        return nil
	},
)
if err != nil {
	panic("add error handling")
}
```

The new SDK returns a dataset object, with the ability to handle different tables as they come:
```go
dataset, err := client.QueryIterative(ctx, "database", query)
if err != nil {
    panic("error handling")
}
defer dataset.Close()

for tableResult := range dataset.Tables() {
    if tableResult.Err() != nil {
        panic("table error handling")
    }
    for rowResult := range primaryResult.Table().Rows() {
        if rowResult.Err() != nil {
            panic("row error handling")
        }
        fmt.Println(rowResult.Row()) // Process each row
    }
}
```

You can also use the `Query` method to get all the results at once:
```go
dataset, err := client.Query(ctx, "database", query)
if err != nil {
    panic("error handling")
}

tables := dataset.Tables()
for _, table := range tables {
    for _, row := range table.Rows() {
        fmt.Println(row) // Process each row
    }
}
```

Working with rows also got easier, with methods to extract specific types:
```go
  row := table.Rows()[0]
  row.IntByName("EventId") // Get the value of the column "EventId" as an int
  row.StringByIndex(0) // Get the value of the first column as a string
```

Or get the table as a slice of structs:
```go
import github.com/Azure/azure-kusto-go/azkustodata/query

  events, err := query.ToStructs[Event]()
  if err != nil {
    panic("error handling")
  }
    for _, event := range events {
        fmt.Println(event) // Process each event
    }
```

Management commands are now called using the `Mgmt` method on the client, and have an identical api to `Query`:
```go
  dataset, err := client.Mgmt(ctx, "database", query)
  if err != nil {
    panic("error handling")
  }
  primaryResult := dataset.Tables()[0]
  for _, row := range primaryResult.Rows() {
    fmt.Println(row) // Process each row
  }
```

## 5. Ingesting Data

The Ingestion API stayed the same, only using the new package:
```go
import github.com/Azure/azure-kusto-go/azkustoingest

kcsb := azkustodata.NewConnectionStringBuilder(`endpoint`).WithAadAppKey("clientID", "clientSecret", "tenentID")
ingestor, err := azkustoingest.New(kcsb, azkustoingest.WithDefaultDatabase("database"), azkustoingest.WithDefaultTable("table"))

if err != nil {
    // Handle error
}

defer ingestor.Close() // Always close the ingestor when done.

ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
defer cancel()

_, err = ingestor.FromFile(ctx, "/path/to/file", azkustoingest.DeleteSource())
```
