# Microsoft Azure Data Explorer Public Preview (Kusto) [![GoDoc](https://godoc.org/github.com/Azure/azure-kusto-go?status.svg)](https://godoc.org/github.com/Azure/azure-kusto-go)

- [About Azure Data Explorer](https://azure.microsoft.com/en-us/services/data-explorer/)
- [Go Client documentation](https://godoc.org/github.com/Azure/azure-kusto-go)

This is a data plane SDK (it is for interacting with Azure Data Explorer (Kusto) service). For the control plane (resource administration), go [here](https://github.com/Azure/azure-sdk-for-go/tree/master/services/kusto/mgmt).

Use the data plane SDK `github.com/Azure/azure-kusto-go/kusto` in your application to:

- Query Kusto/Azure Data Explorer clusters for rows, optionally into structs.
- Import data into Kusto from local file, Azure Blob Storage file, Stream, or an `io.Reader`.

**NOTE**: This library is currently a beta. There may be breaking changes until it reaches semantic version `v1.0.0`.

Key links:
- [Source code](https://github.com/Azure/azure-kusto-go)
- [API Reference Documentation](https://pkg.go.dev/github.com/Azure/azure-kusto-go)
- [Product documentation](https://azure.microsoft.com/en-us/services/data-explorer/)
- [Samples](https://pkg.go.dev/github.com/Azure/azure-kusto-go#readme-examples)

## Key concepts

Azure Data Explorer is a fully managed, high-performance, big data analytics platform that makes it easy to analyze high volumes of data in near real time. The Azure Data Explorer toolbox gives you an end-to-end solution for data ingestion, query, visualization, and management.

An Azure Data Explorer (Kusto) [**cluster**](https://docs.microsoft.com/azure/event-hubs/event-hubs-features#namespace) can have multiple databases. Each database, in turn, contains [**tables**](https://docs.microsoft.com/azure/event-hubs/event-hubs-features#partitions) which store data.

Query Azure Data Explorer with the [Kusto Query Language (KQL)](https://learn.microsoft.com/en-us/azure/data-explorer/kusto/query/), an open-source language initially invented by the team. The language is simple to understand and learn, and highly productive. You can use simple operators and advanced analytics.

For more information about Azure Data Explorer (Kusto), its features, and relevant terminology can be found here: [link](https://learn.microsoft.com/en-us/azure/data-explorer/data-explorer-overview)


## Getting started

### Install the package

Install the Kusto/Azure Data Explorer client module for Go with `go get`:

```bash
go get github.com/Azure/azure-kusto-go
```

### Prerequisites

- Go, version 1.19 or higher
- An [Azure subscription](https://azure.microsoft.com/free/)
- An [Azure Data Explorer Cluster](https://learn.microsoft.com/en-us/azure/data-explorer/).
- An Azure Data Explorer Database. You can create a Database in your Azure Data Explorer Cluster using the [Azure Portal](https://learn.microsoft.com/en-us/azure/data-explorer/create-cluster-database-portal).

## Examples

Examples for various scenarios can be found on [pkg.go.dev](https://pkg.go.dev/github.com/Azure/azure-kusto-go#readme-examples) or in the example*_test.go files in our GitHub repo for [azure-kusto-go](https://github.com/Azure/azure-kusto-go/tree/master/kusto).

### Create the connection string

Azure Data Explorer (Kusto) connection strings are created using a connection string builder for an existing Azure Data Explorer (Kusto) cluster endpoint of the form `https://<cluster name>.<location>.kusto.windows.net`.

```go
kustoConnectionStringBuilder := kusto.NewConnectionStringBuilder(endpoint)
```

### Create and authenticate the client

Azure Data Explorer (Kusto) clients are created from a connection string and authenticated using a credential from the [Azure Identity package][azure_identity_pkg], like [DefaultAzureCredential][default_azure_credential].
You can also authenticate a client using a system- or user-assigned managed identity with Azure Active Directory (AAD) credentials.

#### Using the `DefaultAzureCredential`

```go
// kusto package is: github.com/Azure/azure-kusto-go/kusto

// Initialize a new kusto client using the default Azure credential
kustoConnectionString := kustoConnectionStringBuilder.WithDefaultAzureCredential()
client, err = kusto.New(kustoConnectionString)
if err != nil {
	panic("add error handling")
}
// Be sure to close the client when you're done. (Error handling omitted for brevity.)
defer client.Close()
```

#### Using the `az cli`

```go
kustoConnectionString := kustoConnectionStringBuilder.WithAzCli()
client, err = kusto.New(kustoConnectionString)
```

#### Using a system-assigned managed identity

```go
kustoConnectionString := kustoConnectionStringBuilder.WithSystemManagedIdentity()
client, err = kusto.New(kustoConnectionString)
```

#### Using a user-assigned managed identity

```go
kustoConnectionString := kustoConnectionStringBuilder.WithUserManagedIdentity(clientID)
client, err = kusto.New(kustoConnectionString)
```

#### Using an application token

```go
kustoConnectionString := kustoConnectionStringBuilder.WithApplicationToken(appId, appToken)
client, err = kusto.New(kustoConnectionString)
```

#### Using an application certificate

```go
kustoConnectionString := kustoConnectionStringBuilder.WithAppCertificate(appId, certificate, thumbprint, sendCertChain, authorityID)
client, err = kusto.New(kustoConnectionString)
```

### Querying

#### Query For Rows

The kusto `table` package queries data into a ***table.Row** which can be printed or have the column data extracted.

```go
// table package is: github.com/Azure/azure-kusto-go/kusto/data/table

// Query our database table "systemNodes" for the CollectionTimes and the NodeIds.
iter, err := client.Query(ctx, "database", kusto.NewStmt("systemNodes | project CollectionTime, NodeId"))
if err != nil {
	panic("add error handling")
}
defer iter.Stop()

// .Do() will call the function for every row in the table.
err = iter.Do(
	func(row *table.Row) error {
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
#### Querying with parameters

Users will sometimes want to use query parameters (Read more here - https://learn.microsoft.com/en-us/azure/data-explorer/kusto/query/queryparametersstatement?pivots=azuredataexplorer).
In this case, the StatementBuilder and QueryParameters structs should be used.

```go
dt, _ := time.Parse(time.RFC3339Nano, "2020-03-04T14:05:01.3109965Z")
// Query our database table "systemNodes" for the CollectionTimes and the NodeIds.
statement := kql.NewStatementBuilder("systemNodes | where CollectionTime == time and NodeId == id")
params :=  kql.NewStatementQueryParameters().AddDateTime("time", dt).AddInt("id", 1) 
iter, err := client.Query(ctx, "database", statement, params)

fmt.Println(fmt.Sprintf("%s\n%s", params.ToDeclarationString(), statement.String()))
// Will print:
// declare query_parameters(time:datetime, id:int);
// systemNodes | where CollectionTime == time and NodeId == id
if err != nil {
	panic("add error handling")
}
defer iter.Stop()

// .Do() will call the function for every row in the table.
err = iter.Do(
	func(row *table.Row) error {
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

#### Querying with StatementBuilder
Another way to build your query is using the StatementBuilder struct. With this struct you build your query as a combination of literals and special entities.
This is mostly applicable when your KQL commands which do not support query parameters, or for situations where Kusto entities like database, table, column or function need to be safely added from variables.
```go
dt, _ := time.Parse(time.RFC3339Nano, "2020-03-04T14:05:01.3109965Z")
tableName := "systemNodes"
value := 1
// Query our database table "systemNodes" for the CollectionTimes and the NodeIds.
iter, err := client.Query(ctx, "database", kql.NewStatementBuilder("").AddTable(tableName).AddLiteral(" | where ").AddColumn("CollectionTime").AddLiteral(" == ").AddDateTime(dt).AddLiteral(" and ").AddColumn("NodeId").AddLiteral(" == ").AddInt(value),
if err != nil {
panic("add error handling")
}
defer iter.Stop()

	// .Do() will call the function for every row in the table.
	err = iter.Do(
		func(row *table.Row) error {
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

#### Query Into Structs

Users will often want to turn the returned data into Go structs that are easier to work with.  The ***table.Row** object
that is returned supports this via the `.ToStruct()` method.

```go
// NodeRec represents our Kusto data that will be returned.
type NodeRec struct {
	// ID is the table's NodeId. We use the field tag here to to instruct our client to convert NodeId to ID.
	ID int64 `kusto:"NodeId"`
	// CollectionTime is Go representation of the Kusto datetime type.
	CollectionTime time.Time
}

iter, err := client.Query(ctx, "database", kusto.NewStmt("systemNodes | project CollectionTime, NodeId"))
if err != nil {
	panic("add error handling")
}
defer iter.Stop()

recs := []NodeRec{}
err = iter.Do(
	func(row *table.Row) error {
		rec := NodeRec{}
		if err := row.ToStruct(&rec); err != nil {
			return err
		}
		if row.Replace {
			recs = recs[:0] // Replace flag indicates that the query result should be cleared and replaced with this row
		}
		recs = append(recs, rec)
		return nil
	},
)
if err != nil {
	panic("add error handling")
}
```

### Ingestion

The `ingest` package provides access to Kusto's ingestion service for importing data into Kusto. This requires
some prerequisite knowledge of acceptable data formats, mapping references, etc.

That documentation can be found [here](https://docs.microsoft.com/en-us/azure/kusto/management/data-ingestion/)

Kusto's ingestion service makes no guarantees on when the data will show up in the table and is optimized for
large chunks of data and not small uploads at a high rate.

If ingesting data from memory, it is suggested that you stream the data in via `FromReader()` passing in the reader
from an `io.Pipe()`. The data will not begin ingestion until the writer closes.


#### Setup an ingestion client

Setup is quite simple, simply pass a `*kusto.Client`, the name of the database and table you wish to ingest into.

```go
in, err := ingest.New(kustoClient, "database", "table")
if err != nil {
	panic("add error handling")
}
// Be sure to close the ingestor when you're done. (Error handling omitted for brevity.)
defer in.Close()
```

#### From a File

Ingesting a local file requires simply passing the path to the file to be ingested:

```go
if _, err := in.FromFile(ctx, "/path/to/a/local/file"); err != nil {
	panic("add error handling")
}
```

`FromFile()` will accept Unix path names on Unix platforms and Windows path names on Windows platforms.
The file will not be deleted after upload (there is an option that will allow that though).

#### From a Blob Storage File

This package will also accept ingestion from an Azure Blob Storage file:

```go
if _, err := in.FromFile(ctx, "https://myaccount.blob.core.windows.net/$root/myblob"); err != nil {
	panic("add error handling")
}
```

This will ingest a file from Azure Blob Storage. We only support `https://` paths and your domain name may differ than what is here.

#### Ingestion from an io.Reader

Sometimes you want to ingest a stream of data that you have in memory without writing to disk.  You can do this simply by chunking the
data via an `io.Reader`.

```go
r, w := io.Pipe()

enc := json.NewEncoder(w)
go func() {
	defer w.Close()
	for _, data := range dataSet {
		if err := enc.Encode(data); err != nil {
			panic("add error handling")
		}
	}
}()

if _, err := in.FromReader(ctx, r); err != nil {
	panic("add error handling")
}
```

It is important to remember that `FromReader()` will terminate when it receives an `io.EOF` from the `io.Reader`.  Use `io.Readers` that won't
return `io.EOF` until the `io.Writer` is closed (such as `io.Pipe`).

#### From a Stream

Ingestion from a stream commits blocks of fully formed data encodes (JSON, AVRO, ...) into Kusto:

```go
if err := in.Stream(ctx, jsonEncodedData, ingest.JSON, "mappingName"); err != nil {
	panic("add error handling")
}
```

#### Addtional Ingest Clients
* `ingest.Streaming` and `ingest.Managed` were added to the SDK. Their interface is identical to `ingest.Ingestion` (in fact - they all share an interface `Ingestor`), and are created via `ingest.NewStreaming` and `ingest.NewManaged` respectively.
`ingest.Streaming` uses [streaming ingestion](https://docs.microsoft.com/en-us/azure/data-explorer/ingest-data-streaming?tabs=azure-portal%2Ccsharp) to ingest data to kusto. It supports ingesting from a file or a `Reader`, but not a blob.
`ingest.Managed` is a managed streaming ingest client. It will try to use streaming ingest, but on transient failures and other conditions will fall back to queued ingestion.

#### New APIs for querying

* As mentioned before, RowIterator.Next` and `RowIterator.Do` are now deprecated and replaced by `RowIterator.NextRowOrError` and `RowIterator.DoOnRowOrError`.  The new APIs will act the same as the old, with the following changes:
`RowIterator.NextRowOrError` will return an additional `inlineError` value, when it's non-nil it indicates an inline error. After encountering it, the iteration doesn't end and you should continue consuming the iterator.
`RowIterator.DoOnRowOrError` requires an additional parameter of type `*errors.Error`, which indicates an inline error. It can be ignored or handled, but while returning nil the iteration will continue.

#### Addtional Features
* Support extracting non-primary tables from a `RowIterator` using the following methods - `GetNonPrimary`, `GetExtendedProperties` and `GetQueryCompletionInformation`. Fixed #85 
* Expose `TableFragmentType` via a Replace flag by @w1ndy in https://github.com/Azure/azure-kusto-go/pull/74
* Refactor value converters and implement `ExtractValues` for `Row` by @w1ndy in https://github.com/Azure/azure-kusto-go/pull/75
* Better Dynamic converter by @w1ndy in https://github.com/Azure/azure-kusto-go/pull/78
* Support more forms of decimal type, and accept input of big.Int for it. Fixed #86 

#### Fixes
* Add support for gzipped errors,. Fixed #84
* Moved from the old deprecated `azblob` to the new supported one. This should solve some issues in uploading blobs, specifically memory leaks.

#### Internal Improvements
* Added go-fmt gate check by @AsafMah in https://github.com/Azure/azure-kusto-go/pull/77
* Parallelized tests and made them more robust

### Version 0.5.2
#### Fixes
* **Critical bug** - When ingesting to multiple clusters all data is sent to one cluster.
As always, we recommend re-using clients and ingestors whenever possible.

## Best Practices
See the SDK [best practices guide](https://docs.microsoft.com/azure/data-explorer/kusto/api/netfx/kusto-ingest-best-practices), which though written for the .NET SDK, applies similarly here.

## Contributing

This project welcomes contributions and suggestions.  Most contributions require you to agree to a
Contributor License Agreement (CLA) declaring that you have the right to, and actually do, grant us
the rights to use your contribution. For details, visit https://cla.opensource.microsoft.com.

When you submit a pull request, a CLA bot will automatically determine whether you need to provide
a CLA and decorate the PR appropriately (e.g., status check, comment). Simply follow the instructions
provided by the bot. You will only need to do this once across all repos using our CLA.

This project has adopted the [Microsoft Open Source Code of Conduct](https://opensource.microsoft.com/codeofconduct/).
For more information see the [Code of Conduct FAQ](https://opensource.microsoft.com/codeofconduct/faq/) or
contact [opencode@microsoft.com](mailto:opencode@microsoft.com) with any additional questions or comments.

## Looking for SDKs for other languages/platforms?

- [Node](https://github.com/azure/azure-kusto-node)
- [Java](https://github.com/azure/azure-kusto-java)
- [.NET](https://docs.microsoft.com/en-us/azure/kusto/api/netfx/about-the-sdk)
- [Python](https://github.com/Azure/azure-kusto-python)
- [Azure CLI](https://learn.microsoft.com/en-us/azure/data-explorer/create-cluster-database-cli)
- [PowerShell](https://learn.microsoft.com/en-us/azure/data-explorer/create-cluster-database-powershell)
- [Azure Resource Manager template](https://learn.microsoft.com/en-us/azure/data-explorer/create-cluster-database-resource-manager)
