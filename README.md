# Microsoft Azure Data Explorer Public Preview (Kusto) [![GoDoc](https://godoc.org/github.com/Azure/azure-kusto-go?status.svg)](https://godoc.org/github.com/Azure/azure-kusto-go)

- [About Azure Data Explorer](https://azure.microsoft.com/en-us/services/data-explorer/)
- [Go Client documentation](https://godoc.org/github.com/Azure/azure-kusto-go)

This is a data plane SDK (it is for interacting with Azure Data Explorer service). For the control plane (resource administration), go [here](https://github.com/Azure/azure-sdk-for-go/tree/master/services/kusto/mgmt)

## What's New
### Version 0.6.0
#### Deprecations 
* `Ingestion.Stream` has been deprecated in favor of dedicated streaming clients - `ingest.Streaming` and `ingest.Managed`.
This API was very limited - it required you to create a queued ingestion client, it only accepted a byte array, and had no customization options.
* `RowIterator.Next` and `RowIterator.Do` are now deprecated and replaced by `RowIterator.NextRowOrError` and `RowIterator.DoOnRowOrError`. 
In previous versions, when encountering an error in-line with the results (also known as partial success), the SDK panicked. Now `RowIterator.Next` and `RowIterator.Do` will return the first error they encounter, including in-line errors or partials successes and finish. 
This means that there could be extra data that will be skipped when using these APIs. Fixed #81 

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
* Moved from the old deprecated azblob to the new supported one. This should solve some issues in uploading blobs, specifically memory leaks.

#### Internal Improvements
* Added go-fmt gate check by @AsafMah in https://github.com/Azure/azure-kusto-go/pull/77
* Parallelized tests and made them more robust

### Version 0.5.2
#### Fixes
* **Critical bug** - When ingesting to multiple clusters all data is sent to one cluster.
As always, we recommend re-using clients and ingestors whenever possible.

## Install

* `go get github.com/Azure/azure-kusto-go/kusto`


## Minimum Requirements

* go version 1.16

## Examples

Below are some simple examples to get users up and running quickly. For full examples, please refer to the
GoDoc for the packages.

### Authorizing

```go
// auth package is: "github.com/Azure/go-autorest/autorest/azure/auth"

authorizer := kusto.Authorization{
	Config: auth.NewClientCredentialsConfig(clientID, clientSecret, tenantID),
}
```
This creates a Kusto Authorizer using your client identity, secret and tenant identity.
You may also uses other forms of authorization, please see the Authorization type in the GoDoc for more.

### Creating a Client

```go
client, err := kusto.New(endpoint, authorizer)
if err != nil {
	panic("add error handling")
}
// Be sure to close the client when you're done. (Error handling omitted for brevity.)
defer client.Close()
```
endpoint represents the Kusto endpoint. This will resemble: "https://<instance>.<region>.kusto.windows.net".

### Querying

#### Query For Rows

The Kusto package package queries data into a ***table.Row** which can be printed or have the column data extracted.

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

#### Query Into Structs

Users will often want to turn the returned data into Go structs that are easier to work with.  The ***table.Row** object
that is returned supports this via the **.ToStruct()** method.

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

The **ingest/** package provides access to Kusto's ingestion service for importing data into Kusto. This requires
some prerequisite knowledge of acceptable data formats, mapping references, ...

That documentation can be found [here](https://docs.microsoft.com/en-us/azure/kusto/management/data-ingestion/)

Kusto's ingestion service makes no guarantees on when the data will show up in the table and is optimized for
large chunks of data and not small uploads at a high rate.

If ingesting data from memory, it is suggested that you stream the data in via FromReader() passing in the reader
from an io.Pipe(). The data will not begin ingestion until the writer closes.


#### Setup an ingestion client

Setup is quite simple, simply pass a *kusto.Client, the name of the database and table you wish to ingest into.

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

FromFile() will accept Unix path names on Unix platforms and Windows path names on Windows platforms.
The file will not be deleted after upload (there is an option that will allow that though).

#### From a Blob Storage File

This package will also accept ingestion from an Azure Blob Storage file:

```go
if _, err := in.FromFile(ctx, "https://myaccount.blob.core.windows.net/$root/myblob"); err != nil {
	panic("add error handling")
}
```

This will ingest a file from Azure Blob Storage. We only support https:// paths and your domain name may differ than what is here.

#### Ingestion from an io.Reader

Sometimes you want to ingest a stream of data that you have in memory without writing to disk.  You can do this simply by chunking the
data via an io.Reader.

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

It is important to remember that FromReader() will terminate when it receives an io.EOF from the io.Reader.  Use io.Readers that won't
return io.EOF until the io.Writer is closed (such as io.Pipe).

#### From a Stream

Ingestion from a stream commits blocks of fully formed data encodes (JSON, AVRO, ...) into Kusto:

```go
if err := in.Stream(ctx, jsonEncodedData, ingest.JSON, "mappingName"); err != nil {
	panic("add error handling")
}
```

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
