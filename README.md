# Microsoft Azure Kusto (Azure Data Explorer) SDK for Go

### Install

* `go get github.com/Azure/azure-kusto-go`

### Minimum Requirements

* go version go1.13.3

### Authentication Methods:

* AAD application - Provide app ID and app key

### Usage

#### Query

```go
package main

import (
    "context"
    "fmt"

    "github.com/Azure/azure-kusto-go/kusto"
    "github.com/Azure/go-autorest/autorest/azure/auth"
)

func main() {
    cluster := "https://sampleCluster.kusto.windows.net"
    appId := ""
    appKey := ""
    tenantId := ""

    authorizerConfig := kusto.NewClientCredentialsConfig(appId, appKey, tenantId)
    authorization := kusto.Authorization{
        Config: authorizerConfig,
    }

    iter, err := kustoClient.Query(ctx, db, kusto.NewStmt("MyTable | count "))
if err != nil {
	panic(err)
}

defer iter.Stop()



// Loop through the iterated results, read them into our UserID structs and append them
// to our list of recs.
var recs []CountResult
for {
	row, err := iter.Next()
	if err != nil {
		// This indicates we are done.
		if err == io.EOF {
			break
		}
		// We ran into an error during the stream.
		panic(err)
	}
	rec := CountResult{}
	if err := row.ToStruct(&rec); err != nil {
		panic(err)
	}
	recs = append(recs, rec)
}

fmt.Println(recs)
}
```

#### Ingestion

```go
package main

import (
	"context"
	"fmt"
	"github.com/Azure/azure-kusto-go/kusto"
	"github.com/Azure/azure-kusto-go/kusto/ingest"
	"github.com/Azure/go-autorest/autorest/azure/auth"
	"io"
)

func main()  {
    cluster := "https://sampleCluster.kusto.windows.net"
    dm := "https://ingest-sampleCluster.kusto.windows.net"
    appId := ""
    appKey := ""
    tenantId := ""

    authorizerConfig := auth.NewClientCredentialsConfig(appId, appKey, tenantId)
    authorization := kusto.Authorization{
        Config: authorizerConfig,
    }
    
    ingestor := ingest.New(dm, authorization)
	url := "https://mystorageaccount.blob.core.windows.net/container/folder/data.json?sp=r&st=2020-02-01T18:51:17Z&se=2020-12-13T02:51:17Z&spr=https&sv=2019-02-02&sr=b&sig=***"
	err := ingestor.IngestFromStorage(url, ingest.IngestionProperties{
		DatabaseName:        "Database",
		TableName:           "Table",
		FlushImmediately:    true,
		IngestionMappingRef: "TableData_from_json",
	}, nil)

	if err != nil {
		panic(err)
	} 

}

```

### Contributing

This project welcomes contributions and suggestions.  Most contributions require you to agree to a
Contributor License Agreement (CLA) declaring that you have the right to, and actually do, grant us
the rights to use your contribution. For details, visit https://cla.opensource.microsoft.com.

When you submit a pull request, a CLA bot will automatically determine whether you need to provide
a CLA and decorate the PR appropriately (e.g., status check, comment). Simply follow the instructions
provided by the bot. You will only need to do this once across all repos using our CLA.

This project has adopted the [Microsoft Open Source Code of Conduct](https://opensource.microsoft.com/codeofconduct/).
For more information see the [Code of Conduct FAQ](https://opensource.microsoft.com/codeofconduct/faq/) or
contact [opencode@microsoft.com](mailto:opencode@microsoft.com) with any additional questions or comments.

### Looking for SDKs for other languages/platforms?

- [Node](https://github.com/azure/azure-kusto-node)
- [Java](https://github.com/azure/azure-kusto-java)
- [.NET](https://docs.microsoft.com/en-us/azure/kusto/api/netfx/about-the-sdk)
- [Python](https://github.com/Azure/azure-kusto-python)
