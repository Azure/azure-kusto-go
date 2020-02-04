# Microsoft Azure Kusto (Azure Data Explorer) SDK for Go

### Install

* `go get github.com/Azure/azure-kusto-go`

### Minimum Requirements

* go version go1.13.3

### Authentication Methods:

* AAD application - Provide app ID and app key

### Usage

```go
package main

import (
    "context"
    "fmt"

    "github.com/Azure/azure-kusto-go/azure-kusto-data/azkustodata"
    "github.com/Azure/go-autorest/autorest/azure/auth"
)

func main() {
    cluster := "https://sampleCluster.kusto.windows.net"
    appId := ""
    appKey := ""
    tenantId := ""

    authorizerConfig := auth.NewClientCredentialsConfig(appId, appKey, tenantId)
    authorization := azkustodata.Authorization{
        Config: authorizerConfig,
    }

    kustoClient, err := azkustodata.New(cluster, authorization)
    if err != nil {
        panic(err)
    }

    ctx := context.Background()

    db := "SampleDB"
    query := "SampleTable | take 10"

    response, err := kustoClient.Query(ctx, db, query)
    if err != nil {
        panic(err)
    }

    fmt.Println(response)
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
